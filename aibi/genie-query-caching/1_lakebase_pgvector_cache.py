# Databricks notebook source
# MAGIC %md
# MAGIC # Scenario 1: Lakebase + pgvector Cache
# MAGIC
# MAGIC ![Architecture](scenario1-lakebase-pgvector.png)
# MAGIC
# MAGIC This notebook demonstrates a Lakebase cache for generated Genie SQL.
# MAGIC
# MAGIC **Cache flow:**
# MAGIC 1. Normalize the incoming question.
# MAGIC 2. Check Lakebase for an exact match in the current cache context.
# MAGIC 3. If no exact match exists, generate an embedding and search with pgvector.
# MAGIC 4. On a hit, re-execute cached SQL under the current Databricks identity.
# MAGIC 5. On a miss, call Genie, execute the generated SQL, then cache it.
# MAGIC
# MAGIC **Prerequisites:** Run `0_setup.py` first.

# COMMAND ----------

# MAGIC %pip install "databricks-sdk>=0.85" "psycopg[binary]>=3.1" "pgvector>=0.3" pyyaml --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import time

from utils import (
    GLOBAL_SCOPE,
    build_cache_context,
    call_genie_with_retry,
    execute_cached_sql_with_trace,
    lakebase_cache_lookup,
    lakebase_cache_write,
    load_config,
    print_sql_preview,
    print_summary_table,
    trace_operation,
)

config = load_config("./configs.yaml")
context = build_cache_context(config)

SIMILARITY_THRESHOLD = config.get("thresholds", {}).get("lakebase_similarity", 0.92)
RESULT_ROW_LIMIT = int(config.get("result_row_limit", 100))

print(f"Genie Space:          {config['genie_space_id']}")
print(f"Cache context key:    {context.cache_context_key}")
print(f"Similarity threshold: {SIMILARITY_THRESHOLD}")
print(f"Result row limit:     {RESULT_ROW_LIMIT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo — First Pass
# MAGIC
# MAGIC With `seed_demo_data: false`, each question should miss and call Genie. With
# MAGIC seeded mode enabled, matching questions may hit immediately.

# COMMAND ----------

demo_questions = config.get("demo_questions", [
    "What is the total revenue for last quarter?",
])

results = []

for question in demo_questions:
    with trace_operation(config, "scenario1_question", "CHAIN", {"question": question}):
        print(f"\n{'=' * 60}")
        print(f"Question: {question}")

        t0 = time.time()
        lookup = lakebase_cache_lookup(
            config,
            question,
            threshold=SIMILARITY_THRESHOLD,
            cache_scope=GLOBAL_SCOPE,
        )

        if lookup.hit:
            sql_result = execute_cached_sql_with_trace(
                config, lookup.generated_sql, row_limit=RESULT_ROW_LIMIT
            )
            latency = time.time() - t0
            print(f"  HIT ({lookup.hit_type}, score={lookup.score:.3f}) in {latency:.3f}s")
            print(f"  Cached SQL: {lookup.generated_sql[:160]}...")
            print_sql_preview(sql_result)
            results.append({
                "question": question[:50],
                "first_s": latency,
                "warm_s": None,
                "source": f"hit-{lookup.hit_type}",
                "speedup": "-",
            })
            continue

        print("  MISS -> calling Genie API with retry/backoff...")
        genie_result = call_genie_with_retry(config, question)
        print(f"  Genie returned in {genie_result.latency_seconds:.1f}s")

        if not genie_result.cacheable:
            latency = time.time() - t0
            print("  Genie did not return SQL; not caching this response.")
            results.append({
                "question": question[:50],
                "first_s": latency,
                "warm_s": None,
                "source": "genie-no-sql",
                "speedup": None,
            })
            continue

        sql_result = execute_cached_sql_with_trace(
            config, genie_result.generated_sql, row_limit=RESULT_ROW_LIMIT
        )
        lakebase_cache_write(
            config,
            question=question,
            sql=genie_result.generated_sql,
            response_text=genie_result.response_text or "",
            cache_scope=GLOBAL_SCOPE,
            embedding=lookup.embedding,
            validated=True,
            validation_source="genie",
        )
        latency = time.time() - t0
        print(f"  SQL: {genie_result.generated_sql[:160]}...")
        print_sql_preview(sql_result)
        print(f"  Written to Lakebase cache (total: {latency:.1f}s)")

        results.append({
            "question": question[:50],
            "first_s": latency,
            "warm_s": None,
            "source": "genie",
            "speedup": None,
        })

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo — Warm Pass
# MAGIC
# MAGIC Same questions again. Cache hits still re-execute cached SQL; the cache saves
# MAGIC Genie latency, not SQL execution or authorization.

# COMMAND ----------

for i, question in enumerate(demo_questions):
    with trace_operation(config, "scenario1_warm_question", "CHAIN", {"question": question}):
        print(f"\n{'=' * 60}")
        print(f"Question: {question}")

        t0 = time.time()
        lookup = lakebase_cache_lookup(
            config,
            question,
            threshold=SIMILARITY_THRESHOLD,
            cache_scope=GLOBAL_SCOPE,
        )

        if lookup.hit:
            sql_result = execute_cached_sql_with_trace(
                config, lookup.generated_sql, row_limit=RESULT_ROW_LIMIT
            )
            warm_latency = time.time() - t0
            print(f"  HIT ({lookup.hit_type}, score={lookup.score:.3f}) in {warm_latency:.3f}s")
            print(f"  Cached SQL: {lookup.generated_sql[:160]}...")
            print_sql_preview(sql_result)
        else:
            print("  Unexpected MISS")
            warm_latency = None

        if i < len(results):
            results[i]["warm_s"] = warm_latency
            if warm_latency and results[i]["first_s"]:
                results[i]["speedup"] = f"{results[i]['first_s'] / warm_latency:.0f}x"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo — Semantic Similarity Hit

# COMMAND ----------

if demo_questions:
    paraphrased = "How much total deposit volume did we have in 2024?"
    print(f"Original:    {demo_questions[0]}")
    print(f"Paraphrased: {paraphrased}")

    t0 = time.time()
    lookup = lakebase_cache_lookup(
        config,
        paraphrased,
        threshold=SIMILARITY_THRESHOLD,
        cache_scope=GLOBAL_SCOPE,
    )
    latency = time.time() - t0

    if lookup.hit:
        sql_result = execute_cached_sql_with_trace(
            config, lookup.generated_sql, row_limit=RESULT_ROW_LIMIT
        )
        print(f"\n  HIT ({lookup.hit_type}, score={lookup.score:.3f}) in {latency:.3f}s")
        print("  The paraphrased question matched via vector similarity.")
        print_sql_preview(sql_result)
    else:
        print(f"\n  MISS (score below threshold {SIMILARITY_THRESHOLD})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary — Latency Comparison

# COMMAND ----------

print("\n" + "=" * 80)
print("SCENARIO 1: Lakebase + pgvector Cache - Results")
print("=" * 80)
print_summary_table(results, ["question", "first_s", "warm_s", "source", "speedup"])
print()
print("Cache hits avoid Genie latency but still execute SQL for freshness and access control.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)
# MAGIC
# MAGIC Uncomment to clear the Lakebase cache table:
# MAGIC ```python
# MAGIC from utils import get_lakebase_connection
# MAGIC conn = get_lakebase_connection(config)
# MAGIC conn.cursor().execute("TRUNCATE TABLE genie_cache")
# MAGIC conn.commit()
# MAGIC ```

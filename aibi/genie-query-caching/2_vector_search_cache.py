# Databricks notebook source
# MAGIC %md
# MAGIC # Scenario 2: Vector Search Index Cache
# MAGIC
# MAGIC ![Architecture](scenario2-vector-search.png)
# MAGIC
# MAGIC This notebook demonstrates a Databricks Vector Search cache backed by Delta.
# MAGIC Cache hits retrieve generated SQL, then re-execute that SQL under the current
# MAGIC Databricks identity.
# MAGIC
# MAGIC **Cache flow:**
# MAGIC 1. Hybrid search (semantic + BM25) against the Vector Search index.
# MAGIC 2. Confidence tiering on the returned score.
# MAGIC 3. On a hit, increment hit metadata and re-execute cached SQL.
# MAGIC 4. On a miss, call Genie, execute the generated SQL, upsert it to Delta, and sync.
# MAGIC
# MAGIC **Prerequisites:** Run `0_setup.py` first.

# COMMAND ----------

# MAGIC %pip install "databricks-sdk>=0.85" "databricks-vectorsearch" pyyaml --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import time

from databricks.vector_search.client import VectorSearchClient

from utils import (
    CacheLookupResult,
    GLOBAL_SCOPE,
    build_cache_context,
    call_genie_with_retry,
    delta_cache_upsert,
    delta_increment_hit_count,
    execute_cached_sql_with_trace,
    load_config,
    print_sql_preview,
    print_summary_table,
    sync_vs_index_and_wait,
    table_name,
    trace_operation,
    vector_search_filters,
)

config = load_config("./configs.yaml")
context = build_cache_context(config)

VS_ENDPOINT = config["vs_endpoint"]
CACHE_STORE_TABLE = table_name(config, "cache_store")
CACHE_STORE_INDEX = f"{config['catalog']}.{config['schema']}.cache_store_index"
RESULT_ROW_LIMIT = int(config.get("result_row_limit", 100))

thresholds = config.get("thresholds", {})
AUTO_THRESHOLD = thresholds.get("vs_auto_execute", 0.90)
CONFIRM_THRESHOLD = thresholds.get("vs_confirm", 0.75)

vsc = VectorSearchClient(disable_notice=True)

print(f"VS Index:           {CACHE_STORE_INDEX}")
print(f"Cache context key:  {context.cache_context_key}")
print(f"Auto threshold:     {AUTO_THRESHOLD}")
print(f"Confirm threshold:  {CONFIRM_THRESHOLD}")
print(f"Result row limit:   {RESULT_ROW_LIMIT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cache Lookup — Hybrid Search with Confidence Tiering

# COMMAND ----------


def cache_lookup_vs(
    question: str,
    auto_threshold: float = AUTO_THRESHOLD,
    confirm_threshold: float = CONFIRM_THRESHOLD,
) -> CacheLookupResult:
    """Search the Vector Search index for a validated cache entry."""
    with trace_operation(config, "vector_search_cache_lookup", "RETRIEVER", {"question": question}):
        index = vsc.get_index(endpoint_name=VS_ENDPOINT, index_name=CACHE_STORE_INDEX)
        results = index.similarity_search(
            query_text=question,
            columns=["id", "question_text", "generated_sql", "genie_response_text"],
            filters=vector_search_filters(config, cache_scope=GLOBAL_SCOPE, validated_only=True),
            num_results=1,
            query_type="HYBRID",
        )

    hits = results.get("result", {}).get("data_array", [])
    if not hits:
        return CacheLookupResult(hit_type=None, score=0.0)

    row = hits[0]
    score = float(row[-1]) if row[-1] is not None else 0.0
    if score >= auto_threshold:
        tier = "auto"
    elif score >= confirm_threshold:
        tier = "confirm"
    else:
        return CacheLookupResult(hit_type=None, score=score)

    return CacheLookupResult(
        hit_type="vector-search",
        tier=tier,
        row_id=row[0],
        question_text=row[1],
        generated_sql=row[2],
        genie_response_text=row[3],
        score=score,
    )


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
    with trace_operation(config, "scenario2_question", "CHAIN", {"question": question}):
        print(f"\n{'=' * 60}")
        print(f"Question: {question}")

        t0 = time.time()
        lookup = cache_lookup_vs(question)

        if lookup.hit:
            delta_increment_hit_count(spark, CACHE_STORE_TABLE, lookup.row_id)
            sql_result = execute_cached_sql_with_trace(
                config, lookup.generated_sql, row_limit=RESULT_ROW_LIMIT
            )
            latency = time.time() - t0
            print(f"  HIT (tier={lookup.tier}, score={lookup.score:.3f}) in {latency:.3f}s")
            if lookup.tier == "confirm":
                print("  Moderate confidence; production UIs should ask for user confirmation.")
            print(f"  Cached SQL: {lookup.generated_sql[:160]}...")
            print_sql_preview(sql_result)
            results.append({
                "question": question[:50],
                "first_s": latency,
                "warm_s": None,
                "tier": lookup.tier,
                "source": "vs-hit",
                "speedup": "-",
            })
            continue

        print(f"  MISS (best score={lookup.score:.3f}) -> calling Genie API...")
        genie_result = call_genie_with_retry(config, question)
        print(f"  Genie returned in {genie_result.latency_seconds:.1f}s")

        if not genie_result.cacheable:
            latency = time.time() - t0
            print("  Genie did not return SQL; not caching this response.")
            results.append({
                "question": question[:50],
                "first_s": latency,
                "warm_s": None,
                "tier": "miss",
                "source": "genie-no-sql",
                "speedup": None,
            })
            continue

        sql_result = execute_cached_sql_with_trace(
            config, genie_result.generated_sql, row_limit=RESULT_ROW_LIMIT
        )
        row_id = delta_cache_upsert(
            spark,
            CACHE_STORE_TABLE,
            config,
            question=question,
            sql=genie_result.generated_sql,
            response_text=genie_result.response_text or "",
            cache_scope=GLOBAL_SCOPE,
            validated=True,
            validation_source="genie",
        )
        latency = time.time() - t0
        print(f"  SQL: {genie_result.generated_sql[:160]}...")
        print_sql_preview(sql_result)
        print(f"  Upserted Delta cache row id={row_id}")

        results.append({
            "question": question[:50],
            "first_s": latency,
            "warm_s": None,
            "tier": "miss",
            "source": "genie",
            "speedup": None,
        })

print(f"\n{'=' * 60}")
print("Syncing VS index for warm pass...")
sync_vs_index_and_wait(vsc, VS_ENDPOINT, CACHE_STORE_INDEX)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo — Warm Pass

# COMMAND ----------

for i, question in enumerate(demo_questions):
    with trace_operation(config, "scenario2_warm_question", "CHAIN", {"question": question}):
        print(f"\n{'=' * 60}")
        print(f"Question: {question}")

        t0 = time.time()
        lookup = cache_lookup_vs(question)

        if lookup.hit:
            delta_increment_hit_count(spark, CACHE_STORE_TABLE, lookup.row_id)
            sql_result = execute_cached_sql_with_trace(
                config, lookup.generated_sql, row_limit=RESULT_ROW_LIMIT
            )
            warm_latency = time.time() - t0
            print(f"  HIT (tier={lookup.tier}, score={lookup.score:.3f}) in {warm_latency:.3f}s")
            if lookup.tier == "confirm":
                print("  Moderate confidence; production UIs should ask for user confirmation.")
            print(f"  Cached SQL: {lookup.generated_sql[:160]}...")
            print_sql_preview(sql_result)
        else:
            print(f"  Unexpected MISS (score={lookup.score:.3f})")
            warm_latency = None

        if i < len(results):
            results[i]["warm_s"] = warm_latency
            results[i]["tier"] = lookup.tier or "miss"
            if warm_latency and results[i]["first_s"]:
                results[i]["speedup"] = f"{results[i]['first_s'] / warm_latency:.0f}x"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo — Confidence Tiering with a Paraphrased Question

# COMMAND ----------

if demo_questions:
    paraphrased = "How much total deposit volume did we have in 2024?"
    print(f"Original:    {demo_questions[0]}")
    print(f"Paraphrased: {paraphrased}")

    t0 = time.time()
    lookup = cache_lookup_vs(paraphrased)

    if lookup.hit:
        delta_increment_hit_count(spark, CACHE_STORE_TABLE, lookup.row_id)
        sql_result = execute_cached_sql_with_trace(
            config, lookup.generated_sql, row_limit=RESULT_ROW_LIMIT
        )
        latency = time.time() - t0
        print(f"\n  HIT tier={lookup.tier} (score={lookup.score:.3f}) in {latency:.3f}s")
        print_sql_preview(sql_result)
    else:
        latency = time.time() - t0
        print(f"\n  MISS (score={lookup.score:.3f}) in {latency:.3f}s")
        print(f"  Below confirm threshold ({CONFIRM_THRESHOLD})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary — Latency Comparison

# COMMAND ----------

print("\n" + "=" * 80)
print("SCENARIO 2: Vector Search Cache - Results")
print("=" * 80)
print_summary_table(results, ["question", "first_s", "warm_s", "tier", "source", "speedup"])
print()
print("Cache hits avoid Genie latency but still execute SQL for freshness and access control.")
print("Triggered VS sync is for demo reproducibility; production can use continuous sync.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)
# MAGIC
# MAGIC Uncomment to clear the cache_store table:
# MAGIC ```python
# MAGIC spark.sql(f"TRUNCATE TABLE {CACHE_STORE_TABLE}")
# MAGIC sync_vs_index_and_wait(vsc, VS_ENDPOINT, CACHE_STORE_INDEX)
# MAGIC ```

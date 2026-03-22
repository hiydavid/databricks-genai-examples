# Databricks notebook source
# MAGIC %md
# MAGIC # Scenario 1: Lakebase + pgvector Cache
# MAGIC
# MAGIC ![Architecture](scenario1-lakebase-pgvector.png)
# MAGIC
# MAGIC This notebook demonstrates query caching using **Lakebase** (Databricks-managed
# MAGIC PostgreSQL) with the **pgvector** extension.
# MAGIC
# MAGIC **Cache flow:**
# MAGIC 1. Normalize the incoming question
# MAGIC 2. **Exact match** — look up `question_normalized` in Lakebase
# MAGIC 3. **Vector similarity** — if no exact match, generate an embedding and run
# MAGIC    cosine similarity search via pgvector (threshold ≥ 0.92)
# MAGIC 4. **HIT** → return cached SQL / response immediately
# MAGIC 5. **MISS** → call Genie API (with retry + backoff), cache the response
# MAGIC
# MAGIC **Key properties:** ACID writes, immediate read-after-write, scale-to-zero.
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
    call_genie_with_retry,
    lakebase_cache_lookup,
    lakebase_cache_write,
    load_config,
    print_summary_table,
)

config = load_config("./configs.yaml")

SIMILARITY_THRESHOLD = config.get("thresholds", {}).get("lakebase_similarity", 0.92)

print(f"Genie Space:          {config['genie_space_id']}")
print(f"Similarity threshold: {SIMILARITY_THRESHOLD}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo — Cold Pass (Cache Miss → Genie API)
# MAGIC
# MAGIC Each question goes through:
# MAGIC 1. Cache lookup → expected **MISS**
# MAGIC 2. Genie API call with retry/backoff
# MAGIC 3. Cache write (immediate, ACID)

# COMMAND ----------

demo_questions = config.get("demo_questions", [
    "What is the total revenue for last quarter?",
])

results = []

for question in demo_questions:
    print(f"\n{'=' * 60}")
    print(f"Question: {question}")

    # --- Cache lookup ---
    t0 = time.time()
    hit_type, cached_sql, cached_resp, score, embedding = lakebase_cache_lookup(
        config, question, threshold=SIMILARITY_THRESHOLD
    )

    if hit_type:
        latency = time.time() - t0
        print(f"  HIT ({hit_type}, score={score:.3f}) in {latency:.3f}s")
        results.append({
            "question": question[:50],
            "cold_s": latency,
            "warm_s": None,
            "speedup": "-",
        })
        continue

    # --- Cache miss → Genie API ---
    print("  MISS → calling Genie API with retry/backoff...")
    genie_result = call_genie_with_retry(config, question)
    print(f"  Genie returned in {genie_result.latency_seconds:.1f}s")
    if genie_result.generated_sql:
        print(f"  SQL: {genie_result.generated_sql[:120]}...")

    # --- Cache write (reuse embedding from lookup to avoid duplicate API call) ---
    lakebase_cache_write(
        config, question,
        genie_result.generated_sql or "",
        genie_result.response_text or "",
        embedding=embedding,
    )
    cold_latency = time.time() - t0
    print(f"  Written to Lakebase cache (total: {cold_latency:.1f}s)")

    results.append({
        "question": question[:50],
        "cold_s": cold_latency,
        "warm_s": None,
        "speedup": None,
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo — Warm Pass (Cache Hit)
# MAGIC
# MAGIC Same questions again — all should return from cache (exact match).

# COMMAND ----------

for i, question in enumerate(demo_questions):
    print(f"\n{'=' * 60}")
    print(f"Question: {question}")

    t0 = time.time()
    hit_type, cached_sql, cached_resp, score, _ = lakebase_cache_lookup(
        config, question, threshold=SIMILARITY_THRESHOLD
    )
    warm_latency = time.time() - t0

    if hit_type:
        print(f"  HIT ({hit_type}, score={score:.3f}) in {warm_latency:.3f}s")
        if cached_sql:
            print(f"  Cached SQL: {cached_sql[:120]}...")
    else:
        print("  Unexpected MISS")
        warm_latency = None

    if i < len(results):
        results[i]["warm_s"] = warm_latency
        if warm_latency and results[i]["cold_s"]:
            results[i]["speedup"] = f"{results[i]['cold_s'] / warm_latency:.0f}x"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo — Semantic Similarity Hit
# MAGIC
# MAGIC Ask a **paraphrased** version of a previously cached question.  The exact-match
# MAGIC lookup will miss, but the pgvector similarity search should find a match above
# MAGIC the threshold (≥ 0.92).

# COMMAND ----------

if demo_questions:
    original = demo_questions[0]
    paraphrased = "How much total deposit volume did we have in 2024?"
    print(f"Original:    {original}")
    print(f"Paraphrased: {paraphrased}")

    t0 = time.time()
    hit_type, cached_sql, cached_resp, score, _ = lakebase_cache_lookup(
        config, paraphrased, threshold=SIMILARITY_THRESHOLD
    )
    latency = time.time() - t0

    if hit_type:
        print(f"\n  HIT ({hit_type}, score={score:.3f}) in {latency:.3f}s")
        print("  The paraphrased question matched via vector similarity!")
        if cached_sql:
            print(f"  Cached SQL: {cached_sql[:120]}...")
    else:
        print(f"\n  MISS (score below threshold {SIMILARITY_THRESHOLD})")
        print("  This is expected if the paraphrase differs significantly.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary — Latency Comparison

# COMMAND ----------

print("\n" + "=" * 80)
print("SCENARIO 1: Lakebase + pgvector Cache — Results")
print("=" * 80)
print_summary_table(results, ["question", "cold_s", "warm_s", "speedup"])
print()
print("Cache hit latency is dominated by the Lakebase round-trip (~50-200ms),")
print("vs. Genie API latency (~5-30s).  Expect 50-500x speedup on cache hits.")

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

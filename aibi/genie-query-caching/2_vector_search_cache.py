# Databricks notebook source
# MAGIC %md
# MAGIC # Scenario 2: Vector Search Index Cache
# MAGIC
# MAGIC ![Architecture](scenario2-vector-search.png)
# MAGIC
# MAGIC This notebook demonstrates query caching using a **Databricks Vector Search**
# MAGIC index backed by a Delta table in Unity Catalog.
# MAGIC
# MAGIC **Cache flow:**
# MAGIC 1. **Hybrid search** (semantic + BM25) against the VS index with managed embeddings
# MAGIC 2. **Confidence tiering** on the returned similarity score:
# MAGIC    - ≥ 0.90 → **auto-execute** the cached SQL
# MAGIC    - 0.75–0.90 → **confirm** — return cached result but flag for user review
# MAGIC    - < 0.75 → **fall through** to Genie API
# MAGIC 3. **MISS** → call Genie API (with retry + backoff)
# MAGIC 4. **APPEND** result to Delta table `cache_store`, trigger VS index sync
# MAGIC
# MAGIC **Key properties:** Unity Catalog governed, hybrid semantic + BM25 search,
# MAGIC managed embeddings (no manual embedding generation needed).
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
    call_genie_with_retry,
    generate_id,
    load_config,
    normalize_question,
    print_summary_table,
    sync_vs_index_and_wait,
    utcnow,
)

config = load_config("./configs.yaml")

CATALOG = config["catalog"]
SCHEMA = config["schema"]
VS_ENDPOINT = config["vs_endpoint"]
CACHE_STORE_TABLE = f"{CATALOG}.{SCHEMA}.cache_store"
CACHE_STORE_INDEX = f"{CATALOG}.{SCHEMA}.cache_store_index"

thresholds = config.get("thresholds", {})
AUTO_THRESHOLD = thresholds.get("vs_auto_execute", 0.90)
CONFIRM_THRESHOLD = thresholds.get("vs_confirm", 0.75)

vsc = VectorSearchClient(disable_notice=True)

print(f"VS Index:           {CACHE_STORE_INDEX}")
print(f"Auto threshold:     {AUTO_THRESHOLD}")
print(f"Confirm threshold:  {CONFIRM_THRESHOLD}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cache Lookup — Hybrid Search with Confidence Tiering

# COMMAND ----------


def cache_lookup_vs(
    question: str,
    auto_threshold: float = AUTO_THRESHOLD,
    confirm_threshold: float = CONFIRM_THRESHOLD,
):
    """Search the Vector Search index for a cached response.

    Uses hybrid (semantic + BM25) search with managed embeddings.

    Returns (tier, cached_sql, cached_response, score) where tier is:
    - "auto"    — score ≥ auto_threshold, safe to execute cached SQL directly
    - "confirm" — score between confirm and auto thresholds, flag for review
    - None      — no match above confirm threshold (cache miss)
    """
    index = vsc.get_index(endpoint_name=VS_ENDPOINT, index_name=CACHE_STORE_INDEX)

    results = index.similarity_search(
        query_text=question,
        columns=["id", "question_text", "cached_sql", "cached_response"],
        num_results=1,
        query_type="HYBRID",
    )

    hits = results.get("result", {}).get("data_array", [])
    if not hits:
        return None, None, None, 0.0

    row = hits[0]
    # VS returns: [id, question_text, cached_sql, cached_response, score]
    score = float(row[-1]) if row[-1] is not None else 0.0
    cached_sql = row[2]
    cached_response = row[3]

    if score >= auto_threshold:
        return "auto", cached_sql, cached_response, score
    elif score >= confirm_threshold:
        return "confirm", cached_sql, cached_response, score
    else:
        return None, None, None, score


# COMMAND ----------

# MAGIC %md
# MAGIC ## Cache Write — Append to Delta Table

# COMMAND ----------


def cache_write_delta(question: str, sql: str, response_text: str):
    """Append a new entry to the cache_store Delta table.

    The VS index will pick up the new row on the next sync.
    """
    from pyspark.sql import Row

    row_id = generate_id()
    normalized = normalize_question(question)

    new_row = Row(
        id=row_id,
        question_text=question,
        question_normalized=normalized,
        cached_sql=sql or "",
        cached_response=response_text or "",
        created_at=utcnow(),
        hit_count=0,
    )
    df = spark.createDataFrame([new_row])
    df.write.mode("append").saveAsTable(CACHE_STORE_TABLE)

    print(f"  Written to Delta table {CACHE_STORE_TABLE} (id={row_id})")
    return row_id


# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo — Cold Pass (Cache Miss → Genie API)
# MAGIC
# MAGIC Each question goes through:
# MAGIC 1. VS hybrid search → expected **MISS** (empty index)
# MAGIC 2. Genie API call with retry/backoff
# MAGIC 3. Delta table append → VS index sync

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
    tier, cached_sql, cached_resp, score = cache_lookup_vs(question)

    if tier:
        latency = time.time() - t0
        print(f"  HIT (tier={tier}, score={score:.3f}) in {latency:.3f}s")
        results.append({
            "question": question[:50],
            "cold_s": latency,
            "warm_s": None,
            "tier": tier,
            "speedup": "-",
        })
        continue

    print(f"  MISS (best score={score:.3f}) → calling Genie API...")

    # --- Genie API ---
    genie_result = call_genie_with_retry(config, question)
    print(f"  Genie returned in {genie_result.latency_seconds:.1f}s")
    if genie_result.generated_sql:
        print(f"  SQL: {genie_result.generated_sql[:120]}...")

    # --- Cache write ---
    cache_write_delta(
        question,
        genie_result.generated_sql or "",
        genie_result.response_text or "",
    )
    cold_latency = time.time() - t0

    results.append({
        "question": question[:50],
        "cold_s": cold_latency,
        "warm_s": None,
        "tier": "miss",
        "speedup": None,
    })

# --- Sync VS index so warm pass can find the new entries ---
print(f"\n{'=' * 60}")
print("Syncing VS index for warm pass...")
sync_vs_index_and_wait(vsc, VS_ENDPOINT, CACHE_STORE_INDEX)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo — Warm Pass (Cache Hit with Confidence Tiering)
# MAGIC
# MAGIC Same questions — should now return from VS cache with **auto** tier.

# COMMAND ----------

for i, question in enumerate(demo_questions):
    print(f"\n{'=' * 60}")
    print(f"Question: {question}")

    t0 = time.time()
    tier, cached_sql, cached_resp, score = cache_lookup_vs(question)
    warm_latency = time.time() - t0

    if tier:
        print(f"  HIT (tier={tier}, score={score:.3f}) in {warm_latency:.3f}s")
        if tier == "confirm":
            print("  Confidence is moderate — a production system would ask the user to confirm")
        if cached_sql:
            print(f"  Cached SQL: {cached_sql[:120]}...")
    else:
        print(f"  Unexpected MISS (score={score:.3f})")
        warm_latency = None

    if i < len(results):
        results[i]["warm_s"] = warm_latency
        results[i]["tier"] = tier or "miss"
        if warm_latency and results[i]["cold_s"]:
            results[i]["speedup"] = f"{results[i]['cold_s'] / warm_latency:.0f}x"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo — Confidence Tiering with Paraphrased Question
# MAGIC
# MAGIC A paraphrased version of a cached question may score between the confirm and
# MAGIC auto thresholds, demonstrating the three-tier system.

# COMMAND ----------

if demo_questions:
    original = demo_questions[0]
    paraphrased = "Can you tell me the total revenue from the previous quarter?"
    print(f"Original:    {original}")
    print(f"Paraphrased: {paraphrased}")

    t0 = time.time()
    tier, cached_sql, cached_resp, score = cache_lookup_vs(paraphrased)
    latency = time.time() - t0

    if tier == "auto":
        print(f"\n  AUTO tier (score={score:.3f}) in {latency:.3f}s")
        print("  High confidence — safe to execute cached SQL directly")
    elif tier == "confirm":
        print(f"\n  CONFIRM tier (score={score:.3f}) in {latency:.3f}s")
        print("  Moderate confidence — production system would ask user to review")
    else:
        print(f"\n  MISS (score={score:.3f}) in {latency:.3f}s")
        print(f"  Below confirm threshold ({CONFIRM_THRESHOLD})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary — Latency Comparison

# COMMAND ----------

print("\n" + "=" * 80)
print("SCENARIO 2: Vector Search Cache — Results")
print("=" * 80)
print_summary_table(results, ["question", "cold_s", "warm_s", "tier", "speedup"])
print()
print("Note: Cold latency includes Genie API time + Delta write + VS index sync.")
print("In production, continuous sync eliminates the sync wait (~10-30s).")
print("Cache hit latency is ~200-500ms (VS round-trip + managed embedding).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)
# MAGIC
# MAGIC Uncomment to clear the cache_store table:
# MAGIC ```python
# MAGIC spark.sql(f"TRUNCATE TABLE {CACHE_STORE_TABLE}")
# MAGIC sync_vs_index_and_wait(vsc, VS_ENDPOINT, CACHE_STORE_INDEX)
# MAGIC ```

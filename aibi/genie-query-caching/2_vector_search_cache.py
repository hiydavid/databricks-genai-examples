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
# MAGIC 2. **Reuse policy** (hybrid rank scores are not confidence scores):
# MAGIC    - Same normalized question → **auto-execute** the cached SQL
# MAGIC    - Different question → **confirm** — suggest SQL for review only
# MAGIC    - No candidate → **fall through** to Genie API
# MAGIC 3. **MISS** → call Genie API (with retry + backoff)
# MAGIC 4. **UPSERT** result to Delta table `cache_store`, trigger VS index sync
# MAGIC
# MAGIC **Key properties:** Unity Catalog governed, hybrid semantic + BM25 search,
# MAGIC managed embeddings (no manual embedding generation needed).
# MAGIC
# MAGIC **Prerequisites:** Run `0_setup.py` first.

# COMMAND ----------

# MAGIC %pip install "databricks-sdk>=0.85" "databricks-ai-search>=0.78" pyyaml --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import time

from databricks.ai_search.client import AISearchClient

from utils import (
    call_genie_with_retry,
    execute_cached_sql,
    generate_id,
    load_config,
    normalize_question,
    print_summary_table,
    sync_vs_index_and_wait,
)

config = load_config("./configs.yaml")

CATALOG = config["catalog"]
SCHEMA = config["schema"]
VS_ENDPOINT = config["vs_endpoint"]
CACHE_STORE_TABLE = f"{CATALOG}.{SCHEMA}.cache_store"
CACHE_STORE_INDEX = f"{CATALOG}.{SCHEMA}.cache_store_index"

vsc = AISearchClient(disable_notice=True)

print(f"VS Index:           {CACHE_STORE_INDEX}")
print("Reuse policy:       Exact question → auto; other candidates → review")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cache Lookup — Hybrid Retrieval with Exact-Question Verification

# COMMAND ----------


def cache_lookup_vs(question: str):
    """Search the Vector Search index for a cached response.

    Uses hybrid (semantic + BM25) search with managed embeddings.

    Returns (tier, cached_sql, cached_response, score) where tier is:
    - "auto"    — the candidate answers the same normalized question
    - "confirm" — a candidate for a different question; do not execute it
    - None      — no candidate (cache miss)

    RRF scores rank candidates; they cannot establish that dates, amounts,
    filters, or intent are equivalent. Paraphrases require user review.
    """
    index = vsc.get_index(endpoint_name=VS_ENDPOINT, index_name=CACHE_STORE_INDEX)

    results = index.similarity_search(
        query_text=question,
        columns=["id", "question_text", "cached_sql", "cached_response"],
        num_results=5,
        query_type="HYBRID",
    )

    hits = results.get("result", {}).get("data_array", [])
    if not hits:
        return None, None, None, 0.0

    # VS returns: [id, question_text, cached_sql, cached_response, rank_score].
    # Inspect all candidates: hybrid ranking may put a different question first.
    for row in hits:
        if normalize_question(row[1] or "") == normalize_question(question):
            return "auto", row[2], row[3], float(row[-1] or 0.0)
    row = hits[0]
    return "confirm", row[2], row[3], float(row[-1] or 0.0)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Cache Write — Upsert to Delta Table

# COMMAND ----------


def cache_write_delta(question: str, sql: str, response_text: str):
    """Upsert a cache entry into the cache_store Delta table.

    MERGE on ``question_normalized`` so re-running the notebook refreshes the
    cached answer instead of appending a duplicate row. The VS index picks up
    the change on the next sync (Change Data Feed).
    """
    row_id = generate_id()
    normalized = normalize_question(question)

    spark.createDataFrame(
        [(row_id, normalized, question, sql or "", response_text or "")],
        schema=(
            "id STRING, question_normalized STRING, question_text STRING, "
            "cached_sql STRING, cached_response STRING"
        ),
    ).createOrReplaceTempView("new_cache_entry")

    spark.sql(f"""
        MERGE INTO {CACHE_STORE_TABLE} t
        USING new_cache_entry s
          ON t.question_normalized = s.question_normalized
        WHEN MATCHED THEN UPDATE SET
            t.cached_sql = s.cached_sql,
            t.cached_response = s.cached_response,
            t.created_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT
            (id, question_text, question_normalized, cached_sql, cached_response, created_at, hit_count)
        VALUES
            (s.id, s.question_text, s.question_normalized, s.cached_sql,
             s.cached_response, current_timestamp(), 0)
    """)

    print(f"  Upserted into Delta table {CACHE_STORE_TABLE} (id={row_id})")
    return row_id


def query_with_vs_cache(question: str):
    """Look up, execute or fall back, timing through result materialization."""
    start = time.perf_counter()
    tier, sql, response, score = cache_lookup_vs(question)
    df = None
    if tier == "auto" and sql:
        df = execute_cached_sql(spark, sql)
        if df is None:
            tier = None  # Includes errors raised during Spark's execution action.
    if tier is None:
        print("  Cache miss or failed SQL → calling Genie...")
        genie_result = call_genie_with_retry(config, question)
        sql = genie_result.generated_sql or ""
        response = genie_result.response_text or ""
        cache_write_delta(question, sql, response)
        tier = "miss"
    return {
        "tier": tier, "sql": sql, "response": response, "score": score,
        "df": df, "latency_s": time.perf_counter() - start,
    }


def show_result(result):
    """Display already materialized results; never execute a review candidate."""
    print(f"  Tier: {result['tier']} | Rank score: {result['score']:.3f} | Latency: {result['latency_s']:.3f}s")
    if result["df"] is not None:
        display(result["df"])
    elif result["tier"] == "confirm":
        print("  Different question — review the question's dates, filters, and intent before using this SQL:")
        print(result["sql"] or "  No SQL candidate")
    else:
        print(result["response"] or result["sql"] or "  No answer returned")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo — Cold Pass (Cache Miss → Genie API)
# MAGIC
# MAGIC Each question goes through:
# MAGIC 1. VS hybrid search → expected **MISS** (empty index, unless
# MAGIC    `seed_demo_cache` was enabled in 0_setup)
# MAGIC 2. Genie API call with retry/backoff
# MAGIC 3. Delta table upsert → VS index sync

# COMMAND ----------

demo_questions = config.get("demo_questions", [
    "What is the total revenue for last quarter?",
])

results = []

for question in demo_questions:
    print(f"\n{'=' * 60}")
    print(f"Question: {question}")

    result = query_with_vs_cache(question)
    show_result(result)
    results.append({
        "question": question[:50],
        "cold_s": result["latency_s"],
        "warm_s": None,
        "tier": result["tier"],
        "speedup": None,
    })

# --- Sync VS index so warm pass can find the new entries ---
print(f"\n{'=' * 60}")
print("Syncing VS index for warm pass...")
sync_start = time.perf_counter()
sync_vs_index_and_wait(vsc, VS_ENDPOINT, CACHE_STORE_INDEX)
print(f"Batch index sync: {time.perf_counter() - sync_start:.1f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo — Warm Pass (Verified Question Match)
# MAGIC
# MAGIC Same questions — should now return from VS cache.  **auto**-tier hits
# MAGIC re-execute the cached SQL so the answer reflects the current data;
# MAGIC **confirm**-tier hits return the cached SQL flagged for review without
# MAGIC executing it.

# COMMAND ----------

for i, question in enumerate(demo_questions):
    print(f"\n{'=' * 60}")
    print(f"Question: {question}")

    result = query_with_vs_cache(question)
    show_result(result)
    warm_latency = result["latency_s"]

    if i < len(results):
        results[i]["warm_s"] = warm_latency
        results[i]["tier"] = result["tier"]
        # A suggestion awaiting review has not answered the question.
        if result["tier"] == "auto" and warm_latency and results[i]["cold_s"]:
            results[i]["speedup"] = f"{results[i]['cold_s'] / warm_latency:.0f}x"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo — Review a Paraphrased Question
# MAGIC
# MAGIC A paraphrase remains a review candidate even when its hybrid rank score
# MAGIC is high. The demo does not assume that retrieval proves equivalence.

# COMMAND ----------

if demo_questions:
    original = demo_questions[0]
    paraphrased = "How much total deposit volume did we have in 2024?"
    print(f"Original:    {original}")
    print(f"Paraphrased: {paraphrased}")

    result = query_with_vs_cache(paraphrased)
    show_result(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary — Latency Comparison

# COMMAND ----------

print("\n" + "=" * 80)
print("SCENARIO 2: Vector Search Cache — Results")
print("=" * 80)
print_summary_table(results, ["question", "cold_s", "warm_s", "tier", "speedup"])
print()
print("Cold misses include Genie API time + Delta write; batch index sync is timed separately.")
print("Auto hits include VS lookup + SQL execution and result collection; display rendering is excluded.")
print("Review candidates are suggestions, so no answer speedup is reported for them.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)
# MAGIC
# MAGIC Uncomment to clear the cache_store table:
# MAGIC ```python
# MAGIC spark.sql(f"TRUNCATE TABLE {CACHE_STORE_TABLE}")
# MAGIC sync_vs_index_and_wait(vsc, VS_ENDPOINT, CACHE_STORE_INDEX)
# MAGIC ```

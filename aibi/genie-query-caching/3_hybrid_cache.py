# Databricks notebook source
# MAGIC %md
# MAGIC # Scenario 3: Hybrid Cache — L1 Lakebase + L2 Vector Search (Recommended)
# MAGIC
# MAGIC ![Architecture](scenario3-hybrid.png)
# MAGIC
# MAGIC This notebook demonstrates the **recommended** two-tier caching architecture:
# MAGIC
# MAGIC | Layer | Technology | Purpose | TTL |
# MAGIC |-------|-----------|---------|-----|
# MAGIC | **L1** | Lakebase + pgvector | Session cache — fast exact + vector match | Session lifetime |
# MAGIC | **L2** | Vector Search + Delta | Knowledge base — validated, durable cache | 30+ weeks |
# MAGIC
# MAGIC **Cache flow:**
# MAGIC 1. **L1 check** — exact match + pgvector similarity (threshold ≥ 0.93) in Lakebase
# MAGIC 2. **L1 HIT** → return cached response immediately
# MAGIC 3. **L1 MISS → L2 check** — hybrid semantic + BM25 search (threshold ≥ 0.90)
# MAGIC 4. **L2 HIT** → re-execute cached SQL for freshness, promote to L1
# MAGIC 5. **L2 MISS** → Genie API (with retry + backoff) → write to L1
# MAGIC 6. **Promotion** — entries promoted from L1 → L2 after validation (thumbs-up or hit_count ≥ 3)
# MAGIC
# MAGIC **Prerequisites:** Run `0_setup.py` first.

# COMMAND ----------

# MAGIC %pip install "databricks-sdk>=0.85" "databricks-vectorsearch" "psycopg[binary]>=3.1" "pgvector>=0.3" pyyaml --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import json
import time
import uuid

from databricks.vector_search.client import VectorSearchClient

from utils import (
    call_genie_with_retry,
    generate_id,
    get_lakebase_connection,
    lakebase_cache_lookup,
    lakebase_cache_write,
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
CACHE_KB_TABLE = f"{CATALOG}.{SCHEMA}.cache_knowledge_base"
CACHE_KB_INDEX = f"{CATALOG}.{SCHEMA}.cache_knowledge_base_index"

thresholds = config.get("thresholds", {})
L1_THRESHOLD = thresholds.get("lakebase_hybrid", 0.93)
L2_THRESHOLD = thresholds.get("vs_auto_execute", 0.90)

# Unique session ID for this notebook run — scopes L1 cache entries
SESSION_ID = uuid.uuid4().hex[:8]

vsc = VectorSearchClient(disable_notice=True)

print(f"Session ID:         {SESSION_ID}")
print(f"L1 threshold:       {L1_THRESHOLD} (Lakebase pgvector)")
print(f"L2 threshold:       {L2_THRESHOLD} (Vector Search)")
print(f"L2 Knowledge Base:  {CACHE_KB_INDEX}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## L1 — Lakebase Session Cache
# MAGIC
# MAGIC Uses shared `lakebase_cache_lookup` / `lakebase_cache_write` from `utils.py`
# MAGIC with a `session_id` to scope entries to this notebook run.

# COMMAND ----------


def l1_clear_session(session_id: str = SESSION_ID):
    """Delete all L1 cache entries for a specific session."""
    conn = get_lakebase_connection(config)
    with conn.cursor() as cur:
        cur.execute("DELETE FROM genie_cache WHERE session_id = %s", (session_id,))
    conn.commit()
    print(f"  Cleared L1 cache for session {session_id}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## L2 — Vector Search Knowledge Base

# COMMAND ----------


def l2_cache_lookup(question: str):
    """L2 cache lookup: hybrid semantic + BM25 search on the knowledge base VS index."""
    index = vsc.get_index(endpoint_name=VS_ENDPOINT, index_name=CACHE_KB_INDEX)

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
    score = float(row[-1]) if row[-1] is not None else 0.0
    cached_sql = row[2]
    cached_response = row[3]

    if score >= L2_THRESHOLD:
        return "hit", cached_sql, cached_response, score

    return None, None, None, score


def promote_to_l2(question: str, sql: str, response_text: str, source: str = "auto"):
    """Promote a validated entry from L1 to the L2 knowledge base (Delta table)."""
    from pyspark.sql import Row

    row_id = generate_id()
    normalized = normalize_question(question)

    new_row = Row(
        id=row_id,
        question_text=question,
        question_normalized=normalized,
        cached_sql=sql or "",
        cached_response=response_text or "",
        validated=True,
        validation_source=source,
        created_at=utcnow(),
        hit_count=0,
    )
    df = spark.createDataFrame([new_row])
    df.write.mode("append").saveAsTable(CACHE_KB_TABLE)

    print(f"  Promoted to L2 knowledge base (id={row_id}, source={source})")
    return row_id


# COMMAND ----------

# MAGIC %md
# MAGIC ## Orchestrator — Full Hybrid Cache Flow

# COMMAND ----------


def query_with_hybrid_cache(question: str, session_id: str = SESSION_ID):
    """Execute the full hybrid cache flow: L1 → L2 → Genie API.

    Returns a dict with: source, sql, response, score, latency_s
    """
    start = time.time()

    # --- L1: Lakebase session cache ---
    hit_type, sql, resp, score, embedding = lakebase_cache_lookup(
        config, question, threshold=L1_THRESHOLD, session_id=session_id,
    )
    if hit_type:
        return {
            "source": f"L1-{hit_type}",
            "sql": sql,
            "response": resp,
            "score": score,
            "latency_s": time.time() - start,
        }

    # --- L2: Vector Search knowledge base ---
    hit, sql, resp_text, score = l2_cache_lookup(question)
    if hit:
        # Promote L2 hit to L1 for session-level caching (reuse embedding from L1 lookup)
        lakebase_cache_write(
            config, question, sql, resp_text or "",
            session_id=session_id, embedding=embedding,
        )
        return {
            "source": "L2-hit",
            "sql": sql,
            "response": resp_text,
            "score": score,
            "latency_s": time.time() - start,
        }

    # --- Genie API (with retry/backoff) ---
    genie_result = call_genie_with_retry(config, question)
    sql = genie_result.generated_sql or ""
    text = genie_result.response_text or ""

    # Write to L1 (reuse embedding from L1 lookup)
    lakebase_cache_write(
        config, question, sql, text,
        session_id=session_id, embedding=embedding,
    )

    return {
        "source": "Genie",
        "sql": sql,
        "response": text,
        "score": 0.0,
        "latency_s": time.time() - start,
    }


# COMMAND ----------

# MAGIC %md
# MAGIC ## Pass 1 — Cold Start (L1 Miss, L2 Miss → Genie API)
# MAGIC
# MAGIC All questions hit the Genie API and get written to L1.

# COMMAND ----------

demo_questions = config.get("demo_questions", [
    "What is the total revenue for last quarter?",
])

all_results = {}  # question -> {pass1, pass2, pass3, pass4}

for question in demo_questions:
    print(f"\n{'=' * 60}")
    print(f"Question: {question}")

    result = query_with_hybrid_cache(question)
    print(f"  Source: {result['source']} | Latency: {result['latency_s']:.1f}s")
    if result["sql"]:
        print(f"  SQL: {result['sql'][:120]}...")

    all_results[question] = {"pass1": result}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pass 2 — L1 Warm (Session Cache Hit)
# MAGIC
# MAGIC Same questions → all should return from L1 (Lakebase exact match).

# COMMAND ----------

for question in demo_questions:
    print(f"\n{'=' * 60}")
    print(f"Question: {question}")

    result = query_with_hybrid_cache(question)
    print(f"  Source: {result['source']} | Score: {result['score']:.3f} | Latency: {result['latency_s']:.3f}s")

    all_results[question]["pass2"] = result

# COMMAND ----------

# MAGIC %md
# MAGIC ## Promote to L2 — Simulate Validation
# MAGIC
# MAGIC In production, entries are promoted to L2 after user validation (thumbs-up) or
# MAGIC repeated use (hit_count ≥ 3).  Here we simulate this by promoting all cached
# MAGIC entries.

# COMMAND ----------

for question in demo_questions:
    r = all_results[question]["pass1"]
    resp = r["response"]
    resp_str = resp if isinstance(resp, str) else json.dumps(resp) if resp else ""
    promote_to_l2(question, r["sql"] or "", resp_str, source="manual-validation")

print("\nSyncing L2 VS index...")
sync_vs_index_and_wait(vsc, VS_ENDPOINT, CACHE_KB_INDEX)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clear L1 — Simulate New Session
# MAGIC
# MAGIC Delete the current session's L1 entries to simulate a fresh session.
# MAGIC L2 entries persist.

# COMMAND ----------

l1_clear_session(SESSION_ID)

# Verify L1 is empty for these questions
for question in demo_questions:
    hit_type, _, _, _, _ = lakebase_cache_lookup(
        config, question, threshold=L1_THRESHOLD, session_id=SESSION_ID,
    )
    status = "MISS" if hit_type is None else f"HIT ({hit_type})"
    print(f"  L1 check: {status} — {question[:50]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pass 3 — L2 Warm (Knowledge Base Hit → Promote to L1)
# MAGIC
# MAGIC L1 is empty, but L2 has the promoted entries.  Each L2 hit gets promoted
# MAGIC back to L1 for the rest of the session.

# COMMAND ----------

for question in demo_questions:
    print(f"\n{'=' * 60}")
    print(f"Question: {question}")

    result = query_with_hybrid_cache(question)
    print(f"  Source: {result['source']} | Score: {result['score']:.3f} | Latency: {result['latency_s']:.3f}s")

    all_results[question]["pass3"] = result

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pass 4 — L1 Warm Again (Promoted from L2)
# MAGIC
# MAGIC The L2 hits were promoted to L1 in Pass 3.  Now they should hit L1 directly.

# COMMAND ----------

for question in demo_questions:
    print(f"\n{'=' * 60}")
    print(f"Question: {question}")

    result = query_with_hybrid_cache(question)
    print(f"  Source: {result['source']} | Score: {result['score']:.3f} | Latency: {result['latency_s']:.3f}s")

    all_results[question]["pass4"] = result

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary — Full Lifecycle Comparison

# COMMAND ----------

summary_rows = []
for question in demo_questions:
    r = all_results[question]
    summary_rows.append({
        "question": question[:40],
        "pass1_genie_s": r["pass1"]["latency_s"],
        "pass2_L1_s": r["pass2"]["latency_s"],
        "pass3_L2_s": r.get("pass3", {}).get("latency_s", 0),
        "pass4_L1_s": r.get("pass4", {}).get("latency_s", 0),
    })

print("\n" + "=" * 90)
print("SCENARIO 3: Hybrid Cache (L1 Lakebase + L2 Vector Search) — Results")
print("=" * 90)
print_summary_table(
    summary_rows,
    ["question", "pass1_genie_s", "pass2_L1_s", "pass3_L2_s", "pass4_L1_s"],
)
print()
print("Pass 1: Cold — Genie API (5-30s)")
print("Pass 2: L1 warm — Lakebase exact match (~50-200ms)")
print("Pass 3: L2 warm — VS hybrid search + promote to L1 (~0.5-2s)")
print("Pass 4: L1 warm — promoted entries now in L1 (~50-200ms)")
print()
print("The hybrid approach gives you the best of both worlds:")
print("  - L1 (Lakebase): Blazing fast session cache with ACID writes")
print("  - L2 (Vector Search): Durable knowledge base that persists across sessions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)
# MAGIC
# MAGIC Uncomment to clear all cache data:
# MAGIC ```python
# MAGIC # Clear L1 (Lakebase)
# MAGIC from utils import get_lakebase_connection
# MAGIC conn = get_lakebase_connection(config)
# MAGIC conn.cursor().execute("TRUNCATE TABLE genie_cache")
# MAGIC conn.commit()
# MAGIC
# MAGIC # Clear L2 (Delta)
# MAGIC spark.sql(f"TRUNCATE TABLE {CACHE_KB_TABLE}")
# MAGIC sync_vs_index_and_wait(vsc, VS_ENDPOINT, CACHE_KB_INDEX)
# MAGIC ```

# Databricks notebook source
# MAGIC %md
# MAGIC # Scenario 3: Hybrid Cache — L1 Lakebase + L2 Vector Search
# MAGIC
# MAGIC ![Architecture](scenario3-hybrid.png)
# MAGIC
# MAGIC This notebook demonstrates a two-tier cache:
# MAGIC
# MAGIC | Layer | Technology | Purpose |
# MAGIC |-------|-----------|---------|
# MAGIC | L1 | Lakebase + pgvector | Session-scoped cache for the current workflow |
# MAGIC | L2 | Vector Search + Delta | Validated cross-session SQL knowledge base |
# MAGIC
# MAGIC **Cache flow:**
# MAGIC 1. Check L1 for a session-scoped exact/vector match.
# MAGIC 2. On L1 hit, re-execute cached SQL.
# MAGIC 3. On L1 miss, check validated L2 rows for the current cache context.
# MAGIC 4. On L2 hit, re-execute cached SQL and promote it to L1.
# MAGIC 5. On L2 miss, call Genie, execute generated SQL, and write it to L1.
# MAGIC 6. Simulate validation by promoting selected L1 entries to L2.
# MAGIC
# MAGIC **Prerequisites:** Run `0_setup.py` first.

# COMMAND ----------

# MAGIC %pip install "databricks-sdk>=0.85" "databricks-vectorsearch" "psycopg[binary]>=3.1" "pgvector>=0.3" pyyaml --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import time
import uuid

from databricks.vector_search.client import VectorSearchClient

from utils import (
    CacheLookupResult,
    GLOBAL_SCOPE,
    SESSION_SCOPE,
    build_cache_context,
    call_genie_with_retry,
    delta_cache_upsert,
    delta_increment_hit_count,
    execute_cached_sql_with_trace,
    get_lakebase_connection,
    lakebase_cache_lookup,
    lakebase_cache_write,
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
CACHE_KB_TABLE = table_name(config, "cache_knowledge_base")
CACHE_KB_INDEX = f"{config['catalog']}.{config['schema']}.cache_knowledge_base_index"
RESULT_ROW_LIMIT = int(config.get("result_row_limit", 100))

thresholds = config.get("thresholds", {})
L1_THRESHOLD = thresholds.get("lakebase_hybrid", 0.93)
L2_THRESHOLD = thresholds.get("vs_auto_execute", 0.90)

SESSION_ID = uuid.uuid4().hex[:8]
vsc = VectorSearchClient(disable_notice=True)

print(f"Session ID:         {SESSION_ID}")
print(f"Cache context key:  {context.cache_context_key}")
print(f"L1 threshold:       {L1_THRESHOLD} (Lakebase pgvector)")
print(f"L2 threshold:       {L2_THRESHOLD} (Vector Search)")
print(f"Result row limit:   {RESULT_ROW_LIMIT}")
print(f"L2 Knowledge Base:  {CACHE_KB_INDEX}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## L1 — Lakebase Session Cache

# COMMAND ----------


def l1_clear_session(session_id: str = SESSION_ID):
    """Delete all L1 cache entries for a specific session and context."""
    conn = get_lakebase_connection(config)
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM genie_cache
            WHERE space_id = %s
              AND cache_context_key = %s
              AND cache_scope = %s
              AND session_id = %s
            """,
            (context.space_id, context.cache_context_key, SESSION_SCOPE, session_id),
        )
    conn.commit()
    print(f"  Cleared L1 cache for session {session_id}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## L2 — Vector Search Knowledge Base

# COMMAND ----------


def l2_cache_lookup(question: str) -> CacheLookupResult:
    """Lookup a validated L2 cache entry for the current context."""
    with trace_operation(config, "l2_vector_search_lookup", "RETRIEVER", {"question": question}):
        index = vsc.get_index(endpoint_name=VS_ENDPOINT, index_name=CACHE_KB_INDEX)
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
    if score < L2_THRESHOLD:
        return CacheLookupResult(hit_type=None, score=score)

    return CacheLookupResult(
        hit_type="l2",
        tier="auto",
        row_id=row[0],
        question_text=row[1],
        generated_sql=row[2],
        genie_response_text=row[3],
        score=score,
    )


def promote_to_l2(question: str, sql: str, response_text: str = "", source: str = "manual-validation"):
    """Promote validated SQL into the durable L2 knowledge base."""
    row_id = delta_cache_upsert(
        spark,
        CACHE_KB_TABLE,
        config,
        question=question,
        sql=sql,
        response_text=response_text or "",
        cache_scope=GLOBAL_SCOPE,
        validated=True,
        validation_source=source,
    )
    print(f"  Promoted to L2 knowledge base (id={row_id}, source={source})")
    return row_id


# COMMAND ----------

# MAGIC %md
# MAGIC ## Orchestrator — Full Hybrid Cache Flow

# COMMAND ----------


def query_with_hybrid_cache(question: str, session_id: str = SESSION_ID):
    """Run L1 -> L2 -> Genie and always execute SQL for the final answer."""
    with trace_operation(
        config,
        "query_with_hybrid_cache",
        "CHAIN",
        {"question": question, "session_id": session_id},
    ):
        start = time.time()

        l1 = lakebase_cache_lookup(
            config,
            question,
            threshold=L1_THRESHOLD,
            cache_scope=SESSION_SCOPE,
            session_id=session_id,
        )
        if l1.hit:
            sql_result = execute_cached_sql_with_trace(
                config, l1.generated_sql, row_limit=RESULT_ROW_LIMIT
            )
            return {
                "source": f"L1-{l1.hit_type}",
                "sql": l1.generated_sql,
                "response": l1.genie_response_text,
                "score": l1.score,
                "sql_result": sql_result,
                "latency_s": time.time() - start,
            }

        l2 = l2_cache_lookup(question)
        if l2.hit:
            delta_increment_hit_count(spark, CACHE_KB_TABLE, l2.row_id)
            sql_result = execute_cached_sql_with_trace(
                config, l2.generated_sql, row_limit=RESULT_ROW_LIMIT
            )
            lakebase_cache_write(
                config,
                question=question,
                sql=l2.generated_sql,
                response_text=l2.genie_response_text or "",
                cache_scope=SESSION_SCOPE,
                session_id=session_id,
                embedding=l1.embedding,
                validated=True,
                validation_source="l2-hit",
            )
            return {
                "source": "L2-hit",
                "sql": l2.generated_sql,
                "response": l2.genie_response_text,
                "score": l2.score,
                "sql_result": sql_result,
                "latency_s": time.time() - start,
            }

        genie_result = call_genie_with_retry(config, question)
        if not genie_result.cacheable:
            return {
                "source": "Genie-no-sql",
                "sql": None,
                "response": genie_result.response_text,
                "score": 0.0,
                "sql_result": None,
                "latency_s": time.time() - start,
            }

        sql_result = execute_cached_sql_with_trace(
            config, genie_result.generated_sql, row_limit=RESULT_ROW_LIMIT
        )
        lakebase_cache_write(
            config,
            question=question,
            sql=genie_result.generated_sql,
            response_text=genie_result.response_text or "",
            cache_scope=SESSION_SCOPE,
            session_id=session_id,
            embedding=l1.embedding,
            validated=False,
            validation_source=None,
        )

        return {
            "source": "Genie",
            "sql": genie_result.generated_sql,
            "response": genie_result.response_text,
            "score": 0.0,
            "sql_result": sql_result,
            "latency_s": time.time() - start,
        }


# COMMAND ----------

# MAGIC %md
# MAGIC ## Pass 1 — Cold Start
# MAGIC
# MAGIC With unseeded setup, this path misses L1/L2 and calls Genie. Seeded setup may
# MAGIC hit L2 immediately.

# COMMAND ----------

demo_questions = config.get("demo_questions", [
    "What is the total revenue for last quarter?",
])

all_results = {}

for question in demo_questions:
    print(f"\n{'=' * 60}")
    print(f"Question: {question}")

    result = query_with_hybrid_cache(question)
    print(f"  Source: {result['source']} | Latency: {result['latency_s']:.1f}s")
    if result["sql"]:
        print(f"  SQL: {result['sql'][:160]}...")
    if result["sql_result"]:
        print_sql_preview(result["sql_result"])

    all_results[question] = {"pass1": result}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pass 2 — L1 Warm

# COMMAND ----------

for question in demo_questions:
    print(f"\n{'=' * 60}")
    print(f"Question: {question}")

    result = query_with_hybrid_cache(question)
    print(f"  Source: {result['source']} | Score: {result['score']:.3f} | Latency: {result['latency_s']:.3f}s")
    if result["sql_result"]:
        print_sql_preview(result["sql_result"])

    all_results[question]["pass2"] = result

# COMMAND ----------

# MAGIC %md
# MAGIC ## Promote to L2 — Simulate Validation
# MAGIC
# MAGIC Production systems should promote only after explicit user validation or a
# MAGIC durable quality signal. This demo promotes the SQL produced in Pass 1.

# COMMAND ----------

for question in demo_questions:
    r = all_results[question]["pass1"]
    if r["sql"]:
        promote_to_l2(question, r["sql"], r["response"] or "", source="manual-validation")
    else:
        print(f"  Skipping L2 promotion because no SQL was generated: {question[:60]}")

print("\nSyncing L2 VS index...")
sync_vs_index_and_wait(vsc, VS_ENDPOINT, CACHE_KB_INDEX)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clear L1 — Simulate New Session

# COMMAND ----------

l1_clear_session(SESSION_ID)

for question in demo_questions:
    lookup = lakebase_cache_lookup(
        config,
        question,
        threshold=L1_THRESHOLD,
        cache_scope=SESSION_SCOPE,
        session_id=SESSION_ID,
    )
    status = "MISS" if not lookup.hit else f"HIT ({lookup.hit_type})"
    print(f"  L1 check: {status} - {question[:50]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pass 3 — L2 Warm
# MAGIC
# MAGIC L1 is empty, but L2 has validated entries. Each L2 hit is executed and then
# MAGIC promoted back into L1 for the current session.

# COMMAND ----------

for question in demo_questions:
    print(f"\n{'=' * 60}")
    print(f"Question: {question}")

    result = query_with_hybrid_cache(question)
    print(f"  Source: {result['source']} | Score: {result['score']:.3f} | Latency: {result['latency_s']:.3f}s")
    if result["sql_result"]:
        print_sql_preview(result["sql_result"])

    all_results[question]["pass3"] = result

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pass 4 — L1 Warm Again

# COMMAND ----------

for question in demo_questions:
    print(f"\n{'=' * 60}")
    print(f"Question: {question}")

    result = query_with_hybrid_cache(question)
    print(f"  Source: {result['source']} | Score: {result['score']:.3f} | Latency: {result['latency_s']:.3f}s")
    if result["sql_result"]:
        print_sql_preview(result["sql_result"])

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
        "pass1_s": r["pass1"]["latency_s"],
        "pass2_L1_s": r["pass2"]["latency_s"],
        "pass3_L2_s": r.get("pass3", {}).get("latency_s", 0),
        "pass4_L1_s": r.get("pass4", {}).get("latency_s", 0),
    })

print("\n" + "=" * 90)
print("SCENARIO 3: Hybrid Cache (L1 Lakebase + L2 Vector Search) - Results")
print("=" * 90)
print_summary_table(
    summary_rows,
    ["question", "pass1_s", "pass2_L1_s", "pass3_L2_s", "pass4_L1_s"],
)
print()
print("The cache avoids Genie latency. Every hit still executes SQL for freshness and access control.")

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

# Databricks notebook source
# DBTITLE 1,Parameters
SPACE_ID = "" # Enter space ID
MAX_CONVERSATIONS = 500
MAX_MESSAGES_PER_CONVERSATION = 500
INCLUDE_QUERY_RESULT_PAYLOAD = True
QUERY_RESULT_ATTACHMENT_LIMIT = 3
PAYLOAD_CHAR_LIMIT = 20000
REQUEST_TIMEOUT_SECONDS = 60


# COMMAND ----------

# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # Genie conversation latency report
# MAGIC
# MAGIC This notebook pulls conversation and message details from the Genie conversation API for a single Genie space, expands the response attachments, and writes a reporting table with one row per conversation/message/attachment.
# MAGIC
# MAGIC Fill in `SPACE_ID`, optionally change `DESTINATION_TABLE`, then run the notebook top to bottom. The final table includes:
# MAGIC
# MAGIC * conversation metadata
# MAGIC * user prompt text
# MAGIC * Genie text response
# MAGIC * generated SQL
# MAGIC * query-result payload snippets
# MAGIC * API timings per request
# MAGIC * derived message lifecycle timings based on Genie timestamps
# MAGIC
# MAGIC Note: exact internal step timings are not exposed for every sub-step. This notebook reports the timings the API makes observable: list latency, message detail latency, query-result fetch latency, and message lifecycle duration from `created_timestamp` to `last_updated_timestamp`.

# COMMAND ----------

# DBTITLE 1,Fetch Genie conversations
import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from pyspark.sql import functions as F

if not SPACE_ID:
    raise ValueError("Set SPACE_ID in the parameters cell before running the notebook.")

host = spark.conf.get("spark.databricks.workspaceUrl", None)
if not host:
    host = spark.conf.get("spark.databricks.api.url", None)
if not host:
    raise ValueError("Could not determine workspace host from Spark configuration.")
if not host.startswith("https://"):
    host = f"https://{host}"
base_url = host.rstrip("/")

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
api_token = ctx.apiToken().getOrElse(None)
if not api_token:
    raise ValueError("Could not obtain an API token from the notebook context.")

session = requests.Session()
session.headers.update({
    "Authorization": f"Bearer {api_token}",
    "Content-Type": "application/json",
})


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_json(response: requests.Response) -> Any:
    if not response.text:
        return None
    try:
        return response.json()
    except Exception:
        return {"raw_text": response.text}


def _to_json(value: Any, max_chars: Optional[int] = None) -> Optional[str]:
    if value is None:
        return None
    text = json.dumps(value, default=str, sort_keys=True)
    if max_chars is not None:
        return text[:max_chars]
    return text


def _build_api_call_row(
    *,
    space_id: str,
    request_type: str,
    request: Dict[str, Any],
    conversation_id: Optional[str] = None,
    message_id: Optional[str] = None,
    attachment_id: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "space_id": space_id,
        "conversation_id": conversation_id,
        "message_id": message_id,
        "attachment_id": attachment_id,
        "request_type": request_type,
        "request_path": request["path"],
        "request_started_at": request["started_at"],
        "request_ended_at": request["ended_at"],
        "http_status_code": request["http_status_code"],
        "request_elapsed_ms": request["elapsed_ms"],
        "request_params_json": _to_json(request.get("params")),
        "response_payload_json": _to_json(request.get("payload"), max_chars=PAYLOAD_CHAR_LIMIT),
    }


def api_get(
    path: str,
    params: Optional[Dict[str, Any]] = None,
    raise_for_status: bool = True,
) -> Dict[str, Any]:
    url = f"{base_url}{path}"
    started = time.perf_counter()
    started_at = _now_iso()
    response = session.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
    ended_at = _now_iso()
    elapsed_ms = int(round((time.perf_counter() - started) * 1000))
    payload = _safe_json(response)
    if raise_for_status:
        response.raise_for_status()
    return {
        "path": path,
        "params": params or {},
        "started_at": started_at,
        "ended_at": ended_at,
        "http_status_code": response.status_code,
        "elapsed_ms": elapsed_ms,
        "payload": payload,
    }


def list_paginated(
    path: str,
    array_key: str,
    limit: int,
    request_type: str,
    space_id: str,
    conversation_id: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    records: List[Dict[str, Any]] = []
    api_logs: List[Dict[str, Any]] = []
    page_token = None
    while len(records) < limit:
        params = {"page_size": min(100, limit - len(records))}
        if page_token:
            params["page_token"] = page_token
        result = api_get(path, params=params)
        api_logs.append(
            _build_api_call_row(
                space_id=space_id,
                conversation_id=conversation_id,
                request_type=request_type,
                request=result,
            )
        )
        payload = result["payload"] or {}
        batch = payload.get(array_key, [])
        if not isinstance(batch, list):
            raise ValueError(f"Expected list in payload key '{array_key}' for {path}")
        records.extend(batch)
        page_token = payload.get("next_page_token")
        if not page_token or not batch:
            break
    return records[:limit], api_logs


def unix_ts_to_timestamp_expr(column_name: str):
    """Convert an epoch-millisecond column to a timestamp."""
    return F.to_timestamp(F.from_unixtime(F.col(column_name) / 1000))


def epoch_ms_to_ms_delta(start_ts: Optional[int], end_ts: Optional[int]) -> Optional[int]:
    """Compute delta in milliseconds between two epoch-millisecond timestamps."""
    if start_ts is None or end_ts is None:
        return None
    return int(end_ts) - int(start_ts)


def extract_query_result_preview(payload: Any, max_chars: int = 4000) -> Optional[str]:
    if payload is None:
        return None
    if isinstance(payload, dict):
        for key in ["statement_response", "result", "rows", "data_array", "manifest", "schema"]:
            if key in payload:
                return _to_json({key: payload.get(key)}, max_chars=max_chars)
    return _to_json(payload, max_chars=max_chars)


conversations, api_call_rows = list_paginated(
    path=f"/api/2.0/genie/spaces/{SPACE_ID}/conversations",
    array_key="conversations",
    limit=MAX_CONVERSATIONS,
    request_type="list_conversations",
    space_id=SPACE_ID,
)
list_conversations_api_elapsed_ms_total = sum(row["request_elapsed_ms"] or 0 for row in api_call_rows)

print(f"Fetched {len(conversations)} conversations from space {SPACE_ID}")


# COMMAND ----------

# DBTITLE 1,Expand messages and attachments
report_rows: List[Dict[str, Any]] = []
query_attachment_rows: List[Dict[str, Any]] = []

for conversation in conversations:
    conversation_id = conversation["conversation_id"]
    messages, message_list_logs = list_paginated(
        path=f"/api/2.0/genie/spaces/{SPACE_ID}/conversations/{conversation_id}/messages",
        array_key="messages",
        limit=MAX_MESSAGES_PER_CONVERSATION,
        request_type="list_messages",
        space_id=SPACE_ID,
        conversation_id=conversation_id,
    )
    api_call_rows.extend(message_list_logs)
    list_messages_api_elapsed_ms_total = sum(row["request_elapsed_ms"] or 0 for row in message_list_logs)

    for message_stub in messages:
        message_id = message_stub.get("message_id") or message_stub.get("id")
        detail = api_get(
            f"/api/2.0/genie/spaces/{SPACE_ID}/conversations/{conversation_id}/messages/{message_id}"
        )
        api_call_rows.append(
            _build_api_call_row(
                space_id=SPACE_ID,
                conversation_id=conversation_id,
                message_id=message_id,
                request_type="get_message",
                request=detail,
            )
        )

        message = detail["payload"] or {}
        attachments = message.get("attachments") or []

        # Collapse all attachments into per-message fields
        genie_text_response = None
        generated_sql = None
        statement_id = None
        query_attachment_id = None
        sql_output_payload_json = None
        sql_output_preview_json = None
        query_result_api_elapsed_ms = None
        query_result_http_status_code = None

        query_attachments_for_msg: List[Dict[str, Any]] = []
        for attachment in attachments:
            attachment_query = attachment.get("query") if isinstance(attachment.get("query"), dict) else {}
            # Prefer the first non-empty text response; unwrap {"content": "..."} if needed
            if genie_text_response is None and attachment.get("text"):
                text_val = attachment["text"]
                genie_text_response = text_val.get("content") if isinstance(text_val, dict) else text_val
            # Collect ALL query attachments (Agent Mode can produce multiple per message)
            if attachment_query or attachment.get("attachment_type") == "query":
                att_id  = attachment.get("attachment_id") or attachment.get("id")
                att_sql = attachment_query.get("query") or (attachment.get("query") if isinstance(attachment.get("query"), str) else None)
                att_sid = attachment_query.get("statement_id")
                query_attachments_for_msg.append({
                    "space_id": SPACE_ID,
                    "conversation_id": conversation_id,
                    "message_id": message_id,
                    "statement_id": att_sid,
                    "generated_sql": att_sql,
                    "attachment_id": att_id,
                    "query_ordinal": len(query_attachments_for_msg),
                })
                # Backward compat: keep the first one for the report_row summary
                if query_attachment_id is None:
                    generated_sql = att_sql
                    statement_id  = att_sid
                    query_attachment_id = att_id
        query_attachment_rows.extend(query_attachments_for_msg)

        # Fetch SQL query result for the query attachment
        if INCLUDE_QUERY_RESULT_PAYLOAD and query_attachment_id:
            query_result = api_get(
                f"/api/2.0/genie/spaces/{SPACE_ID}/conversations/{conversation_id}/messages/{message_id}/attachments/{query_attachment_id}/query-result",
                raise_for_status=False,
            )
            api_call_rows.append(
                _build_api_call_row(
                    space_id=SPACE_ID,
                    conversation_id=conversation_id,
                    message_id=message_id,
                    attachment_id=query_attachment_id,
                    request_type="get_query_result",
                    request=query_result,
                )
            )
            query_result_api_elapsed_ms = query_result["elapsed_ms"]
            query_result_http_status_code = query_result["http_status_code"]
            sql_output_payload_json = _to_json(query_result["payload"], max_chars=PAYLOAD_CHAR_LIMIT)
            sql_output_preview_json = extract_query_result_preview(query_result["payload"])

        report_rows.append({
            "space_id": SPACE_ID,
            "conversation_id": conversation_id,
            "conversation_title": conversation.get("title"),
            "conversation_created_timestamp": conversation.get("created_timestamp"),
            "message_id": message.get("id", message_id),
            "message_user_id": message.get("user_id"),
            "message_status": message.get("status"),
            "message_error_json": _to_json(message.get("error"), max_chars=PAYLOAD_CHAR_LIMIT),
            "prompt_text": message.get("content"),
            "genie_text_response": genie_text_response,
            "generated_sql": generated_sql,
            "statement_id": statement_id,
            "attachments_count": len(attachments),
            "message_created_timestamp": message.get("created_timestamp"),
            "message_last_updated_timestamp": message.get("last_updated_timestamp"),
            "message_lifecycle_ms": epoch_ms_to_ms_delta(
                message.get("created_timestamp"),
                message.get("last_updated_timestamp"),
            ),
            "list_conversations_api_elapsed_ms_total": list_conversations_api_elapsed_ms_total,
            "list_messages_api_elapsed_ms_total": list_messages_api_elapsed_ms_total,
            "message_api_elapsed_ms": detail["elapsed_ms"],
            "message_api_started_at": detail["started_at"],
            "message_api_ended_at": detail["ended_at"],
            "query_result_api_elapsed_ms": query_result_api_elapsed_ms,
            "query_result_http_status_code": query_result_http_status_code,
            "sql_output_preview_json": sql_output_preview_json,
            "sql_output_payload_json": sql_output_payload_json,
            "message_payload_json": _to_json(message, max_chars=PAYLOAD_CHAR_LIMIT),
        })

print(f"Built {len(report_rows)} rows (one per message) across {len(conversations)} conversations")


# COMMAND ----------

# DBTITLE 1,Build reporting table
if not report_rows:
    raise ValueError(f"No messages were found for Genie space {SPACE_ID}.")

report_df = (
    spark.createDataFrame(__import__('pandas').DataFrame(report_rows))
    .withColumn("conversation_created_ts", unix_ts_to_timestamp_expr("conversation_created_timestamp"))
    .withColumn("message_created_ts", unix_ts_to_timestamp_expr("message_created_timestamp"))
    .withColumn("message_last_updated_ts", unix_ts_to_timestamp_expr("message_last_updated_timestamp"))
)

# Convert all timing columns from milliseconds to seconds
_timing_ms_cols = [
    "message_lifecycle_ms",
    "list_conversations_api_elapsed_ms_total",
    "list_messages_api_elapsed_ms_total",
    "message_api_elapsed_ms",
    "query_result_api_elapsed_ms",
]
for _col in _timing_ms_cols:
    if _col in report_df.columns:
        report_df = report_df.withColumn(_col.replace("_ms", "_s"), F.round(F.col(_col) / 1000.0, 3)).drop(_col)

display(report_df.orderBy(F.col("message_created_ts").desc()))


# COMMAND ----------

# DBTITLE 1,Metric definitions
# MAGIC %md
# MAGIC ## Key metric definitions
# MAGIC
# MAGIC ### Per-message metrics — same value on every row for the same message
# MAGIC
# MAGIC * **`message_lifecycle_s`** — Total wall-clock time from when the user submitted the question to when Genie finished its last update. The "total time to answer" from the user's perspective.
# MAGIC * **`sql_generation_s`** — Time spent *outside* SQL execution: LLM reasoning, SQL generation, and final answer synthesis. Computed as `message_lifecycle_s − sql_wall_clock_s`.
# MAGIC * **`sql_wall_clock_s`** — Wall-clock duration of SQL execution: `max(query_end_ts) − min(query_start_ts)` across all sub-queries. Handles parallel queries correctly — does not double-count overlapping execution time.
# MAGIC * **`sum_query_cpu_s`** — Sum of `query_total_s` across all sub-queries. Represents total CPU time; can exceed `sql_wall_clock_s` when queries ran in parallel.
# MAGIC * **`query_count`** — Number of SQL queries Genie executed to answer this message.
# MAGIC
# MAGIC ### Per-sub-query metrics — unique per row
# MAGIC
# MAGIC * **`query_total_s`** — Wall-clock time for this specific SQL query (source: `system.query.history`).
# MAGIC * **`query_compilation_s`** — Time loading metadata and optimizing the execution plan.
# MAGIC * **`query_waiting_for_compute_s`** — Time waiting for the warehouse to be provisioned.
# MAGIC * **`query_waiting_at_capacity_s`** — Time waiting in queue because the warehouse was at capacity.
# MAGIC * **`query_execution_s`** — Time actually executing this query on the warehouse.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Chat mode vs Agent mode
# MAGIC
# MAGIC **Chat mode** (standard Genie) — one question → one SQL query → one result.
# MAGIC * `query_count = 1` on every row.
# MAGIC * `sql_wall_clock_s ≈ query_total_s` — single query, no parallelism to account for.
# MAGIC * `sql_generation_s = message_lifecycle_s − query_total_s` gives a clean estimate of LLM planning time.
# MAGIC
# MAGIC **Agent mode** — one question → Genie plans autonomously and may issue multiple SQL queries (in parallel or sequentially).
# MAGIC * `query_count > 1` when multiple sub-queries were executed; each sub-query gets its own row with the same `message_lifecycle_s`, `sql_generation_s`, and `sql_wall_clock_s`.
# MAGIC * **Parallel sub-queries**: `sql_wall_clock_s ≪ sum_query_cpu_s` — queries overlapped; wall-clock time is much shorter than total CPU.
# MAGIC * **Sequential sub-queries**: `sql_wall_clock_s ≈ sum_query_cpu_s` — queries ran back-to-back; wall-clock includes the gaps between them (which are LLM inter-query planning time, already captured in `sql_generation_s`).
# MAGIC * `sql_generation_s` covers all LLM time: initial planning + synthesis after the last query + any thinking between sequential sub-queries.

# COMMAND ----------

# DBTITLE 1,Break down lifecycle: SQL generation vs query execution
if not query_attachment_rows:
    print("No query attachments found — skipping query history join.")
else:
    qa_df = spark.createDataFrame(__import__('pandas').DataFrame(query_attachment_rows))

    all_statement_ids = [
        r.statement_id
        for r in qa_df.select("statement_id").collect()
        if r.statement_id is not None
    ]

    if not all_statement_ids:
        print("No statement_ids found in query attachments.")
    else:
        _workspace_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().workspaceId().getOrElse(None)

        qh_df = (
            spark.table("system.query.history")
            .filter(F.col("workspace_id") == _workspace_id)
            .filter(F.col("statement_id").isin(all_statement_ids))
            .select(
                "statement_id",
                F.col("execution_status").alias("query_execution_status"),
                "executed_by",
                F.col("start_time").alias("query_start_ts"),
                F.col("end_time").alias("query_end_ts"),
                F.round(F.col("total_duration_ms")               / 1000.0, 3).alias("query_total_s"),
                F.round(F.col("compilation_duration_ms")         / 1000.0, 3).alias("query_compilation_s"),
                F.round(F.col("waiting_for_compute_duration_ms") / 1000.0, 3).alias("query_waiting_for_compute_s"),
                F.round(F.col("waiting_at_capacity_duration_ms") / 1000.0, 3).alias("query_waiting_at_capacity_s"),
                F.round(F.col("execution_duration_ms")           / 1000.0, 3).alias("query_execution_s"),
            )
        )

        # Message-level fields from report_df
        msg_df = report_df.select(
            "message_id", "message_created_ts", "message_user_id",
            "message_lifecycle_s", "prompt_text",
        )

        from pyspark.sql import Window as W
        _msg_win = W.partitionBy("message_id")

        breakdown_df = (
            qa_df
            .join(qh_df, on="statement_id", how="left")
            .join(msg_df, on="message_id", how="left")
            # Per-message window aggregates using real wall-clock timestamps
            .withColumn("query_count",          F.count("*").over(_msg_win))
            .withColumn("sum_query_cpu_s",       F.round(F.sum("query_total_s").over(_msg_win), 3))
            .withColumn("_first_query_start_ts", F.min("query_start_ts").over(_msg_win))
            .withColumn("_last_query_end_ts",    F.max("query_end_ts").over(_msg_win))
            # sql_wall_clock_s = max(end) - min(start): correct for both parallel and sequential
            .withColumn(
                "sql_wall_clock_s",
                F.round(
                    (F.unix_timestamp("_last_query_end_ts") - F.unix_timestamp("_first_query_start_ts")),
                    3,
                ),
            )
            # sql_generation_s = everything outside SQL execution:
            # initial LLM planning + synthesis + inter-query planning (if sequential)
            .withColumn(
                "sql_generation_s",
                F.round(F.col("message_lifecycle_s") - F.col("sql_wall_clock_s"), 3),
            )
            .select(
                "message_created_ts",
                F.coalesce(F.col("executed_by"), F.col("message_user_id").cast("string")).alias("submitted_by"),
                "prompt_text",
                "query_count",
                "query_ordinal",
                "generated_sql",
                "message_lifecycle_s",
                "sql_generation_s",
                "sql_wall_clock_s",
                "sum_query_cpu_s",
                "query_start_ts",
                "query_end_ts",
                "query_total_s",
                "query_compilation_s",
                "query_waiting_for_compute_s",
                "query_waiting_at_capacity_s",
                "query_execution_s",
                "query_execution_status",
                "statement_id",
                "message_id",
            )
            .drop("_first_query_start_ts", "_last_query_end_ts")
            .orderBy(F.col("message_created_ts").desc(), F.col("query_ordinal"))
        )

        display(breakdown_df)

# COMMAND ----------

# DBTITLE 1,Summarize timings
summary_df = (
    report_df
    .groupBy("message_status")
    .agg(
        F.countDistinct("conversation_id").alias("conversation_count"),
        F.countDistinct("message_id").alias("message_count"),
        F.avg("list_messages_api_elapsed_s_total").alias("avg_list_messages_api_elapsed_s_total"),
        F.avg("message_api_elapsed_s").alias("avg_message_api_elapsed_s"),
        F.expr("percentile_approx(message_api_elapsed_s, 0.5)").alias("p50_message_api_elapsed_s"),
        F.expr("percentile_approx(message_api_elapsed_s, 0.95)").alias("p95_message_api_elapsed_s"),
        F.avg("query_result_api_elapsed_s").alias("avg_query_result_api_elapsed_s"),
        F.avg("message_lifecycle_s").alias("avg_message_lifecycle_s"),
        F.expr("percentile_approx(message_lifecycle_s, 0.95)").alias("p95_message_lifecycle_s"),
    )
    .orderBy(F.col("message_count").desc())
)

display(summary_df)

# Databricks notebook source
# MAGIC %md
# MAGIC # Genie API Tracing Demo
# MAGIC
# MAGIC Trace every step of a Genie query — from metadata fetching through SQL generation to result delivery — using the REST API and MLflow Tracing.
# MAGIC
# MAGIC ## Note on timing
# MAGIC
# MAGIC Step durations (`+Xs`) are computed from server-side `last_updated_timestamp` values, so they reflect actual Genie processing time. The elapsed time (`t=`) is measured client-side and depends on the polling interval, which defaults to 1 second. (GET polls do not count toward QPM limits.)
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC 1. A **Genie Space ID** — paste it into the `GENIE_SPACE_ID` config value below
# MAGIC 2. Databricks workspace access (auto-detected on clusters, or set `DATABRICKS_HOST` / `DATABRICKS_TOKEN` env vars)
# MAGIC 3. An MLflow experiment — defaults to `/Shared/genie-latency-tracing`

# COMMAND ----------

# MAGIC %pip install mlflow[databricks]==3.10.0 requests==2.33.1 -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
import os
import time

import mlflow
import requests

GENIE_SPACE_ID = ""  # Required: paste your Genie Space ID
QUESTION = "What were the top 10 offers by redemption rate last month?"
EXPERIMENT = "/Shared/genie-latency-tracing"
POLL_INTERVAL_SECONDS = 1.0
TIMEOUT_SECONDS = 300
LOG_FULL_RESPONSES = False

# Auto-detect Databricks environment
try:
    HOST = spark.conf.get("spark.databricks.workspaceUrl")
    TOKEN = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .apiToken()
        .get()
    )
except Exception:
    HOST = os.getenv("DATABRICKS_HOST", "")
    TOKEN = os.getenv("DATABRICKS_TOKEN", "")

mlflow.set_experiment(EXPERIMENT)
print(f"Space: {GENIE_SPACE_ID} | Host: {HOST}")

# COMMAND ----------


TERMINAL_STATUSES = {
    "COMPLETED",
    "FAILED",
    "CANCELLED",
    "QUERY_RESULT_EXPIRED",
}
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
FAIL_FAST_STATUS_CODES = {400, 401, 403, 404}


def _short_response(resp, limit: int = 500) -> str:
    """Return a short response body for trace attributes and notebook output."""
    return (getattr(resp, "text", "") or "")[:limit]


def _new_result(question: str) -> dict:
    """Create a stable result shape so every exit path is easy to inspect."""
    return {
        "question": question,
        "sql": None,
        "description": None,
        "columns": [],
        "rows": 0,
        "sample": [],
        "error": None,
        "duration": 0.0,
        "conversation_id": None,
        "message_id": None,
        "final_status": None,
    }


def _finish_result(
    result: dict,
    start: float,
    error: str = None,
    final_status: str = None,
):
    """Update common result fields before returning."""
    if error:
        result["error"] = error
    if final_status:
        result["final_status"] = final_status
    result["duration"] = round(time.time() - start, 2)
    return result


def _normalize_server_timestamp(timestamp):
    """Convert epoch seconds or milliseconds to epoch seconds; return None if unknown."""
    if timestamp is None:
        return None
    try:
        ts = float(timestamp)
    except (TypeError, ValueError):
        return None
    if ts > 10_000_000_000:
        return ts / 1000
    return ts


def _extract_query_attachment(message: dict):
    """Return (sql, description, att_id) from the first query attachment, or (None, None, None)."""
    for att in message.get("attachments") or []:
        if att.get("query"):
            return (
                att["query"].get("query"),
                att["query"].get("description"),
                att.get("attachment_id") or att.get("id"),
            )
    return None, None, None


def _set_span_error(span, status_code, body):
    span.set_attributes(
        {
            "error": body,
            "http_status_code": status_code,
        }
    )


def ask_genie_with_full_trace(
    question: str,
    space_id: str,
    host: str,
    token: str,
    poll_interval_seconds: float = POLL_INTERVAL_SECONDS,
    timeout_seconds: int = TIMEOUT_SECONDS,
    log_full_responses: bool = LOG_FULL_RESPONSES,
):
    """
    Ask Genie using REST API with full MLflow tracing.
    State changes are traced without full payloads unless log_full_responses=True.
    """
    start = time.time()
    result = _new_result(question)

    with mlflow.start_span(name="genie_query") as root:
        root.set_inputs({"question": question, "space_id": space_id})

        missing_config = []
        if not space_id or not space_id.strip():
            missing_config.append("GENIE_SPACE_ID")
        if not host or not host.strip():
            missing_config.append("HOST")
        if not token or not token.strip():
            missing_config.append("TOKEN")

        if missing_config:
            error = f"Missing required configuration: {', '.join(missing_config)}"
            root.set_attributes({"error": error, "final_status": "CONFIG_ERROR"})
            _finish_result(result, start, error=error, final_status="CONFIG_ERROR")
            root.set_outputs(result)
            return result

        host_clean = host.rstrip("/").removeprefix("https://").removeprefix("http://")
        base_url = f"https://{host_clean}/api/2.0/genie/spaces/{space_id}"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        with mlflow.start_span(name="1_start_conversation") as span:
            span.set_inputs({"question": question})

            resp = requests.post(
                f"{base_url}/start-conversation",
                headers=headers,
                json={"content": question},
            )

            if resp.status_code != 200:
                body = _short_response(resp)
                _set_span_error(span, resp.status_code, body)
                _finish_result(
                    result,
                    start,
                    error=f"start-conversation failed ({resp.status_code}): {body}",
                    final_status="START_CONVERSATION_FAILED",
                )
                root.set_outputs(result)
                return result

            data = resp.json()
            conv_id = data.get("conversation_id") or data.get("conversation", {}).get(
                "id"
            )
            msg_id = data.get("message_id") or data.get("message", {}).get("id")

            if not conv_id or not msg_id:
                error = f"Could not extract conversation/message IDs: {data}"
                span.set_attributes({"error": error})
                _finish_result(
                    result,
                    start,
                    error=error,
                    final_status="INVALID_START_RESPONSE",
                )
                root.set_outputs(result)
                return result

            result["conversation_id"] = conv_id
            result["message_id"] = msg_id
            outputs = {"conversation_id": conv_id, "message_id": msg_id}
            if log_full_responses:
                outputs["response"] = data
            span.set_outputs(outputs)

        with mlflow.start_span(name="2_poll_states") as poll_span:
            last_status = None
            status = None
            message = None
            last_server_ts = None
            pending_state = None
            poll_start = time.time()
            retry_sleep_seconds = poll_interval_seconds

            while (time.time() - poll_start) < timeout_seconds:
                iter_start = time.time()
                resp = requests.get(
                    f"{base_url}/conversations/{conv_id}/messages/{msg_id}",
                    headers=headers,
                )
                if resp.status_code != 200:
                    body = _short_response(resp)
                    if resp.status_code in RETRYABLE_STATUS_CODES:
                        poll_span.set_attributes(
                            {
                                "last_retryable_http_status_code": resp.status_code,
                                "last_retryable_response": body,
                            }
                        )
                        time.sleep(retry_sleep_seconds)
                        retry_sleep_seconds = min(retry_sleep_seconds * 2, 60)
                        continue

                    if resp.status_code in FAIL_FAST_STATUS_CODES:
                        error = (
                            f"poll message failed ({resp.status_code}): {body}"
                        )
                        _set_span_error(poll_span, resp.status_code, body)
                        _finish_result(
                            result,
                            start,
                            error=error,
                            final_status="POLL_FAILED",
                        )
                        root.set_outputs(result)
                        return result

                    error = f"unexpected poll response ({resp.status_code}): {body}"
                    _set_span_error(poll_span, resp.status_code, body)
                    _finish_result(
                        result,
                        start,
                        error=error,
                        final_status="POLL_FAILED",
                    )
                    root.set_outputs(result)
                    return result

                retry_sleep_seconds = poll_interval_seconds
                message = resp.json()
                status = message.get("status", "UNKNOWN")
                result["final_status"] = status

                if status != last_status:
                    with mlflow.start_span(name=f"state_{status}") as state_span:
                        raw_server_ts = message.get("last_updated_timestamp")
                        server_ts = _normalize_server_timestamp(raw_server_ts)
                        elapsed = round(time.time() - start, 2)
                        prev_duration = (
                            round(server_ts - last_server_ts, 2)
                            if (server_ts and last_server_ts)
                            else None
                        )

                        if pending_state:
                            prev_status, prev_elapsed = pending_state
                            dur_str = (
                                f"+{prev_duration:.1f}s"
                                if prev_duration is not None
                                else ""
                            )
                            print(
                                f"  [t={prev_elapsed:5.1f}s {dur_str:>7s}] {prev_status}"
                            )

                        last_server_ts = server_ts
                        sql, _, _ = _extract_query_attachment(message)

                        state_span.set_inputs(
                            {
                                "elapsed_sec": elapsed,
                                "server_timestamp": raw_server_ts,
                                "normalized_server_timestamp": server_ts,
                            }
                        )
                        attrs = {
                            "status": status,
                            "has_sql": sql is not None,
                            "attachment_count": len(
                                message.get("attachments") or []
                            ),
                        }
                        if prev_duration is not None:
                            attrs["prev_state_duration_sec"] = prev_duration
                        state_span.set_attributes(attrs)
                        if sql:
                            state_span.set_attributes({"generated_sql": sql})
                        if message.get("error"):
                            state_span.set_attributes(
                                {"error": json.dumps(message["error"])}
                            )

                        outputs = {"status": status, "generated_sql": sql}
                        if log_full_responses:
                            outputs["full_response"] = message
                        state_span.set_outputs(outputs)

                    pending_state = (status, elapsed)
                    last_status = status

                if status in TERMINAL_STATUSES:
                    if pending_state:
                        print(
                            f"  [t={pending_state[1]:5.1f}s        ] {pending_state[0]}"
                        )
                    break
                time.sleep(max(0, poll_interval_seconds - (time.time() - iter_start)))
            else:
                error = f"Timed out after {timeout_seconds}s waiting for Genie message"
                poll_span.set_attributes({"error": error})
                _finish_result(result, start, error=error, final_status="TIMEOUT")
                root.set_outputs(result)
                return result

            poll_span.set_outputs(
                {"final_status": status, "total_time": round(time.time() - start, 2)}
            )

        if message and message.get("status") == "COMPLETED":
            with mlflow.start_span(name="3_extract_results") as extract_span:
                sql, description, att_id = _extract_query_attachment(message)
                if sql:
                    result["sql"] = sql
                    result["description"] = description

                    extract_span.set_outputs(
                        {
                            "generated_sql": sql,
                            "description": description,
                        }
                    )

                    if att_id:
                        with mlflow.start_span(name="3a_fetch_data") as data_span:
                            resp = requests.get(
                                f"{base_url}/conversations/{conv_id}/messages/{msg_id}/query-result/{att_id}",
                                headers=headers,
                            )
                            if resp.status_code == 200:
                                stmt = resp.json().get("statement_response", {})
                                cols = [
                                    c["name"]
                                    for c in stmt.get("manifest", {})
                                    .get("schema", {})
                                    .get("columns", [])
                                ]
                                rows = stmt.get("result", {}).get("data_array", [])
                                result["columns"] = cols
                                result["rows"] = len(rows)
                                result["sample"] = [
                                    dict(zip(cols, r)) for r in rows[:3]
                                ]

                                outputs = {"columns": cols, "row_count": len(rows)}
                                if log_full_responses:
                                    outputs["sample"] = result["sample"]
                                data_span.set_outputs(outputs)
                            else:
                                body = _short_response(resp)
                                _set_span_error(data_span, resp.status_code, body)
                                result["error"] = (
                                    f"query-result failed ({resp.status_code}): {body}"
                                )
                                result["final_status"] = "QUERY_RESULT_FAILED"
                    else:
                        result["error"] = (
                            "Completed message did not include a query attachment ID"
                        )
                        result["final_status"] = "MISSING_QUERY_ATTACHMENT_ID"
                        extract_span.set_attributes({"error": result["error"]})
                else:
                    result["error"] = "Completed message did not include a query attachment"
                    result["final_status"] = "MISSING_QUERY_ATTACHMENT"
                    extract_span.set_attributes({"error": result["error"]})
        elif result["final_status"] in {"FAILED", "CANCELLED", "QUERY_RESULT_EXPIRED"}:
            result["error"] = f"Genie message ended with status {result['final_status']}"

        result["duration"] = round(time.time() - start, 2)
        root.set_outputs(
            {
                "success": result["sql"] is not None and result["error"] is None,
                "generated_sql": result["sql"],
                "row_count": result["rows"],
                "duration_sec": result["duration"],
                "final_status": result["final_status"],
                "error": result["error"],
            }
        )

    return result


# COMMAND ----------

if __name__ == "__main__" or os.getenv("DATABRICKS_RUNTIME_VERSION"):
    with mlflow.start_run(run_name="genie_full_visibility"):

        print(f"Question: {QUESTION}\n")

        result = ask_genie_with_full_trace(QUESTION, GENIE_SPACE_ID, HOST, TOKEN)

        print(f"\n{'='*60}")
        print(f"Duration: {result['duration']}s")
        if result.get("error"):
            print(f"Error: {result['error']}")
        print(f"\nGenerated SQL:\n{'-'*40}\n{result.get('sql') or 'N/A'}\n{'-'*40}")
        print(f"\nResults: {result['rows']} rows")
        if result.get("sample"):
            for i, row in enumerate(result["sample"]):
                print(f"  {i+1}. {row}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What You'll See in MLflow Traces
# MAGIC
# MAGIC Open the **Experiments** page in the Databricks sidebar, select the `/Shared/genie-latency-tracing` experiment, then click into the run to view traces.
# MAGIC
# MAGIC ```
# MAGIC genie_query
# MAGIC ├── 1_start_conversation
# MAGIC │     └── Outputs: conversation_id, message_id
# MAGIC │
# MAGIC ├── 2_poll_states
# MAGIC │     ├── state_SUBMITTED            → Message has been submitted
# MAGIC │     ├── state_FETCHING_METADATA    → Fetching metadata from the data sources
# MAGIC │     ├── state_FILTERING_CONTEXT    → Running smart context to determine relevant context
# MAGIC │     ├── state_ASKING_AI            → Waiting for the LLM to respond
# MAGIC │     ├── state_PENDING_WAREHOUSE    → Waiting for warehouse before SQL can execute
# MAGIC │     ├── state_EXECUTING_QUERY      → Executing the generated SQL query
# MAGIC │     └── state_COMPLETED            → Results are in the attachments field
# MAGIC │
# MAGIC └── 3_extract_results
# MAGIC       ├── Outputs: generated_sql, description
# MAGIC       └── 3a_fetch_data
# MAGIC             └── Outputs: columns, row_count
# MAGIC ```
# MAGIC
# MAGIC Set `LOG_FULL_RESPONSES = True` to include complete API payloads and sample result rows in MLflow traces. Keep the default `False` for spaces that may return sensitive data.

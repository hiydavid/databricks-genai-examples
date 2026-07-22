# Databricks notebook source
# MAGIC %md
# MAGIC # Genie API Tracing Demo
# MAGIC
# MAGIC Trace every step of a Genie query — from metadata fetching through SQL generation to result delivery — using the REST API and MLflow Tracing.
# MAGIC
# MAGIC Supports both Genie modes via the `MODE` config below:
# MAGIC
# MAGIC - **`chat`** — the Genie Conversation API (GA). Polls message status (`FETCHING_METADATA` → `ASKING_AI` → `EXECUTING_QUERY` → ...).
# MAGIC - **`agent`** — the [Agent mode API](https://docs.databricks.com/aws/en/genie-agents/api) (Beta). Starts a response, then either streams SSE events (`AGENT_TRANSPORT = "stream"`, default — true server-pushed timing) or polls conversation items (`"poll"`): `reasoning` → `function_call` → `function_call_output` → ... → final report. Agent mode runs multi-step research, so expect minutes, not seconds.
# MAGIC
# MAGIC ## Note on timing
# MAGIC
# MAGIC **Chat mode:** step durations (`+Xs`) are computed from server-side `last_updated_timestamp` values, so they reflect actual Genie processing time. **Agent mode (stream):** events are server-pushed, so `+Xs` is the true gap since the previous event (e.g., `function_call_output` added → done ≈ query execution time). **Agent mode (poll):** the items API exposes no per-item server timestamps and may surface items only once finalized, so step durations are coarse poll-observed deltas. In all modes the elapsed time (`t=`) is client-side; the poll interval defaults to 1 second. (GET polls do not count toward QPM limits.)
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC 1. A **Genie Space ID** — paste it into the `GENIE_SPACE_ID` config value below. For `agent` mode, the same 32-character ID is used as the `agent_id`.
# MAGIC 2. Databricks workspace access (auto-detected on clusters, or set `DATABRICKS_HOST` / `DATABRICKS_TOKEN` env vars)
# MAGIC 3. An MLflow experiment — defaults to `/Shared/genie-latency-tracing`
# MAGIC 4. For `agent` mode only: Unity Catalog enabled, partner-powered AI features enabled, and — because the Agent mode API is in **Beta** — a workspace admin must first enable the **Genie Agent Mode API for Genie Agents** feature in the workspace **Previews** portal (see [requirements](https://docs.databricks.com/aws/en/genie-agents/api#requirements))

# COMMAND ----------

# MAGIC %pip install mlflow[databricks]==3.10.0 requests==2.33.1 -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
import os
import time

import mlflow
import requests

MODE = "chat"  # "chat" (Conversation API) or "agent" (Agent mode API, Beta)
GENIE_SPACE_ID = ""  # Required: paste your Genie Space ID (same ID is the agent_id in agent mode)
QUESTION = "What were the top 10 offers by redemption rate last month?"
EXPERIMENT = "/Shared/genie-latency-tracing"
POLL_INTERVAL_SECONDS = 1.0
TIMEOUT_SECONDS = 300  # chat mode: stop polling after this many seconds
AGENT_TIMEOUT_SECONDS = 900  # agent mode: multi-step research can take several minutes
AGENT_TRANSPORT = "stream"  # agent mode: "stream" (SSE, true event timing) or "poll" (items endpoint, coarser)
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
print(f"Mode: {MODE} | Space: {GENIE_SPACE_ID} | Host: {HOST}")

# COMMAND ----------


TERMINAL_STATUSES = {
    "COMPLETED",
    "FAILED",
    "CANCELLED",
    "QUERY_RESULT_EXPIRED",
}
# Agent mode: top-level `status` on the conversation items response
AGENT_TERMINAL_STATUSES = {"completed", "failed"}
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
FAIL_FAST_STATUS_CODES = {400, 401, 403, 404, 409}


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
        # Agent mode only (empty in chat mode)
        "sql_queries": [],
        "report": None,
        "tables": [],
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

# MAGIC %md
# MAGIC ## Agent mode (Beta)
# MAGIC
# MAGIC The [Agent mode API](https://docs.databricks.com/aws/en/genie-agents/api) differs from chat mode in three ways that matter for tracing:
# MAGIC
# MAGIC 1. **Invoke** — `POST /api/2.0/genie/agents/{agent_id}/responses` opens a Server-Sent Events stream. The first event (`response.created`) carries the `conversation_id`; we close the stream right after reading it.
# MAGIC 2. **Poll** — `GET .../conversations/{conversation_id}/items` returns all output items plus a top-level `status` (`in_progress` → `completed` / `failed`). The granular latency signal is item transitions: `reasoning` → `function_call` (SQL) → `function_call_output` (results) → ... → assistant `message` (final report).
# MAGIC 3. **Timing** — items carry no server-side timestamps. With `AGENT_TRANSPORT = "stream"` the server pushes events in real time (accurate); with `"poll"` the durations below are poll-observed deltas (quantized by the poll interval, and items may only surface once finalized).
# MAGIC
# MAGIC ### Poll vs stream (`AGENT_TRANSPORT`)
# MAGIC
# MAGIC - **`"stream"` (default)** — holds the SSE stream open and traces each event as the server pushes it: `response.output_item.added` (item appears — a `function_call_output` arriving `in_progress` means the query started), `.updated` (content changed — e.g., results arrived), `.done` (item finalized). True sub-second transition timing, but keeps an HTTP connection open for the run's duration (server-side stream limit: 90 minutes).
# MAGIC - **`"poll"`** — reads only the first SSE event, then polls the items endpoint. More robust for very long runs or flaky networks, but items may surface only once finalized, so fast transitions get batched and query execution can hide inside a reasoning window.

# COMMAND ----------


def _missing_config_error(space_id, host, token):
    """Return an error string if required config is missing, else None."""
    missing = []
    if not space_id or not space_id.strip():
        missing.append("GENIE_SPACE_ID")
    if not host or not host.strip():
        missing.append("HOST")
    if not token or not token.strip():
        missing.append("TOKEN")
    if missing:
        return f"Missing required configuration: {', '.join(missing)}"
    return None


def _iter_sse_events(resp):
    """Yield (event_name, data_dict) for each Server-Sent Event in an open stream."""
    event = None
    data_lines = []
    for line in resp.iter_lines(decode_unicode=True):
        if line is None:
            continue
        if line == "":
            # blank line = event boundary
            if data_lines:
                payload = "\n".join(data_lines)
                data_lines = []
                try:
                    yield event, json.loads(payload)
                except ValueError:
                    yield event, None
                event = None
            continue
        if line.startswith(":"):
            continue  # SSE comment / keep-alive
        if line.startswith("event:"):
            event = line[len("event:") :].strip()
        elif line.startswith("data:"):
            data_lines.append(line[len("data:") :].lstrip())
    if data_lines:
        try:
            yield event, json.loads("\n".join(data_lines))
        except ValueError:
            yield event, None


def _read_first_sse_event(resp):
    """Read the first Server-Sent Event from an open stream. Returns (event_name, data_dict)."""
    return next(_iter_sse_events(resp), (None, None))


def _fetch_all_agent_items(base_url, headers, conv_id):
    """Fetch all conversation items across pages.

    Returns (200, {"data": [...], "status": ...}) or (http_status, error_text).
    """
    items = []
    after = None
    top_status = None
    while True:
        params = {"limit": 100, "order": "asc"}
        if after:
            params["after"] = after
        resp = requests.get(
            f"{base_url}/conversations/{conv_id}/items",
            headers=headers,
            params=params,
        )
        if resp.status_code != 200:
            return resp.status_code, _short_response(resp)
        page = resp.json()
        items.extend(page.get("data") or [])
        top_status = page.get("status")
        after = page.get("last_id")
        if not page.get("has_more") or not after:
            break
    return 200, {"data": items, "status": top_status}


def _parse_function_call_arguments(item):
    """Parse a function_call item's arguments JSON. Returns (title, sql)."""
    try:
        args = json.loads(item.get("arguments") or "{}")
    except ValueError:
        return None, None
    return args.get("title"), args.get("sql")


def _populate_agent_result(items, result):
    """Populate result fields from agent-mode output items (shared by poll and stream).

    Fills sql_queries, report, tables, plus the headline sql/columns/rows/sample
    fields (from the final table chunk) for a stable cross-mode result shape.
    """
    sql_queries = []
    report_chunks = []
    tables = []
    for item in items:
        if item.get("type") == "function_call":
            title, sql = _parse_function_call_arguments(item)
            if sql:
                sql_queries.append({"title": title, "sql": sql})
        elif item.get("type") == "message" and item.get("role") == "assistant":
            for chunk in item.get("content") or []:
                if chunk.get("type") != "output_text":
                    continue
                meta = chunk.get("metadata")
                if meta and meta.get("columns"):
                    tables.append(
                        {
                            "columns": [c.get("name") for c in meta["columns"]],
                            "preview_rows": meta.get("preview_rows") or [],
                            "total_row_count": meta.get("total_row_count"),
                            "sql": meta.get("sql"),
                        }
                    )
                elif chunk.get("text"):
                    report_chunks.append(chunk["text"])

    result["sql_queries"] = sql_queries
    result["report"] = "\n".join(report_chunks) or None
    result["tables"] = tables
    if sql_queries:
        result["sql"] = sql_queries[-1]["sql"]
    if tables:
        last_table = tables[-1]
        result["columns"] = last_table["columns"]
        result["rows"] = last_table["total_row_count"] or len(
            last_table["preview_rows"]
        )
        result["sample"] = [
            dict(zip(last_table["columns"], r)) for r in last_table["preview_rows"][:3]
        ]
    if not sql_queries and not tables and not result["report"]:
        result["error"] = "Completed response contained no queries or report"
        result["final_status"] = "MISSING_AGENT_OUTPUT"


def ask_genie_agent_with_full_trace(
    question: str,
    agent_id: str,
    host: str,
    token: str,
    poll_interval_seconds: float = POLL_INTERVAL_SECONDS,
    timeout_seconds: int = AGENT_TIMEOUT_SECONDS,
    log_full_responses: bool = LOG_FULL_RESPONSES,
):
    """
    Ask Genie in Agent mode (Beta) via the Agent mode API with full MLflow tracing.
    Reads the first SSE event to obtain the conversation ID, then polls the
    conversation items endpoint. Step timings are client-side, poll-observed deltas.
    """
    start = time.time()
    result = _new_result(question)

    with mlflow.start_span(name="genie_query") as root:
        root.set_inputs({"question": question, "agent_id": agent_id, "mode": "agent"})
        root.set_attributes({"mode": "agent", "transport": "poll"})

        config_error = _missing_config_error(agent_id, host, token)
        if config_error:
            root.set_attributes({"error": config_error, "final_status": "CONFIG_ERROR"})
            _finish_result(result, start, error=config_error, final_status="CONFIG_ERROR")
            root.set_outputs(result)
            return result

        host_clean = host.rstrip("/").removeprefix("https://").removeprefix("http://")
        base_url = f"https://{host_clean}/api/2.0/genie/agents/{agent_id}"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        with mlflow.start_span(name="1_create_response") as span:
            span.set_inputs({"question": question})

            try:
                resp = requests.post(
                    f"{base_url}/responses",
                    headers={**headers, "Accept": "text/event-stream"},
                    json={
                        "input": [
                            {
                                "type": "message",
                                "role": "user",
                                "content": [{"type": "input_text", "text": question}],
                            }
                        ]
                    },
                    stream=True,
                    timeout=60,
                )
            except requests.RequestException as e:
                error = f"create-response request failed: {e}"
                span.set_attributes({"error": error})
                _finish_result(
                    result, start, error=error, final_status="CREATE_RESPONSE_FAILED"
                )
                root.set_outputs(result)
                return result

            if resp.status_code != 200:
                body = _short_response(resp)
                resp.close()
                error = f"create-response failed ({resp.status_code}): {body}"
                if "FEATURE_DISABLED" in body:
                    error += (
                        " — a workspace admin must enable the"
                        " 'Genie Agent Mode API for Genie Agents' feature"
                        " in the workspace Previews portal"
                    )
                _set_span_error(span, resp.status_code, body)
                _finish_result(
                    result, start, error=error, final_status="CREATE_RESPONSE_FAILED"
                )
                root.set_outputs(result)
                return result

            event, data = _read_first_sse_event(resp)
            resp.close()

            response_obj = (data or {}).get("response") or {}
            conv_id = response_obj.get("conversation_id")
            response_id = response_obj.get("id")

            if event != "response.created" or not conv_id:
                error = f"Could not extract conversation ID from first SSE event (event={event}): {data}"
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
            span.set_attributes({"response_created_at": response_obj.get("created_at")})
            outputs = {"conversation_id": conv_id, "response_id": response_id}
            if log_full_responses:
                outputs["response_created_event"] = data
            span.set_outputs(outputs)

        items = []
        top_status = None
        with mlflow.start_span(name="2_poll_states") as poll_span:
            seen = {}  # item_id -> last observed status
            labels = {}  # item_id -> label like "reasoning_1"
            counters = {}  # item_type -> occurrence count
            last_transition_ts = None  # client-side time of last observed transition
            pending = None  # (label, status, elapsed) awaiting print
            poll_start = time.time()
            retry_sleep_seconds = poll_interval_seconds

            while (time.time() - poll_start) < timeout_seconds:
                iter_start = time.time()
                http_status, body = _fetch_all_agent_items(base_url, headers, conv_id)
                if http_status != 200:
                    if http_status in RETRYABLE_STATUS_CODES:
                        poll_span.set_attributes(
                            {
                                "last_retryable_http_status_code": http_status,
                                "last_retryable_response": body,
                            }
                        )
                        time.sleep(retry_sleep_seconds)
                        retry_sleep_seconds = min(retry_sleep_seconds * 2, 60)
                        continue

                    error = f"poll items failed ({http_status}): {body}"
                    _set_span_error(poll_span, http_status, body)
                    _finish_result(
                        result,
                        start,
                        error=error,
                        final_status="POLL_FAILED",
                    )
                    root.set_outputs(result)
                    return result

                retry_sleep_seconds = poll_interval_seconds
                items = body["data"]
                top_status = body.get("status") or "unknown"
                result["final_status"] = top_status

                # Detect new items and item status flips, in chronological order
                transitions = []
                for item in items:
                    iid = item.get("id")
                    if iid is None:
                        continue
                    istatus = item.get("status") or "unknown"
                    if iid not in seen:
                        itype = item.get("type") or "unknown"
                        counters[itype] = counters.get(itype, 0) + 1
                        labels[iid] = f"{itype}_{counters[itype]}"
                        seen[iid] = istatus
                        transitions.append((item, labels[iid], istatus, True))
                    elif seen[iid] != istatus:
                        seen[iid] = istatus
                        transitions.append((item, labels[iid], istatus, False))

                for item, label, istatus, is_new in transitions:
                    now = time.time()
                    elapsed = round(now - start, 2)
                    prev_duration = (
                        round(now - last_transition_ts, 2)
                        if last_transition_ts
                        else None
                    )

                    if pending:
                        dur_str = (
                            f"+{prev_duration:.1f}s"
                            if prev_duration is not None
                            else ""
                        )
                        print(
                            f"  [t={pending[2]:5.1f}s {dur_str:>7s}] {pending[0]} ({pending[1]})"
                        )
                    last_transition_ts = now

                    iid = item.get("id")
                    itype = item.get("type")
                    sql = None
                    span_name = f"state_{label}" if is_new else f"state_{label}_{istatus}"
                    with mlflow.start_span(name=span_name) as state_span:
                        state_span.set_inputs(
                            {
                                "elapsed_sec": elapsed,
                                "item_id": iid,
                                "observed_status": istatus,
                            }
                        )
                        attrs = {
                            "item_id": iid,
                            "item_type": itype,
                            "item_status": istatus,
                            "conversation_status": top_status,
                            "item_count": len(items),
                        }
                        if prev_duration is not None:
                            attrs["prev_transition_duration_sec"] = prev_duration
                        if itype == "function_call":
                            title, sql = _parse_function_call_arguments(item)
                            if title:
                                attrs["query_title"] = title
                            if sql:
                                attrs["generated_sql"] = sql
                        if itype == "message" and item.get("role"):
                            attrs["message_role"] = item["role"]
                        state_span.set_attributes(attrs)

                        outputs = {"label": label, "item_status": istatus}
                        if sql:
                            outputs["generated_sql"] = sql
                        if log_full_responses:
                            outputs["item"] = item
                        state_span.set_outputs(outputs)

                    pending = (label, istatus, elapsed)

                if top_status in AGENT_TERMINAL_STATUSES:
                    if pending:
                        print(
                            f"  [t={pending[2]:5.1f}s        ] {pending[0]} ({pending[1]})"
                        )
                    break
                time.sleep(max(0, poll_interval_seconds - (time.time() - iter_start)))
            else:
                error = f"Timed out after {timeout_seconds}s waiting for Agent mode response"
                poll_span.set_attributes({"error": error})
                _finish_result(result, start, error=error, final_status="TIMEOUT")
                root.set_outputs(result)
                return result

            poll_span.set_outputs(
                {
                    "final_status": top_status,
                    "item_count": len(items),
                    "total_time": round(time.time() - start, 2),
                }
            )

        if top_status == "completed":
            with mlflow.start_span(name="3_extract_results") as extract_span:
                _populate_agent_result(items, result)
                outputs = {
                    "num_sql_queries": len(result["sql_queries"]),
                    "num_tables": len(result["tables"]),
                    "report_chars": len(result["report"] or ""),
                }
                if log_full_responses:
                    outputs["sql_queries"] = result["sql_queries"]
                    outputs["report"] = result["report"]
                extract_span.set_outputs(outputs)
                if result["error"]:
                    extract_span.set_attributes({"error": result["error"]})
        elif top_status == "failed":
            # The structured error rides on the SSE response.failed event, which we
            # don't read; the items list carries a system error message instead.
            system_text = None
            for item in items:
                if item.get("type") == "message" and item.get("role") == "system":
                    chunks = [
                        c.get("text")
                        for c in (item.get("content") or [])
                        if c.get("text")
                    ]
                    system_text = "\n".join(chunks) or system_text
            result["error"] = "Agent response failed" + (
                f": {system_text}" if system_text else ""
            )

        result["duration"] = round(time.time() - start, 2)
        root.set_outputs(
            {
                "success": result["final_status"] == "completed"
                and result["error"] is None,
                "num_sql_queries": len(result["sql_queries"]),
                "num_tables": len(result["tables"]),
                "report_chars": len(result["report"] or ""),
                "duration_sec": result["duration"],
                "final_status": result["final_status"],
                "error": result["error"],
            }
        )

    return result


def ask_genie_agent_stream_with_full_trace(
    question: str,
    agent_id: str,
    host: str,
    token: str,
    timeout_seconds: int = AGENT_TIMEOUT_SECONDS,
    log_full_responses: bool = LOG_FULL_RESPONSES,
):
    """
    Ask Genie in Agent mode (Beta) via the Agent mode API SSE stream with full MLflow tracing.
    Events are traced as the server pushes them, so step timings are true transitions
    (sub-second, not poll-quantized). Holds the HTTP connection open for the run's
    duration (server-side stream limit: 90 minutes).
    """
    start = time.time()
    result = _new_result(question)

    with mlflow.start_span(name="genie_query") as root:
        root.set_inputs({"question": question, "agent_id": agent_id, "mode": "agent"})
        root.set_attributes({"mode": "agent", "transport": "stream"})

        config_error = _missing_config_error(agent_id, host, token)
        if config_error:
            root.set_attributes({"error": config_error, "final_status": "CONFIG_ERROR"})
            _finish_result(result, start, error=config_error, final_status="CONFIG_ERROR")
            root.set_outputs(result)
            return result

        host_clean = host.rstrip("/").removeprefix("https://").removeprefix("http://")
        base_url = f"https://{host_clean}/api/2.0/genie/agents/{agent_id}"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        with mlflow.start_span(name="1_stream_response") as stream_span:
            stream_span.set_inputs({"question": question})

            try:
                resp = requests.post(
                    f"{base_url}/responses",
                    headers={**headers, "Accept": "text/event-stream"},
                    json={
                        "input": [
                            {
                                "type": "message",
                                "role": "user",
                                "content": [{"type": "input_text", "text": question}],
                            }
                        ]
                    },
                    stream=True,
                    # 30s connect; 300s max gap between events (server sends keep-alives)
                    timeout=(30, 300),
                )
            except requests.RequestException as e:
                error = f"create-response request failed: {e}"
                stream_span.set_attributes({"error": error})
                _finish_result(
                    result, start, error=error, final_status="CREATE_RESPONSE_FAILED"
                )
                root.set_outputs(result)
                return result

            if resp.status_code != 200:
                body = _short_response(resp)
                resp.close()
                error = f"create-response failed ({resp.status_code}): {body}"
                if "FEATURE_DISABLED" in body:
                    error += (
                        " — a workspace admin must enable the"
                        " 'Genie Agent Mode API for Genie Agents' feature"
                        " in the workspace Previews portal"
                    )
                _set_span_error(stream_span, resp.status_code, body)
                _finish_result(
                    result, start, error=error, final_status="CREATE_RESPONSE_FAILED"
                )
                root.set_outputs(result)
                return result

            labels = {}  # item_id -> label like "reasoning_1"
            counters = {}  # item_type -> occurrence count
            last_event_ts = None
            final_status = None
            output_items = []
            error_info = None
            event_count = 0

            try:
                for event, data in _iter_sse_events(resp):
                    if time.time() - start > timeout_seconds:
                        final_status = "TIMEOUT"
                        break
                    if data is None:
                        continue  # unparseable event payload
                    event_count += 1
                    now = time.time()
                    elapsed = round(now - start, 2)
                    delta = round(now - last_event_ts, 2) if last_event_ts else None
                    last_event_ts = now
                    dur_str = f"+{delta:.1f}s" if delta is not None else ""

                    if event == "response.created":
                        robj = data.get("response") or {}
                        result["conversation_id"] = robj.get("conversation_id")
                        stream_span.set_attributes(
                            {
                                "response_id": robj.get("id"),
                                "response_created_at": robj.get("created_at"),
                            }
                        )
                        print(f"  [t={elapsed:5.1f}s        ] response_created")

                    elif event in (
                        "response.output_item.added",
                        "response.output_item.updated",
                        "response.output_item.done",
                    ):
                        item = data.get("item") or {}
                        iid = item.get("id") or f"unknown_{event_count}"
                        itype = item.get("type") or "unknown"
                        istatus = item.get("status") or "unknown"
                        phase = event.rsplit(".", 1)[-1]  # added / updated / done
                        if iid not in labels:
                            counters[itype] = counters.get(itype, 0) + 1
                            labels[iid] = f"{itype}_{counters[itype]}"
                        label = labels[iid]

                        print(
                            f"  [t={elapsed:5.1f}s {dur_str:>7s}] {label} ({phase}: {istatus})"
                        )

                        span_name = (
                            f"state_{label}"
                            if phase == "added"
                            else f"state_{label}_{phase}"
                        )
                        with mlflow.start_span(name=span_name) as ev_span:
                            ev_span.set_inputs(
                                {
                                    "elapsed_sec": elapsed,
                                    "sequence_number": data.get("sequence_number"),
                                    "event_type": event,
                                    "item_id": iid,
                                }
                            )
                            attrs = {
                                "item_id": iid,
                                "item_type": itype,
                                "item_status": istatus,
                                "event_type": event,
                                "sequence_number": data.get("sequence_number"),
                            }
                            if delta is not None:
                                attrs["prev_event_delta_sec"] = delta
                            sql = None
                            if itype == "function_call":
                                title, sql = _parse_function_call_arguments(item)
                                if title:
                                    attrs["query_title"] = title
                                if sql:
                                    attrs["generated_sql"] = sql
                            if itype == "message" and item.get("role"):
                                attrs["message_role"] = item["role"]
                            ev_span.set_attributes(attrs)

                            outputs = {
                                "label": label,
                                "phase": phase,
                                "item_status": istatus,
                            }
                            if sql:
                                outputs["generated_sql"] = sql
                            if log_full_responses:
                                outputs["item"] = item
                            ev_span.set_outputs(outputs)

                    elif event == "response.completed":
                        robj = data.get("response") or {}
                        final_status = robj.get("status") or "completed"
                        output_items = robj.get("output") or []
                        print(f"  [t={elapsed:5.1f}s {dur_str:>7s}] response_completed")

                    elif event == "response.failed":
                        robj = data.get("response") or {}
                        final_status = "failed"
                        output_items = robj.get("output") or []
                        error_info = robj.get("error") or {}
                        print(f"  [t={elapsed:5.1f}s {dur_str:>7s}] response_failed")
            except requests.exceptions.ReadTimeout:
                resp.close()
                error = f"No SSE events for 300s (read timeout) after {round(time.time() - start, 1)}s elapsed"
                stream_span.set_attributes({"error": error})
                _finish_result(result, start, error=error, final_status="TIMEOUT")
                root.set_outputs(result)
                return result
            except requests.RequestException as e:
                resp.close()
                error = f"SSE stream interrupted after {round(time.time() - start, 1)}s: {e}"
                stream_span.set_attributes({"error": error})
                _finish_result(result, start, error=error, final_status="STREAM_ERROR")
                root.set_outputs(result)
                return result
            resp.close()

            if final_status is None:
                error = "SSE stream ended without a terminal event"
                stream_span.set_attributes({"error": error})
                _finish_result(result, start, error=error, final_status="STREAM_ENDED")
                root.set_outputs(result)
                return result
            if final_status == "TIMEOUT":
                error = f"Timed out after {timeout_seconds}s waiting for Agent mode response"
                stream_span.set_attributes({"error": error})
                _finish_result(result, start, error=error, final_status="TIMEOUT")
                root.set_outputs(result)
                return result

            result["final_status"] = final_status
            stream_span.set_outputs(
                {
                    "final_status": final_status,
                    "event_count": event_count,
                    "total_time": round(time.time() - start, 2),
                }
            )

        if final_status == "completed":
            with mlflow.start_span(name="2_extract_results") as extract_span:
                _populate_agent_result(output_items, result)
                outputs = {
                    "num_sql_queries": len(result["sql_queries"]),
                    "num_tables": len(result["tables"]),
                    "report_chars": len(result["report"] or ""),
                }
                if log_full_responses:
                    outputs["sql_queries"] = result["sql_queries"]
                    outputs["report"] = result["report"]
                extract_span.set_outputs(outputs)
                if result["error"]:
                    extract_span.set_attributes({"error": result["error"]})
        elif final_status == "failed":
            if error_info:
                code = error_info.get("code") or error_info.get("type") or "unknown"
                result["error"] = (
                    f"Agent response failed ({code}): {error_info.get('message')}"
                )
            else:
                result["error"] = "Agent response failed"

        result["duration"] = round(time.time() - start, 2)
        root.set_outputs(
            {
                "success": result["final_status"] == "completed"
                and result["error"] is None,
                "num_sql_queries": len(result["sql_queries"]),
                "num_tables": len(result["tables"]),
                "report_chars": len(result["report"] or ""),
                "duration_sec": result["duration"],
                "final_status": result["final_status"],
                "error": result["error"],
            }
        )

    return result


# COMMAND ----------

if __name__ == "__main__" or os.getenv("DATABRICKS_RUNTIME_VERSION"):
    with mlflow.start_run(run_name=f"genie_{MODE}_visibility"):

        print(f"Mode: {MODE} | Question: {QUESTION}\n")

        if MODE == "agent":
            if AGENT_TRANSPORT == "stream":
                result = ask_genie_agent_stream_with_full_trace(QUESTION, GENIE_SPACE_ID, HOST, TOKEN)
            else:
                result = ask_genie_agent_with_full_trace(QUESTION, GENIE_SPACE_ID, HOST, TOKEN)
        else:
            result = ask_genie_with_full_trace(QUESTION, GENIE_SPACE_ID, HOST, TOKEN)

        print(f"\n{'='*60}")
        print(f"Duration: {result['duration']}s")
        if result.get("error"):
            print(f"Error: {result['error']}")
        if MODE == "agent":
            print(f"\nSQL queries run: {len(result['sql_queries'])}")
            for i, q in enumerate(result["sql_queries"]):
                print(f"  {i+1}. {q.get('title') or '(untitled)'}")
            print(f"\nReport:\n{'-'*40}\n{(result.get('report') or 'N/A')[:2000]}\n{'-'*40}")
        else:
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
# MAGIC Chat mode (`MODE = "chat"`):
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
# MAGIC Agent mode (`MODE = "agent"`, Beta) — reasoning/query cycles repeat per query.
# MAGIC
# MAGIC With `AGENT_TRANSPORT = "stream"` (default), one span per SSE event as it arrives:
# MAGIC
# MAGIC ```
# MAGIC genie_query
# MAGIC ├── 1_stream_response
# MAGIC │     ├── state_reasoning_1                    → added (in_progress): agent planning
# MAGIC │     ├── state_reasoning_1_done               → planning done
# MAGIC │     ├── state_function_call_1                → SQL submitted (generated_sql attribute)
# MAGIC │     ├── state_function_call_output_1         → query started (title only)
# MAGIC │     ├── state_function_call_output_1_updated → results arrived
# MAGIC │     ├── state_function_call_output_1_done    → query finalized
# MAGIC │     ├── ... reasoning / function_call / function_call_output repeat ...
# MAGIC │     └── state_message_N_done                 → final assistant report
# MAGIC │
# MAGIC └── 2_extract_results
# MAGIC       └── Outputs: num_sql_queries, num_tables, report_chars
# MAGIC ```
# MAGIC
# MAGIC With `AGENT_TRANSPORT = "poll"`, one span per observed item transition (coarser):
# MAGIC
# MAGIC ```
# MAGIC genie_query
# MAGIC ├── 1_create_response
# MAGIC │     └── Outputs: conversation_id, response_id (from the first SSE event)
# MAGIC │
# MAGIC ├── 2_poll_states
# MAGIC │     ├── state_message_1                      → User input item (anchors t≈0)
# MAGIC │     ├── state_reasoning_1                    → Agent planning (in_progress)
# MAGIC │     ├── state_reasoning_1_completed          → Planning done
# MAGIC │     ├── state_function_call_1                → SQL submitted (generated_sql attribute)
# MAGIC │     ├── state_function_call_output_1         → Query running (title only)
# MAGIC │     ├── state_function_call_output_1_completed → Query results arrived
# MAGIC │     ├── ... reasoning / function_call / function_call_output repeat ...
# MAGIC │     └── state_message_N                      → Final assistant report (text + inline tables)
# MAGIC │
# MAGIC └── 3_extract_results
# MAGIC       └── Outputs: num_sql_queries, num_tables, report_chars
# MAGIC ```
# MAGIC
# MAGIC Set `LOG_FULL_RESPONSES = True` to include complete API payloads, agent reasoning/report text, and sample result rows in MLflow traces. Keep the default `False` for spaces that may return sensitive data.

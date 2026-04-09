# Databricks notebook source
# MAGIC %md
# MAGIC # Genie API Tracing Demo
# MAGIC
# MAGIC Trace every step of a Genie query — from metadata fetching through SQL generation to result delivery — using the REST API and MLflow Tracing.
# MAGIC
# MAGIC ## Note on timing
# MAGIC
# MAGIC Step durations (`+Xs`) are computed from server-side `last_updated_timestamp` values, so they reflect actual Genie processing time. The elapsed time (`t=`) is measured client-side and depends on the 0.5s polling interval, so state changes may appear up to 0.5s after they actually occurred. (GET polls do not count toward QPM limits.)
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC 1. A **Genie Space ID** — set via `GENIE_SPACE_ID` env var or edit the config cell below
# MAGIC 2. Databricks workspace access (auto-detected on clusters, or set `DATABRICKS_HOST` / `DATABRICKS_TOKEN` env vars)
# MAGIC 3. An MLflow experiment — defaults to `/Shared/genie-visibility-demo`

# COMMAND ----------

# MAGIC %pip install mlflow[databricks]==3.10.0 requests==2.33.1 -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
import os
import time

import mlflow
import requests

GENIE_SPACE_ID = ""  # Replace with your Genie Space ID
QUESTION = "What were the top 10 offers by redemption rate last month?"
EXPERIMENT = "/Shared/genie-visibility-demo"

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


def _extract_query_attachment(message: dict):
    """Return (sql, description, att_id) from the first query attachment, or (None, None, None)."""
    for att in message.get("attachments") or []:
        if att.get("query"):
            return att["query"].get("query"), att["query"].get("description"), att.get("id")
    return None, None, None


def ask_genie_with_full_trace(question: str, space_id: str, host: str, token: str):
    """
    Ask Genie using REST API with full MLflow tracing.
    Every state change logs the complete API response.
    """
    host_clean = host.rstrip("/").removeprefix("https://").removeprefix("http://")
    base_url = f"https://{host_clean}/api/2.0/genie/spaces/{space_id}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    start = time.time()
    result = {"question": question, "sql": None, "rows": 0, "error": None}

    with mlflow.start_span(name="genie_query") as root:
        root.set_inputs({"question": question, "space_id": space_id})

        with mlflow.start_span(name="1_start_conversation") as span:
            span.set_inputs({"question": question})

            resp = requests.post(
                f"{base_url}/start-conversation",
                headers=headers,
                json={"content": question},
            )

            if resp.status_code != 200:
                span.set_attributes({"error": resp.text[:500]})
                result["error"] = resp.text
                return result

            data = resp.json()
            conv_id = data.get("conversation_id") or data.get("conversation", {}).get(
                "id"
            )
            msg_id = data.get("message_id") or data.get("message", {}).get("id")

            if not conv_id or not msg_id:
                span.set_attributes({"error": f"Could not extract IDs: {data}"})
                result["error"] = "Could not extract conversation/message IDs"
                return result

            span.set_outputs(
                {"conversation_id": conv_id, "message_id": msg_id, "response": data}
            )

        with mlflow.start_span(name="2_poll_states") as poll_span:
            last_status = None
            status = None
            message = None
            last_server_ts = None
            pending_state = None
            poll_start = time.time()

            while (time.time() - poll_start) < 300:  # 5 min timeout
                iter_start = time.time()
                resp = requests.get(
                    f"{base_url}/conversations/{conv_id}/messages/{msg_id}",
                    headers=headers,
                )
                if resp.status_code != 200:
                    time.sleep(max(0, 0.5 - (time.time() - iter_start)))
                    continue
                message = resp.json()
                status = message.get("status", "UNKNOWN")

                if status != last_status:
                    with mlflow.start_span(name=f"state_{status}") as state_span:
                        server_ts = message.get("last_updated_timestamp")
                        elapsed = round(time.time() - start, 2)
                        prev_duration = (
                            round((server_ts - last_server_ts) / 1000, 2)
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
                                "server_timestamp": server_ts,
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

                        state_span.set_outputs(
                            {
                                "status": status,
                                "generated_sql": sql,
                                "full_response": message,
                            }
                        )

                    pending_state = (status, elapsed)
                    last_status = status

                if status in [
                    "COMPLETED",
                    "FAILED",
                    "CANCELLED",
                    "QUERY_RESULT_EXPIRED",
                ]:
                    if pending_state:
                        print(
                            f"  [t={pending_state[1]:5.1f}s        ] {pending_state[0]}"
                        )
                    break
                time.sleep(max(0, 0.5 - (time.time() - iter_start)))

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

                                data_span.set_outputs(
                                    {
                                        "columns": cols,
                                        "row_count": len(rows),
                                        "sample": result["sample"],
                                    }
                                )

        result["duration"] = round(time.time() - start, 2)
        root.set_outputs(
            {
                "success": result["sql"] is not None,
                "generated_sql": result["sql"],
                "row_count": result["rows"],
                "duration_sec": result["duration"],
            }
        )

    return result


# COMMAND ----------

with mlflow.start_run(run_name="genie_full_visibility"):

    print(f"Question: {QUESTION}\n")

    result = ask_genie_with_full_trace(QUESTION, GENIE_SPACE_ID, HOST, TOKEN)

    print(f"\n{'='*60}")
    print(f"Duration: {result['duration']}s")
    print(f"\nGenerated SQL:\n{'-'*40}\n{result.get('sql', 'N/A')}\n{'-'*40}")
    print(f"\nResults: {result['rows']} rows")
    if result.get("sample"):
        for i, row in enumerate(result["sample"]):
            print(f"  {i+1}. {row}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What You'll See in MLflow Traces
# MAGIC
# MAGIC Open the **Experiments** page in the Databricks sidebar, select the `/Shared/genie-visibility-demo` experiment, then click into the run to view traces.
# MAGIC
# MAGIC ```
# MAGIC genie_query
# MAGIC ├── 1_start_conversation
# MAGIC │     └── Outputs: conversation_id, message_id, full response
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
# MAGIC             └── Outputs: columns, row_count, sample rows
# MAGIC ```
# MAGIC
# MAGIC Click any `state_*` span, then the **Outputs** tab, and expand `full_response` to see the complete API payload at that stage.

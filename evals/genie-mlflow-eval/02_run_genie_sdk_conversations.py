# Databricks notebook source
# MAGIC %md
# MAGIC # 02. Run Genie SDK Conversations
# MAGIC
# MAGIC Execute the scripted multi-turn scenarios with the Databricks Python SDK and
# MAGIC log one MLflow trace per turn. Traces from the same scenario share one
# MAGIC `mlflow.trace.session` value so MLflow can evaluate the whole conversation.
# MAGIC
# MAGIC Edit the user configuration before running this notebook.

# COMMAND ----------

import time
from datetime import datetime, timedelta, timezone
from pprint import pprint
from typing import Optional

import mlflow
import pandas as pd
from databricks.sdk import WorkspaceClient
from mlflow.entities import SpanType

# COMMAND ----------

# User configuration. Match these values to `00_setup.py`.
GENIE_SPACE_ID = ""
MLFLOW_EXPERIMENT_NAME = "/Shared/genie-mlflow-eval"
EVAL_DATASET_NAME = "main.default.genie_conversation_eval_dataset"
RUN_BATCH_ID = datetime.now(timezone.utc).strftime("manual-%Y%m%d-%H%M%S")
GENIE_TIMEOUT_MINUTES = 5
FETCH_QUERY_RESULTS = False

# COMMAND ----------

if not GENIE_SPACE_ID:
    raise ValueError(
        "Set GENIE_SPACE_ID at the top of this notebook before running."
    )

mlflow.set_tracking_uri("databricks")
mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)

experiment = mlflow.get_experiment_by_name(MLFLOW_EXPERIMENT_NAME)
if experiment is None:
    raise RuntimeError(f"Could not load experiment: {MLFLOW_EXPERIMENT_NAME}")

print(f"MLflow experiment: {MLFLOW_EXPERIMENT_NAME}")
print(f"Evaluation dataset: {EVAL_DATASET_NAME}")
print(f"Run batch ID: {RUN_BATCH_ID}")
print(f"Fetch query results: {FETCH_QUERY_RESULTS}")

# COMMAND ----------

eval_dataset = mlflow.genai.datasets.get_dataset(name=EVAL_DATASET_NAME)
eval_records = eval_dataset.to_df()[["inputs", "expectations"]].to_dict(
    orient="records"
)

if not eval_records:
    raise ValueError(
        f"Dataset {EVAL_DATASET_NAME} is empty. Run 01_create_conversation_dataset.py first."
    )

print(f"Loaded {len(eval_records)} scenario records.")
display(pd.DataFrame(eval_records))

# COMMAND ----------

w = WorkspaceClient()

TERMINAL_ERROR_STATUSES = {"FAILED", "CANCELLED", "QUERY_RESULT_EXPIRED"}


def status_name(status) -> Optional[str]:
    if status is None:
        return None
    return getattr(status, "value", None) or str(status)


def safe_error(exc: Exception) -> str:
    text = str(exc).replace("\n", " ").strip()
    if len(text) > 600:
        text = text[:600] + "..."
    return f"{exc.__class__.__name__}: {text}"


def as_dict(obj) -> dict:
    if obj is None:
        return {}
    if hasattr(obj, "as_dict"):
        return obj.as_dict()
    if hasattr(obj, "as_shallow_dict"):
        return obj.as_shallow_dict()
    return {}


def summarize_query_result(response) -> dict:
    response_dict = as_dict(response)
    statement = response_dict.get("statement_response") or {}
    manifest = statement.get("manifest") or {}
    result = statement.get("result") or {}
    schema = manifest.get("schema") or {}
    columns = [col.get("name") for col in schema.get("columns", []) if col.get("name")]
    return {
        "statement_id": statement.get("statement_id"),
        "status": statement.get("status"),
        "row_count": manifest.get("total_row_count"),
        "truncated": manifest.get("truncated"),
        "columns": columns,
        "sample_rows": (result.get("data_array") or [])[:5],
    }


def parse_genie_message(message, fetch_query_results: bool = False) -> dict:
    generated_sql = None
    response_text_parts = []
    attachments = []

    for attachment in message.attachments or []:
        attachment_id = getattr(attachment, "attachment_id", None) or getattr(
            attachment, "id", None
        )
        attachment_summary = {"attachment_id": attachment_id}

        text = getattr(attachment, "text", None)
        if text is not None and getattr(text, "content", None):
            response_text_parts.append(text.content)
            attachment_summary["text"] = {
                "content": text.content,
                "purpose": status_name(getattr(text, "purpose", None)),
            }

        query = getattr(attachment, "query", None)
        if query is not None:
            query_sql = getattr(query, "query", None)
            if query_sql:
                generated_sql = query_sql
            attachment_summary["query"] = {
                "title": getattr(query, "title", None),
                "description": getattr(query, "description", None),
                "query": query_sql,
                "statement_id": getattr(query, "statement_id", None),
                "row_count": getattr(
                    getattr(query, "query_result_metadata", None), "row_count", None
                ),
                "is_truncated": getattr(
                    getattr(query, "query_result_metadata", None), "is_truncated", None
                ),
            }

            if fetch_query_results and attachment_id:
                try:
                    query_result = w.genie.get_message_attachment_query_result(
                        space_id=message.space_id,
                        conversation_id=message.conversation_id,
                        message_id=message.message_id,
                        attachment_id=attachment_id,
                    )
                    attachment_summary["query_result"] = summarize_query_result(
                        query_result
                    )
                except Exception as exc:
                    attachment_summary["query_result_error"] = safe_error(exc)

        suggested_questions = getattr(attachment, "suggested_questions", None)
        if suggested_questions is not None:
            attachment_summary["suggested_questions"] = getattr(
                suggested_questions, "questions", None
            )

        attachments.append(attachment_summary)

    return {
        "conversation_id": getattr(message, "conversation_id", None),
        "message_id": getattr(message, "message_id", None) or getattr(message, "id", None),
        "status": status_name(getattr(message, "status", None)),
        "response_text": "\n\n".join(response_text_parts) or None,
        "generated_sql": generated_sql,
        "attachments": attachments,
    }

# COMMAND ----------


@mlflow.trace(name="genie_turn", span_type=SpanType.TOOL)
def run_genie_turn(
    *,
    space_id: str,
    content: str,
    session_id: str,
    scenario_id: str,
    turn_index: int,
    conversation_id: Optional[str],
    timeout_minutes: int,
    fetch_query_results: bool,
) -> dict:
    mlflow.update_current_trace(
        metadata={
            "mlflow.trace.session": session_id,
            "mlflow.trace.user": "genie-mlflow-eval-demo",
            "genie.eval.dataset": EVAL_DATASET_NAME,
            "genie.eval.batch_id": RUN_BATCH_ID,
            "genie.eval.scenario_id": scenario_id,
            "genie.eval.turn_index": str(turn_index),
        }
    )

    start = time.time()
    result = {
        "conversation_id": conversation_id,
        "message_id": None,
        "status": None,
        "response_text": None,
        "generated_sql": None,
        "attachments": [],
        "latency_seconds": None,
        "error": None,
    }

    try:
        if conversation_id:
            message = w.genie.create_message_and_wait(
                space_id=space_id,
                conversation_id=conversation_id,
                content=content,
                timeout=timedelta(minutes=timeout_minutes),
            )
        else:
            message = w.genie.start_conversation_and_wait(
                space_id=space_id,
                content=content,
                timeout=timedelta(minutes=timeout_minutes),
            )

        result.update(parse_genie_message(message, fetch_query_results))

        if result["status"] in TERMINAL_ERROR_STATUSES:
            message_error = getattr(message, "error", None)
            error_text = getattr(message_error, "message", None) or as_dict(
                message_error
            )
            result["error"] = f"Genie message ended with status {result['status']}: {error_text}"

    except Exception as exc:
        result["error"] = safe_error(exc)

    result["latency_seconds"] = round(time.time() - start, 2)
    return result


def get_last_trace_id() -> Optional[str]:
    try:
        return mlflow.get_last_active_trace_id()
    except Exception:
        return None


def log_scenario_expectations(trace_id: Optional[str], expectations: dict) -> None:
    if not trace_id or not expectations:
        return
    try:
        mlflow.log_expectation(
            trace_id=trace_id,
            name="scenario_expectations",
            value=expectations,
            metadata={
                "dataset": EVAL_DATASET_NAME,
                "batch_id": RUN_BATCH_ID,
            },
        )
    except Exception as exc:
        print(f"Warning: failed to log scenario expectations: {safe_error(exc)}")


def run_scenario(record: dict) -> dict:
    inputs = record["inputs"]
    expectations = record.get("expectations") or {}
    scenario_id = inputs["scenario_id"]
    turns = inputs["turns"]
    session_id = f"genie-eval-{RUN_BATCH_ID}-{scenario_id}"
    conversation_id = None
    turn_results = []

    for turn_index, turn in enumerate(turns):
        result = run_genie_turn(
            space_id=GENIE_SPACE_ID,
            content=turn["content"],
            session_id=session_id,
            scenario_id=scenario_id,
            turn_index=turn_index,
            conversation_id=conversation_id,
            timeout_minutes=GENIE_TIMEOUT_MINUTES,
            fetch_query_results=FETCH_QUERY_RESULTS,
        )
        result["trace_id"] = get_last_trace_id()
        result["turn_index"] = turn_index
        result["user_content"] = turn["content"]

        if turn_index == 0:
            log_scenario_expectations(result["trace_id"], expectations)

        turn_results.append(result)
        conversation_id = result.get("conversation_id") or conversation_id

        if result.get("error"):
            break

    return {
        "scenario_id": scenario_id,
        "session_id": session_id,
        "conversation_id": conversation_id,
        "turn_count": len(turn_results),
        "completed": len(turn_results) == len(turns)
        and not any(turn.get("error") for turn in turn_results),
        "turns": turn_results,
    }

# COMMAND ----------

scenario_results = []
for record in eval_records:
    scenario_id = record["inputs"]["scenario_id"]
    print(f"Running scenario: {scenario_id}")
    scenario_result = run_scenario(record)
    scenario_results.append(scenario_result)
    pprint(
        {
            "scenario_id": scenario_result["scenario_id"],
            "session_id": scenario_result["session_id"],
            "completed": scenario_result["completed"],
            "turn_count": scenario_result["turn_count"],
        }
    )

flat_rows = []
for scenario in scenario_results:
    for turn in scenario["turns"]:
        flat_rows.append(
            {
                "scenario_id": scenario["scenario_id"],
                "session_id": scenario["session_id"],
                "conversation_id": scenario["conversation_id"],
                "turn_index": turn["turn_index"],
                "trace_id": turn["trace_id"],
                "status": turn["status"],
                "latency_seconds": turn["latency_seconds"],
                "has_generated_sql": bool(turn["generated_sql"]),
                "has_response_text": bool(turn["response_text"]),
                "error": turn["error"],
            }
        )

results_df = pd.DataFrame(flat_rows)

print("Conversation run complete.")
print(f"Run batch ID for evaluation: {RUN_BATCH_ID}")
display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next
# MAGIC
# MAGIC Run `03_evaluate_multiturn_sessions.py` using the same `RUN_BATCH_ID`.

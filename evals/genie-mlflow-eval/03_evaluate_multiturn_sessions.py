# Databricks notebook source
# MAGIC %md
# MAGIC # 03. Evaluate Multi-turn Genie Sessions
# MAGIC
# MAGIC Search the traces from a `RUN_BATCH_ID`, group them by
# MAGIC `mlflow.trace.session`, and evaluate the full conversation sessions with
# MAGIC MLflow GenAI multi-turn scorers.
# MAGIC
# MAGIC This notebook requires MLflow 3.10 or later. Databricks Serverless
# MAGIC environment version 5 includes MLflow 3.8.x, so the next cell upgrades
# MAGIC MLflow in the notebook-scoped environment before evaluation.
# MAGIC
# MAGIC Edit the user configuration before running this notebook.

# COMMAND ----------

# MAGIC %uv pip install "mlflow[databricks]>=3.10.0,<4"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import re
from collections import defaultdict
from importlib.metadata import PackageNotFoundError, version
from typing import Optional

import mlflow
import pandas as pd

# COMMAND ----------


def parse_version(value: str) -> tuple[int, ...]:
    parts = []
    for part in re.split(r"[.+-]", value):
        if part.isdigit():
            parts.append(int(part))
        else:
            break
    return tuple(parts)


def installed_version(distribution_names: list[str]) -> Optional[str]:
    for distribution_name in distribution_names:
        try:
            return version(distribution_name)
        except PackageNotFoundError:
            continue
    return None


mlflow_version = installed_version(["mlflow", "mlflow-skinny"])
if mlflow_version is None or parse_version(mlflow_version) < parse_version("3.10.0"):
    raise RuntimeError(
        "Notebook 03 requires MLflow 3.10 or later for GenAI multi-turn "
        "evaluation scorers. Databricks Serverless environment version 5 "
        "currently includes MLflow 3.8.x.\n\n"
        "Run the MLflow install cell near the top of this notebook, restart "
        "Python, then rerun notebook 03. The install command is:\n"
        '%uv pip install "mlflow[databricks]>=3.10.0,<4"\n'
        "dbutils.library.restartPython()\n\n"
        f"Installed MLflow version: {mlflow_version or 'not installed'}"
    )

print(f"MLflow version check passed: {mlflow_version}")

# COMMAND ----------

# User configuration. Copy RUN_BATCH_ID from `02_run_genie_sdk_conversations.py`.
MLFLOW_EXPERIMENT_NAME = "/Shared/genie-mlflow-eval"
EVAL_DATASET_NAME = "main.default.genie_conversation_eval_dataset"
RUN_BATCH_ID = ""
JUDGE_MODEL = "databricks:/databricks-claude-sonnet-4-6"
SUCCESS_SCORER_NAME = "genie_conversation_success_criteria"

# COMMAND ----------

if not RUN_BATCH_ID:
    raise ValueError(
        "Set RUN_BATCH_ID at the top of this notebook to the value printed by "
        "02_run_genie_sdk_conversations.py."
    )

mlflow.set_tracking_uri("databricks")
mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)

experiment = mlflow.get_experiment_by_name(MLFLOW_EXPERIMENT_NAME)
if experiment is None:
    raise RuntimeError(f"Could not load experiment: {MLFLOW_EXPERIMENT_NAME}")

print(f"MLflow experiment: {MLFLOW_EXPERIMENT_NAME}")
print(f"MLflow experiment ID: {experiment.experiment_id}")
print(f"Evaluation dataset: {EVAL_DATASET_NAME}")
print(f"Run batch ID: {RUN_BATCH_ID}")
print(f"Judge model: {JUDGE_MODEL}")

# COMMAND ----------

filter_string = f"metadata.`genie.eval.batch_id` = '{RUN_BATCH_ID}'"

traces = mlflow.search_traces(
    experiment_ids=[experiment.experiment_id],
    filter_string=filter_string,
    return_type="list",
)

if not traces:
    raise ValueError(
        "No traces found for this RUN_BATCH_ID. Run 02_run_genie_sdk_conversations.py "
        "first, then copy its RUN_BATCH_ID into this notebook."
    )

print(f"Found {len(traces)} traces for RUN_BATCH_ID={RUN_BATCH_ID}.")

# COMMAND ----------


def trace_metadata(trace) -> dict:
    info = getattr(trace, "info", None)
    if info is None:
        return {}
    for attr in ("trace_metadata", "metadata"):
        value = getattr(info, attr, None)
        if isinstance(value, dict):
            return value
    if hasattr(info, "to_dict"):
        info_dict = info.to_dict()
        for key in ("trace_metadata", "metadata"):
            if isinstance(info_dict.get(key), dict):
                return info_dict[key]
    return {}


def trace_start_time_ms(trace) -> int:
    info = getattr(trace, "info", None)
    if info is None:
        return 0
    return (
        getattr(info, "start_time_ms", None)
        or getattr(info, "timestamp_ms", None)
        or 0
    )


sessions = defaultdict(list)
trace_rows = []

for trace in traces:
    metadata = trace_metadata(trace)
    session_id = metadata.get("mlflow.trace.session", "missing-session-id")
    scenario_id = metadata.get("genie.eval.scenario_id", "missing-scenario-id")
    turn_index = metadata.get("genie.eval.turn_index", "")
    sessions[session_id].append(trace)
    trace_rows.append(
        {
            "trace_id": trace.info.trace_id,
            "session_id": session_id,
            "scenario_id": scenario_id,
            "turn_index": turn_index,
            "start_time_ms": trace_start_time_ms(trace),
        }
    )

for session_id in sessions:
    sessions[session_id] = sorted(sessions[session_id], key=trace_start_time_ms)

trace_summary_df = pd.DataFrame(trace_rows).sort_values(
    ["scenario_id", "turn_index", "start_time_ms"]
)
display(trace_summary_df)

print(f"Found {len(sessions)} conversation sessions.")

# COMMAND ----------

eval_dataset = mlflow.genai.datasets.get_dataset(name=EVAL_DATASET_NAME)
dataset_records = eval_dataset.to_df()[["inputs", "expectations"]].to_dict(
    orient="records"
)

expectations_by_scenario = {
    record["inputs"]["scenario_id"]: record.get("expectations", {})
    for record in dataset_records
}

print(f"Loaded expectations for {len(expectations_by_scenario)} scenarios.")

# COMMAND ----------


evaluation_preview_rows = []
missing_expectation_scenarios = set()
for trace in traces:
    metadata = trace_metadata(trace)
    scenario_id = metadata.get("genie.eval.scenario_id", "missing-scenario-id")
    session_id = metadata.get("mlflow.trace.session", "missing-session-id")
    expectations = expectations_by_scenario.get(scenario_id)
    if not expectations:
        missing_expectation_scenarios.add(scenario_id)
    evaluation_preview_rows.append(
        {
            "trace_id": trace.info.trace_id,
            "session_id": session_id,
            "scenario_id": scenario_id,
            "has_expectations": bool(expectations),
            "expectations": expectations or {},
        }
    )

if missing_expectation_scenarios:
    missing_values = ", ".join(sorted(missing_expectation_scenarios))
    raise ValueError(
        "The custom conversation judge needs expectations for every scenario. "
        f"No expectations were found for scenario_id values: {missing_values}. "
        "Confirm that EVAL_DATASET_NAME matches the dataset used in notebook 02."
    )

display(pd.DataFrame(evaluation_preview_rows))

# COMMAND ----------


def log_missing_session_expectations() -> None:
    """Best-effort expectation logging for imported traces or rerun cells."""
    for session_id, session_traces in sessions.items():
        if not session_traces:
            continue
        first_trace = session_traces[0]
        first_trace_id = first_trace.info.trace_id
        if trace_has_expectation(first_trace_id, "scenario_expectations"):
            continue
        scenario_id = trace_metadata(first_trace).get("genie.eval.scenario_id")
        expectations = expectations_by_scenario.get(scenario_id)
        if not expectations:
            continue
        try:
            mlflow.log_expectation(
                trace_id=first_trace_id,
                name="scenario_expectations",
                value=expectations,
                metadata={
                    "dataset": EVAL_DATASET_NAME,
                    "batch_id": RUN_BATCH_ID,
                    "session_id": session_id,
                },
            )
        except Exception as exc:
            print(
                "Warning: failed to log expectations for "
                f"{session_id}: {exc.__class__.__name__}: {exc}"
            )


def trace_has_expectation(trace_id: str, name: str) -> bool:
    try:
        full_trace = mlflow.get_trace(trace_id)
    except Exception:
        return False

    for obj in (full_trace, getattr(full_trace, "data", None), getattr(full_trace, "info", None)):
        assessments = getattr(obj, "assessments", None)
        if not assessments:
            continue
        for assessment in assessments:
            if getattr(assessment, "name", None) == name:
                return True
            if isinstance(assessment, dict) and assessment.get("name") == name:
                return True
    return False


log_missing_session_expectations()

# COMMAND ----------

from mlflow.genai.judges import make_judge
from mlflow.genai.scorers import (
    ConversationCompleteness,
    ConversationalGuidelines,
    KnowledgeRetention,
    scorer,
)

conversation_success_judge = make_judge(
    name=SUCCESS_SCORER_NAME,
    instructions=(
        "Evaluate the full Genie conversation in {{ conversation }} against "
        "the scenario expectations in {{ expectations }}. Return yes only if "
        "the assistant satisfies the success criteria across the conversation. "
        "Generated SQL does not need to exactly match expected SQL patterns, but "
        "it must be directionally consistent when expectations include SQL hints. "
        "Your response must be a boolean: yes or no."
    ),
    model=JUDGE_MODEL,
    feedback_value_type=bool,
)


success_score_cache = {}


@scorer(name=SUCCESS_SCORER_NAME)
def score_genie_conversation_success(trace):
    metadata = trace_metadata(trace)
    session_id = metadata.get("mlflow.trace.session")
    scenario_id = metadata.get("genie.eval.scenario_id")
    if not session_id:
        raise ValueError(f"Trace {trace.info.trace_id} is missing mlflow.trace.session")
    if not scenario_id:
        raise ValueError(f"Trace {trace.info.trace_id} is missing genie.eval.scenario_id")

    cache_key = (session_id, scenario_id)
    if cache_key in success_score_cache:
        return success_score_cache[cache_key]

    session_traces = sessions.get(session_id)
    if not session_traces:
        raise ValueError(f"No session traces found for session_id={session_id}")

    scenario_ids = {
        trace_metadata(session_trace).get("genie.eval.scenario_id")
        for session_trace in session_traces
        if trace_metadata(session_trace).get("genie.eval.scenario_id")
    }
    if scenario_ids != {scenario_id}:
        raise ValueError(
            "Expected each evaluated session to map to exactly one scenario_id, "
            f"but session_id={session_id} has scenario_id values: {sorted(scenario_ids)}"
        )

    expectations = expectations_by_scenario.get(scenario_id)
    if not expectations:
        raise ValueError(f"No expectations found for scenario_id={scenario_id}")

    feedback = conversation_success_judge(
        session=sorted(session_traces, key=trace_start_time_ms),
        expectations=expectations,
    )
    success_score_cache[cache_key] = getattr(feedback, "value", feedback)
    return success_score_cache[cache_key]


scorers = [
    ConversationCompleteness(model=JUDGE_MODEL),
    KnowledgeRetention(model=JUDGE_MODEL),
    ConversationalGuidelines(
        name="genie_conversation_guidelines",
        guidelines=[
            "The assistant should maintain context across follow-up questions.",
            "The assistant should provide a clear answer or explain why the requested answer is unavailable.",
            "The assistant should not invent metrics, dimensions, or filters that were not supported by the conversation or data context.",
        ],
        model=JUDGE_MODEL,
    ),
    score_genie_conversation_success,
]

print("Configured scorers:")
for scorer in scorers:
    print(f"- {scorer.name}")

# COMMAND ----------

with mlflow.start_run(run_name=f"genie-multiturn-evaluation-{RUN_BATCH_ID}") as run:
    mlflow.log_params(
        {
            "eval_dataset_name": EVAL_DATASET_NAME,
            "run_batch_id": RUN_BATCH_ID,
            "judge_model": JUDGE_MODEL,
            "trace_count": len(traces),
            "session_count": len(sessions),
        }
    )

    eval_results = mlflow.genai.evaluate(
        data=traces,
        scorers=scorers,
    )

print("Multi-turn evaluation complete.")

if hasattr(eval_results, "metrics"):
    print("Metrics:")
    print(eval_results.metrics)

display(eval_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review
# MAGIC
# MAGIC Open the MLflow experiment, then review the evaluation run and the session-level
# MAGIC assessments attached to the first trace in each conversation session.

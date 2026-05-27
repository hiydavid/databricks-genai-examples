# Genie MLflow Evaluation

This example demonstrates how to evaluate scripted multi-turn AI/BI Genie
conversations with MLflow GenAI evaluation.

The notebooks assume you already have a curated Genie Space. They use the
Databricks Python SDK to call Genie, MLflow tracing to group each conversation
into a session, and MLflow GenAI scorers to evaluate the completed sessions.

## Files

```text
evals/genie-mlflow-eval/
|-- 00_setup.py
|-- 01_create_conversation_dataset.py
|-- 02_run_genie_sdk_conversations.py
|-- 03_evaluate_multiturn_sessions.py
|-- AGENTS.md
|-- README.md
`-- requirements.txt
```

## Prerequisites

- Databricks workspace with AI/BI Genie enabled.
- Databricks Serverless environment version 5.
- Existing Genie Space ID.
- Permission to use the Genie Space and its SQL warehouse.
- MLflow 3.10 or later for multi-turn evaluation.
- Databricks SDK for Python 0.67.0 or later.

Serverless environment version 5 is the recommended base because it includes
`pandas` and the Databricks SDK. At the time this example was written, the
documented env v5 base includes MLflow 3.8.1. That is enough for setup,
dataset creation, and Genie conversation tracing. The final multi-turn
evaluation notebook requires MLflow 3.10 or later, so notebook 03 includes a
notebook-scoped install cell:

```text
%uv pip install "mlflow[databricks]>=3.10.0,<4"
```

It then restarts Python in the following cell:

```python
dbutils.library.restartPython()
```

The notebooks use Databricks default notebook authentication through
`WorkspaceClient()`. For local execution, configure Databricks unified
authentication and install `requirements.txt` before running the code.

## Run Order

1. Import this folder into a Databricks workspace or open it from a Databricks
   Git folder.
2. Select Serverless environment version 5 for the notebooks.
3. Run `00_setup.py` after editing the user configuration globals at the top.
4. Edit the `SCENARIOS` list in `01_create_conversation_dataset.py` so the
   questions match your Genie Space.
5. Run `01_create_conversation_dataset.py` to create or update the MLflow GenAI
   evaluation dataset.
6. Run `02_run_genie_sdk_conversations.py` to execute the scripted multi-turn
   conversations and log one MLflow trace per turn. All turns in a scenario use
   the same `mlflow.trace.session` value. This notebook does not create an
   MLflow evaluation run.
7. Run `03_evaluate_multiturn_sessions.py` with the same `RUN_BATCH_ID` to
   evaluate the conversation sessions. Run the MLflow install cell near the top
   of notebook 03 if you are using the base serverless environment. This
   notebook creates the MLflow evaluation run with scorer metrics.

## Configuration Globals

The dataset name is prefilled as `main.default.genie_conversation_eval_dataset`.
Change it to a Unity Catalog table name you can create and update.

Each notebook has a user configuration cell with plain Python globals. Edit
those values directly before running the notebook.

The shared globals are:

- `GENIE_SPACE_ID`
- `MLFLOW_EXPERIMENT_NAME`
- `EVAL_DATASET_NAME`
- `RUN_BATCH_ID`
- `JUDGE_MODEL`
- `SUCCESS_SCORER_NAME`
- `GENIE_TIMEOUT_MINUTES`
- `FETCH_QUERY_RESULTS`

`RUN_BATCH_ID` is generated in `02_run_genie_sdk_conversations.py`. Copy the
printed value into `03_evaluate_multiturn_sessions.py` to evaluate the traces
from that run.

## What Gets Logged

Each turn output contains:

- `conversation_id`
- `message_id`
- `status`
- `response_text`
- `generated_sql`
- `attachments`
- `latency_seconds`
- `error`

Each trace includes metadata for filtering and session grouping:

- `mlflow.trace.session`
- `mlflow.trace.user`
- `genie.eval.dataset`
- `genie.eval.batch_id`
- `genie.eval.scenario_id`
- `genie.eval.turn_index`

The first turn trace in each scenario also receives an MLflow expectation named
`scenario_expectations`, which stores the scenario success criteria from the
evaluation dataset.

Notebook 03 maps each trace back to its session and scenario expectations, then
uses a custom scorer to judge the full session conversation. The scorer is
trace-level from MLflow's point of view, so every turn receives a value, but the
judge still sees the complete multi-turn conversation through
`{{ conversation }}` and the scenario criteria through `{{ expectations }}`.

For a clean rerun after changing scorer logic, run notebook 02 again to create
a fresh `RUN_BATCH_ID`, then evaluate that new batch in notebook 03. MLflow
keeps prior assessments attached to old traces.

## References

- [MLflow multi-turn evaluation](https://mlflow.org/docs/latest/genai/eval-monitor/running-evaluation/multi-turn/)
- [MLflow users and sessions](https://mlflow.org/docs/latest/genai/tracing/track-users-sessions)
- [Databricks Serverless environment version 5](https://docs.databricks.com/aws/en/release-notes/serverless/environment-version/five)
- [Databricks Genie API](https://docs.databricks.com/aws/en/genie/conversation-api)
- [Databricks SDK Genie API](https://databricks-sdk-py.readthedocs.io/en/latest/workspace/dashboards/genie.html)

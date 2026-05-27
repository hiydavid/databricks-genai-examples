# Databricks notebook source
# MAGIC %md
# MAGIC # 00. Setup
# MAGIC
# MAGIC Configure MLflow, validate Databricks SDK authentication, and confirm access to
# MAGIC an existing Genie Space.
# MAGIC
# MAGIC Use Databricks Serverless environment version 5 as the base environment.
# MAGIC Serverless env v5 includes pandas, the Databricks SDK, and MLflow 3.8.x.
# MAGIC The final multi-turn evaluation notebook needs MLflow 3.10 or later, and it
# MAGIC will show the upgrade command when needed.
# MAGIC
# MAGIC Run this notebook first, then use the same configuration values in the
# MAGIC following notebooks.

# COMMAND ----------

import re
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError, version
from typing import Optional

# COMMAND ----------


def parse_version(value: str) -> tuple[int, ...]:
    """Parse a package version for simple minimum-version comparisons."""
    parts = []
    for part in re.split(r"[.+-]", value):
        if part.isdigit():
            parts.append(int(part))
        else:
            break
    return tuple(parts)


def installed_version(distribution_names: list[str]) -> Optional[str]:
    """Return the first installed distribution version from a list of names."""
    for distribution_name in distribution_names:
        try:
            return version(distribution_name)
        except PackageNotFoundError:
            continue
    return None


required_packages = [
    {
        "label": "MLflow",
        "distributions": ["mlflow", "mlflow-skinny"],
        "minimum": "3.8.1",
    },
    {
        "label": "Databricks SDK",
        "distributions": ["databricks-sdk"],
        "minimum": "0.67.0",
    },
    {
        "label": "pandas",
        "distributions": ["pandas"],
        "minimum": "2.0.0",
    },
]

version_errors = []
for package in required_packages:
    current = installed_version(package["distributions"])
    if current is None:
        version_errors.append(
            f"{package['label']} is not installed; required >= {package['minimum']}"
        )
    elif parse_version(current) < parse_version(package["minimum"]):
        version_errors.append(
            f"{package['label']} {current} is installed; required >= {package['minimum']}"
        )

if version_errors:
    raise RuntimeError(
        "This example is intended for Databricks Serverless environment version 5. "
        "The documented env v5 base includes MLflow 3.8.1, pandas 2.2.3, "
        "and databricks-sdk 0.67.0.\n\n"
        + "\n".join(f"- {error}" for error in version_errors)
    )

print("Library version check passed.")
for package in required_packages:
    current = installed_version(package["distributions"])
    print(f"{package['label']}: {current}")

mlflow_version = installed_version(["mlflow", "mlflow-skinny"])
if mlflow_version and parse_version(mlflow_version) < parse_version("3.10.0"):
    print(
        "Note: MLflow multi-turn evaluation in 03_evaluate_multiturn_sessions.py "
        "requires MLflow 3.10 or later. You can continue through setup, dataset "
        "creation, and Genie conversation tracing on serverless env v5. Upgrade "
        "MLflow before running notebook 03."
    )

# COMMAND ----------

import mlflow
from databricks.sdk import WorkspaceClient

# COMMAND ----------

# User configuration. Edit these values before running the notebook.
GENIE_SPACE_ID = ""
MLFLOW_EXPERIMENT_NAME = "/Shared/genie-mlflow-eval"
EVAL_DATASET_NAME = "main.default.genie_conversation_eval_dataset"
RUN_BATCH_ID = datetime.now(timezone.utc).strftime("manual-%Y%m%d-%H%M%S")
JUDGE_MODEL = "databricks:/databricks-claude-sonnet-4-6"
GENIE_TIMEOUT_MINUTES = 5
FETCH_QUERY_RESULTS = False

# COMMAND ----------

if not GENIE_SPACE_ID:
    raise ValueError("Set GENIE_SPACE_ID at the top of this notebook before running.")

mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)

experiment = mlflow.get_experiment_by_name(MLFLOW_EXPERIMENT_NAME)
if experiment is None:
    raise RuntimeError(f"Could not create or load experiment: {MLFLOW_EXPERIMENT_NAME}")

print(f"MLflow experiment: {MLFLOW_EXPERIMENT_NAME}")
print(f"MLflow experiment ID: {experiment.experiment_id}")
print(f"Evaluation dataset: {EVAL_DATASET_NAME}")
print(f"Run batch ID: {RUN_BATCH_ID}")
print(f"Judge model: {JUDGE_MODEL}")

# COMMAND ----------

w = WorkspaceClient()
space = w.genie.get_space(space_id=GENIE_SPACE_ID)

print("Genie Space validation succeeded.")
print(f"Title: {getattr(space, 'title', None)}")
print(f"Space ID: {getattr(space, 'space_id', GENIE_SPACE_ID)}")
print(f"Warehouse ID: {getattr(space, 'warehouse_id', None)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next
# MAGIC
# MAGIC Open `01_create_conversation_dataset.py`, edit the `SCENARIOS` list so the
# MAGIC questions match this Genie Space, then run that notebook.

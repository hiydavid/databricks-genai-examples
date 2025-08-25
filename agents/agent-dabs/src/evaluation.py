# Databricks notebook source

# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

# MAGIC %pip install -q -U -r ../requirements.txt

dbutils.library.restartPython()

# COMMAND ----------

import json
import os

import mlflow
import pandas as pd

# COMMAND ----------

# Load configuration
configs = mlflow.models.ModelConfig(development_config="./configs.yaml")
databricks_configs = configs.get("databricks_configs")
agent_configs = configs.get("agent_configs")

# Extract configuration values
CATALOG = databricks_configs.get("catalog")
SCHEMA = databricks_configs.get("schema")
UC_MODEL = databricks_configs.get("model")
WORKSPACE_URL = databricks_configs.get("workspace_url")
SQL_WAREHOUSE_ID = databricks_configs.get("sql_warehouse_id")
USERNAME = databricks_configs.get("username")
AGENT_NAME = agent_configs.get("agent_name")
SECRET_SCOPE_NAME = databricks_configs.get("databricks_pat").get("secret_scope_name")
SECRET_KEY_NAME = databricks_configs.get("databricks_pat").get("secret_key_name")

# Set environment variables
os.environ["DB_MODEL_SERVING_HOST_URL"] = WORKSPACE_URL
os.environ["DATABRICKS_GENIE_PAT"] = dbutils.secrets.get(
    scope=SECRET_SCOPE_NAME, key=SECRET_KEY_NAME
)

# Set up MLflow experiment using DAB configuration
experiment_name = (
    dbutils.widgets.get("mlflow_experiment")
    if dbutils.widgets.get("mlflow_experiment")
    else f"/Users/{USERNAME}/agents-on-databricks-dev"
)

experiment = mlflow.get_experiment_by_name(experiment_name)

if experiment:
    experiment_id = experiment.experiment_id
    mlflow.set_experiment(experiment_name)
    print(f"Using experiment: {experiment_name}")
else:
    raise ValueError(
        f"Experiment {experiment_name} not found. Make sure DAB deployment created it."
    )

# COMMAND ----------
# MAGIC %md
# MAGIC # Test Run

# COMMAND ----------

sample_questions = "What's the debt-to-asset ratio for American Express from 2012 to 2021, compare to that of Bank of America?"

input_example = {
    "messages": [
        {
            "role": "user",
            "content": sample_questions,
        }
    ]
}

# COMMAND ----------
from multiagent_genie import AGENT

response = AGENT.predict(input_example)

# COMMAND ----------

for event in AGENT.predict_stream(input_example):
    print(event, "-----------\n")


# COMMAND ----------

# MAGIC %md
# MAGIC # Run Evaluation

# COMMAND ----------

evals_json_path = "../data/evals/eval-questions.json"

with open(evals_json_path, "r") as f:
    eval_dataset_list = json.load(f)

eval_dataset = spark.createDataFrame(eval_dataset_list)
print(f"Loaded {len(eval_dataset_list)} evaluation questions")

# COMMAND ----------

from mlflow.genai.scorers import Correctness, RelevanceToQuery, Safety
from mlflow.types.agent import ChatAgentMessage


def my_predict_fn(messages):
    """Prediction function for evaluation"""
    return AGENT.predict({"messages": messages})


eval_results = mlflow.genai.evaluate(
    data=pd.DataFrame(eval_dataset_list),
    predict_fn=my_predict_fn,
    scorers=[
        Correctness(),
        RelevanceToQuery(),
        Safety(),
    ],
)

print("Evaluation completed!")
print(f"Results logged to experiment: {experiment_name}")

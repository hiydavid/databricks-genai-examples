# Databricks notebook source
# MAGIC %md
# MAGIC # Agent Evaluation
# MAGIC
# MAGIC This notebook evaluates the retrieval agent using MLflow's evaluation framework.
# MAGIC It imports and runs the agent directly to capture full trace hierarchy.

# COMMAND ----------

# MAGIC %pip install -r ../requirements.txt --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import mlflow
import pandas as pd
import yaml
from mlflow.genai.scorers import (
    Correctness,
    RelevanceToQuery,
    RetrievalGroundedness,
    RetrievalRelevance,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Configuration

# COMMAND ----------

# Job parameters (DAB populates these via base_parameters)
dbutils.widgets.text("catalog", "")  # Non-DAB: set default, e.g. "users"
dbutils.widgets.text("schema", "")  # Non-DAB: set default, e.g. "your_schema"
dbutils.widgets.text("experiment_name", "")  # Non-DAB: set, e.g. "/Users/you@email.com/my-experiment"

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
EXPERIMENT_NAME = dbutils.widgets.get("experiment_name")

if not CATALOG or not SCHEMA or not EXPERIMENT_NAME:
    raise ValueError("Required parameters: catalog, schema, experiment_name")

# Derive paths from parameters
EVAL_TABLE = f"{CATALOG}.{SCHEMA}.eval_dataset"
WORKSPACE_URL = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"

print(f"Catalog: {CATALOG}")
print(f"Schema: {SCHEMA}")
print(f"Evaluation Table: {EVAL_TABLE}")
print(f"MLflow Experiment: {EXPERIMENT_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Agent Config
# MAGIC
# MAGIC Create complete config with databricks_configs for the agent to use.

# COMMAND ----------

# Load agent runtime config from template
with open("configs.template.yaml", "r") as f:
    agent_configs = yaml.safe_load(f)["agent_configs"]

# Build complete config (same structure as deployment)
eval_config = {
    "databricks_configs": {
        "catalog": CATALOG,
        "schema": SCHEMA,
        "workspace_url": WORKSPACE_URL,
    },
    "agent_configs": agent_configs,
}

# Write config file for agent to load
with open("configs.yaml", "w") as f:
    yaml.dump(eval_config, f)

print("Created configs.yaml for evaluation")
print(f"Config: {eval_config}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Agent

# COMMAND ----------

# Import agent AFTER config file is created
from agent import AGENT

print("Agent imported successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set Up MLflow

# COMMAND ----------

# Configure MLflow
mlflow.set_tracking_uri("databricks")
mlflow.set_experiment(EXPERIMENT_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Evaluation Dataset

# COMMAND ----------

# Try to load evaluation dataset from Delta table
# If it doesn't exist, create a sample dataset
try:
    eval_df = spark.read.table(EVAL_TABLE).toPandas()
    print(f"Loaded {len(eval_df)} evaluation examples from {EVAL_TABLE}")
except Exception as e:
    print(f"Evaluation table {EVAL_TABLE} not found: {e}")
    print("Creating sample evaluation dataset...")

    # Create sample evaluation data
    # In production, this should be your curated eval dataset
    sample_data = [
        {
            "question": "How do I configure the system settings?",
            "expected_answer": "The system settings can be configured through the admin panel.",
        },
        {
            "question": "What are the authentication options?",
            "expected_answer": "Authentication options include SSO, LDAP, and local accounts.",
        },
        {
            "question": "How do I troubleshoot connection issues?",
            "expected_answer": "Check network settings and firewall rules.",
        },
    ]

    eval_df = pd.DataFrame(sample_data)
    print(f"Using sample dataset with {len(eval_df)} examples")

    # Save to Delta table for future use
    spark.createDataFrame(eval_df).write.format("delta").mode("overwrite").saveAsTable(
        EVAL_TABLE
    )
    print(f"Sample dataset saved to {EVAL_TABLE}")

eval_df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Evaluation Data

# COMMAND ----------

# Prepare data in MLflow GenAI evaluation format
# Input format must match what the endpoint expects (messages array)
eval_data = pd.DataFrame(
    {
        "inputs": [
            {"messages": [{"role": "user", "content": q}]} for q in eval_df["question"]
        ],
        "expectations": [{"expected_response": a} for a in eval_df["expected_answer"]],
    }
)

print(f"Prepared {len(eval_data)} evaluation examples")
eval_data.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Prediction Function

# COMMAND ----------

# Create predict function that calls the agent directly
# This captures full trace hierarchy (predict → call_llm → retrieve_documents)
from mlflow.types.agent import ChatAgentMessage


def predict_fn(messages: list) -> str:
    """Prediction function for evaluation."""
    # Convert message dicts to ChatAgentMessage objects
    agent_messages = [
        ChatAgentMessage(role=m["role"], content=m["content"]) for m in messages
    ]
    response = AGENT.predict(messages=agent_messages)
    return response.messages[-1].content if response.messages else ""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Evaluation

# COMMAND ----------

# Run evaluation using mlflow.genai.evaluate
results = mlflow.genai.evaluate(
    data=eval_data,
    predict_fn=predict_fn,
    scorers=[
        Correctness(),  # Requires expectations (ground truth)
        RelevanceToQuery(),  # Is response relevant to question?
        RetrievalGroundedness(),  # Is response grounded in retrieved context?
        RetrievalRelevance(),  # Is retrieved context relevant to query?
    ],
)

print("\n=== Evaluation Results ===")
for metric_name, metric_value in results.metrics.items():
    print(f"{metric_name}: {metric_value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Detailed Results

# COMMAND ----------

# Display per-example results via traces
traces_df = mlflow.search_traces(run_id=results.run_id)
traces_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n" + "=" * 50)
print("EVALUATION SUMMARY")
print("=" * 50)
print(f"Evaluation Examples: {len(eval_data)}")
print(f"MLflow Run ID: {results.run_id}")
print(f"MLflow Experiment: {EXPERIMENT_NAME}")
print("\nKey Metrics:")
for metric_name, metric_value in results.metrics.items():
    if "mean" in metric_name or "std" not in metric_name:
        print(
            f"  {metric_name}: {metric_value:.4f}"
            if isinstance(metric_value, float)
            else f"  {metric_name}: {metric_value}"
        )
print("=" * 50)
print("\nView detailed results in the MLflow UI.")

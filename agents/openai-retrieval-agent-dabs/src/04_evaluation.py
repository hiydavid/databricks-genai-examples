# Databricks notebook source
# MAGIC %md
# MAGIC # Agent Evaluation
# MAGIC
# MAGIC This notebook evaluates the deployed retrieval agent using MLflow's evaluation framework.
# MAGIC It runs the agent against an evaluation dataset and logs quality metrics.

# COMMAND ----------

# MAGIC %pip install -r ../requirements.txt --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import mlflow
import pandas as pd
from databricks import agents
from mlflow.deployments import get_deploy_client
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
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("experiment_name", "")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
EXPERIMENT_NAME = dbutils.widgets.get("experiment_name")

if not CATALOG or not SCHEMA or not EXPERIMENT_NAME:
    raise ValueError("Required parameters: catalog, schema, experiment_name")

# Derive paths from parameters
UC_MODEL_NAME = f"{CATALOG}.{SCHEMA}.retrieval_agent"
EVAL_TABLE = f"{CATALOG}.{SCHEMA}.eval_dataset"

# Get endpoint name from deployed agent
deployments = agents.get_deployments(UC_MODEL_NAME)
if not deployments:
    raise ValueError(f"No deployment found for {UC_MODEL_NAME}. Deploy the agent first.")
AGENT_ENDPOINT_NAME = deployments[0].endpoint_name

print(f"Catalog: {CATALOG}")
print(f"Schema: {SCHEMA}")
print(f"Agent Endpoint: {AGENT_ENDPOINT_NAME}")
print(f"Evaluation Table: {EVAL_TABLE}")
print(f"MLflow Experiment: {EXPERIMENT_NAME}")

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
eval_data = pd.DataFrame(
    {
        "inputs": [{"question": q} for q in eval_df["question"]],
        "expectations": [{"expected_response": a} for a in eval_df["expected_answer"]],
    }
)

print(f"Prepared {len(eval_data)} evaluation examples")
eval_data.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Prediction Function

# COMMAND ----------

# Get deployment client
client = get_deploy_client("databricks")


def predict_fn(question: str) -> str:
    """
    Wrapper function to call the deployed agent.
    Takes a question string and returns response string.
    """
    messages = {"messages": [{"role": "user", "content": question}]}

    try:
        response = client.predict(endpoint=AGENT_ENDPOINT_NAME, inputs=messages)

        # Extract the assistant message from response
        if "choices" in response:
            content = response["choices"][0]["message"]["content"]
        elif "messages" in response:
            content = response["messages"][-1]["content"]
        else:
            content = str(response)

        return content
    except Exception as e:
        print(f"Error calling agent: {e}")
        return f"Error: {str(e)}"


# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Agent Connection

# COMMAND ----------

# Test with a single example
test_input = {"messages": [{"role": "user", "content": "Hello, are you working?"}]}

try:
    test_response = client.predict(endpoint=AGENT_ENDPOINT_NAME, inputs=test_input)
    print("Agent connection successful!")
    print(f"Test response: {test_response}")
except Exception as e:
    print(f"Error connecting to agent: {e}")
    print("Make sure the agent is deployed and the endpoint is ready.")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Evaluation

# COMMAND ----------

# Run evaluation with MLflow
with mlflow.start_run(run_name="agent-evaluation") as run:
    # Log evaluation configuration
    mlflow.log_param("agent_endpoint", AGENT_ENDPOINT_NAME)
    mlflow.log_param("num_examples", len(eval_data))
    mlflow.log_param("eval_table", EVAL_TABLE)

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

    print(f"\nRun ID: {run.info.run_id}")
    print(f"Experiment: {EXPERIMENT_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Detailed Results

# COMMAND ----------

# Display per-example results via traces
traces_df = mlflow.search_traces(run_id=run.info.run_id)
traces_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n" + "=" * 50)
print("EVALUATION SUMMARY")
print("=" * 50)
print(f"Agent Endpoint: {AGENT_ENDPOINT_NAME}")
print(f"Evaluation Examples: {len(eval_data)}")
print(f"MLflow Run ID: {run.info.run_id}")
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

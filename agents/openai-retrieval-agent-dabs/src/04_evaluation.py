# Databricks notebook source
# MAGIC %md
# MAGIC # Agent Evaluation
# MAGIC
# MAGIC This notebook evaluates the deployed retrieval agent using MLflow's evaluation framework.
# MAGIC It runs the agent against an evaluation dataset and logs quality metrics.

# COMMAND ----------

# MAGIC %pip install mlflow>=2.21.0 databricks-agents>=0.16.0 PyYAML pandas
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import mlflow
import pandas as pd
import yaml
from mlflow.deployments import get_deploy_client

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Configuration

# COMMAND ----------

# Load configuration from YAML
with open("configs.yaml", "r") as f:
    config = yaml.safe_load(f)

databricks_configs = config["databricks_configs"]
agent_configs = config["agent_configs"]

CATALOG = databricks_configs["catalog"]
SCHEMA = databricks_configs["schema"]
EXPERIMENT_NAME = databricks_configs["mlflow_experiment"]
AGENT_NAME = agent_configs["agent_name"]

# Agent endpoint name (derived from UC model name)
UC_MODEL_NAME = f"{CATALOG}.{SCHEMA}.retrieval_agent"
AGENT_ENDPOINT_NAME = UC_MODEL_NAME.replace(".", "_")

# Evaluation dataset table
EVAL_TABLE = f"{CATALOG}.{SCHEMA}.eval_dataset"

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

# Prepare data in MLflow evaluation format
eval_data = pd.DataFrame(
    {
        "inputs": [
            {"messages": [{"role": "user", "content": q}]}
            for q in eval_df["question"]
        ],
        "ground_truth": eval_df["expected_answer"].tolist(),
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


def predict_fn(inputs_df: pd.DataFrame) -> list[str]:
    """
    Wrapper function to call the deployed agent.
    Takes a DataFrame with 'inputs' column and returns list of responses.
    """
    responses = []

    for _, row in inputs_df.iterrows():
        input_data = row["inputs"]

        try:
            response = client.predict(endpoint=AGENT_ENDPOINT_NAME, inputs=input_data)

            # Extract the assistant message from response
            if "choices" in response:
                content = response["choices"][0]["message"]["content"]
            elif "messages" in response:
                content = response["messages"][-1]["content"]
            else:
                content = str(response)

            responses.append(content)
        except Exception as e:
            print(f"Error calling agent: {e}")
            responses.append(f"Error: {str(e)}")

    return responses


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

    # Run evaluation
    results = mlflow.evaluate(
        model=predict_fn,
        data=eval_data,
        targets="ground_truth",
        model_type="question-answering",
        evaluators="default",
        evaluator_config={
            "col_mapping": {"inputs": "inputs", "ground_truth": "ground_truth"}
        },
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

# Display per-example results
results_df = results.tables["eval_results_table"]
results_df

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
        print(f"  {metric_name}: {metric_value:.4f}" if isinstance(metric_value, float) else f"  {metric_name}: {metric_value}")
print("=" * 50)
print("\nView detailed results in the MLflow UI.")

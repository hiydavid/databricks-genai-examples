# Databricks notebook source
# MAGIC %md
# MAGIC # Agent Deployment
# MAGIC
# MAGIC This notebook logs the retrieval agent to MLflow, registers it in Unity Catalog,
# MAGIC and deploys it to Model Serving using `agents.deploy()`.

# COMMAND ----------

# MAGIC %pip install mlflow>=2.21.0 databricks-agents>=0.16.0 PyYAML
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os

import mlflow
import yaml
from databricks import agents
from mlflow.models.resources import DatabricksServingEndpoint, DatabricksVectorSearchIndex

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Configuration

# COMMAND ----------

# Load configuration from widgets (set by DABs job) or defaults
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "default")
dbutils.widgets.text("mlflow_experiment", "")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
EXPERIMENT_NAME = dbutils.widgets.get("mlflow_experiment")

# Get current username for defaults
USERNAME = spark.sql("SELECT current_user()").collect()[0][0]

# Load additional config from YAML
try:
    with open("configs.yaml", "r") as f:
        config = yaml.safe_load(f)
    databricks_configs = config.get("databricks_configs", {})
    agent_configs = config.get("agent_configs", {})

    WORKSPACE_URL = databricks_configs.get("workspace_url", "")
    AGENT_NAME = agent_configs.get("agent_name", "user-guide-retrieval-agent")
    LLM_ENDPOINT = agent_configs.get("llm", {}).get("endpoint_name", "databricks-claude-sonnet-4")

    vs_config = agent_configs.get("vector_search", {})
    VS_ENDPOINT = vs_config.get("endpoint_name", "vs-endpoint")
    VS_INDEX = vs_config.get("index_name", f"{CATALOG}.{SCHEMA}.user_guide_chunks_index")
except FileNotFoundError:
    print("Warning: configs.yaml not found, using defaults")
    WORKSPACE_URL = ""
    AGENT_NAME = "user-guide-retrieval-agent"
    LLM_ENDPOINT = "databricks-claude-sonnet-4"
    VS_ENDPOINT = "vs-endpoint"
    VS_INDEX = f"{CATALOG}.{SCHEMA}.user_guide_chunks_index"

# Set experiment name if not provided
if not EXPERIMENT_NAME:
    EXPERIMENT_NAME = f"/Users/{USERNAME}/retrieval-agent-mcp"

# Unity Catalog model name
UC_MODEL_NAME = f"{CATALOG}.{SCHEMA}.retrieval_agent"

print(f"Catalog: {CATALOG}")
print(f"Schema: {SCHEMA}")
print(f"UC Model: {UC_MODEL_NAME}")
print(f"MLflow Experiment: {EXPERIMENT_NAME}")
print(f"LLM Endpoint: {LLM_ENDPOINT}")
print(f"Vector Search Index: {VS_INDEX}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set Up MLflow

# COMMAND ----------

# Configure MLflow
mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks-uc")

# Create or get experiment
experiment = mlflow.get_experiment_by_name(EXPERIMENT_NAME)
if experiment:
    experiment_id = experiment.experiment_id
    mlflow.set_experiment(EXPERIMENT_NAME)
    print(f"Using existing experiment: {EXPERIMENT_NAME}")
else:
    experiment_id = mlflow.create_experiment(EXPERIMENT_NAME)
    mlflow.set_experiment(EXPERIMENT_NAME)
    print(f"Created new experiment: {EXPERIMENT_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Resources for Auto-Auth

# COMMAND ----------

# Define resources that the agent needs access to
# This enables automatic authentication passthrough
resources = [
    DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT),
    DatabricksVectorSearchIndex(index_name=VS_INDEX),
]

print("Resources for auto-auth:")
for r in resources:
    print(f"  - {r}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log Model to MLflow

# COMMAND ----------

# Input example for model signature
input_example = {
    "messages": [
        {
            "role": "user",
            "content": "How do I configure the system?",
        }
    ]
}

# Get the current working directory for file paths
cwd = os.getcwd()
agent_path = os.path.join(cwd, "03_agent.py")
config_path = os.path.join(cwd, "configs.yaml")

print(f"Agent path: {agent_path}")
print(f"Config path: {config_path}")

# COMMAND ----------

# Log the model
with mlflow.start_run(run_name="retrieval-agent-deployment") as run:
    logged_agent_info = mlflow.pyfunc.log_model(
        artifact_path=AGENT_NAME,
        python_model=agent_path,
        model_config=config_path,
        input_example=input_example,
        resources=resources,
        pip_requirements=[
            "mlflow>=2.21.0",
            "openai>=1.50.0",
            "databricks-sdk>=0.40.0",
            "databricks-mcp>=0.1.0",
            "pydantic>=2.0.0",
            "backoff>=2.0.0",
        ],
    )

    print(f"Model logged to run: {run.info.run_id}")
    print(f"Model URI: {logged_agent_info.model_uri}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Model Before Deployment

# COMMAND ----------

# Test the model locally before deployment
print("Validating model...")

try:
    result = mlflow.models.predict(
        model_uri=f"runs:/{run.info.run_id}/{AGENT_NAME}",
        input_data=input_example,
        env_manager="uv",
    )
    print("Model validation successful!")
    print(f"Sample response: {result}")
except Exception as e:
    print(f"Warning: Model validation failed: {e}")
    print("Proceeding with deployment anyway...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register Model to Unity Catalog

# COMMAND ----------

# Register to Unity Catalog
print(f"Registering model to {UC_MODEL_NAME}...")

uc_registered_model_info = mlflow.register_model(
    model_uri=logged_agent_info.model_uri,
    name=UC_MODEL_NAME,
)

print(f"Model registered: {UC_MODEL_NAME}")
print(f"Version: {uc_registered_model_info.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy Agent

# COMMAND ----------

# Deploy using agents.deploy()
print(f"Deploying agent {UC_MODEL_NAME} version {uc_registered_model_info.version}...")

try:
    deployment = agents.deploy(
        UC_MODEL_NAME,
        uc_registered_model_info.version,
        tags={"endpointSource": "retrieval-agent-mcp"},
    )
    print(f"Deployment initiated successfully!")
    print(f"Endpoint will be available at: {UC_MODEL_NAME.replace('.', '_')}")
except ValueError as e:
    if "currently updating" in str(e):
        print("Endpoint is already updating. Deployment will complete shortly.")
    else:
        raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n" + "=" * 50)
print("DEPLOYMENT SUMMARY")
print("=" * 50)
print(f"MLflow Experiment: {EXPERIMENT_NAME}")
print(f"Run ID: {run.info.run_id}")
print(f"UC Model: {UC_MODEL_NAME}")
print(f"Model Version: {uc_registered_model_info.version}")
print(f"LLM Endpoint: {LLM_ENDPOINT}")
print(f"Vector Search Index: {VS_INDEX}")
print("=" * 50)
print("\nThe agent endpoint should be available shortly.")
print("Check Model Serving in the Databricks UI to monitor deployment status.")

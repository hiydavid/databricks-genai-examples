# Databricks notebook source
# MAGIC %md
# MAGIC # Agent Deployment
# MAGIC
# MAGIC This notebook logs the retrieval agent to MLflow, registers it in Unity Catalog,
# MAGIC and deploys it to Model Serving using `agents.deploy()`.

# COMMAND ----------

# MAGIC %pip install -r ../requirements.txt --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os
import tempfile

import mlflow
import yaml
from databricks import agents
from mlflow.models.resources import (
    DatabricksServingEndpoint,
    DatabricksVectorSearchIndex,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Configuration

# COMMAND ----------

# Job parameters (DAB populates these via base_parameters)
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("experiment_name", "")
dbutils.widgets.text("vs_endpoint", "")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
EXPERIMENT_NAME = dbutils.widgets.get("experiment_name")
VS_ENDPOINT = dbutils.widgets.get("vs_endpoint")

if not CATALOG or not SCHEMA or not EXPERIMENT_NAME or not VS_ENDPOINT:
    raise ValueError("Required parameters: catalog, schema, experiment_name, vs_endpoint")

# Derive workspace URL from Spark context
WORKSPACE_URL = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"

# Load agent runtime config from configs.yaml (LLM settings, agent name, etc.)
with open("configs.yaml", "r") as f:
    agent_configs = yaml.safe_load(f)["agent_configs"]

AGENT_NAME = agent_configs["agent_name"]
LLM_ENDPOINT = agent_configs["llm"]["endpoint_name"]

# Derive paths from parameters
VS_INDEX = f"{CATALOG}.{SCHEMA}.user_guide_chunks_index"
UC_MODEL_NAME = f"{CATALOG}.{SCHEMA}.retrieval_agent"

print(f"Catalog: {CATALOG}")
print(f"Schema: {SCHEMA}")
print(f"Workspace URL: {WORKSPACE_URL}")
print(f"UC Model: {UC_MODEL_NAME}")
print(f"MLflow Experiment: {EXPERIMENT_NAME}")
print(f"LLM Endpoint: {LLM_ENDPOINT}")
print(f"Vector Search Endpoint: {VS_ENDPOINT}")
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
agent_path = os.path.join(cwd, "agent.py")

print(f"Agent path: {agent_path}")

# Build deployment config (passed to agent.py at inference time via mlflow.models.ModelConfig)
# This combines infrastructure configs from DAB parameters with agent runtime config from configs.yaml
deployment_config = {
    "databricks_configs": {
        "catalog": CATALOG,
        "schema": SCHEMA,
        "workspace_url": WORKSPACE_URL,
    },
    "agent_configs": {
        **agent_configs,
        "vector_search": {
            "endpoint_name": VS_ENDPOINT,
            "index_name": VS_INDEX,
        },
    },
}

# Write temp config for model logging
temp_config_file = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False)
yaml.dump(deployment_config, temp_config_file)
temp_config_file.close()
temp_config_path = temp_config_file.name

print(f"Deployment config written to: {temp_config_path}")
print(f"Config contents: {deployment_config}")

# COMMAND ----------

# Log the model
with mlflow.start_run(run_name="retrieval-agent-deployment") as run:
    logged_agent_info = mlflow.pyfunc.log_model(
        artifact_path=AGENT_NAME,
        python_model=agent_path,
        model_config=temp_config_path,
        input_example=input_example,
        resources=resources,
        pip_requirements="../requirements.txt",
    )

    print(f"Model logged to run: {run.info.run_id}")
    print(f"Model URI: {logged_agent_info.model_uri}")

# Clean up temp config file
os.unlink(temp_config_path)

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
    AGENT_ENDPOINT_NAME = deployment.endpoint_name
    print(f"Deployment initiated successfully!")
    print(f"Endpoint name: {AGENT_ENDPOINT_NAME}")
except ValueError as e:
    if "currently updating" in str(e):
        print("Endpoint is already updating. Deployment will complete shortly.")
        # Get endpoint name from existing deployment
        deployments = agents.get_deployments(UC_MODEL_NAME)
        AGENT_ENDPOINT_NAME = deployments[0].endpoint_name if deployments else "unknown"
        print(f"Endpoint name: {AGENT_ENDPOINT_NAME}")
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
print(f"Agent Endpoint: {AGENT_ENDPOINT_NAME}")
print(f"LLM Endpoint: {LLM_ENDPOINT}")
print(f"Vector Search Index: {VS_INDEX}")
print("=" * 50)
print("\nThe agent endpoint should be available shortly.")
print("Check Model Serving in the Databricks UI to monitor deployment status.")

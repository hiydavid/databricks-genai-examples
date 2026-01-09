# Databricks notebook source
# MAGIC %md
# MAGIC # Create and Deploy Claims Extraction Agent
# MAGIC
# MAGIC This notebook creates a non-conversational PyFunc agent for classifying and extracting
# MAGIC information from medical claims documents, then deploys it to Model Serving.

# COMMAND ----------

# MAGIC %pip install -r ../../requirements.txt --quiet
# MAGIC %restart_python

# COMMAND ----------

import json
import os
import sys
from pathlib import Path

import mlflow
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput
from mlflow.models.resources import DatabricksServingEndpoint

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

cfg = Config.from_yaml()

CATALOG = cfg.catalog
SCHEMA = cfg.schema
MODEL_NAME = cfg.model_name
ENDPOINT_NAME = cfg.endpoint_name
LLM_ENDPOINT = cfg.llm_endpoint
REGISTERED_MODEL_NAME = cfg.registered_model_name

# Databricks authentication config
DATABRICKS_HOST = cfg.databricks_host
SECRET_SCOPE = cfg.secret_scope
SECRET_KEY = cfg.secret_key

# MLflow config
MLFLOW_EXPERIMENT_ID = cfg.mlflow_experiment_id

# Set environment variables for the agent (used in load_context)
# Get token from Databricks secrets
db_token = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY)  # noqa: F821
os.environ["DATABRICKS_HOST"] = DATABRICKS_HOST
os.environ["DATABRICKS_TOKEN"] = db_token

# Set MLflow experiment
mlflow.set_experiment(experiment_id=MLFLOW_EXPERIMENT_ID)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Agent from agent.py
# MAGIC
# MAGIC The agent class and all schemas are defined in `agent.py` for code-based MLflow logging.

# COMMAND ----------

# Add the src directory to the path so we can import agent.py
# In Databricks, use the notebook context to find the repo root
try:
    notebook_path = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )  # noqa: F821
    parts = notebook_path.split("/")
    if "src" in parts:
        src_idx = parts.index("src")
        src_dir = "/Workspace" + "/".join(parts[:src_idx]) + "/src"
    else:
        src_dir = "/Workspace" + "/".join(parts[:-1])
except Exception:
    # Local execution - go up from setup/ to src/
    src_dir = str(Path(__file__).parent.parent) if "__file__" in dir() else ".."

if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

# Import from agent.py (without triggering set_model during import)
from agent import AgentInput, ClaimsExtractionAgent

print(f"Imported agent from: {src_dir}/agent.py")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Test the Agent Class
# MAGIC
# MAGIC Test the agent with example data before logging.

# COMMAND ----------

# Load example text from local file (for testing before PARSED_TABLE exists)
example_input_path = os.path.join(src_dir, "setup", "example_input.txt")
with open(example_input_path, "r") as f:
    lines = f.readlines()

# Skip the first 3 metadata lines (Loaded document, Text length, First 500 chars)
example_text = "".join(lines[3:])

print(f"Loaded example from: {example_input_path}")
print(f"Text length: {len(example_text)} characters")
print(f"First 500 chars:\n{example_text}...")

# COMMAND ----------

# Prepare input for the agent
test_input = [AgentInput(text=example_text).model_dump()]

# Instantiate the agent and initialize it
agent = ClaimsExtractionAgent()
agent.load_context(context=None)

# Run the agent's predict method
result = agent.predict(context=None, model_input=test_input)

print(json.dumps(result, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log and Register the Model
# MAGIC
# MAGIC Uses code-based logging with the separate `agent.py` file.
# MAGIC This is required for models with `@mlflow.trace` decorators.

# COMMAND ----------

# Get the path to agent.py - reuse src_dir from import cell
agent_code_path = os.path.join(src_dir, "agent.py")
print(f"Agent code path: {agent_code_path}")

# COMMAND ----------

# Create example input for signature inference
example_input = [AgentInput(text=example_text).model_dump()]

# Define resources that the agent depends on
resources = [
    DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT),
]

for resource in resources:
    print(f"Resource: {resource.to_dict()}")

# COMMAND ----------

# Experiment is already set at the top of the notebook via experiment_id
with mlflow.start_run(run_name="claims_extraction_agent") as run:
    # Log the model using code-based logging
    model_info = mlflow.pyfunc.log_model(
        name="model",
        python_model=agent_code_path,  # Path to the agent.py file
        input_example=example_input,
        pip_requirements=["-r ../../requirements.txt"],
        resources=resources,
        registered_model_name=REGISTERED_MODEL_NAME,
    )

    print(f"Model logged: {model_info.model_uri}")
    print(f"Registered as: {REGISTERED_MODEL_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-deployment Validation
# MAGIC
# MAGIC Test the logged model locally before deploying to ensure it works correctly.

# COMMAND ----------

# Validate the model before deployment
mlflow.models.predict(
    model_uri=f"runs:/{model_info.run_id}/model",
    input_data=[{"text": example_text}],
    env_manager="uv",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy to Model Serving
# MAGIC
# MAGIC Uses manual SDK deployment for non-conversational agents.
# MAGIC Reference: https://docs.databricks.com/aws/en/generative-ai/agent-framework/non-conversational-agents

# COMMAND ----------

w = WorkspaceClient()

# Use the model version from the logged model
model_version = model_info.registered_model_version
print(f"Deploying model version: {model_version}")

new_entity = ServedEntityInput(
    entity_name=REGISTERED_MODEL_NAME,
    entity_version=model_version,
    name=f"{MODEL_NAME}-{model_version}",
    workload_size="Small",
    scale_to_zero_enabled=True,
    environment_vars={
        "DATABRICKS_HOST": DATABRICKS_HOST,
        "DATABRICKS_TOKEN": f"{{{{secrets/{SECRET_SCOPE}/{SECRET_KEY}}}}}",
        "LLM_ENDPOINT": LLM_ENDPOINT,
    },
)

# Check if endpoint exists and create or update accordingly
try:
    existing_endpoint = w.serving_endpoints.get(ENDPOINT_NAME)
    print(f"Endpoint {ENDPOINT_NAME} exists, updating...")
    w.serving_endpoints.update_config(name=ENDPOINT_NAME, served_entities=[new_entity])
except Exception:
    print(f"Creating new endpoint: {ENDPOINT_NAME}")
    w.serving_endpoints.create(
        name=ENDPOINT_NAME,
        config=EndpointCoreConfigInput(
            name=ENDPOINT_NAME, served_entities=[new_entity]
        ),
    )

# Wait for endpoint to be ready
print("Waiting for endpoint to be ready...")
w.serving_endpoints.wait_get_serving_endpoint_not_updating(ENDPOINT_NAME)
print(f"Endpoint ready!")
print(f"Access at: {DATABRICKS_HOST}/serving-endpoints/{ENDPOINT_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test the Endpoint
# MAGIC
# MAGIC Note: Wait for the endpoint to be ready before running this cell.

# COMMAND ----------

from mlflow.deployments import get_deploy_client

client = get_deploy_client("databricks")

# Use the same example text from earlier test
endpoint_test_input = {"inputs": [{"text": example_text}]}

response = client.predict(endpoint=ENDPOINT_NAME, inputs=endpoint_test_input)
print(json.dumps(response, indent=2))

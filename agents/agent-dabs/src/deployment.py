# Databricks notebook source

# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

# MAGIC %pip install -q -U -r ../requirements.txt
# MAGIC %pip install uv --quiet

dbutils.library.restartPython()

# COMMAND ----------

import os

import mlflow

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
# MAGIC # Log Agent

# COMMAND ----------

# Determine Databricks resources to specify for automatic auth passthrough at deployment time
import os

import mlflow
from mlflow.models.resources import (
    DatabricksGenieSpace,
    DatabricksServingEndpoint,
    DatabricksSQLWarehouse,
)

from multiagent_genie import AGENT, GENIE_SPACE_ID, LLM_ENDPOINT_NAME

# Log the agent as an MLflow model
resources = [
    DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT_NAME),
    DatabricksGenieSpace(genie_space_id=GENIE_SPACE_ID),
    DatabricksSQLWarehouse(warehouse_id=SQL_WAREHOUSE_ID),
]

input_example = {
    "messages": [
        {
            "role": "user",
            "content": "What was BAC's net income in the last 5 years",
        }
    ]
}

with mlflow.start_run():
    logged_agent_info = mlflow.pyfunc.log_model(
        name=AGENT_NAME,
        python_model=os.path.join(
            os.getcwd(), "multiagent_genie.py"
        ),  # point to the agent code
        model_config=os.path.join(
            os.getcwd(), "configs.yaml"
        ),  # point to the config file
        input_example=input_example,
        resources=resources,
        pip_requirements=["-r ../requirements.txt"],
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # Pre-deployment agent validation

# COMMAND ----------

mlflow.models.predict(
    model_uri=f"runs:/{logged_agent_info.run_id}/{AGENT_NAME}",
    input_data=input_example,
    env_manager="uv",
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Register the model to Unity Catalog

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")
UC_MODEL_NAME = f"{CATALOG}.{SCHEMA}.{UC_MODEL}"

uc_registered_model_info = mlflow.register_model(
    model_uri=logged_agent_info.model_uri, name=UC_MODEL_NAME
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Deploy the agent

# COMMAND ----------

from databricks import agents

try:
    agents.deploy(
        UC_MODEL_NAME,
        uc_registered_model_info.version,
        tags={"endpointSource": "docs"},
        environment_vars={
            "DATABRICKS_GENIE_PAT": f"{{{{secrets/{SECRET_SCOPE_NAME}/{SECRET_KEY_NAME}}}}}"
        },
    )
    print(f"✅ Deployment initiated successfully for {UC_MODEL_NAME}")
except ValueError as e:
    if "currently updating" in str(e):
        print(
            f"✅ Endpoint is already updating. Deployment was initiated successfully."
        )
        print(f"📊 Check the serving endpoints page to monitor deployment progress.")
    else:
        raise e

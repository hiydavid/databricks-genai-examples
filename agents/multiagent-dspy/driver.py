# Databricks notebook source
# MAGIC %md
# MAGIC # Multi-agent system with DSPy
# MAGIC
# MAGIC Based on: https://docs.databricks.com/aws/en/generative-ai/agent-framework/multi-agent-genie
# MAGIC
# MAGIC In this notebook, you:
# MAGIC 1. Author a multi-agent system using DSPy instead of LangGraph.
# MAGIC 1. Wrap the DSPy agent with MLflow `ChatAgent` to ensure compatibility with Databricks features.
# MAGIC 1. Manually test the multi-agent system's output.
# MAGIC 1. Log and deploy the multi-agent system.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC - Address all `TODO`s in this notebook.
# MAGIC - Create a Genie Space, see Databricks documentation ([AWS](https://docs.databricks.com/aws/genie/set-up) | [Azure](https://learn.microsoft.com/azure/databricks/genie/set-up)).
# MAGIC - Create a Personal Access Token (PAT) as a Databricks secret
# MAGIC   - This can either be your own PAT or that of a System Principal ([AWS](https://docs.databricks.com/aws/en/dev-tools/auth/oauth-m2m) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/oauth-m2m)). You will have to rotate this token yourself upon expiry.
# MAGIC   - Add secrets-based environment variables to a model serving endpoint ([AWS](https://docs.databricks.com/aws/en/machine-learning/model-serving/store-env-variable-model-serving#add-secrets-based-environment-variables) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/model-serving/store-env-variable-model-serving#add-secrets-based-environment-variables)).
# MAGIC   - You can reference the table in the deploy docs for the right permissions level for each resource: ([AWS](https://docs.databricks.com/aws/en/generative-ai/agent-framework/deploy-agent#automatic-authentication-passthrough) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/generative-ai/agent-framework/deploy-agent#automatic-authentication-passthrough)).
# MAGIC     - Provision with `CAN RUN` on the Genie Space
# MAGIC     - Provision with `CAN USE` on the SQL Warehouse powering the Genie Space
# MAGIC     - Provision with `SELECT` on underlying Unity Catalog Tables
# MAGIC     - Provision with `EXECUTE` on underyling Unity Catalog Functions

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# MAGIC %pip install -q -U -r requirements.txt
# MAGIC %pip install uv --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set variables and configuration
# MAGIC Be sure to go to the `configs.yaml` file and input your resource variables and configuration.

# COMMAND ----------

import os

import mlflow

# TODO make sure you update the config file before this

configs = mlflow.models.ModelConfig(development_config="./configs.yaml")
databricks_configs = configs.get("databricks_configs")
agent_configs = configs.get("agent_configs")

CATALOG = databricks_configs.get("catalog")
SCHEMA = databricks_configs.get("schema")
UC_MODEL = databricks_configs.get("model")
MLFLOW_EXPERIMENT_NAME = databricks_configs.get("mlflow_experiment_name")
WORKSPACE_URL = databricks_configs.get("workspace_url")
SQL_WAREHOUSE_ID = databricks_configs.get("sql_warehouse_id")
AGENT_NAME = agent_configs.get("agent_name")

SECRET_SCOPE_NAME = databricks_configs.get("databricks_pat").get("secret_scope_name")
SECRET_KEY_NAME = databricks_configs.get("databricks_pat").get("secret_key_name")

os.environ["DB_MODEL_SERVING_HOST_URL"] = WORKSPACE_URL
os.environ["DATABRICKS_GENIE_PAT"] = dbutils.secrets.get(
    scope=SECRET_SCOPE_NAME, key=SECRET_KEY_NAME
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set MLflow Experiment

# COMMAND ----------

import os

import mlflow

experiment_fqdn = f"{os.getcwd()}/{MLFLOW_EXPERIMENT_NAME}"
experiment = mlflow.get_experiment_by_name(experiment_fqdn)

if experiment:
    experiment_id = experiment.experiment_id
else:
    experiment_id = mlflow.create_experiment(experiment_fqdn)

mlflow.set_experiment(experiment_fqdn)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Load the `agent` notebook
# MAGIC
# MAGIC Create a multi-agent system using DSPy framework with a supervisor agent directing the following agent modules:
# MAGIC - **GenieModule**: DSPy module wrapping the Genie agent that queries and reasons over structured data.
# MAGIC - **ParallelExecutorModule**: DSPy module that creates research plans and makes parallel calls to the GenieAgent for complex multi-step reasoning questions.
# MAGIC
# MAGIC The DSPy framework provides several advantages over LangGraph:
# MAGIC - **Automatic Prompt Optimization**: DSPy can automatically optimize prompts and weights across the entire pipeline
# MAGIC - **Modular Programming**: Program with modules instead of prompts, making the system more maintainable
# MAGIC - **Declarative Signatures**: Clear interfaces between modules with input/output field definitions
# MAGIC - **Built-in Optimization**: Support for automatic tuning based on example data
# MAGIC
# MAGIC #### Wrap the DSPy system using the `ChatAgent` interface
# MAGIC
# MAGIC Databricks recommends using `ChatAgent` to ensure compatibility with Databricks AI features and to simplify authoring multi-turn conversational agents using an open source standard.
# MAGIC
# MAGIC The `DSPyChatAgent` class implements the `ChatAgent` interface to wrap the DSPy multi-agent system.
# MAGIC
# MAGIC See MLflow's [ChatAgent documentation](https://mlflow.org/docs/latest/python_api/mlflow.pyfunc.html#mlflow.pyfunc.ChatAgent).

# COMMAND ----------

# MAGIC %run ./agent

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test the agent
# MAGIC
# MAGIC Interact with the agent to test its output. Since this notebook called `mlflow.langchain.autolog()` you can view the trace for each step the agent takes.
# MAGIC
# MAGIC TODO: Replace this placeholder `input_example` with a domain-specific prompt for your agent.

# COMMAND ----------

sample_questions = [
    "What's the debt-to-asset ratio for American Express from 2012 to 2021, compare to that of Bank of America?",
    "Give me an executive summary comparing year-on-year revenue growth from 2012 to 2021 between the AAPL and BAC?",
    "Why is BAC's revenue growth so volatile between the years 2012 to 2021?",
]

input_example = {
    "messages": [
        {
            "role": "user",
            "content": sample_questions[0],
        }
    ]
}

# COMMAND ----------

# invoke the agent
response = AGENT.predict(input_example)

# COMMAND ----------

print("### FINAL RESPONSE:")
print(response.messages[-1].content)

# COMMAND ----------

# for event in AGENT.predict_stream(input_example):
#     print(event, "-----------\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log the agent as an MLflow model
# MAGIC
# MAGIC Log the agent as code from the `agent.py` file. See [MLflow - Models from Code](https://mlflow.org/docs/latest/models.html#models-from-code).
# MAGIC
# MAGIC ### Enable automatic authentication for Databricks resources
# MAGIC For the most common Databricks resource types, Databricks supports and recommends declaring resource dependencies for the agent upfront during logging. This enables automatic authentication passthrough when you deploy the agent. With automatic authentication passthrough, Databricks automatically provisions, rotates, and manages short-lived credentials to securely access these resource dependencies from within the agent endpoint.
# MAGIC
# MAGIC To enable automatic authentication, specify the dependent Databricks resources when calling `mlflow.pyfunc.log_model().`
# MAGIC   - **TODO**: If your Unity Catalog tool queries a [vector search index](docs link) or leverages [external functions](docs link), you need to include the dependent vector search index and UC connection objects, respectively, as resources. See docs ([AWS](https://docs.databricks.com/aws/generative-ai/agent-framework/log-agent#resources) | [Azure](https://learn.microsoft.com/azure/databricks/generative-ai/agent-framework/log-agent#resources)).
# MAGIC
# MAGIC   - **TODO**: If the SQL Warehouse powering your Genie space has secured permissions, include the warehouse ID and table name in your resources to enable passthrough authentication. ([AWS](https://docs.databricks.com/aws/generative-ai/agent-framework/log-agent#-specify-resources-for-automatic-authentication-passthrough-system-authentication) | [Azure](https://learn.microsoft.com/azure/databricks/generative-ai/agent-framework/log-agent#-specify-resources-for-automatic-authentication-passthrough-system-authentication)).

# COMMAND ----------

# Determine Databricks resources to specify for automatic auth passthrough at deployment time
import mlflow
from mlflow.models.resources import (
    DatabricksGenieSpace,
    DatabricksServingEndpoint,
    DatabricksSQLWarehouse,
)

resources = [
    DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT_NAME),
    DatabricksGenieSpace(genie_space_id=GENIE_SPACE_ID),
    DatabricksSQLWarehouse(warehouse_id=SQL_WAREHOUSE_ID),
]

print(resources)

# COMMAND ----------

with mlflow.start_run():
    logged_agent_info = mlflow.pyfunc.log_model(
        name=AGENT_NAME,
        python_model=os.path.join(
            os.getcwd(), "agent"
        ),  # point to the agent code
        model_config=os.path.join(
            os.getcwd(), "configs.yaml"
        ),  # point to the config file
        input_example=input_example,
        resources=resources,
        pip_requirements=["-r requirements.txt"],
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-deployment agent validation
# MAGIC Before registering and deploying the agent, perform pre-deployment checks using the [mlflow.models.predict()](https://mlflow.org/docs/latest/python_api/mlflow.models.html#mlflow.models.predict) API. See Databricks documentation ([AWS](https://docs.databricks.com/en/machine-learning/model-serving/model-serving-debug.html#validate-inputs) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/model-serving/model-serving-debug#before-model-deployment-validation-checks)).

# COMMAND ----------

mlflow.models.predict(
    model_uri=f"runs:/{logged_agent_info.run_id}/{AGENT_NAME}",
    input_data=input_example,
    env_manager="uv",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register the model to Unity Catalog
# MAGIC
# MAGIC Update the `catalog`, `schema`, and `model_name` below to register the MLflow model to Unity Catalog.

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")
UC_MODEL_NAME = f"{CATALOG}.{SCHEMA}.{UC_MODEL}"

uc_registered_model_info = mlflow.register_model(
    model_uri=logged_agent_info.model_uri, name=UC_MODEL_NAME
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy the agent

# COMMAND ----------

from databricks import agents

agents.deploy(
    UC_MODEL_NAME,
    uc_registered_model_info.version,
    tags={"endpointSource": "docs"},
    environment_vars={
        "DATABRICKS_GENIE_PAT": f"{{{{secrets/{SECRET_SCOPE_NAME}/{SECRET_KEY_NAME}}}}}"
    },
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC
# MAGIC After your agent is deployed, you can chat with it in AI playground to perform additional checks, share it with SMEs in your organization for feedback, or embed it in a production application. See Databricks documentation ([AWS](https://docs.databricks.com/en/generative-ai/deploy-agent.html) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/generative-ai/deploy-agent)).
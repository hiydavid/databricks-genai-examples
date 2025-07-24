# Databricks notebook source
# MAGIC %md
# MAGIC # Multi-agent system with Genie
# MAGIC
# MAGIC Based on: https://docs.databricks.com/aws/en/generative-ai/agent-framework/multi-agent-genie
# MAGIC
# MAGIC In this notebook, you:
# MAGIC 1. Author a multi-agent system using LangGraph.
# MAGIC 1. Wrap the LangGraph agent with MLflow `ChatAgent` to ensure compatibility with Databricks features.
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

# MAGIC %pip install -U -qqq mlflow-skinny[databricks] langgraph==0.3.4 databricks-langchain databricks-agents uv

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
TABLES = databricks_configs.get("tables")
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

# Check if the experiment exists
experiment = mlflow.get_experiment_by_name(experiment_fqdn)

if experiment:
    experiment_id = experiment.experiment_id
else:
    # Create the experiment if it does not exist
    experiment_id = mlflow.create_experiment(experiment_fqdn)

mlflow.set_experiment(experiment_fqdn)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Load the `parallel-genie-multiagent` notebook
# MAGIC
# MAGIC Create a multi-agent system in LangGraph using a supervisor agent node directing the following agent nodes:
# MAGIC - **GenieAgent**: The Genie agent that queries and reasons over structured data.
# MAGIC - **Tool-calling agent**: An agent that calls Unity Catalog function tools.
# MAGIC - **Research planner agent**: An agent that can create a research plan and then make parallel calls to the GenieAgent for complex multi-step reasoning questions.
# MAGIC
# MAGIC In this example, the tool-calling agent uses the built-in Unity Catalog function `system.ai.python_exec` to execute Python code.
# MAGIC For examples of other tools you can add to your agents, see Databricks documentation ([AWS](https://docs.databricks.com/aws/generative-ai/agent-framework/agent-tool) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/generative-ai/agent-framework/agent-tool)).
# MAGIC
# MAGIC
# MAGIC #### Wrap the LangGraph agent using the `ChatAgent` interface
# MAGIC
# MAGIC Databricks recommends using `ChatAgent` to ensure compatibility with Databricks AI features and to simplify authoring multi-turn conversational agents using an open source standard.
# MAGIC
# MAGIC The `LangGraphChatAgent` class implements the `ChatAgent` interface to wrap the LangGraph agent.
# MAGIC
# MAGIC See MLflow's [ChatAgent documentation](https://mlflow.org/docs/latest/python_api/mlflow.pyfunc.html#mlflow.pyfunc.ChatAgent).
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run ./parallel-genie-multiagent

# COMMAND ----------

from IPython.display import Image, display

display(Image(AGENT.agent.get_graph().draw_mermaid_png()))

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
from databricks_langchain import UnityCatalogTool, VectorSearchRetrieverTool
from mlflow.models.resources import (
    DatabricksFunction,
    DatabricksGenieSpace,
    DatabricksServingEndpoint,
    DatabricksSQLWarehouse,
    DatabricksTable,
)

resources = [
    DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT_NAME),
    DatabricksGenieSpace(genie_space_id=GENIE_SPACE_ID),
    DatabricksSQLWarehouse(warehouse_id=SQL_WAREHOUSE_ID),
]

for table in TABLES:
    resources.append(
        DatabricksTable(
            table_name=f"{CATALOG}.{SCHEMA}.{table}",
        )
    )

for tool in tools:
    if isinstance(tool, VectorSearchRetrieverTool):
        resources.extend(tool.resources)
    elif isinstance(tool, UnityCatalogTool):
        resources.append(DatabricksFunction(function_name=tool.uc_function_name))

print(resources)

# COMMAND ----------

from pkg_resources import get_distribution

with mlflow.start_run():
    logged_agent_info = mlflow.pyfunc.log_model(
        name=AGENT_NAME,
        python_model=os.path.join(
            os.getcwd(), "parallel-genie-multiagent"
        ),  # point to the agent code
        model_config=os.path.join(
            os.getcwd(), "configs.yaml"
        ),  # point to the config file
        input_example=input_example,
        resources=resources,
        pip_requirements=[
            f"databricks-connect=={get_distribution('databricks-connect').version}",
            f"mlflow=={get_distribution('mlflow').version}",
            f"databricks-langchain=={get_distribution('databricks-langchain').version}",
            f"langgraph=={get_distribution('langgraph').version}",
        ],
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

# register the model to UC
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

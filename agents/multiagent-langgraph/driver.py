# Databricks notebook source
# MAGIC %md
# MAGIC # Driver Notebook

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC # Enables autoreload; learn more at https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules
# MAGIC # To disable autoreload; run %autoreload 0

# COMMAND ----------

# MAGIC %pip install -q -U -r requirements.txt
# MAGIC %pip install uv --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # Load Configs

# COMMAND ----------

import mlflow

configs = mlflow.models.ModelConfig(development_config="./configs.yaml")

databricks_configs = configs.get("databricks_configs")
tool_configs = configs.get("tool_configs")
agent_configs = configs.get("agent_configs")

catalog = databricks_configs.get("catalog")
schema = databricks_configs.get("schema")
mlflow_experiment_name = databricks_configs.get("mlflow_experiment_name")
model_name = databricks_configs["agent_name"]
eval_table_name = databricks_configs.get("eval_table_name")

vs_indexes = tool_configs.get("retrievers").get("indexes")
uc_tools = tool_configs.get("uc_tools")
llm_endpoint = agent_configs.get("validator_agent").get("llm").get("llm_endpoint_name")
embedding_model_endpoint = tool_configs.get("retrievers").get("embedding_model")


# COMMAND ----------

import os

from dbruntime.databricks_repl_context import get_context
from mlflow.utils.databricks_utils import get_databricks_host_creds

HOSTNAME = get_context().browserHostName
USERNAME = get_context().user
TOKEN = dbutils.secrets.get(
    "secret-scope", "secret-key"
)  # TODO: Replace with your own token

os.environ["DATABRICKS_TOKEN"] = TOKEN
os.environ["DATABRICKS_URL"] = get_context().apiUrl

# COMMAND ----------

experiment_fqdn = f"{os.getcwd()}/{mlflow_experiment_name}"
experiment = mlflow.get_experiment_by_name(experiment_fqdn)

if experiment:
    experiment_id = experiment.experiment_id
else:
    experiment_id = mlflow.create_experiment(experiment_fqdn)

mlflow.set_experiment(experiment_fqdn)

# COMMAND ----------

# MAGIC %md
# MAGIC # Run Agent Code

# COMMAND ----------

# MAGIC %run ./agent

# COMMAND ----------

# MAGIC %md
# MAGIC # Test Agent

# COMMAND ----------

test_questions = [
    "What risks does NVDA face in terms of AI advancements coming from China?",
    "How is Amazon's Tranium chip contributing to its AWS business?",
    "what is tesla's robotaxi strategy?",
    "what is Meta's AI strategy?",
    "What is Exxon Mobile doing in renewables if at all?",
    "What is the TAM for robotaxis according to Tesla?",
    "What risks does Trump's tariffs have on Home Depot?",
]

example_input = {
    "messages": [
        {
            "role": "user",
            "content": test_questions[0],
        }
    ]
}

example_input

# COMMAND ----------

response = AGENT.predict(example_input)
print(response.messages[0].content)

# COMMAND ----------

# MAGIC %md
# MAGIC # Evaluate Agent

# COMMAND ----------

# MAGIC %md
# MAGIC ## Curated Dataset

# COMMAND ----------

from mlflow.genai import datasets, evaluate, scorers

EVAL_DATASET_NAME = f"{catalog}.{schema}.{eval_table_name}"
dataset = datasets.get_dataset(EVAL_DATASET_NAME)
evals_df = dataset.to_df()

display(evals_df)

# COMMAND ----------


def predict_fn(messages):
    return AGENT.predict(messages=[ChatAgentMessage(**message) for message in messages])


# COMMAND ----------

evals = evaluate(
    data=evals_df, predict_fn=predict_fn, scorers=scorers.get_all_scorers()
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Log Agent as Model

# COMMAND ----------

import mlflow
from mlflow.models.resources import (
    DatabricksFunction,
    DatabricksServingEndpoint,
    DatabricksSQLWarehouse,
    DatabricksTable,
    DatabricksUCConnection,
    DatabricksVectorSearchIndex,
)

with mlflow.start_run():
    logged_chain_info = mlflow.pyfunc.log_model(
        name=model_name,
        python_model=os.path.join(os.getcwd(), "agent"),
        model_config=os.path.join(os.getcwd(), "configs.yaml"),
        input_example=example_input,
        resources=[
            DatabricksVectorSearchIndex(
                index_name=f"{catalog}.{schema}.{vs_indexes.get('sec_10k_business').get('index_name')}"
            ),
            DatabricksVectorSearchIndex(
                index_name=f"{catalog}.{schema}.{vs_indexes.get('sec_10k_others').get('index_name')}"
            ),
            DatabricksVectorSearchIndex(
                index_name=f"{catalog}.{schema}.{vs_indexes.get('earnings_call').get('index_name')}"
            ),
            DatabricksServingEndpoint(endpoint_name=llm_endpoint),
            DatabricksServingEndpoint(endpoint_name=embedding_model_endpoint),
            DatabricksFunction(
                function_name=f"{catalog}.{schema}.{uc_tools.get('validator').get('by_name')}"
            ),
            DatabricksFunction(
                function_name=f"{catalog}.{schema}.{uc_tools.get('validator').get('by_ticker')}"
            ),
        ],
        pip_requirements=["-r requirements.txt"],
    )

# COMMAND ----------

mlflow.models.predict(
    model_uri=f"runs:/{logged_chain_info.run_id}/{model_name}",
    input_data=example_input,
    env_manager="uv",
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Register Agent as Model

# COMMAND ----------

# register the model to UC
mlflow.set_registry_uri("databricks-uc")

uc_registered_model_info = mlflow.register_model(
    model_uri=logged_chain_info.model_uri, name=f"{catalog}.{schema}.{model_name}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Deploy Agent

# COMMAND ----------

from databricks import agents

agents.deploy(
    model_name=uc_registered_model_info.name,
    model_version=uc_registered_model_info.version,
    scale_to_zero=True,
    environment_vars={
        "DATABRICKS_URL": os.environ["DATABRICKS_URL"],
        "DATABRICKS_TOKEN": os.environ["DATABRICKS_TOKEN"],
    },
)

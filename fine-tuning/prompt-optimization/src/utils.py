import mlflow
import yaml
import os
from openai import OpenAI
from pyspark.sql import SparkSession


def load_config(config_path: str = None):
    if config_path is None:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(base_dir, "config.yaml")

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    if "mlflow_experiment_name" in config and "user" in config:
        config["mlflow_experiment_name"] = config["mlflow_experiment_name"].format(
            user=config["user"]
        )

    return config


def setup_mlflow():
    config = load_config()
    if "DATABRICKS_HOST" not in os.environ and "model_serving_base_url" in config:
        os.environ["DATABRICKS_HOST"] = config["model_serving_base_url"]

    mlflow.set_registry_uri("databricks-uc")
    mlflow.set_tracking_uri("databricks")

    experiment_name = config["mlflow_experiment_name"]

    try:
        experiment_id = mlflow.create_experiment(name=experiment_name)
        mlflow.set_experiment(experiment_name)
        print(
            f"Created and set new experiment: {experiment_name} (ID: {experiment_id})"
        )
    except mlflow.exceptions.RestException as e:
        if "already exists" in str(e):
            print(f"Set to existing MLflow experiment: {experiment_name}")
            mlflow.set_experiment(experiment_name)
        elif "401" in str(e) or "Credential" in str(e):
            print(
                f"Error: Authentication failed. Please ensure you have set your Databricks credentials."
            )
            print(
                f"You can set DATABRICKS_HOST and DATABRICKS_TOKEN environment variables, or configure the Databricks CLI."
            )
            print(f"Current Host: {os.environ.get('DATABRICKS_HOST')}")
            raise e
        else:
            raise e

    mlflow.openai.autolog()


def create_predict_fn(prompt_uri: str):
    prompt = mlflow.genai.load_prompt(prompt_uri)

    def predict_fn(query: str):
        config = load_config()
        api_key = os.environ.get("DATABRICKS_TOKEN")
        if not api_key:
            try:
                from databricks.sdk.runtime import dbutils

                api_key = dbutils.secrets.get(
                    scope=config["databricks_secret_scope"],
                    key=config["databricks_secret_key"],
                )
            except ImportError:
                pass

        if not api_key:
            raise ValueError(
                "DATABRICKS_TOKEN not found in environment and dbutils not available."
            )

        client = OpenAI(
            api_key=api_key,
            base_url=config["model_serving_base_url"] + "/serving-endpoints/",
        )

        response_format = {
            "type": "json_schema",
            "json_schema": {
                "name": "lease_agreement_extraction",
                "schema": {
                    "type": "object",
                    "properties": {
                        "start_date": {"type": "string"},
                        "end_date": {"type": "string"},
                        "leased_space": {"type": "string"},
                        "lessee": {"type": "string"},
                        "lessor": {"type": "string"},
                        "signing_date": {"type": "string"},
                        "term_of_payment": {"type": "string"},
                        "designated_use": {"type": "string"},
                        "extension_period": {"type": "string"},
                        "expiration_date_of_lease": {"type": "string"},
                    },
                    "additionalProperties": False,
                    "required": [
                        "start_date",
                        "end_date",
                        "leased_space",
                        "lessee",
                        "lessor",
                        "signing_date",
                        "term_of_payment",
                        "designated_use",
                        "extension_period",
                        "expiration_date_of_lease",
                    ],
                },
                "strict": True,
            },
        }

        messages = [
            {
                "role": "user",
                "content": prompt.format(query=query),
            },
        ]

        response = client.chat.completions.create(
            model=config["model_serving_endpoint"],
            messages=messages,
            response_format=response_format,
        )

        return response

    return predict_fn

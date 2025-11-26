import difflib
import json
import os

import mlflow
import yaml
from mlflow.entities import Feedback
from mlflow.genai.scorers import scorer
from openai import OpenAI

# Field names for entity extraction - used in response_format and scorers
EXTRACTION_FIELDS = [
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
]


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
    import os

    os.environ["MLFLOW_GENAI_EVAL_MAX_WORKERS"] = "1"
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
                        field: {"type": "string"} for field in EXTRACTION_FIELDS
                    },
                    "additionalProperties": False,
                    "required": EXTRACTION_FIELDS,
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


def fuzzy_match_score(predicted_value: str, expected_value: str) -> float:
    """
    Calculate fuzzy match score between predicted and expected values.
    Returns a score between 0.0 and 1.0, where 1.0 is a perfect match.
    """
    if not predicted_value and not expected_value:
        return 1.0
    if not predicted_value or not expected_value:
        return 0.0

    pred_str = str(predicted_value).lower().strip()
    exp_str = str(expected_value).lower().strip()

    return difflib.SequenceMatcher(None, pred_str, exp_str).ratio()


def _extract_text_from_content(content) -> str:
    """
    Extract text content from various message content formats.
    Handles plain strings and list of content blocks (with reasoning/text types).
    """
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        # Find the text block in content list (skip reasoning blocks)
        for block in content:
            if isinstance(block, dict) and block.get("type") == "text":
                return block.get("text", "")
        # Fallback: return first string-like content
        for block in content:
            if isinstance(block, str):
                return block
    return ""


def extract_field_from_json(json_data, field_name: str) -> str:
    """
    Extract a specific field value from various JSON structures.
    Handles ChatCompletion objects, direct JSON strings, and nested structures.
    Returns empty string if field not found or JSON is invalid.
    """
    try:
        # Handle ChatCompletion object (direct from OpenAI client)
        if hasattr(json_data, "choices") and len(json_data.choices) > 0:
            content = json_data.choices[0].message.content
            text_content = _extract_text_from_content(content)
            data = json.loads(text_content)
            field_value = data.get(field_name)
        # Handle dict with nested "choices" (serialized ChatCompletion)
        elif isinstance(json_data, dict) and "choices" in json_data:
            content = json_data["choices"][0]["message"]["content"]
            text_content = _extract_text_from_content(content)
            data = (
                json.loads(text_content)
                if isinstance(text_content, str)
                else text_content
            )
            field_value = data.get(field_name)
        # Handle dict with expected_response (expectations format)
        elif isinstance(json_data, dict) and "expected_response" in json_data:
            nested = json_data["expected_response"]
            nested_json = json.loads(nested) if isinstance(nested, str) else nested
            field_value = nested_json.get(field_name)
        # Handle direct dict with field values
        elif isinstance(json_data, dict):
            field_value = json_data.get(field_name)
        # Handle JSON string
        elif isinstance(json_data, str):
            data = json.loads(json_data)
            # Recursively handle parsed JSON
            return extract_field_from_json(data, field_name)
        else:
            return ""

        if field_value is None:
            return ""
        return str(field_value).strip()
    except (json.JSONDecodeError, TypeError, KeyError, AttributeError, IndexError):
        return ""


def create_fuzzy_scorer(field_name: str, threshold: float = 0.7):
    """
    Factory function to create a fuzzy matching scorer for a specific field.

    Args:
        field_name: The JSON field name to extract and compare
        threshold: Minimum score (0.0-1.0) to pass. Default 0.7.

    Returns:
        A scorer function decorated with @scorer that returns numeric score (0.0-1.0)
    """

    @scorer(name=field_name)
    def field_scorer(outputs, expectations) -> Feedback:
        predicted = extract_field_from_json(outputs, field_name)
        expected = extract_field_from_json(expectations, field_name)
        score = fuzzy_match_score(predicted, expected)

        return Feedback(
            value=score,
            rationale=f"Fuzzy match score: {score:.1%}. Predicted: '{predicted}', Expected: '{expected}'. Threshold: {threshold:.0%}",
        )

    return field_scorer


def create_fuzzy_scorers(fields: list[str] = None, threshold: float = 0.7) -> list:
    """
    Create fuzzy matching scorers for all specified fields.

    Args:
        fields: List of field names. Defaults to EXTRACTION_FIELDS.
        threshold: Minimum score to pass. Default 0.7.

    Returns:
        List of scorer functions ready for use with mlflow.genai.evaluate()
    """
    if fields is None:
        fields = EXTRACTION_FIELDS

    return [create_fuzzy_scorer(field, threshold) for field in fields]

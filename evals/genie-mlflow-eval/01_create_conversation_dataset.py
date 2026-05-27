# Databricks notebook source
# MAGIC %md
# MAGIC # 01. Create Conversation Evaluation Dataset
# MAGIC
# MAGIC Create or update an MLflow GenAI evaluation dataset containing scripted
# MAGIC multi-turn conversation scenarios for an existing Genie Space.
# MAGIC
# MAGIC Edit the user configuration and `SCENARIOS` before running this notebook.

# COMMAND ----------

import mlflow
import pandas as pd

# COMMAND ----------

# User configuration. Match these values to `00_setup.py`.
MLFLOW_EXPERIMENT_NAME = "/Shared/genie-mlflow-eval"
EVAL_DATASET_NAME = "main.default.genie_conversation_eval_dataset"

# COMMAND ----------

mlflow.set_tracking_uri("databricks")
mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)

experiment = mlflow.get_experiment_by_name(MLFLOW_EXPERIMENT_NAME)
if experiment is None:
    raise RuntimeError(f"Could not load experiment: {MLFLOW_EXPERIMENT_NAME}")

print(f"MLflow experiment: {MLFLOW_EXPERIMENT_NAME}")
print(f"Evaluation dataset: {EVAL_DATASET_NAME}")

# COMMAND ----------

# Scenarios tailored to your test Genie Space

SCENARIOS = [
    {
        "scenario_id": "transaction_volume_by_channel",
        "turns": [
            {"content": "What was the total transaction amount in USD last quarter?"},
            {"content": "Break that down by channel."},
        ],
        "success_criteria": [
            "The assistant answers both turns using the same conversation context.",
            "The second answer treats 'that' as the total transaction amount from the first turn.",
            "The breakdown includes channels such as ATM, Mobile, Online, Branch, and Wire.",
            "Generated SQL references the transactions table and filters to the correct quarter.",
        ],
        "expected_sql_patterns": ["GROUP BY", "channel", "amount_usd"],
    },
    {
        "scenario_id": "flagged_transactions_by_category",
        "turns": [
            {"content": "How many transactions were flagged this year?"},
            {
                "content": "Which merchant categories do those flagged transactions fall into?"
            },
        ],
        "success_criteria": [
            "The assistant preserves the flagged-transaction filter across turns.",
            "The second answer groups by merchant_category.",
            "Results include categories like Groceries, Entertainment, Dining, or Travel.",
            "Generated SQL uses is_flagged = true as a filter in both turns.",
        ],
        "expected_sql_patterns": ["is_flagged", "merchant_category", "GROUP BY"],
    },
    {
        "scenario_id": "wealth_management_account_types",
        "turns": [
            {"content": "How many customers are in the Wealth Management segment?"},
            {
                "content": "What account types do they hold and what is the average balance for each?"
            },
        ],
        "success_criteria": [
            "The assistant connects customers to accounts via customer_id.",
            "The second turn preserves the Wealth Management segment filter.",
            "Results reference account types like Checking, Savings, Credit Card, Mortgage, etc.",
            "The average balance calculation uses current_balance_usd from the accounts table.",
        ],
        "expected_sql_patterns": ["JOIN", "customer_segment", "account_type", "AVG"],
    },
    {
        "scenario_id": "delinquent_accounts_by_region",
        "turns": [
            {"content": "How many accounts are currently in Delinquent status?"},
            {"content": "Which region has the most delinquent accounts?"},
        ],
        "success_criteria": [
            "The assistant correctly filters accounts by status = 'Delinquent'.",
            "The second turn joins to customers or branches to determine region.",
            "The response identifies a specific region (Northeast, Southeast, Midwest, West, or Southwest).",
            "Context about the delinquent filter is preserved between turns.",
        ],
        "expected_sql_patterns": ["Delinquent", "region", "GROUP BY"],
    },
    {
        "scenario_id": "branch_operating_cost_efficiency",
        "turns": [
            {
                "content": "What is the total monthly operating cost across all active branches?"
            },
            {"content": "Which branch type has the lowest cost per headcount?"},
        ],
        "success_criteria": [
            "The first turn filters branches by is_active = true.",
            "The second turn computes cost-per-headcount (monthly_operating_cost_usd / headcount).",
            "Results distinguish between Full Service, Limited Service, and Drive-Through branch types.",
            "The assistant returns a clear ranking or identifies the most efficient type.",
        ],
        "expected_sql_patterns": [
            "branch_type",
            "monthly_operating_cost_usd",
            "headcount",
        ],
    },
]

# COMMAND ----------


def validate_scenario(scenario: dict) -> None:
    if not scenario.get("scenario_id"):
        raise ValueError("Each scenario needs a non-empty scenario_id.")
    turns = scenario.get("turns")
    if not isinstance(turns, list) or len(turns) < 2:
        raise ValueError(
            f"Scenario {scenario['scenario_id']} must contain at least two turns."
        )
    for index, turn in enumerate(turns):
        if not isinstance(turn, dict) or not turn.get("content"):
            raise ValueError(
                f"Scenario {scenario['scenario_id']} turn {index} needs content."
            )
    criteria = scenario.get("success_criteria")
    if not isinstance(criteria, list) or not criteria:
        raise ValueError(f"Scenario {scenario['scenario_id']} needs success_criteria.")


def build_record(scenario: dict) -> dict:
    validate_scenario(scenario)

    expectations = {"success_criteria": scenario["success_criteria"]}
    if scenario.get("expected_sql_patterns"):
        expectations["expected_sql_patterns"] = scenario["expected_sql_patterns"]

    return {
        "inputs": {
            "scenario_id": scenario["scenario_id"],
            "turns": [{"content": turn["content"]} for turn in scenario["turns"]],
        },
        "expectations": expectations,
    }


records = [build_record(scenario) for scenario in SCENARIOS]
pd.DataFrame(records)

# COMMAND ----------


def get_or_create_dataset(name: str):
    try:
        dataset = mlflow.genai.datasets.get_dataset(name=name)
        print(f"Loaded existing dataset: {name}")
        return dataset
    except Exception as exc:
        print(f"Creating dataset: {name}")
        print(f"Reason existing dataset was not loaded: {exc.__class__.__name__}")
        return mlflow.genai.datasets.create_dataset(
            name=name,
            experiment_id=[experiment.experiment_id],
        )


eval_dataset = get_or_create_dataset(EVAL_DATASET_NAME)
eval_dataset.merge_records(records)
eval_dataset = mlflow.genai.datasets.get_dataset(name=EVAL_DATASET_NAME)

print(f"Merged {len(records)} scenario records into {EVAL_DATASET_NAME}.")

# COMMAND ----------

preview_df = eval_dataset.to_df()
print(f"Dataset rows: {len(preview_df)}")
display(preview_df[["inputs", "expectations"]])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next
# MAGIC
# MAGIC Run `02_run_genie_sdk_conversations.py` to execute these scenarios through the
# MAGIC Genie API and log MLflow traces.

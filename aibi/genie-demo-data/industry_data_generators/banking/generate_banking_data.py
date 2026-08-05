# Databricks notebook source

# MAGIC %md
# MAGIC # Bigly Bank — Run All Data Generators
# MAGIC
# MAGIC Thin orchestrator for the domain notebooks in this folder. The caller
# MAGIC chooses the Unity Catalog and schema prefix; this notebook does not write
# MAGIC domain tables itself.

# COMMAND ----------

from datetime import date
import json

try:
    dbutils.widgets.text("catalog", "", "Unity Catalog (required)")
except Exception:
    pass
try:
    dbutils.widgets.text("schema_prefix", "", "Schema prefix (required)")
except Exception:
    pass
try:
    dbutils.widgets.text("seed", "42", "Deterministic seed")
except Exception:
    pass
try:
    dbutils.widgets.text("as_of_date", "2025-12-31", "Inclusive as-of date")
except Exception:
    pass
try:
    dbutils.widgets.text(
        "notebook_base_path",
        "",
        "Optional workspace folder containing the child notebooks",
    )
except Exception:
    pass
try:
    dbutils.widgets.dropdown(
        "enable_finance",
        "false",
        ["true", "false"],
        "Generate the optional Finance and Treasury domain",
    )
except Exception:
    pass

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA_PREFIX = dbutils.widgets.get("schema_prefix").strip()
SEED = dbutils.widgets.get("seed").strip()
AS_OF_DATE = dbutils.widgets.get("as_of_date").strip()
NOTEBOOK_BASE_PATH = dbutils.widgets.get("notebook_base_path").strip().rstrip("/")
ENABLE_FINANCE = dbutils.widgets.get("enable_finance").strip().lower() == "true"

if not CATALOG:
    raise ValueError("catalog is required")
if not SCHEMA_PREFIX:
    raise ValueError("schema_prefix is required")
if "`" in CATALOG or "`" in SCHEMA_PREFIX:
    raise ValueError("catalog and schema_prefix cannot contain backticks")

int(SEED)
date.fromisoformat(AS_OF_DATE)


def child_path(file_name):
    if NOTEBOOK_BASE_PATH:
        return f"{NOTEBOOK_BASE_PATH}/{file_name}"
    return f"./{file_name}"


common_arguments = {
    "catalog": CATALOG,
    "schema_prefix": SCHEMA_PREFIX,
    "seed": SEED,
    "as_of_date": AS_OF_DATE,
}

generation_notebooks = [
    ("core", "generate_banking_core_data.py"),
    ("retail_deposits", "generate_banking_deposits_data.py"),
    ("credit_cards", "generate_banking_cards_data.py"),
    ("consumer_lending", "generate_banking_consumer_lending_data.py"),
    ("commercial_banking", "generate_banking_commercial_data.py"),
    ("wealth_management", "generate_banking_wealth_data.py"),
    ("service_operations", "generate_banking_service_ops_data.py"),
    ("financial_crime", "generate_banking_financial_crime_data.py"),
]

if ENABLE_FINANCE:
    generation_notebooks.append(("finance_treasury", "generate_banking_finance_data.py"))

results = {}
for phase_name, file_name in generation_notebooks:
    path = child_path(file_name)
    print(f"Starting {phase_name}: {path}")
    results[phase_name] = json.loads(
        dbutils.notebook.run(path, 0, common_arguments)
    )
    print(f"Completed {phase_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Semantic layer and cross-domain validation

# COMMAND ----------

final_arguments = {
    **common_arguments,
    "enable_finance": str(ENABLE_FINANCE).lower(),
}

semantic_path = child_path("generate_banking_semantic_layer.py")
print(f"Starting semantic_layer: {semantic_path}")
results["semantic_layer"] = json.loads(
    dbutils.notebook.run(semantic_path, 0, final_arguments)
)

validation_path = child_path("validate_banking_data.py")
print(f"Starting validation: {validation_path}")
results["validation"] = json.loads(
    dbutils.notebook.run(validation_path, 0, final_arguments)
)

print(
    f"Bigly Bank generation complete in catalog {CATALOG} "
    f"with schema prefix {SCHEMA_PREFIX}."
)
dbutils.notebook.exit(
    json.dumps(
        {
            "catalog": CATALOG,
            "schema_prefix": SCHEMA_PREFIX,
            "finance_enabled": ENABLE_FINANCE,
            "phases": results,
        }
    )
)

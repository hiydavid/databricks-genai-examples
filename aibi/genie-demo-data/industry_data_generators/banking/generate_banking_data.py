# Databricks notebook source

# MAGIC %md
# MAGIC # Bigly Bank — Run All Data Generators
# MAGIC
# MAGIC Thin orchestrator for the domain notebooks in this folder. The caller
# MAGIC chooses the Unity Catalog and schema prefix; this notebook does not write
# MAGIC domain tables itself.
# MAGIC
# MAGIC **How to run:** set `DEFAULT_CATALOG` and `DEFAULT_SCHEMA_PREFIX` in the
# MAGIC Configuration cell below, then click **Run All**. Widget values — and any
# MAGIC parameters passed by a job or parent notebook — override those defaults;
# MAGIC clear a widget to fall back to its default.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from datetime import date
import json

# =============================================================================
# CONFIGURATION — edit these values, then Run All
# Widget values and parameters passed by a job or parent notebook override
# these defaults; clear a widget to fall back to its default.
# =============================================================================
DEFAULT_CATALOG = ""  # REQUIRED: your Unity Catalog, e.g. "my_catalog"
DEFAULT_SCHEMA_PREFIX = ""  # REQUIRED: schema prefix, e.g. "bigly_bank"
DEFAULT_SEED = "42"  # deterministic seed shared by every child notebook
DEFAULT_AS_OF_DATE = "2025-12-31"  # inclusive end date for generated history
DEFAULT_NOTEBOOK_BASE_PATH = ""  # blank = child notebooks sit next to this one
DEFAULT_ENABLE_FINANCE = "false"  # "true" adds the Finance & Treasury domain
# =============================================================================


def widget_value(name, default, label, choices=None):
    """Read a widget, falling back to the configuration default above.

    Widgets keep the notebook parameterizable: a job, a parent notebook, or a
    manual entry in the widget panel overrides the defaults.
    """
    try:
        if choices:
            dbutils.widgets.dropdown(name, default, choices, label)
        else:
            dbutils.widgets.text(name, default, label)
    except Exception:
        pass
    try:
        return dbutils.widgets.get(name).strip() or default
    except Exception:
        return default


CATALOG = widget_value("catalog", DEFAULT_CATALOG, "Unity Catalog (required)")
SCHEMA_PREFIX = widget_value(
    "schema_prefix", DEFAULT_SCHEMA_PREFIX, "Schema prefix (required)"
)
SEED = widget_value("seed", DEFAULT_SEED, "Deterministic seed")
AS_OF_DATE = widget_value("as_of_date", DEFAULT_AS_OF_DATE, "Inclusive as-of date")
NOTEBOOK_BASE_PATH = widget_value(
    "notebook_base_path",
    DEFAULT_NOTEBOOK_BASE_PATH,
    "Optional workspace folder containing the child notebooks",
).rstrip("/")
ENABLE_FINANCE = (
    widget_value(
        "enable_finance",
        DEFAULT_ENABLE_FINANCE,
        "Generate the optional Finance and Treasury domain",
        ["true", "false"],
    ).lower()
    == "true"
)

if not CATALOG:
    raise ValueError(
        "catalog is required — set DEFAULT_CATALOG in the Configuration cell "
        "or pass the catalog widget/parameter"
    )
if not SCHEMA_PREFIX:
    raise ValueError(
        "schema_prefix is required — set DEFAULT_SCHEMA_PREFIX in the "
        "Configuration cell or pass the schema_prefix widget/parameter"
    )
if "`" in CATALOG or "`" in SCHEMA_PREFIX:
    raise ValueError("catalog and schema_prefix cannot contain backticks")

int(SEED)  # fail fast on a non-numeric seed
date.fromisoformat(AS_OF_DATE)  # fail fast on a malformed date

print(
    f"catalog={CATALOG} schema_prefix={SCHEMA_PREFIX} seed={SEED} "
    f"as_of_date={AS_OF_DATE} enable_finance={ENABLE_FINANCE}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run the domain generators

# COMMAND ----------

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

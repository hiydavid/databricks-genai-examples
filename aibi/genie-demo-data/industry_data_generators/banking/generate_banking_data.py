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
# MAGIC
# MAGIC **Compute:** runs on classic clusters (DBR 17.2+) and serverless notebooks
# MAGIC (environment version 5+). Library installs are pinned and restart-free so
# MAGIC the same notebooks work on both.
# MAGIC
# MAGIC **On failure:** `dbutils.notebook.run` raises only a generic
# MAGIC `WorkflowException` when a child notebook fails — the real error lives in
# MAGIC the child's run output. This orchestrator fetches that run output via the
# MAGIC Databricks SDK and prints the child's actual error before stopping.

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
# MAGIC
# MAGIC Generators run in dependency order: the shared CORE dimensions first, then
# MAGIC each business domain. The run stops at the first failure, printing the
# MAGIC child notebook's real error from its run output.

# COMMAND ----------

import time


def child_path(file_name):
    if NOTEBOOK_BASE_PATH:
        return f"{NOTEBOOK_BASE_PATH}/{file_name}"
    return f"./{file_name}"


def report_child_failure(phase_name, path, exc, started_at_ms):
    """Print the child notebook's real error after a dbutils.notebook.run failure.

    dbutils.notebook.run wraps child failures in a generic WorkflowException
    ("Workload failed, see run output for details"). Best-effort: look up the
    failed one-off run via the Databricks SDK and print its output here so the
    actual cause is visible without hunting through the Jobs UI.
    """
    print(f"\n=== FAILED: {phase_name} ({path}) ===")
    first_line = str(exc).splitlines()[:1]
    if first_line:
        print(f"dbutils.notebook.run raised: {first_line[0]}")
    found_details = False
    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        # The failed child is the most recent FAILED run started after this
        # phase began (with a small slack for clock skew).
        for run in w.jobs.list_runs(limit=25):
            started = getattr(run, "start_time", None)
            if started is None or started < started_at_ms - 60_000:
                continue
            state = getattr(run, "state", None)
            result_state = getattr(state, "result_state", None) if state else None
            result_state = getattr(result_state, "value", result_state)
            if result_state != "FAILED":
                continue
            print(
                f"Failed child run: {getattr(run, 'run_name', path)} "
                f"({run.run_page_url})"
            )
            try:
                output = w.jobs.get_run_output(run.run_id)
            except Exception:
                output = None
            error_text = getattr(output, "error", None) if output else None
            if error_text:
                print(f"Child error:\n{error_text}")
                found_details = True
            notebook_output = getattr(output, "notebook_output", None) if output else None
            if notebook_output is not None:
                for field in ("error", "traceback"):
                    text = getattr(notebook_output, field, None)
                    if text:
                        print(f"Child {field}:\n{text}")
                        found_details = True
            break
    except Exception as lookup_error:
        print(f"(Automatic run-output lookup unavailable: {lookup_error})")
    if not found_details:
        print(
            "Open the failed run's output for the full child traceback: "
            "Jobs & Pipelines > Runs, or the 'Run output' link on this cell's "
            "error."
        )


def run_child(phase_name, file_name, arguments):
    """Run one child notebook and return its parsed JSON exit value."""
    path = child_path(file_name)
    print(f"Starting {phase_name}: {path}")
    started_at_ms = int(time.time() * 1000)
    try:
        exit_value = dbutils.notebook.run(path, 0, arguments)
    except Exception as exc:
        report_child_failure(phase_name, path, exc, started_at_ms)
        raise
    try:
        result = json.loads(exit_value)
    except (TypeError, ValueError):
        raise ValueError(
            f"{phase_name} ({path}) exited without JSON — its "
            f"dbutils.notebook.exit value was: {exit_value!r}"
        )
    print(f"Completed {phase_name}")
    return result


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
    results[phase_name] = run_child(phase_name, file_name, common_arguments)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Semantic layer and cross-domain validation

# COMMAND ----------

final_arguments = {
    **common_arguments,
    "enable_finance": str(ENABLE_FINANCE).lower(),
}

results["semantic_layer"] = run_child(
    "semantic_layer", "generate_banking_semantic_layer.py", final_arguments
)

results["validation"] = run_child(
    "validation", "validate_banking_data.py", final_arguments
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

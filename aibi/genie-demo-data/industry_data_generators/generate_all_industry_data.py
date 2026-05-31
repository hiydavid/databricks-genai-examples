# Databricks notebook source

# MAGIC %md
# MAGIC # Bulk Industry Synthetic Dataset Generator
# MAGIC
# MAGIC Runs all industry data generator notebooks in this folder to create the demo schemas in Unity Catalog.
# MAGIC
# MAGIC **Setup:** Import this notebook and the sibling generator notebooks into the same Databricks workspace folder, set the `catalog` widget, then **Run All**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

# =============================================================================
# CONFIGURATION
# =============================================================================
DEFAULT_CATALOG = "my_catalog"

GENERATOR_NOTEBOOKS = [
    {
        "industry": "Horizon Bank",
        "notebook": "generate_banking_data",
        "schema": "horizon_bank",
    },
    {
        "industry": "Hospital Readmission",
        "notebook": "generate_hospital_readmission_data",
        "schema": "hospital_readmission",
    },
    {
        "industry": "Retail Apparel",
        "notebook": "generate_retail_apparel_data",
        "schema": "retail_apparel",
    },
    {
        "industry": "SaaS Churn",
        "notebook": "generate_saas_churn_data",
        "schema": "saas_churn",
    },
    {
        "industry": "Wind Turbine Maintenance",
        "notebook": "generate_wind_turbine_maintenance_data",
        "schema": "wind_turbine_maintenance",
    },
    {
        "industry": "Talent Advisory",
        "notebook": "generate_talent_advisory_data",
        "schema": "talent_advisory",
    },
]

try:
    dbutils.widgets.text("catalog", DEFAULT_CATALOG, "Unity Catalog name")
except Exception:
    pass

try:
    dbutils.widgets.text(
        "notebook_base_path",
        "",
        "Workspace folder containing the generator notebooks; leave blank for this folder",
    )
except Exception:
    pass

try:
    dbutils.widgets.dropdown(
        "continue_on_error",
        "false",
        ["false", "true"],
        "Continue running remaining notebooks after a failure",
    )
except Exception:
    pass


def get_widget(name, default=""):
    try:
        return dbutils.widgets.get(name).strip()
    except Exception:
        return default


CATALOG = get_widget("catalog", DEFAULT_CATALOG) or DEFAULT_CATALOG
NOTEBOOK_BASE_PATH = get_widget("notebook_base_path", "")
CONTINUE_ON_ERROR = get_widget("continue_on_error", "false").lower() in ("1", "true", "yes", "y")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Notebook Runner

# COMMAND ----------


def current_notebook_dir():
    context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    return context.notebookPath().get().rsplit("/", 1)[0]


def resolve_base_path():
    if NOTEBOOK_BASE_PATH:
        return NOTEBOOK_BASE_PATH.rstrip("/")
    return current_notebook_dir().rstrip("/")


def join_notebook_path(base_path, notebook_name):
    if not base_path:
        return notebook_name
    return f"{base_path}/{notebook_name}"


def is_notebook_path_error(error, candidate_path):
    message = str(error).lower()
    markers = (
        "notebook not found",
        "notebook does not exist",
        "path not found",
        "path does not exist",
    )
    if any(marker in message for marker in markers):
        return True

    return (
        "resource_does_not_exist" in message
        and candidate_path.lower() in message
        and any(marker in message for marker in ("notebook", "path", "workspace"))
    )


def run_generator(base_path, generator):
    notebook_name = generator["notebook"]
    arguments = {
        "catalog": CATALOG,
        "schema": generator["schema"],
    }
    candidate_paths = [
        join_notebook_path(base_path, notebook_name),
        join_notebook_path(base_path, f"{notebook_name}.py"),
    ]

    try:
        return dbutils.notebook.run(candidate_paths[0], 0, arguments)
    except Exception as first_error:
        if not is_notebook_path_error(first_error, candidate_paths[0]):
            raise

        print(f"Notebook path not found: {candidate_paths[0]}")
        print(f"Retrying with source-file path: {candidate_paths[1]}")
        return dbutils.notebook.run(candidate_paths[1], 0, arguments)


def print_summary(results):
    print("")
    print("=" * 80)
    print("Bulk industry data generation summary")
    print("=" * 80)

    for result in results:
        status = result["status"]
        industry = result["industry"]
        schema = result["schema"]
        print(f"{status:7} {industry:28} {CATALOG}.{schema}")
        if result.get("error"):
            print(f"        {result['error']}")

    succeeded = sum(1 for result in results if result["status"] == "SUCCESS")
    failed = sum(1 for result in results if result["status"] == "FAILED")
    print("-" * 80)
    print(f"Succeeded: {succeeded} | Failed: {failed}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Run All Generators

# COMMAND ----------

base_path = resolve_base_path()
results = []

print(f"Catalog: {CATALOG}")
print(f"Notebook base path: {base_path}")
print(f"Continue on error: {CONTINUE_ON_ERROR}")
print("")

for generator in GENERATOR_NOTEBOOKS:
    industry = generator["industry"]
    schema = generator["schema"]
    print("=" * 80)
    print(f"Running {industry}: {CATALOG}.{schema}")
    print("=" * 80)

    try:
        output = run_generator(base_path, generator)
        results.append(
            {
                "industry": industry,
                "schema": schema,
                "status": "SUCCESS",
                "output": output,
            }
        )
        print(f"Completed {industry}.")
        if output:
            print(f"Notebook output: {output}")
    except Exception as error:
        results.append(
            {
                "industry": industry,
                "schema": schema,
                "status": "FAILED",
                "error": str(error),
            }
        )
        print(f"FAILED {industry}: {error}")
        if not CONTINUE_ON_ERROR:
            print_summary(results)
            raise

print_summary(results)

failed_results = [result for result in results if result["status"] == "FAILED"]
if failed_results:
    failed_industries = ", ".join(result["industry"] for result in failed_results)
    raise RuntimeError(f"One or more industry generators failed: {failed_industries}")

print("All industry synthetic datasets were generated successfully.")

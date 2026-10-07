# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# dependencies = [
#   "databricks-sdk>=0.102.0",
# ]
# ///
# DBTITLE 1,Deploy Genie PBI Import Workflow
# MAGIC %md
# MAGIC # Deploy — Genie PBI Import Workflow
# MAGIC
# MAGIC One-time deployment notebook. Creates the two Genie Code automations (from the prompt `.md` files in `prompts/`) and the 6-task job `genie-pbi-import-workflow` in the current workspace.
# MAGIC
# MAGIC **Prerequisites**
# MAGIC - The **Genie Code Job Task** beta must be enabled for your account/workspace in the [Databricks preview portal](https://previews.databricks.com). Without it, the `genie_task` entries in the job definition are not recognized — the job is still created, but the Genie Code tasks appear in the workflow UI as unconfigured tasks you must set up manually.
# MAGIC - **Partner-powered AI** enabled for the account and workspace (required by `/importBI`).
# MAGIC - The task notebooks and `prompts/*.md` are already uploaded to the workspace (e.g. from inside this example's folder: `databricks workspace import-dir . /Workspace/Users/you@company.com/genie-pbi-import-workflow`; see the README). Workspace `.md` files are plain Workspace files and can be read directly with `open()` on DBR 14.2+.
# MAGIC - Run this notebook in the target workspace. The SDK authenticates with the notebook's own context — no token needed.

# COMMAND ----------

# DBTITLE 1,Widgets
# MAGIC %md
# MAGIC ## Deployment parameters
# MAGIC
# MAGIC These values become the job's default parameters. You can override the per-file values later with **Run now with different parameters**.
# MAGIC
# MAGIC | Widget | Expected value | Example | Notes |
# MAGIC |---|---|---|---|
# MAGIC | `pbit_volume_path` | Full path to the UC Volume directory containing the `.pbit` file | `/Volumes/main/lending_demo/raw_data/powerbi_files` | May end with `/`. This location is independent of the metric-view destination and source datasets. |
# MAGIC | `metric_view_catalog` | Destination catalog for generated metric views | `analytics` | Must contain the destination schema. Leave empty, together with `metric_view_schema`, to make job runs default to dry-run mode. |
# MAGIC | `metric_view_schema` | Destination schema for generated metric views | `power_bi` | Holds the generated metric view and workflow artifacts table. It does not need to contain the PBIT file or underlying source tables. |
# MAGIC | `warehouse_id` | SQL warehouse ID | `abc123def4567890` | Copy the ID from the warehouse URL or warehouse details page. Required when the job creates a Genie Agent. |
# MAGIC | `pbit_filename` | File name ending in `.pbit` | `sales_model.pbit` | Enter only the file name, not a path. Usually overridden for each run. Empty makes the default job run a dry run. |
# MAGIC | `metric_view_name` | Name for the generated metric view | `sales_metrics` | Created as `<metric_view_catalog>.<metric_view_schema>.<metric_view_name>`. Use a valid Unity Catalog object name. Required for a non-dry run. |
# MAGIC | `agent_name` | Display name for the generated Genie Agent | `Sales Analytics` | Required when agent creation is enabled. Usually overridden for each run. |
# MAGIC
# MAGIC The deployed job also has `create_agent` (default `true`) and `agent_instructions` (default empty) parameters. They are not deployment widgets; override them per run when you want to skip agent creation or supply business context.

# COMMAND ----------

# DBTITLE 1,Widget Inputs
# Run this cell first to create the widgets, fill them in, then Run all.
# Saved as job parameter defaults so "Run now" works without extra input. Leave
# destination catalog/schema or pbit_filename empty to make the default run a dry run;
# change them later under Job parameters in the Jobs UI.
dbutils.widgets.text("pbit_volume_path", "")
dbutils.widgets.text("metric_view_catalog", "")
dbutils.widgets.text("metric_view_schema", "")
dbutils.widgets.text("warehouse_id", "")
# Per-file settings: usually overridden per run, one .pbit per run.
dbutils.widgets.text("pbit_filename", "")
dbutils.widgets.text("metric_view_name", "")
dbutils.widgets.text("agent_name", "")

# COMMAND ----------

# DBTITLE 1,Parameters
import json
from pathlib import Path

from databricks.sdk import WorkspaceClient

job_defaults = {
    name: dbutils.widgets.get(name).strip()
    for name in ("pbit_volume_path", "metric_view_catalog", "metric_view_schema", "warehouse_id",
                 "pbit_filename", "metric_view_name", "agent_name")
}

w = WorkspaceClient()
me = w.current_user.me()
uid = str(me.id)

# notebooks/ and prompts/ sit next to this notebook, so derive the root from its own
# workspace path. notebookPath() omits the /Workspace prefix that file reads need.
deploy_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
notebook_root = str(Path(deploy_path).parent)
if not notebook_root.startswith("/Workspace/"):
    notebook_root = f"/Workspace{notebook_root}"
prompts_dir = f"{notebook_root}/prompts"

print("=" * 60)
print("[DEPLOY] Genie PBI Import Workflow")
print("=" * 60)
print(f"  Target workspace: {w.config.host}")
print(f"  User ID:          {uid}")
print(f"  Notebook root:    {notebook_root}")
print(f"  Prompts dir:      {prompts_dir}")
for name, value in job_defaults.items():
    print(f"  {name + ':':<18}{value or '(empty)'}")

# COMMAND ----------

# DBTITLE 1,Helpers
def create_automation(w: WorkspaceClient, uid: str, prompt: str, name: str) -> str:
    """Create a Genie Code automation (no schedule) and return its configuration_id."""
    resp = w.api_client.do(
        "POST",
        "/api/2.0/alerts-internal/scheduled-insights",
        body={
            "parent_asset_name": f"users/{uid}",
            "scheduled_insight": {
                "insight_type": "GENIE_CODE",
                "user_prompt": prompt,
            },
        },
    )
    config_id = resp["name"]
    print(f"  ✓ Created automation from {name} prompt: {config_id}")
    return config_id


def create_job(
    w: WorkspaceClient,
    notebook_root: str,
    import_config_id: str,
    agent_config_id: str,
    job_defaults: dict,
) -> dict:
    """Create the 6-task Genie PBI Import Workflow job."""
    job_def = {
        "name": "genie-pbi-import-workflow",
        "max_concurrent_runs": 1,
        "queue": {"enabled": True},
        "parameters": [
            {"name": "run_id", "default": "{{job.run_id}}"},
            {"name": "pbit_volume_path", "default": job_defaults["pbit_volume_path"]},
            {"name": "metric_view_catalog", "default": job_defaults["metric_view_catalog"]},
            {"name": "metric_view_schema", "default": job_defaults["metric_view_schema"]},
            {"name": "pbit_filename", "default": job_defaults["pbit_filename"]},
            {"name": "metric_view_name", "default": job_defaults["metric_view_name"]},
            {"name": "create_agent", "default": "true"},
            {"name": "agent_name", "default": job_defaults["agent_name"]},
            # Domain context for the agent's general instructions. Empty: derived from
            # the metric view's comments.
            {"name": "agent_instructions", "default": ""},
            {"name": "warehouse_id", "default": job_defaults["warehouse_id"]},
        ],
        "tasks": [
            {
                "task_key": "setup_and_preflight",
                "notebook_task": {
                    "notebook_path": f"{notebook_root}/notebooks/setup_and_preflight",
                    "source": "WORKSPACE",
                },
                "timeout_seconds": 1800,
            },
            {
                "task_key": "import_metric_view",
                "depends_on": [{"task_key": "setup_and_preflight"}],
                # /importBI on a large model can take a while; a 10-minute timeout
                # leaves no room for retries inside the agent.
                "timeout_seconds": 3600,
                "genie_task": {
                    "configuration_id": import_config_id,
                    "parameters": {
                        "run_id": "{{job.parameters.run_id}}",
                        "pbit_volume_path": "{{job.parameters.pbit_volume_path}}",
                        "metric_view_catalog": "{{job.parameters.metric_view_catalog}}",
                        "metric_view_schema": "{{job.parameters.metric_view_schema}}",
                        "pbit_filename": "{{job.parameters.pbit_filename}}",
                        "metric_view_name": "{{job.parameters.metric_view_name}}",
                        "warehouse_id": "{{job.parameters.warehouse_id}}",
                    },
                },
            },
            {
                "task_key": "validate_metric_view",
                "depends_on": [{"task_key": "import_metric_view"}],
                "notebook_task": {
                    "notebook_path": f"{notebook_root}/notebooks/validate_metric_view",
                    "source": "WORKSPACE",
                },
                "timeout_seconds": 1800,
            },
            {
                # Validation failures fail validate_metric_view, so this only decides
                # whether the user asked for an agent.
                "task_key": "should_create_agent",
                "depends_on": [{"task_key": "validate_metric_view"}],
                "condition_task": {
                    "op": "EQUAL_TO",
                    "left": "{{job.parameters.create_agent}}",
                    "right": "true",
                },
            },
            {
                "task_key": "create_genie_agent",
                "depends_on": [{"task_key": "should_create_agent", "outcome": "true"}],
                "timeout_seconds": 1800,
                "genie_task": {
                    "configuration_id": agent_config_id,
                    "parameters": {
                        "run_id": "{{job.parameters.run_id}}",
                        "metric_view_catalog": "{{job.parameters.metric_view_catalog}}",
                        "metric_view_schema": "{{job.parameters.metric_view_schema}}",
                        "pbit_filename": "{{job.parameters.pbit_filename}}",
                        "metric_view_name": "{{job.parameters.metric_view_name}}",
                        "agent_name": "{{job.parameters.agent_name}}",
                        "agent_instructions": "{{job.parameters.agent_instructions}}",
                        "warehouse_id": "{{job.parameters.warehouse_id}}",
                    },
                },
            },
            {
                "task_key": "audit_and_report",
                # Also depends on validate_metric_view: when create_agent is false,
                # create_genie_agent is excluded, and a task whose dependencies are all
                # excluded is excluded too, even with run_if ALL_DONE.
                "depends_on": [
                    {"task_key": "validate_metric_view"},
                    {"task_key": "create_genie_agent"},
                ],
                # Run even when upstream tasks fail or time out, so every run ends with
                # an audit_summary (INCOMPLETE, with errors, when artifacts are missing).
                "run_if": "ALL_DONE",
                "notebook_task": {
                    "notebook_path": f"{notebook_root}/notebooks/audit_and_report",
                    "source": "WORKSPACE",
                },
                "timeout_seconds": 1800,
            },
        ],
    }

    return w.api_client.do("POST", "/api/2.1/jobs/create", body=job_def)

# COMMAND ----------

# DBTITLE 1,Step 1 — Create Genie Code automations
# Read the prompt files from the workspace (uploaded together with the notebooks).
import_prompt_path = Path(f"{prompts_dir}/import_metric_view.md")
agent_prompt_path = Path(f"{prompts_dir}/create_genie_agent.md")

for p in (import_prompt_path, agent_prompt_path):
    if not p.exists():
        raise FileNotFoundError(
            f"Prompt file not found: {p}. "
            "Upload the prompts/ directory to the workspace first (see the README)."
        )

print("Step 1: Creating Genie Code automations...")
import_config_id = create_automation(w, uid, import_prompt_path.read_text(), "import_metric_view")
agent_config_id = create_automation(w, uid, agent_prompt_path.read_text(), "create_genie_agent")

# COMMAND ----------

# DBTITLE 1,Step 2 — Create the job
print("Step 2: Creating job...")
resp = create_job(w, notebook_root, import_config_id, agent_config_id, job_defaults)
job_id = resp.get("job_id")
print(f"  ✓ Created job: {job_id}")
print(f"  URL: {w.config.host}/jobs/{job_id}")

# COMMAND ----------

# DBTITLE 1,Summary
print("=" * 60)
print("Deployment complete!")
print("=" * 60)
print(f"  Job ID:                        {job_id}")
print(f"  import_metric_view automation: {import_config_id}")
print(f"  create_genie_agent automation: {agent_config_id}")
print()
print("To run:")
# Preflight requires these for the default run (create_agent defaults to "true").
required = ("pbit_volume_path", "metric_view_catalog", "metric_view_schema", "pbit_filename",
            "metric_view_name", "agent_name", "warehouse_id")
missing = [k for k in required if not job_defaults[k]]
if not missing:
    print(f"  Click Run now on the job page, or: databricks jobs run-now {job_id}")
else:
    print(f"  Not set: {', '.join(missing)}. A plain Run now is a dry run or fails preflight.")
    print("  Set them under Job parameters, or pass them at run time:")
    placeholders = {k: f"<{k}>" for k in missing}
    print(f"  databricks jobs run-now {job_id} --json '{json.dumps({'job_parameters': placeholders})}'")

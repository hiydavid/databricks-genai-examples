# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Deploy GSO Prototype v2
# MAGIC %md
# MAGIC # Deploy — GSO Prototype
# MAGIC
# MAGIC One-time deployment notebook. Creates the two Genie Code automations (from the prompt `.md` files in `prompts/`) and the 5-task job `gso-prototype-v2` in the current workspace.
# MAGIC
# MAGIC **Prerequisites**
# MAGIC - The **Genie Code Job Task** beta must be enabled for your account/workspace in the [Databricks preview portal](https://previews.databricks.com). Without it, the `genie_task` entries in the job definition are not recognized — the job is still created, but the Genie Code tasks appear in the workflow UI as unconfigured tasks you must set up manually.
# MAGIC - The task notebooks and `prompts/*.md` are already uploaded to the workspace (e.g. via `databricks workspace import-dir ./genie-optimization-workflow /Workspace/Users/you@company.com/gso-prototype`). Workspace `.md` files are plain Workspace files and can be read directly with `open()` on DBR 14.2+.
# MAGIC - Run this notebook in the target workspace. The SDK authenticates with the notebook's own context — no token needed.

# COMMAND ----------

# DBTITLE 1,Parameters
from pathlib import Path

from databricks.sdk import WorkspaceClient

# -- Parameters --
# Workspace directory holding the task notebooks + prompts/ (default: your home + /gso-prototype)
dbutils.widgets.text("notebook_root", "")
# Directory holding the prompt .md files (default: <notebook_root>/prompts)
dbutils.widgets.text("prompts_dir", "")

notebook_root = dbutils.widgets.get("notebook_root").strip()
prompts_dir = dbutils.widgets.get("prompts_dir").strip()

w = WorkspaceClient()
me = w.current_user.me()
uid = str(me.id)

if not notebook_root:
    notebook_root = f"/Workspace/Users/{me.user_name}/gso-prototype"
if not prompts_dir:
    prompts_dir = f"{notebook_root}/prompts"

print("=" * 60)
print("[DEPLOY] GSO Prototype v2")
print("=" * 60)
print(f"  Target workspace: {w.config.host}")
print(f"  User ID:          {uid}")
print(f"  Notebook root:    {notebook_root}")
print(f"  Prompts dir:      {prompts_dir}")

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
    print(f"  \u2713 Created automation from {name} prompt: {config_id}")
    return config_id


def create_job(
    w: WorkspaceClient,
    notebook_root: str,
    benchmark_qc_config_id: str,
    optimize_config_id: str,
) -> dict:
    """Create the 5-task GSO prototype job."""
    job_def = {
        "name": "gso-prototype-v2",
        "max_concurrent_runs": 1,
        "queue": {"enabled": True},
        "parameters": [
            {"name": "run_id", "default": "{{job.run_id}}"},
            {"name": "space_id", "default": ""},
            {"name": "catalog", "default": ""},
            {"name": "schema", "default": ""},
            {"name": "levers", "default": "[1,2,3,4]"},
            {"name": "max_rounds", "default": "3"},
            {"name": "target_accuracy", "default": "0.90"},
            {"name": "benchmark_repair_max_tries", "default": "3"},
            {"name": "benchmark_policy", "default": "repair_allowed"},
            {"name": "triggered_by", "default": ""},
            {"name": "warehouse_id", "default": ""},
        ],
        "tasks": [
            {
                "task_key": "intake_and_snapshot",
                "notebook_task": {
                    "notebook_path": f"{notebook_root}/intake_and_snapshot",
                    "source": "WORKSPACE",
                },
                "timeout_seconds": 3600,
            },
            {
                "task_key": "benchmark_qc",
                "depends_on": [{"task_key": "intake_and_snapshot"}],
                "timeout_seconds": 3600,
                "genie_task": {
                    "configuration_id": benchmark_qc_config_id,
                    "parameters": {
                        "run_id": "{{job.parameters.run_id}}",
                        "space_id": "{{job.parameters.space_id}}",
                        "catalog": "{{job.parameters.catalog}}",
                        "schema": "{{job.parameters.schema}}",
                        "benchmark_policy": "{{job.parameters.benchmark_policy}}",
                        "benchmark_repair_max_tries": "{{job.parameters.benchmark_repair_max_tries}}",
                        "warehouse_id": "{{job.parameters.warehouse_id}}",
                    },
                },
            },
            {
                "task_key": "begin_baseline_run",
                "depends_on": [{"task_key": "benchmark_qc"}],
                "notebook_task": {
                    "notebook_path": f"{notebook_root}/begin_baseline_run",
                    "source": "WORKSPACE",
                },
                "timeout_seconds": 3600,
            },
            {
                "task_key": "optimize",
                "depends_on": [{"task_key": "begin_baseline_run"}],
                "timeout_seconds": 3600,
                "genie_task": {
                    "configuration_id": optimize_config_id,
                    "parameters": {
                        "run_id": "{{job.parameters.run_id}}",
                        "space_id": "{{job.parameters.space_id}}",
                        "catalog": "{{job.parameters.catalog}}",
                        "schema": "{{job.parameters.schema}}",
                        "target_accuracy": "{{job.parameters.target_accuracy}}",
                        "max_rounds": "{{job.parameters.max_rounds}}",
                        "levers": "{{job.parameters.levers}}",
                        "warehouse_id": "{{job.parameters.warehouse_id}}",
                    },
                },
            },
            {
                "task_key": "publish_and_audit",
                "depends_on": [{"task_key": "optimize"}],
                "notebook_task": {
                    "notebook_path": f"{notebook_root}/publish_and_audit",
                    "source": "WORKSPACE",
                },
                "timeout_seconds": 3600,
            },
        ],
    }

    return w.api_client.do("POST", "/api/2.1/jobs/create", body=job_def)

# COMMAND ----------

# DBTITLE 1,Step 1 — Create Genie Code automations
# Read the prompt files from the workspace (uploaded together with the notebooks).
benchmark_qc_prompt_path = Path(f"{prompts_dir}/benchmark_qc.md")
optimize_prompt_path = Path(f"{prompts_dir}/optimize.md")

for p in (benchmark_qc_prompt_path, optimize_prompt_path):
    if not p.exists():
        raise FileNotFoundError(
            f"Prompt file not found: {p}. "
            "Upload the prompts/ directory to the workspace first (see the README)."
        )

print("Step 1: Creating Genie Code automations...")
benchmark_qc_config_id = create_automation(
    w, uid, benchmark_qc_prompt_path.read_text(), "benchmark_qc"
)
optimize_config_id = create_automation(
    w, uid, optimize_prompt_path.read_text(), "optimize"
)

# COMMAND ----------

# DBTITLE 1,Step 2 — Create the job
print("Step 2: Creating job...")
resp = create_job(w, notebook_root, benchmark_qc_config_id, optimize_config_id)
job_id = resp.get("job_id")
print(f"  \u2713 Created job: {job_id}")
print(f"  URL: {w.config.host}/jobs/{job_id}")

# COMMAND ----------

# DBTITLE 1,Summary
print("=" * 60)
print("Deployment complete!")
print("=" * 60)
print(f"  Job ID:                  {job_id}")
print(f"  benchmark_qc automation: {benchmark_qc_config_id}")
print(f"  optimize automation:     {optimize_config_id}")
print()
print("To run:")
print(f"  databricks jobs run-now {job_id} --json '{{\"job_parameters\": {{\"space_id\": \"<your-space-id>\", \"catalog\": \"<catalog>\", \"schema\": \"<schema>\", \"warehouse_id\": \"<warehouse-id>\", \"triggered_by\": \"<your-email>\"}}}}'")

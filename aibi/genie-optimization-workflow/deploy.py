"""deploy.py — Deploy the GSO Prototype v2 DAG to any Databricks workspace.

Usage:
    # Set DATABRICKS_HOST and DATABRICKS_TOKEN for the target workspace, then:
    python deploy.py \
        --notebook-root /Workspace/Users/you@company.com/gso-prototype \
        --prompts-dir ./prompts

    # Or with defaults (auto-detects current user home):
    python deploy.py

Prerequisites:
    - `databricks-sdk` installed (`pip install databricks-sdk`)
    - Notebooks and prompt files already uploaded to the target workspace
      under <notebook-root>/. Use `databricks workspace import-dir` to upload:

        databricks workspace import-dir ./gso-prototype /Workspace/Users/you@company.com/gso-prototype

What this script does:
    1. Reads prompt files (prompts/benchmark_qc.md, prompts/optimize.md)
    2. Creates Genie Code automations in the target workspace
    3. Creates the 5-task job wired to the automations + notebooks
"""

import argparse
import json
import os
import sys
from pathlib import Path

from databricks.sdk import WorkspaceClient


def get_user_id(w: WorkspaceClient) -> str:
    """Get the numeric user ID of the authenticated user."""
    return str(w.current_user.me().id)


def get_user_home(w: WorkspaceClient) -> str:
    """Get the workspace home path for the authenticated user."""
    email = w.current_user.me().user_name
    return f"/Workspace/Users/{email}"


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
    print(f"  \u2713 Created automation '{name}': {config_id}")
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
            {"name": "run_id", "default": ""},
            {"name": "space_id", "default": ""},
            {"name": "domain", "default": "default"},
            {"name": "catalog", "default": ""},
            {"name": "schema", "default": ""},
            {"name": "apply_mode", "default": "genie_config"},
            {"name": "levers", "default": "[1,2,3,4,5,6]"},
            {"name": "max_rounds", "default": "3"},
            {"name": "target_accuracy", "default": "0.90"},
            {"name": "benchmark_repair_max_tries", "default": "3"},
            {"name": "benchmark_policy", "default": "repair_allowed"},
            {"name": "triggered_by", "default": ""},
            {"name": "warehouse_id", "default": ""},
            {"name": "llm_model", "default": "databricks-claude-sonnet-4-6"},
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
                "genie_task": {
                    "configuration_id": benchmark_qc_config_id,
                    "parameters": {
                        "run_id": "{{job.parameters.run_id}}",
                        "space_id": "{{job.parameters.space_id}}",
                        "domain": "{{job.parameters.domain}}",
                        "catalog": "{{job.parameters.catalog}}",
                        "schema": "{{job.parameters.schema}}",
                        "benchmark_policy": "{{job.parameters.benchmark_policy}}",
                        "benchmark_repair_max_tries": "{{job.parameters.benchmark_repair_max_tries}}",
                        "warehouse_id": "{{job.parameters.warehouse_id}}",
                    },
                },
            },
            {
                "task_key": "eval_baseline",
                "depends_on": [{"task_key": "benchmark_qc"}],
                "notebook_task": {
                    "notebook_path": f"{notebook_root}/eval_baseline",
                    "source": "WORKSPACE",
                },
                "timeout_seconds": 3600,
            },
            {
                "task_key": "optimize",
                "depends_on": [{"task_key": "eval_baseline"}],
                "genie_task": {
                    "configuration_id": optimize_config_id,
                    "parameters": {
                        "run_id": "{{job.parameters.run_id}}",
                        "space_id": "{{job.parameters.space_id}}",
                        "domain": "{{job.parameters.domain}}",
                        "catalog": "{{job.parameters.catalog}}",
                        "schema": "{{job.parameters.schema}}",
                        "target_accuracy": "{{job.parameters.target_accuracy}}",
                        "max_rounds": "{{job.parameters.max_rounds}}",
                        "levers": "{{job.parameters.levers}}",
                        "apply_mode": "{{job.parameters.apply_mode}}",
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

    resp = w.api_client.do("POST", "/api/2.1/jobs/create", body=job_def)
    return resp


def main():
    parser = argparse.ArgumentParser(description="Deploy GSO Prototype v2 DAG")
    parser.add_argument(
        "--notebook-root",
        default=None,
        help="Workspace path where notebooks live (default: auto-detect from current user)",
    )
    parser.add_argument(
        "--prompts-dir",
        default="prompts",
        help="Local directory containing prompt .md files (default: ./prompts)",
    )
    args = parser.parse_args()

    w = WorkspaceClient()
    uid = get_user_id(w)
    notebook_root = args.notebook_root or f"{get_user_home(w)}/gso-prototype"
    prompts_dir = Path(args.prompts_dir)

    print(f"Deploying GSO Prototype v2")
    print(f"  Target workspace: {w.config.host}")
    print(f"  User ID:          {uid}")
    print(f"  Notebook root:    {notebook_root}")
    print(f"  Prompts dir:      {prompts_dir}")
    print()

    # --- Step 1: Create Genie Code automations ---
    print("Step 1: Creating Genie Code automations...")

    benchmark_qc_prompt_path = prompts_dir / "benchmark_qc.md"
    optimize_prompt_path = prompts_dir / "optimize.md"

    if not benchmark_qc_prompt_path.exists():
        print(f"  \u2717 Prompt file not found: {benchmark_qc_prompt_path}")
        sys.exit(1)
    if not optimize_prompt_path.exists():
        print(f"  \u2717 Prompt file not found: {optimize_prompt_path}")
        sys.exit(1)

    benchmark_qc_config_id = create_automation(
        w, uid, benchmark_qc_prompt_path.read_text(), "benchmark_qc"
    )
    optimize_config_id = create_automation(
        w, uid, optimize_prompt_path.read_text(), "optimize"
    )
    print()

    # --- Step 2: Create the job ---
    print("Step 2: Creating job...")
    resp = create_job(w, notebook_root, benchmark_qc_config_id, optimize_config_id)
    job_id = resp.get("job_id")
    print(f"  \u2713 Created job: {job_id}")
    print(f"  URL: {w.config.host}/jobs/{job_id}")
    print()

    # --- Summary ---
    print("=" * 60)
    print("Deployment complete!")
    print("=" * 60)
    print(f"  Job ID:                  {job_id}")
    print(f"  benchmark_qc automation: {benchmark_qc_config_id}")
    print(f"  optimize automation:     {optimize_config_id}")
    print()
    print("To run:")
    print(f"  databricks jobs run-now {job_id} --json '{{"job_parameters": {{"space_id": "<your-space-id>", "catalog": "<catalog>", "schema": "<schema>"}}}}'")


if __name__ == "__main__":
    main()

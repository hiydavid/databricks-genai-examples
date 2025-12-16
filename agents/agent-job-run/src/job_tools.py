"""Utilities for submitting and managing Databricks Lakeflow Jobs."""

from typing import Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

from models.research_plan import ResearchPlan


def submit_research_job(
    research_plan: ResearchPlan,
    notebook_path: str,
    workspace_client: Optional[WorkspaceClient] = None,
) -> dict:
    """
    Submit an async research job via Databricks Lakeflow.

    Returns immediately with run_id - does not wait for completion.

    Args:
        research_plan: The research plan to execute
        notebook_path: Workspace path to the researcher notebook
        workspace_client: Optional WorkspaceClient (creates new one if not provided)

    Returns:
        dict with run_id, status, output_path, and run_page_url
    """
    w = workspace_client or WorkspaceClient()

    # Encode plan for passing via job parameters
    plan_b64 = research_plan.to_base64()

    # Submit job with serverless compute
    # Omit cluster specification to use serverless
    # Dependencies are handled by %pip install in the notebook
    waiter = w.jobs.submit(
        run_name=f"async_research_{research_plan.topic[:30]}",
        tasks=[
            jobs.SubmitTask(
                task_key="execute_research",
                notebook_task=jobs.NotebookTask(
                    notebook_path=notebook_path,
                    base_parameters={"research_plan_b64": plan_b64},
                    source=jobs.Source.WORKSPACE,
                ),
            )
        ],
        timeout_seconds=3600,  # 1 hour max
    )

    return {
        "run_id": waiter.run_id,
        "status": "SUBMITTED",
        "output_path": research_plan.full_output_path,
        "run_page_url": f"{w.config.host}/jobs/{waiter.run_id}",
    }


def check_job_status(
    run_id: int,
    workspace_client: Optional[WorkspaceClient] = None,
) -> dict:
    """
    Check status of a running research job without blocking.

    Args:
        run_id: The Databricks job run ID
        workspace_client: Optional WorkspaceClient

    Returns:
        dict with state, result_state, run_page_url, and timing info
    """
    w = workspace_client or WorkspaceClient()
    run = w.jobs.get_run(run_id=run_id)

    status_info = {
        "run_id": run_id,
        "state": run.state.life_cycle_state.value if run.state else "UNKNOWN",
        "result_state": (
            run.state.result_state.value
            if run.state and run.state.result_state
            else None
        ),
        "state_message": run.state.state_message if run.state else None,
        "start_time": run.start_time,
        "end_time": run.end_time,
        "run_page_url": run.run_page_url,
    }

    # Add task-level status for more detail
    if run.tasks:
        task = run.tasks[0]
        status_info["task_state"] = (
            task.state.life_cycle_state.value if task.state else "UNKNOWN"
        )

    return status_info


def is_job_complete(run_id: int, workspace_client: Optional[WorkspaceClient] = None) -> bool:
    """Check if job has finished (successfully or not)."""
    status = check_job_status(run_id, workspace_client)
    return status["state"] in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR")


def is_job_successful(run_id: int, workspace_client: Optional[WorkspaceClient] = None) -> bool:
    """Check if job completed successfully."""
    status = check_job_status(run_id, workspace_client)
    return status["state"] == "TERMINATED" and status["result_state"] == "SUCCESS"


def get_job_output(
    run_id: int,
    output_path: str,
    workspace_client: Optional[WorkspaceClient] = None,
) -> str:
    """
    Retrieve the research report from UC Volume after job completion.

    Args:
        run_id: The job run ID (for status check)
        output_path: Full path to the output file in UC Volume

    Returns:
        The report content as string, or error message if not ready
    """
    w = workspace_client or WorkspaceClient()
    status = check_job_status(run_id, w)

    if status["state"] != "TERMINATED":
        return f"Job still running: {status['state']}"

    if status["result_state"] != "SUCCESS":
        return f"Job failed: {status['result_state']} - {status.get('state_message', '')}"

    # Read from UC Volume
    try:
        with open(output_path, "r") as f:
            return f.read()
    except FileNotFoundError:
        return f"Report not found at {output_path}"

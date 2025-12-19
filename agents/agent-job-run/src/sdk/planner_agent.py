"""SDK-based Planner Agent for creating research plans and managing async jobs.

Uses OpenAI Agents SDK with function tools.
"""

import json
from uuid import uuid4

from agents import Agent, Runner, function_tool
from agents.run_context import RunContextWrapper
from databricks.sdk import WorkspaceClient

from job_tools import check_job_status, get_job_output, submit_research_job
from models.research_plan import ResearchPlan
from sdk.context import PlannerContext


PLANNER_INSTRUCTIONS = """You are a research planning assistant. Your role is to help users develop comprehensive research plans.

## Your Workflow:

1. **Understand the Topic**: When a user provides a research topic, ask clarifying questions to understand their goals and scope.

2. **Propose Research Questions**: Suggest 3-5 specific, well-defined research questions. Explain why each question is valuable.

3. **Refine the Plan**: Work with the user to refine the questions until they're satisfied.

4. **Execute**: When the user approves the plan, use the submit_research_plan tool to start async execution.

5. **Monitor**: After submitting a job, proactively use list_active_jobs to check progress. Don't wait for users to ask - check status and report back.

6. **Deliver Results**: When complete, use get_research_report to retrieve and present the findings.

## Guidelines:
- Be conversational and collaborative
- Ask clarifying questions before proposing questions
- Explain your reasoning
- Keep research questions specific and answerable via web search
- When proposing questions, format them clearly as a numbered list
"""


# --- Function Tools ---


@function_tool
def submit_research_plan(
    ctx: RunContextWrapper[PlannerContext],
    topic: str,
    research_questions: list[str],
) -> str:
    """Submit an approved research plan for async execution via Databricks Lakeflow Job.

    Use this when the user has approved the research questions.

    Args:
        topic: The main research topic
        research_questions: List of specific research questions to investigate
    """
    plan = ResearchPlan(
        topic=topic,
        research_questions=research_questions,
        output_volume_path=ctx.context.output_volume_path,
        output_filename=f"report_{uuid4().hex[:8]}.md",
    )

    # Create fresh WorkspaceClient to avoid pickle issues with dbutils
    result = submit_research_job(
        research_plan=plan,
        notebook_path=ctx.context.notebook_path,
        workspace_client=WorkspaceClient(),
    )

    # Track the job
    ctx.context.active_jobs[result["run_id"]] = {
        "plan": plan,
        "output_path": result["output_path"],
    }

    return json.dumps({
        "status": "submitted",
        "run_id": result["run_id"],
        "message": (
            f"Research job submitted successfully! "
            f"Run ID: {result['run_id']}. "
            f"The job is running asynchronously. "
            f"You can check status anytime or continue with other work."
        ),
        "output_path": result["output_path"],
    })


@function_tool
def list_active_jobs(ctx: RunContextWrapper[PlannerContext]) -> str:
    """List all research jobs that have been submitted in this session with their current status.

    Use this to check on job progress without needing to know specific run IDs.
    Call this proactively after submitting a job to monitor its progress.
    """
    if not ctx.context.active_jobs:
        return json.dumps({"jobs": [], "message": "No research jobs have been submitted yet."})

    # Create fresh WorkspaceClient to avoid pickle issues with dbutils
    ws = WorkspaceClient()
    jobs = []
    for run_id, info in ctx.context.active_jobs.items():
        status = check_job_status(run_id, ws)
        jobs.append({
            "run_id": run_id,
            "topic": info["plan"].topic,
            "state": status["state"],
            "output_path": info["output_path"],
        })

    return json.dumps({"jobs": jobs, "count": len(jobs)})


@function_tool
def check_job_status_tool(ctx: RunContextWrapper[PlannerContext], run_id: int) -> str:
    """Check detailed status of a specific research job by run ID.

    Args:
        run_id: The Databricks job run ID
    """
    # Create fresh WorkspaceClient to avoid pickle issues with dbutils
    status = check_job_status(run_id, WorkspaceClient())
    return json.dumps(status)


@function_tool
def get_research_report(ctx: RunContextWrapper[PlannerContext], run_id: int) -> str:
    """Retrieve a completed research report from UC Volume.

    Args:
        run_id: The job run ID to get the report for
    """
    job_info = ctx.context.active_jobs.get(run_id, {})
    output_path = job_info.get("output_path", "")

    if not output_path:
        return json.dumps({"error": f"No output path found for run_id {run_id}"})

    # Create fresh WorkspaceClient to avoid pickle issues with dbutils
    report = get_job_output(run_id, output_path, WorkspaceClient())
    return report


# --- Agent Definition ---


def create_planner_agent(model: str = "databricks-claude-sonnet-4") -> Agent[PlannerContext]:
    """Create the planner agent with all tools.

    Args:
        model: The model endpoint name to use

    Returns:
        Configured Agent instance
    """
    return Agent(
        name="ResearchPlanner",
        instructions=PLANNER_INSTRUCTIONS,
        model=model,
        tools=[
            submit_research_plan,
            list_active_jobs,
            check_job_status_tool,
            get_research_report,
        ],
    )


# --- High-level API ---


class PlannerAgent:
    """High-level wrapper for the SDK-based planner agent.

    Provides a simple chat interface similar to the legacy implementation.

    Note: WorkspaceClient is intentionally NOT stored to avoid pickle/serialization
    issues with dbutils. Each operation creates a fresh client as needed.
    """

    def __init__(
        self,
        llm_endpoint: str,
        researcher_notebook_path: str,
        output_volume_path: str,
    ):
        """Initialize the planner agent.

        Args:
            llm_endpoint: Databricks Foundation Model endpoint name
            researcher_notebook_path: Workspace path to the researcher job notebook
            output_volume_path: UC Volume path for output
        """
        self.llm_endpoint = llm_endpoint

        self.context = PlannerContext(
            notebook_path=researcher_notebook_path,
            output_volume_path=output_volume_path,
        )

        self.agent = create_planner_agent(model=llm_endpoint)
        self.conversation_history: list[dict] = []

    async def chat_async(self, user_message: str) -> str:
        """Send a message and get a response (async version).

        Args:
            user_message: The user's input message

        Returns:
            The assistant's response text
        """
        # Build input from conversation history + new message
        messages = self.conversation_history + [{"role": "user", "content": user_message}]

        result = await Runner.run(
            self.agent,
            input=messages,
            context=self.context,
        )

        # Update conversation history
        self.conversation_history.append({"role": "user", "content": user_message})
        self.conversation_history.append({
            "role": "assistant",
            "content": result.final_output,
        })

        return result.final_output

    def chat(self, user_message: str) -> str:
        """Send a message and get a response (sync wrapper).

        Args:
            user_message: The user's input message

        Returns:
            The assistant's response text
        """
        import asyncio

        return asyncio.run(self.chat_async(user_message))

    @property
    def active_jobs(self) -> dict:
        """Return active jobs from context."""
        return self.context.active_jobs

    def get_active_jobs(self) -> list[dict]:
        """Get all active jobs with their current status.

        Returns:
            List of job info dicts with run_id, topic, state, and output_path
        """
        # Create fresh WorkspaceClient to avoid pickle issues with dbutils
        ws = WorkspaceClient()
        jobs = []
        for run_id, info in self.context.active_jobs.items():
            status = check_job_status(run_id, ws)
            jobs.append({
                "run_id": run_id,
                "topic": info["plan"].topic,
                "state": status["state"],
                "output_path": info["output_path"],
            })
        return jobs

    def reset_conversation(self):
        """Reset conversation history (keeps active jobs)."""
        self.conversation_history = []

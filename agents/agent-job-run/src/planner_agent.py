"""Interactive Planner Agent for creating research plans and managing async jobs.

This agent converses with users to develop research plans, then kicks off
Lakeflow Jobs for async execution.
"""

import json
from typing import Optional
from uuid import uuid4

from databricks.sdk import WorkspaceClient

from job_tools import check_job_status, get_job_output, submit_research_job
from models.research_plan import ResearchPlan


SYSTEM_PROMPT = """You are a research planning assistant. Your role is to help users develop comprehensive research plans.

## Your Workflow:

1. **Understand the Topic**: When a user provides a research topic, ask clarifying questions to understand their goals and scope.

2. **Propose Research Questions**: Suggest 3-5 specific, well-defined research questions. Explain why each question is valuable.

3. **Refine the Plan**: Work with the user to refine the questions until they're satisfied.

4. **Execute**: When the user approves the plan, use the submit_research_plan tool to start async execution.

5. **Monitor**: Users can ask about job status anytime. Use check_job_status to provide updates.

6. **Deliver Results**: When complete, use get_research_report to retrieve and present the findings.

## Guidelines:
- Be conversational and collaborative
- Ask clarifying questions before proposing questions
- Explain your reasoning
- Keep research questions specific and answerable via web search
- When proposing questions, format them clearly as a numbered list
"""


class PlannerAgent:
    """Interactive agent for research planning with async job execution."""

    def __init__(
        self,
        llm_endpoint: str,
        researcher_notebook_path: str,
        output_volume_path: str,
        workspace_client: Optional[WorkspaceClient] = None,
    ):
        """
        Initialize the planner agent.

        Args:
            llm_endpoint: Databricks Foundation Model endpoint name
            researcher_notebook_path: Workspace path to the researcher job notebook
            output_volume_path: UC Volume path for output (e.g., /Volumes/catalog/schema/volume)
            workspace_client: Optional WorkspaceClient instance
        """
        self.ws = workspace_client or WorkspaceClient()
        self.llm_endpoint = llm_endpoint
        self.researcher_notebook_path = researcher_notebook_path
        self.output_volume_path = output_volume_path
        self.client = self.ws.serving_endpoints.get_open_ai_client()

        # Track active jobs: {run_id: {"plan": ResearchPlan, "output_path": str}}
        self.active_jobs: dict[int, dict] = {}

        # Conversation history
        self.messages: list[dict] = [{"role": "system", "content": SYSTEM_PROMPT}]

    def get_tools(self) -> list[dict]:
        """Return tool definitions for the LLM."""
        return [
            {
                "type": "function",
                "function": {
                    "name": "submit_research_plan",
                    "description": (
                        "Submit an approved research plan for async execution via Databricks Lakeflow Job. "
                        "Use this when the user has approved the research questions."
                    ),
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "topic": {
                                "type": "string",
                                "description": "The main research topic",
                            },
                            "research_questions": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "List of specific research questions to investigate",
                            },
                        },
                        "required": ["topic", "research_questions"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "check_job_status",
                    "description": "Check the status of a running research job",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "run_id": {
                                "type": "integer",
                                "description": "The Databricks job run ID",
                            },
                        },
                        "required": ["run_id"],
                    },
                },
            },
            {
                "type": "function",
                "function": {
                    "name": "get_research_report",
                    "description": "Retrieve a completed research report from UC Volume",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "run_id": {
                                "type": "integer",
                                "description": "The job run ID to get the report for",
                            },
                        },
                        "required": ["run_id"],
                    },
                },
            },
        ]

    def execute_tool(self, tool_name: str, args: dict) -> str:
        """Execute a tool call and return the result."""
        if tool_name == "submit_research_plan":
            # Create research plan
            plan = ResearchPlan(
                topic=args["topic"],
                research_questions=args["research_questions"],
                output_volume_path=self.output_volume_path,
                output_filename=f"report_{uuid4().hex[:8]}.md",
            )

            # Submit async job
            result = submit_research_job(
                research_plan=plan,
                notebook_path=self.researcher_notebook_path,
                workspace_client=self.ws,
            )

            # Track the job
            self.active_jobs[result["run_id"]] = {
                "plan": plan,
                "output_path": result["output_path"],
            }

            return json.dumps(
                {
                    "status": "submitted",
                    "run_id": result["run_id"],
                    "message": (
                        f"Research job submitted successfully! "
                        f"Run ID: {result['run_id']}. "
                        f"The job is running asynchronously. "
                        f"You can check status anytime or continue with other work."
                    ),
                    "output_path": result["output_path"],
                }
            )

        elif tool_name == "check_job_status":
            run_id = args["run_id"]
            status = check_job_status(run_id, self.ws)
            return json.dumps(status)

        elif tool_name == "get_research_report":
            run_id = args["run_id"]
            job_info = self.active_jobs.get(run_id, {})
            output_path = job_info.get("output_path", "")

            if not output_path:
                return json.dumps({"error": f"No output path found for run_id {run_id}"})

            report = get_job_output(run_id, output_path, self.ws)
            return report

        else:
            return json.dumps({"error": f"Unknown tool: {tool_name}"})

    def chat(self, user_message: str) -> str:
        """
        Send a message and get a response, handling any tool calls.

        Args:
            user_message: The user's input message

        Returns:
            The assistant's response text
        """
        self.messages.append({"role": "user", "content": user_message})

        while True:
            response = self.client.chat.completions.create(
                model=self.llm_endpoint,
                messages=self.messages,
                tools=self.get_tools(),
                temperature=0.7,
            )

            assistant_message = response.choices[0].message

            # Add assistant message to history
            self.messages.append(assistant_message.model_dump())

            # Check for tool calls
            if assistant_message.tool_calls:
                for tool_call in assistant_message.tool_calls:
                    tool_name = tool_call.function.name
                    tool_args = json.loads(tool_call.function.arguments)

                    # Execute tool
                    tool_result = self.execute_tool(tool_name, tool_args)

                    # Add tool result to history
                    self.messages.append(
                        {
                            "role": "tool",
                            "tool_call_id": tool_call.id,
                            "content": tool_result,
                        }
                    )

                # Continue loop to get response after tool execution
                continue

            # No tool calls - return the response
            return assistant_message.content

    def get_active_jobs(self) -> list[dict]:
        """Return list of active jobs with their status."""
        jobs = []
        for run_id, info in self.active_jobs.items():
            status = check_job_status(run_id, self.ws)
            jobs.append(
                {
                    "run_id": run_id,
                    "topic": info["plan"].topic,
                    "state": status["state"],
                    "output_path": info["output_path"],
                }
            )
        return jobs

    def reset_conversation(self):
        """Reset conversation history (keeps active jobs)."""
        self.messages = [{"role": "system", "content": SYSTEM_PROMPT}]

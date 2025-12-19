"""Context classes for OpenAI Agents SDK.

Context is passed to all tools and provides access to state without
sending it to the LLM.
"""

from dataclasses import dataclass, field
from typing import Optional

from databricks.sdk import WorkspaceClient  # Still needed for ResearchContext

from models.research_plan import ResearchPlan
from models.research_state import ResearchState


@dataclass
class ResearchContext:
    """Context for researcher agent - passed through all tools.

    This context is NOT sent to the LLM. It provides local state
    that tools can read and modify during execution.
    """

    state: ResearchState
    plan: ResearchPlan
    ws: Optional[WorkspaceClient] = None

    # Tracking for reflection (managed by outer loop)
    turns_since_reflection: int = 0


@dataclass
class PlannerContext:
    """Context for planner agent.

    Note: WorkspaceClient is intentionally NOT stored here to avoid
    pickle/serialization issues with dbutils. Tools create their own
    WorkspaceClient instances as needed.
    """

    notebook_path: str
    output_volume_path: str
    active_jobs: dict = field(default_factory=dict)

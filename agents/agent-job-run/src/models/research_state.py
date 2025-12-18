"""State tracking for agentic research execution."""

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class FindingConfidence(str, Enum):
    """Confidence level for a research finding."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class Finding(BaseModel):
    """A single piece of discovered information."""

    question: str = Field(description="Which research question this finding addresses")
    content: str = Field(description="The finding content")
    source_queries: list[str] = Field(
        default_factory=list, description="Search queries that led to this finding"
    )
    confidence: FindingConfidence = Field(
        default=FindingConfidence.MEDIUM, description="Confidence in this finding"
    )
    timestamp: str = Field(
        default_factory=lambda: datetime.now().isoformat(),
        description="When this finding was recorded",
    )


class ResearchGap(BaseModel):
    """An identified gap in the research."""

    description: str = Field(description="Description of the gap")
    priority: int = Field(
        default=3, ge=1, le=5, description="Priority 1-5, higher = more important"
    )
    suggested_queries: list[str] = Field(
        default_factory=list, description="Suggested queries to fill this gap"
    )


class ResearchState(BaseModel):
    """Tracks the full state of an agentic research session."""

    # Original plan reference
    topic: str = Field(description="The research topic")
    original_questions: list[str] = Field(
        description="Questions from the original plan"
    )

    # Dynamic question tracking
    active_questions: list[str] = Field(
        default_factory=list, description="Questions currently being researched"
    )
    completed_questions: list[str] = Field(
        default_factory=list, description="Questions that have been fully addressed"
    )
    generated_questions: list[str] = Field(
        default_factory=list, description="New questions discovered by the agent"
    )

    # Findings accumulator
    findings: list[Finding] = Field(
        default_factory=list, description="All findings collected during research"
    )

    # Gap tracking
    identified_gaps: list[ResearchGap] = Field(
        default_factory=list, description="Identified gaps in the research"
    )

    # Synthesized content per question
    synthesized_sections: dict[str, str] = Field(
        default_factory=dict,
        description="Synthesized markdown content keyed by question",
    )

    # Action history for context
    action_history: list[dict] = Field(
        default_factory=list,
        description="History of actions: {type, input, output, timestamp}",
    )

    # Budget tracking
    total_searches: int = Field(default=0, description="Total web searches executed")
    total_llm_calls: int = Field(default=0, description="Total LLM calls made")
    iterations: int = Field(default=0, description="Total agent loop iterations")

    # Reflection state
    last_reflection_iteration: int = Field(
        default=0, description="Iteration when last reflection occurred"
    )
    completion_confidence: float = Field(
        default=0.0, ge=0.0, le=1.0, description="Agent's self-assessed confidence (0-1)"
    )
    should_stop: bool = Field(default=False, description="Whether agent decided to stop")
    stop_reason: Optional[str] = Field(
        default=None, description="Reason for stopping if applicable"
    )

    def get_findings_for_question(self, question: str) -> list[Finding]:
        """Get all findings relevant to a specific question."""
        return [f for f in self.findings if f.question == question]

    def get_recent_actions(self, n: int = 5) -> list[dict]:
        """Get the N most recent actions."""
        return self.action_history[-n:] if self.action_history else []

    def get_findings_summary(self, max_chars: int = 2000) -> str:
        """Get a truncated summary of findings for prompts."""
        summaries = []
        for f in self.findings:
            summary = f"[{f.confidence.value}] {f.question}: {f.content[:200]}..."
            summaries.append(summary)
        result = "\n".join(summaries)
        if len(result) > max_chars:
            result = result[:max_chars] + "\n... (truncated)"
        return result

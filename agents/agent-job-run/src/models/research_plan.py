"""Pydantic model for research plan serialization between agents."""

import base64
import json
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class ResearchPlan(BaseModel):
    """Research plan that gets passed from Planner Agent to Researcher Agent via Lakeflow Job."""

    topic: str = Field(description="The main research topic")
    research_questions: list[str] = Field(
        description="List of specific research questions to investigate"
    )
    output_volume_path: str = Field(
        description="UC Volume path for output, e.g., /Volumes/catalog/schema/volume"
    )
    output_filename: str = Field(description="Output filename, e.g., report_abc123.md")
    max_searches_per_question: int = Field(
        default=3, description="Maximum web searches per research question (legacy)"
    )
    created_at: str = Field(
        default_factory=lambda: datetime.now().isoformat(),
        description="Timestamp when plan was created",
    )
    user_id: Optional[str] = Field(
        default=None, description="Optional user identifier"
    )

    # Agentic parameters
    max_iterations: int = Field(
        default=20,
        description="Soft cap on agent loop iterations",
    )
    max_total_searches: int = Field(
        default=30,
        description="Soft cap on total web searches across all questions",
    )
    reflection_interval: int = Field(
        default=5,
        description="Run reflection every N actions to assess progress",
    )
    min_confidence_threshold: float = Field(
        default=0.7,
        ge=0.0,
        le=1.0,
        description="Minimum confidence (0-1) before agent can choose to stop",
    )
    allow_new_questions: bool = Field(
        default=True,
        description="Allow agent to generate new questions beyond original plan",
    )
    max_generated_questions: int = Field(
        default=3,
        description="Maximum new questions agent can generate",
    )

    def to_base64(self) -> str:
        """Encode plan to base64 for passing via job parameters."""
        return base64.b64encode(self.model_dump_json().encode()).decode()

    @classmethod
    def from_base64(cls, encoded: str) -> "ResearchPlan":
        """Decode plan from base64 job parameter."""
        json_str = base64.b64decode(encoded).decode()
        return cls.model_validate_json(json_str)

    @property
    def full_output_path(self) -> str:
        """Full path to the output file."""
        return f"{self.output_volume_path}/{self.output_filename}"

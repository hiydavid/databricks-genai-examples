"""Report generation for the SDK-based researcher agent."""

from datetime import datetime
from typing import Optional

from databricks.sdk import WorkspaceClient

from models.research_plan import ResearchPlan
from models.research_state import ResearchState
from prompts import EXECUTIVE_SUMMARY_PROMPT, SYNTHESIS_PROMPT


def _synthesize_question_if_needed(
    question: str,
    state: ResearchState,
    llm_endpoint: str,
    ws: WorkspaceClient,
) -> str:
    """Synthesize a question's findings if not already done."""
    if question in state.synthesized_sections:
        return state.synthesized_sections[question]

    # Get findings for this question
    findings = state.get_findings_for_question(question)
    findings_str = "\n\n".join(
        [f"[{f.confidence.value}] {f.content}" for f in findings]
    ) or "No specific findings recorded."

    prompt = SYNTHESIS_PROMPT.format(
        question=question,
        findings=findings_str,
    )

    client = ws.serving_endpoints.get_open_ai_client()
    response = client.chat.completions.create(
        model=llm_endpoint,
        messages=[
            {"role": "system", "content": "You are a research writer. Synthesize findings into clear prose."},
            {"role": "user", "content": prompt},
        ],
        temperature=0.3,
    )

    synthesis = response.choices[0].message.content
    state.synthesized_sections[question] = synthesis
    state.total_llm_calls += 1

    return synthesis


def generate_report(
    state: ResearchState,
    plan: ResearchPlan,
    llm_endpoint: str,
    ws: WorkspaceClient,
) -> str:
    """Generate final markdown report from accumulated research state.

    Args:
        state: The research state with all findings and syntheses
        plan: The original research plan
        llm_endpoint: Model endpoint for any needed LLM calls
        ws: WorkspaceClient for API access

    Returns:
        Complete markdown report
    """
    state.total_llm_calls += 1

    # Build sections for original questions
    sections = []
    for question in plan.research_questions:
        section_content = _synthesize_question_if_needed(question, state, llm_endpoint, ws)
        sections.append(f"### {question}\n\n{section_content}")

    # Add generated questions
    for question in state.generated_questions:
        if question in state.synthesized_sections:
            sections.append(f"### {question} (discovered)\n\n{state.synthesized_sections[question]}")

    # Generate executive summary
    sections_combined = "\n\n".join(sections)
    summary_prompt = EXECUTIVE_SUMMARY_PROMPT.format(
        topic=plan.topic,
        sections=sections_combined[:4000],
    )

    client = ws.serving_endpoints.get_open_ai_client()
    summary_response = client.chat.completions.create(
        model=llm_endpoint,
        messages=[
            {"role": "system", "content": "You are a research writer. Write a concise executive summary."},
            {"role": "user", "content": summary_prompt},
        ],
        temperature=0.3,
    )
    summary = summary_response.choices[0].message.content

    # Build methodology section
    methodology = f"""## Methodology

This report was generated using an agentic research system powered by OpenAI Agents SDK on Databricks.

### Research Process
- **Mode**: Agentic (SDK-based with reflection loop)
- **Original questions**: {len(plan.research_questions)}
- **Additional questions discovered**: {len(state.generated_questions)}
- **Total iterations**: {state.iterations}
- **Total web searches**: {state.total_searches}
- **Total LLM calls**: {state.total_llm_calls}
- **Final confidence**: {state.completion_confidence:.0%}
- **Stop reason**: {state.stop_reason or "N/A"}

### Configuration
- LLM endpoint: {llm_endpoint}
- Max iterations: {plan.max_iterations}
- Max searches: {plan.max_total_searches}
- Reflection interval: Every {plan.reflection_interval} actions
- Confidence threshold: {plan.min_confidence_threshold:.0%}
"""

    report = f"""# Research Report: {plan.topic}

**Generated:** {datetime.now().isoformat()}

## Executive Summary

{summary}

## Research Findings

{sections_combined}

---

{methodology}
"""
    return report


def save_report(report: str, output_path: str) -> str:
    """Save report to file (UC Volume or local).

    Args:
        report: The markdown report content
        output_path: Full path to save the report

    Returns:
        The output path
    """
    with open(output_path, "w") as f:
        f.write(report)
    return output_path

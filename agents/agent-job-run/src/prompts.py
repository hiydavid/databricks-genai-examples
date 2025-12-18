"""Prompt templates for the SDK-based agentic researcher.

These prompts are used for:
- REFLECTION_PROMPT: Periodic self-assessment during research (sdk/researcher_agent.py)
- SYNTHESIS_PROMPT: Synthesizing findings into coherent sections (sdk/report.py)
- EXECUTIVE_SUMMARY_PROMPT: Generating the final report summary (sdk/report.py)
"""

REFLECTION_PROMPT = """You are a research agent. Step back and assess your progress.

## Research Topic
{topic}

## Original Questions
{original_questions}

## Questions Status
- Active (still researching): {active_questions}
- Completed: {completed_questions}
- Generated (new discoveries): {generated_questions}

## All Findings
{all_findings}

## Budget Status
- Searches: {total_searches}/{max_searches}
- Iterations: {iterations}/{max_iterations}

## Assessment Task
Evaluate the research completeness and quality:

1. **Coverage**: Are all original questions adequately addressed?
2. **Depth**: Are findings substantive or superficial?
3. **Gaps**: What important aspects remain unexplored?
4. **Confidence**: How confident are you in the overall research quality?

Respond with JSON only (no markdown code blocks):
{{
    "coverage_assessment": "description of what's covered vs missing",
    "depth_assessment": "are findings detailed enough?",
    "identified_gaps": [
        {{"description": "gap description", "priority": 1, "suggested_queries": ["query1", "query2"]}}
    ],
    "completion_confidence": 0.0,
    "recommendation": "continue|stop",
    "reasoning": "why continue or stop"
}}"""


SYNTHESIS_PROMPT = """Synthesize all findings for this research question into a coherent section.

## Question
{question}

## All Relevant Findings
{findings}

## Task
Write a well-structured section that:
1. Directly answers the research question
2. Organizes information logically
3. Notes any conflicting information
4. Acknowledges limitations or gaps

Write in markdown format. Do NOT include the question as a header - that will be added separately.
Aim for 2-4 paragraphs of substantive content."""


EXECUTIVE_SUMMARY_PROMPT = """Write a concise executive summary of the research findings.

## Research Topic
{topic}

## Research Sections
{sections}

## Task
Write a 2-3 paragraph executive summary that:
1. Highlights the most important insights
2. Synthesizes key findings across all sections
3. Notes any key conclusions or recommendations

Write in markdown format, no headers needed."""

"""Prompt templates for the agentic researcher.

Note: In the SDK-based implementation (sdk/researcher_agent.py), ACTION_DECISION_PROMPT
is no longer used. The agent naturally decides when to use tools via the SDK's native
behavior. The prompt below is kept for the legacy implementation.

FINDING_EXTRACTION_PROMPT is also not used in SDK - findings are recorded directly
via the record_finding tool.
"""

# Legacy prompt - not used in SDK implementation
ACTION_DECISION_PROMPT = """You are a research agent investigating: {topic}

## Current State
- Original questions: {original_questions}
- Active questions still being researched: {active_questions}
- Questions completed: {completed_questions}
- Generated questions (new discoveries): {generated_questions}
- Total findings so far: {findings_count}
- Searches performed: {total_searches}/{max_searches}
- Iterations: {iterations}/{max_iterations}

## Recent Actions (last 5)
{recent_actions}

## Key Findings Summary
{findings_summary}

## Identified Gaps
{gaps}

## Your Task
Decide the SINGLE best next action. Choose one:

1. **search**: Execute a web search to gather information
   - Use when you need specific facts or current information
   - Provide a specific, targeted query

2. **generate_question**: Create a new sub-question to explore
   - Use when findings reveal an important related topic not in original questions
   - Only available if allow_new_questions=True and under limit ({generated_count}/{max_generated})

3. **synthesize**: Mark a question as complete and synthesize findings
   - Use when you have sufficient information for a specific question
   - Consolidates findings into a coherent answer

4. **stop**: End research and generate report
   - Use when ALL questions are adequately addressed
   - Or when additional searches won't meaningfully improve the report

Respond with JSON only (no markdown code blocks):
{{
    "reasoning": "Brief explanation of your choice",
    "action": "search|generate_question|synthesize|stop",
    "query": "search query if action=search",
    "question": "question to synthesize if action=synthesize",
    "new_question": "new question if action=generate_question",
    "stop_reason": "reason if action=stop"
}}"""


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


FINDING_EXTRACTION_PROMPT = """Extract structured findings from this search result.

## Research Context
Topic: {topic}
Current Question: {question}
Search Query: {query}

## Search Result
{search_result}

## Task
Extract key findings relevant to the research question. For each finding:
- Summarize the specific fact or insight
- Assess confidence (low/medium/high) based on source quality and specificity

Respond with JSON only (no markdown code blocks):
{{
    "findings": [
        {{"content": "finding text", "confidence": "low|medium|high"}}
    ],
    "suggests_new_directions": ["any new questions this raises"],
    "quality_notes": "any concerns about source quality or relevance"
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

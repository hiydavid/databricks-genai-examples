"""SDK-based Researcher Agent with hybrid reflection loop.

Uses OpenAI Agents SDK for tool handling + outer loop for periodic reflection.
"""

import json
from datetime import datetime
from typing import Optional

import mlflow
from agents import Agent, Runner, function_tool
from agents.run_context import RunContextWrapper
from databricks.sdk import WorkspaceClient
from databricks_openai.agents import McpServer

from models.research_plan import ResearchPlan
from models.research_state import (
    Finding,
    FindingConfidence,
    ResearchGap,
    ResearchState,
)
from sdk.config import init_mcp_server
from sdk.context import ResearchContext


# --- Agent Instructions ---


RESEARCHER_INSTRUCTIONS = """You are an expert research agent investigating a specific topic.

## Your Goal
Thoroughly research the given topic by answering all research questions. Use web search to gather information, then synthesize findings into coherent answers.

## Available Tools

1. **Web Search** (from MCP): Search the web to find information relevant to your research questions.

2. **record_finding**: After each search, use this to record important findings. Include:
   - The content of the finding
   - Which question it addresses
   - Your confidence level (low/medium/high)

3. **synthesize_question**: When you have gathered enough information for a question, synthesize your findings into a coherent answer. This marks the question as complete.

4. **add_sub_question**: If you discover an important related topic during research, you can add a new sub-question to explore (limited to {max_generated} new questions).

## Research Strategy

1. Start by searching for information about each research question
2. After each search, record the relevant findings with appropriate confidence
3. Search multiple times per question to get diverse perspectives
4. When you have sufficient findings for a question (usually 3-5 searches), synthesize them
5. Move to the next question
6. If you discover important related topics, add sub-questions

## Current Research State

Topic: {topic}

Questions to answer:
{questions_status}

Findings so far: {findings_count}
Searches performed: {total_searches}/{max_searches}

{gaps_section}

## Guidelines
- Be thorough but efficient - don't repeat similar searches
- Record findings as you go, don't wait until the end
- Synthesize questions when you have enough information, don't over-research
- Focus on factual, verifiable information
- Note confidence levels honestly
"""


# --- State Management Tools ---


@function_tool
def record_finding(
    ctx: RunContextWrapper[ResearchContext],
    content: str,
    question: str,
    confidence: str = "medium",
) -> str:
    """Record a finding from search results.

    Use this after each search to capture important information.

    Args:
        content: The finding content - a specific fact or insight
        question: Which research question this finding addresses
        confidence: Your confidence level (low/medium/high) based on source quality
    """
    try:
        conf = FindingConfidence(confidence.lower())
    except ValueError:
        conf = FindingConfidence.MEDIUM

    finding = Finding(
        question=question,
        content=content,
        source_queries=[],  # SDK doesn't track this automatically
        confidence=conf,
    )
    ctx.context.state.findings.append(finding)
    ctx.context.state.total_llm_calls += 1

    return f"Recorded finding for '{question}' with {confidence} confidence."


@function_tool
def synthesize_question(
    ctx: RunContextWrapper[ResearchContext],
    question: str,
    synthesis: str,
) -> str:
    """Mark a question as complete with a synthesized answer.

    Use this when you have gathered enough information for a question.

    Args:
        question: The research question being synthesized
        synthesis: Your synthesized answer (2-4 paragraphs of markdown)
    """
    state = ctx.context.state

    # Store synthesis
    state.synthesized_sections[question] = synthesis

    # Move question from active to completed
    if question in state.active_questions:
        state.active_questions.remove(question)
    if question not in state.completed_questions:
        state.completed_questions.append(question)

    state.total_llm_calls += 1

    remaining = len(state.active_questions)
    return f"Synthesized '{question}'. {remaining} questions remaining."


@function_tool
def add_sub_question(
    ctx: RunContextWrapper[ResearchContext],
    new_question: str,
) -> str:
    """Add a new sub-question discovered during research.

    Use this when findings reveal an important related topic not covered by existing questions.

    Args:
        new_question: The new research question to add
    """
    state = ctx.context.state
    plan = ctx.context.plan

    # Check if allowed
    if not plan.allow_new_questions:
        return "Adding new questions is disabled for this research plan."

    if len(state.generated_questions) >= plan.max_generated_questions:
        return f"Maximum of {plan.max_generated_questions} generated questions reached."

    if new_question in state.generated_questions or new_question in state.original_questions:
        return f"Question already exists: {new_question}"

    state.generated_questions.append(new_question)
    state.active_questions.append(new_question)

    return f"Added new question: {new_question}"


# --- Agent Creation ---


def create_researcher_agent(
    mcp_server: McpServer,
    model: str = "databricks-claude-sonnet-4",
) -> Agent[ResearchContext]:
    """Create the researcher agent with MCP search and state tools.

    Args:
        mcp_server: Configured MCP server for web search
        model: Model endpoint name

    Returns:
        Configured Agent instance
    """
    return Agent(
        name="Researcher",
        instructions=RESEARCHER_INSTRUCTIONS,
        model=model,
        mcp_servers=[mcp_server],
        tools=[record_finding, synthesize_question, add_sub_question],
    )


# --- Prompt Building ---


def build_research_prompt(state: ResearchState, plan: ResearchPlan) -> str:
    """Build the current state prompt for the agent."""
    # Format questions status
    questions_lines = []
    for q in state.original_questions:
        if q in state.completed_questions:
            questions_lines.append(f"- [COMPLETED] {q}")
        elif q in state.active_questions:
            questions_lines.append(f"- [ACTIVE] {q}")
        else:
            questions_lines.append(f"- {q}")

    for q in state.generated_questions:
        if q in state.completed_questions:
            questions_lines.append(f"- [COMPLETED] (discovered) {q}")
        elif q in state.active_questions:
            questions_lines.append(f"- [ACTIVE] (discovered) {q}")

    questions_status = "\n".join(questions_lines)

    # Format gaps section
    if state.identified_gaps:
        gaps_lines = ["Identified gaps to address:"]
        for gap in state.identified_gaps:
            gaps_lines.append(f"- [{gap.priority}] {gap.description}")
        gaps_section = "\n".join(gaps_lines)
    else:
        gaps_section = ""

    return RESEARCHER_INSTRUCTIONS.format(
        topic=state.topic,
        questions_status=questions_status,
        findings_count=len(state.findings),
        total_searches=state.total_searches,
        max_searches=plan.max_total_searches,
        max_generated=plan.max_generated_questions,
        gaps_section=gaps_section,
    )


# --- Reflection Logic ---


async def perform_reflection(
    ctx: ResearchContext,
    llm_client,
    llm_endpoint: str,
) -> None:
    """Perform periodic reflection to assess research progress.

    This runs outside the SDK agent loop to maintain explicit control
    over reflection timing.
    """
    from prompts import REFLECTION_PROMPT

    state = ctx.state
    plan = ctx.plan

    state.total_llm_calls += 1
    state.last_reflection_iteration = state.iterations

    # Format all findings for reflection
    all_findings_str = "\n".join(
        [f"- [{f.confidence.value}] {f.question}: {f.content[:150]}..."
         for f in state.findings]
    ) or "No findings yet."

    prompt = REFLECTION_PROMPT.format(
        topic=state.topic,
        original_questions="\n".join([f"- {q}" for q in state.original_questions]),
        active_questions=", ".join(state.active_questions) or "None",
        completed_questions=", ".join(state.completed_questions) or "None",
        generated_questions=", ".join(state.generated_questions) or "None",
        all_findings=all_findings_str[:3000],
        total_searches=state.total_searches,
        max_searches=plan.max_total_searches,
        iterations=state.iterations,
        max_iterations=plan.max_iterations,
    )

    response = llm_client.chat.completions.create(
        model=llm_endpoint,
        messages=[
            {"role": "system", "content": "You are a research evaluator. Assess progress and respond with JSON."},
            {"role": "user", "content": prompt},
        ],
        temperature=0.3,
    )

    result_text = response.choices[0].message.content

    # Parse JSON response
    parsed = _parse_json_response(result_text)

    # Update state from reflection
    state.completion_confidence = float(parsed.get("completion_confidence", 0.0))

    # Update gaps
    state.identified_gaps = []
    for gap in parsed.get("identified_gaps", []):
        state.identified_gaps.append(
            ResearchGap(
                description=gap.get("description", ""),
                priority=gap.get("priority", 3),
                suggested_queries=gap.get("suggested_queries", []),
            )
        )

    # Check if reflection recommends stopping
    if parsed.get("recommendation") == "stop":
        if state.completion_confidence >= plan.min_confidence_threshold:
            state.should_stop = True
            state.stop_reason = parsed.get("reasoning", "Reflection recommended stop")

    print(f"  [Reflection] Confidence: {state.completion_confidence:.0%}, "
          f"Gaps: {len(state.identified_gaps)}, "
          f"Recommendation: {parsed.get('recommendation', 'continue')}")


def _parse_json_response(response: str) -> dict:
    """Parse JSON from LLM response, handling markdown code blocks."""
    text = response.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        lines = [line for line in lines if not line.strip().startswith("```")]
        text = "\n".join(lines)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {}


# --- Termination Logic ---


def should_terminate(state: ResearchState, plan: ResearchPlan) -> bool:
    """Determine if the research loop should stop."""
    # Agent decided to stop
    if state.should_stop:
        return True

    # Soft budget: iterations exceeded
    if state.iterations >= plan.max_iterations:
        state.stop_reason = f"Reached max iterations ({plan.max_iterations})"
        return True

    # Soft budget: searches exceeded
    if state.total_searches >= plan.max_total_searches:
        state.stop_reason = f"Reached max searches ({plan.max_total_searches})"
        return True

    # All questions completed AND confidence threshold met
    if (
        len(state.active_questions) == 0
        and state.completion_confidence >= plan.min_confidence_threshold
    ):
        state.stop_reason = "All questions addressed with sufficient confidence"
        return True

    return False


# --- Main Execution ---


@mlflow.trace(name="research_execution")
async def execute_research(
    plan: ResearchPlan,
    host: str,
    mcp_connection: str,
    llm_endpoint: str = "databricks-claude-sonnet-4",
    workspace_client: Optional[WorkspaceClient] = None,
) -> str:
    """Execute a research plan using the SDK-based agent with reflection loop.

    Args:
        plan: The research plan to execute
        host: Databricks workspace host URL
        mcp_connection: MCP connection name for web search
        llm_endpoint: Model endpoint name
        workspace_client: Optional WorkspaceClient

    Returns:
        Complete markdown research report
    """
    from sdk.report import generate_report

    ws = workspace_client or WorkspaceClient()

    # Initialize state
    state = ResearchState(
        topic=plan.topic,
        original_questions=list(plan.research_questions),
        active_questions=list(plan.research_questions),
    )

    ctx = ResearchContext(state=state, plan=plan, ws=ws)

    # Get LLM client for reflection calls
    llm_client = ws.serving_endpoints.get_open_ai_client()

    print(f"Starting research on: {plan.topic}")
    print(f"Questions: {len(plan.research_questions)}")

    async with await init_mcp_server(host, mcp_connection) as mcp_server:
        agent = create_researcher_agent(mcp_server, model=llm_endpoint)

        while not should_terminate(state, plan):
            # Determine turns for this batch
            turns_this_batch = min(
                plan.reflection_interval,
                plan.max_iterations - state.iterations,
            )

            print(f"\n[Batch] Running {turns_this_batch} turns (iteration {state.iterations + 1}-{state.iterations + turns_this_batch})")

            # Run agent for reflection_interval turns
            prompt = build_research_prompt(state, plan)

            try:
                result = await Runner.run(
                    agent,
                    input=prompt,
                    context=ctx,
                    max_turns=turns_this_batch,
                )

                # Count actual turns taken
                state.iterations += turns_this_batch

            except Exception as e:
                print(f"  [Error] Agent error: {e}")
                state.iterations += 1
                continue

            # Perform reflection after each batch
            if state.iterations % plan.reflection_interval == 0:
                await perform_reflection(ctx, llm_client, llm_endpoint)

    # Generate final report
    print(f"\n[Complete] Generating report...")
    report = generate_report(state, plan, llm_endpoint, ws)

    return report


# --- Sync Wrapper ---


def execute_research_sync(
    plan: ResearchPlan,
    host: str,
    mcp_connection: str,
    llm_endpoint: str = "databricks-claude-sonnet-4",
    workspace_client: Optional[WorkspaceClient] = None,
) -> str:
    """Synchronous wrapper for execute_research.

    Use this in notebooks with nest_asyncio.apply().
    """
    import asyncio

    return asyncio.run(execute_research(
        plan=plan,
        host=host,
        mcp_connection=mcp_connection,
        llm_endpoint=llm_endpoint,
        workspace_client=workspace_client,
    ))

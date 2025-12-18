"""Agentic Researcher that executes research plans using MCP web search tools.

This agent uses a ReAct-style loop with periodic reflection to:
- Decide when to search, synthesize, or generate new questions
- Self-assess research completeness via confidence scoring
- Adapt research direction based on findings

Runs asynchronously via Databricks Lakeflow Job.
"""

import json
from datetime import datetime
from typing import Optional

import mlflow
from databricks.sdk import WorkspaceClient
from databricks_mcp import DatabricksMCPClient

from models.research_plan import ResearchPlan
from models.research_state import (
    Finding,
    FindingConfidence,
    ResearchGap,
    ResearchState,
)
from prompts import (
    ACTION_DECISION_PROMPT,
    EXECUTIVE_SUMMARY_PROMPT,
    FINDING_EXTRACTION_PROMPT,
    REFLECTION_PROMPT,
    SYNTHESIS_PROMPT,
)


class AgentAction:
    """Represents a decided action from the agent."""

    def __init__(
        self,
        action_type: str,
        query: Optional[str] = None,
        question: Optional[str] = None,
        new_question: Optional[str] = None,
        stop_reason: Optional[str] = None,
        reasoning: str = "",
    ):
        self.type = action_type
        self.query = query
        self.question = question
        self.new_question = new_question
        self.stop_reason = stop_reason
        self.reasoning = reasoning


class ResearcherAgent:
    """Agentic researcher that executes research plans using web search via MCP."""

    def __init__(
        self,
        llm_endpoint: str,
        mcp_server_url: str,
        workspace_client: Optional[WorkspaceClient] = None,
    ):
        """
        Initialize the researcher agent.

        Args:
            llm_endpoint: Databricks Foundation Model endpoint name
            mcp_server_url: URL to the MCP server with search tools
            workspace_client: Optional WorkspaceClient instance
        """
        self.ws = workspace_client or WorkspaceClient()
        self.llm_endpoint = llm_endpoint
        self.client = self.ws.serving_endpoints.get_open_ai_client()

        # Initialize MCP client for web search
        self.mcp_client = DatabricksMCPClient(
            server_url=mcp_server_url,
            workspace_client=self.ws,
        )

        # Discover available tools
        self.tools = {tool.name: tool for tool in self.mcp_client.list_tools()}

        # Find the search tool
        self.search_tool_name = None
        for name in self.tools:
            if "search" in name.lower():
                self.search_tool_name = name
                break

    def _call_llm(self, system_prompt: str, user_prompt: str, temperature: float = 0.3) -> str:
        """Make an LLM call and return the response content."""
        with mlflow.start_span(name="llm_call") as span:
            span.set_inputs({
                "system_prompt": system_prompt[:200] + "..." if len(system_prompt) > 200 else system_prompt,
                "user_prompt": user_prompt[:500] + "..." if len(user_prompt) > 500 else user_prompt,
                "temperature": temperature,
            })
            response = self.client.chat.completions.create(
                model=self.llm_endpoint,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=temperature,
            )
            result = response.choices[0].message.content
            span.set_outputs({
                "response": result[:500] + "..." if len(result) > 500 else result,
            })
            return result

    def _parse_json_response(self, response: str) -> dict:
        """Parse JSON from LLM response, handling markdown code blocks."""
        text = response.strip()
        # Remove markdown code blocks if present
        if text.startswith("```"):
            lines = text.split("\n")
            # Remove first and last lines (```json and ```)
            lines = [l for l in lines if not l.strip().startswith("```")]
            text = "\n".join(lines)
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            # Fallback: return empty dict on parse failure
            return {}

    def search_web(self, query: str) -> str:
        """Execute a web search via MCP tool."""
        with mlflow.start_span(name="mcp_search") as span:
            span.set_inputs({"query": query, "tool": self.search_tool_name})

            if not self.search_tool_name:
                result = f"No search tool available. Available tools: {list(self.tools.keys())}"
                span.set_outputs({"error": result})
                return result

            response = self.mcp_client.call_tool(self.search_tool_name, {"query": query})
            result = "".join([c.text for c in response.content if hasattr(c, "text")])
            span.set_outputs({"result_length": len(result)})
            return result

    def _decide_next_action(self, state: ResearchState, plan: ResearchPlan) -> AgentAction:
        """Use LLM to decide the next action based on current state."""
        with mlflow.start_span(name="decide_action") as span:
            span.set_inputs({
                "iteration": state.iterations,
                "total_searches": state.total_searches,
                "active_questions": len(state.active_questions),
                "findings_count": len(state.findings),
            })

            state.total_llm_calls += 1

            # Format recent actions
            recent_actions = state.get_recent_actions(5)
            recent_str = "\n".join(
                [f"- {a.get('type', 'unknown')}: {a.get('input', {})}" for a in recent_actions]
            ) or "None yet"

            # Format gaps
            gaps_str = "\n".join(
                [f"- [{g.priority}] {g.description}" for g in state.identified_gaps]
            ) or "None identified"

            prompt = ACTION_DECISION_PROMPT.format(
                topic=state.topic,
                original_questions=", ".join(state.original_questions),
                active_questions=", ".join(state.active_questions) or "None",
                completed_questions=", ".join(state.completed_questions) or "None",
                generated_questions=", ".join(state.generated_questions) or "None",
                findings_count=len(state.findings),
                total_searches=state.total_searches,
                max_searches=plan.max_total_searches,
                iterations=state.iterations,
                max_iterations=plan.max_iterations,
                recent_actions=recent_str,
                findings_summary=state.get_findings_summary(1500),
                gaps=gaps_str,
                generated_count=len(state.generated_questions),
                max_generated=plan.max_generated_questions,
            )

            response = self._call_llm(
                "You are a research planning agent. Respond with valid JSON only.",
                prompt,
                temperature=0.4,
            )

            parsed = self._parse_json_response(response)

            action = AgentAction(
                action_type=parsed.get("action", "stop"),
                query=parsed.get("query"),
                question=parsed.get("question"),
                new_question=parsed.get("new_question"),
                stop_reason=parsed.get("stop_reason", "No valid action determined"),
                reasoning=parsed.get("reasoning", ""),
            )

            span.set_outputs({
                "action": action.type,
                "reasoning": action.reasoning[:200] + "..." if len(action.reasoning) > 200 else action.reasoning,
            })

            return action

    def _execute_search(self, query: str, state: ResearchState, current_question: str) -> str:
        """Execute a search and extract findings into state."""
        state.total_searches += 1

        # Print search progress
        print(f"\n[Search {state.total_searches}] {query}")
        truncated_q = current_question[:80] + "..." if len(current_question) > 80 else current_question
        print(f"  Question: {truncated_q}")

        with mlflow.start_span(name="execute_search") as span:
            span.set_inputs({"query": query, "question": current_question})

            # Perform the search
            result = self.search_web(query)

            # Extract structured findings using LLM
            state.total_llm_calls += 1
            prompt = FINDING_EXTRACTION_PROMPT.format(
                topic=state.topic,
                question=current_question,
                query=query,
                search_result=result[:4000],  # Truncate long results
            )

            response = self._call_llm(
                "You are a research analyst. Extract findings as JSON.",
                prompt,
                temperature=0.2,
            )

            parsed = self._parse_json_response(response)

            # Add findings to state
            findings_count = 0
            for f in parsed.get("findings", []):
                confidence_str = f.get("confidence", "medium").lower()
                try:
                    confidence = FindingConfidence(confidence_str)
                except ValueError:
                    confidence = FindingConfidence.MEDIUM

                finding = Finding(
                    question=current_question,
                    content=f.get("content", ""),
                    source_queries=[query],
                    confidence=confidence,
                )
                state.findings.append(finding)
                findings_count += 1

            span.set_outputs({"findings_count": findings_count, "result_length": len(result)})
            return result

    def _generate_sub_question(
        self, context: str, state: ResearchState, plan: ResearchPlan
    ) -> Optional[str]:
        """Generate a new sub-question based on findings."""
        if not plan.allow_new_questions:
            return None
        if len(state.generated_questions) >= plan.max_generated_questions:
            return None

        # The context/new_question comes from the action decision
        if context and context not in state.generated_questions:
            state.generated_questions.append(context)
            state.active_questions.append(context)
            return context

        return None

    def _synthesize_question(self, question: str, state: ResearchState) -> str:
        """Synthesize findings for a question into a coherent section."""
        with mlflow.start_span(name="synthesize") as span:
            span.set_inputs({"question": question})

            state.total_llm_calls += 1

            # Get findings for this question
            findings = state.get_findings_for_question(question)
            findings_str = "\n\n".join(
                [f"[{f.confidence.value}] {f.content}" for f in findings]
            ) or "No specific findings recorded."

            prompt = SYNTHESIS_PROMPT.format(
                question=question,
                findings=findings_str,
            )

            response = self._call_llm(
                "You are a research writer. Synthesize findings into clear prose.",
                prompt,
                temperature=0.3,
            )

            # Store synthesized content
            state.synthesized_sections[question] = response

            # Mark question as completed
            if question in state.active_questions:
                state.active_questions.remove(question)
            if question not in state.completed_questions:
                state.completed_questions.append(question)

            span.set_outputs({
                "synthesis_length": len(response),
                "findings_used": len(findings),
            })

            return response

    def _reflect_on_progress(self, state: ResearchState, plan: ResearchPlan) -> None:
        """Periodic reflection to assess research completeness."""
        with mlflow.start_span(name="reflect") as span:
            span.set_inputs({
                "iteration": state.iterations,
                "findings_count": len(state.findings),
                "active_questions": len(state.active_questions),
            })

            state.total_llm_calls += 1
            state.last_reflection_iteration = state.iterations

            # Format all findings
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

            response = self._call_llm(
                "You are a research evaluator. Assess progress and respond with JSON.",
                prompt,
                temperature=0.3,
            )

            parsed = self._parse_json_response(response)

            # Update state from reflection
            old_confidence = state.completion_confidence
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

            span.set_outputs({
                "confidence": state.completion_confidence,
                "confidence_delta": state.completion_confidence - old_confidence,
                "gaps_identified": len(state.identified_gaps),
            })

    def _should_terminate(self, state: ResearchState, plan: ResearchPlan) -> bool:
        """Determine if the agent loop should stop."""
        # Hard stop: agent decided to stop
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

    def _get_current_question(self, state: ResearchState) -> str:
        """Get the current question being researched."""
        if state.active_questions:
            return state.active_questions[0]
        if state.original_questions:
            return state.original_questions[0]
        return state.topic

    def _generate_report(self, state: ResearchState, plan: ResearchPlan) -> str:
        """Generate final report from accumulated research state."""
        state.total_llm_calls += 1

        # Build sections for original questions
        sections = []
        for question in plan.research_questions:
            if question in state.synthesized_sections:
                section_content = state.synthesized_sections[question]
            else:
                # Synthesize if not already done
                section_content = self._synthesize_question(question, state)
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

        summary = self._call_llm(
            "You are a research writer. Write a concise executive summary.",
            summary_prompt,
            temperature=0.3,
        )

        # Build methodology section
        methodology = f"""## Methodology

This report was generated using an agentic research system powered by Databricks Lakeflow Jobs.

### Research Process
- **Mode**: Agentic (ReAct loop with reflection)
- **Original questions**: {len(plan.research_questions)}
- **Additional questions discovered**: {len(state.generated_questions)}
- **Total iterations**: {state.iterations}
- **Total web searches**: {state.total_searches}
- **Total LLM calls**: {state.total_llm_calls}
- **Final confidence**: {state.completion_confidence:.0%}
- **Stop reason**: {state.stop_reason or "N/A"}

### Configuration
- LLM endpoint: {self.llm_endpoint}
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

    @mlflow.trace(name="research_execution")
    def execute_research_plan(self, plan: ResearchPlan) -> str:
        """
        Execute a complete research plan using an agentic loop.

        The agent will:
        1. Decide next action (search, generate question, synthesize, or stop)
        2. Execute the action and update state
        3. Periodically reflect on progress
        4. Stop when confident or budget exhausted

        Args:
            plan: The research plan to execute

        Returns:
            Complete markdown research report
        """
        # Log trace inputs
        span = mlflow.get_current_active_span()
        if span:
            span.set_inputs({"topic": plan.topic, "questions": plan.research_questions})

        # Initialize state
        state = ResearchState(
            topic=plan.topic,
            original_questions=list(plan.research_questions),
            active_questions=list(plan.research_questions),
        )

        # Main agent loop
        while not self._should_terminate(state, plan):
            state.iterations += 1

            # 1. Decide next action
            action = self._decide_next_action(state, plan)

            # 2. Execute the chosen action
            current_question = self._get_current_question(state)

            if action.type == "search" and action.query:
                result = self._execute_search(action.query, state, current_question)
                action_output = f"Search completed: {len(result)} chars"

            elif action.type == "generate_question" and action.new_question:
                result = self._generate_sub_question(action.new_question, state, plan)
                action_output = f"Generated question: {result}" if result else "Question generation skipped"

            elif action.type == "synthesize" and action.question:
                result = self._synthesize_question(action.question, state)
                action_output = f"Synthesized: {action.question}"

            elif action.type == "stop":
                state.should_stop = True
                state.stop_reason = action.stop_reason or "Agent decided to stop"
                action_output = f"Stopping: {state.stop_reason}"

            else:
                # Invalid action, skip
                action_output = f"Skipped invalid action: {action.type}"

            # 3. Record action in history
            state.action_history.append({
                "type": action.type,
                "input": {
                    "query": action.query,
                    "question": action.question,
                    "new_question": action.new_question,
                    "reasoning": action.reasoning,
                },
                "output": action_output,
                "timestamp": datetime.now().isoformat(),
            })

            # 4. Reflect periodically
            if state.iterations % plan.reflection_interval == 0:
                self._reflect_on_progress(state, plan)

        # Generate final report from accumulated state
        report = self._generate_report(state, plan)

        # Log trace outputs
        span = mlflow.get_current_active_span()
        if span:
            span.set_outputs({
                "iterations": state.iterations,
                "total_searches": state.total_searches,
                "total_llm_calls": state.total_llm_calls,
                "completion_confidence": state.completion_confidence,
                "stop_reason": state.stop_reason,
            })

        return report

    def save_report(self, report: str, output_path: str) -> str:
        """Save report to UC Volume."""
        with open(output_path, "w") as f:
            f.write(report)
        return output_path

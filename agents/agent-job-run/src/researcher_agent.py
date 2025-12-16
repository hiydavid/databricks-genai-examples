"""Researcher Agent that executes research plans using MCP web search tools.

This agent runs asynchronously via Databricks Lakeflow Job.
"""

from datetime import datetime
from typing import Optional

from databricks.sdk import WorkspaceClient
from databricks_mcp import DatabricksMCPClient

from models.research_plan import ResearchPlan


class ResearcherAgent:
    """Agent that executes research plans using web search via MCP."""

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

    def search_web(self, query: str) -> str:
        """Execute a web search via MCP tool."""
        # Find the search tool (name may vary based on MCP configuration)
        search_tool_name = None
        for name in self.tools:
            if "search" in name.lower():
                search_tool_name = name
                break

        if not search_tool_name:
            return f"No search tool available. Available tools: {list(self.tools.keys())}"

        response = self.mcp_client.call_tool(search_tool_name, {"query": query})
        return "".join([c.text for c in response.content if hasattr(c, "text")])

    def synthesize_findings(self, question: str, search_results: list[str]) -> str:
        """Use LLM to synthesize search results into a coherent section."""
        combined_results = "\n\n---\n\n".join(search_results)

        response = self.client.chat.completions.create(
            model=self.llm_endpoint,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a research analyst. Synthesize the provided search results "
                        "into a clear, well-structured section that answers the research question. "
                        "Include key findings, cite sources where possible, and note any conflicting information."
                    ),
                },
                {
                    "role": "user",
                    "content": f"Research Question: {question}\n\nSearch Results:\n{combined_results}",
                },
            ],
            temperature=0.3,
        )

        return response.choices[0].message.content

    def research_question(self, question: str, max_searches: int = 3) -> str:
        """
        Research a single question using web searches.

        Args:
            question: The research question to investigate
            max_searches: Maximum number of search queries to run

        Returns:
            A synthesized markdown section answering the question
        """
        # Generate search queries for this question
        query_response = self.client.chat.completions.create(
            model=self.llm_endpoint,
            messages=[
                {
                    "role": "system",
                    "content": (
                        f"Generate {max_searches} specific web search queries to research this question. "
                        "Return only the queries, one per line, no numbering or bullets."
                    ),
                },
                {"role": "user", "content": question},
            ],
            temperature=0.5,
        )

        queries = query_response.choices[0].message.content.strip().split("\n")
        queries = [q.strip() for q in queries if q.strip()][:max_searches]

        # Execute searches
        search_results = []
        for query in queries:
            result = self.search_web(query)
            search_results.append(f"Query: {query}\n\n{result}")

        # Synthesize findings
        synthesis = self.synthesize_findings(question, search_results)

        return f"### {question}\n\n{synthesis}\n"

    def generate_summary(self, sections: list[str]) -> str:
        """Generate an executive summary from all research sections."""
        combined = "\n\n".join(sections)

        response = self.client.chat.completions.create(
            model=self.llm_endpoint,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Write a concise executive summary (2-3 paragraphs) of the research findings. "
                        "Highlight the most important insights and any key conclusions."
                    ),
                },
                {"role": "user", "content": combined},
            ],
            temperature=0.3,
        )

        return response.choices[0].message.content

    def execute_research_plan(self, plan: ResearchPlan) -> str:
        """
        Execute a complete research plan and return the full report.

        Args:
            plan: The research plan to execute

        Returns:
            Complete markdown research report
        """
        sections = []

        for question in plan.research_questions:
            section = self.research_question(
                question, max_searches=plan.max_searches_per_question
            )
            sections.append(section)

        summary = self.generate_summary(sections)

        report = f"""# Research Report: {plan.topic}

**Generated:** {datetime.now().isoformat()}

## Executive Summary

{summary}

## Research Findings

{"".join(sections)}

---

## Methodology

This report was generated using async agentic research powered by Databricks Lakeflow Jobs.
- Research questions: {len(plan.research_questions)}
- Max searches per question: {plan.max_searches_per_question}
- LLM endpoint: {self.llm_endpoint}
"""

        return report

    def save_report(self, report: str, output_path: str) -> str:
        """Save report to UC Volume."""
        with open(output_path, "w") as f:
            f.write(report)
        return output_path

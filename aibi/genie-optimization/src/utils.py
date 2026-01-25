"""
Genie Space Optimizer Utilities

Helper classes and constants for analyzing and optimizing Databricks Genie Spaces.
"""

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
from openai import OpenAI


def load_config(config_path: str = None) -> dict:
    """Load configuration from YAML file."""
    if config_path is None:
        # Look for config.yaml in parent directory of src/
        config_path = Path(__file__).parent.parent / "config.yaml"
    with open(config_path) as f:
        return yaml.safe_load(f)

# =============================================================================
# Evaluation Checklists
# =============================================================================

CHECKLIST_DATA_SOURCES = """
### Tables
**Table Selection:**
- Between 1 and 25 tables are configured
- Tables are focused (only necessary tables for intended questions)
- Tables are well-annotated with descriptions
- Datasets are simplified (prejoined where appropriate, unnecessary columns removed)

**Column Descriptions:**
- Columns have descriptions defined
- Descriptions provide clear, contextual information beyond what column names convey

**Column Synonyms:**
- Key columns have synonyms defined
- Synonyms include business terminology, abbreviations, and alternative phrasings users would naturally use

**Example Values / Value Dictionary:**
- Filterable columns have `get_example_values` enabled
- Columns with discrete values have `build_value_dictionary` enabled

**Column Exclusions:**
- No duplicative columns exist within the same table
- Columns not relevant to the space's purpose are hidden

### Metric Views
- Metric views have descriptions (if any exist)
- Pre-computed metrics have comments explaining valid aggregations
"""

CHECKLIST_INSTRUCTIONS = """
### Text Instructions
- At least 1 text instruction exists
- Instructions are focused and minimal (not excessive)
- Instructions provide globally-applied context
- Business jargon is mapped to standard terminology where needed
- SQL examples, metrics, join logic, and filters are moved to their respective sections (not embedded in text instructions)

### Example Question SQLs
**Example Questions:**
- At least 1 example question-SQL pair exists
- Examples cover complex, multi-part questions with intricate SQL patterns
- Examples are diverse (not redundant)
- Queries are as short as possible while remaining complete

**Parameters:**
- Parameters have descriptions defined (if parameters exist)
- Parameters are used for commonly varied values (dates, names, limits)

**Usage Guidance:**
- Complex examples have usage guidance describing applicable scenarios and trigger keywords

### SQL Functions
- SQL functions are registered and documented in Unity Catalog (if any defined)

### Join Specs
- Join specs are defined for multi-table relationships and complex scenarios like self-joins (if applicable)
- Foreign key references are defined in Unity Catalog when possible
- Join specs have comments explaining the relationship

### SQL Snippets
**Filters:**
- Common time period filters exist
- Business-specific filters are defined

**Expressions:**
- Reusable expressions are defined for common categorizations
- Expressions include synonyms for user terminology

**Measures:**
- More than 1 measure is defined (consider adding more if only 1 exists)
- Measures cover standard business concepts used across queries
"""

CHECKLIST_BENCHMARKS = """
### Questions
- At least 10 diverse benchmark Q&A pairs exist, covering different use cases and topics
"""

# =============================================================================
# HTMLRenderer
# =============================================================================


class HTMLRenderer:
    """Renders HTML components for notebook display."""

    @staticmethod
    def _escape_html(text: str) -> str:
        """Escape HTML special characters."""
        return (
            str(text)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&#39;")
        )

    @staticmethod
    def render_json_section(title: str, data: Any, collapsed: bool = True) -> str:
        """Render a collapsible JSON section."""
        json_str = json.dumps(data, indent=2, default=str) if data else "null"
        escaped_json = HTMLRenderer._escape_html(json_str)
        collapse_state = "none" if collapsed else "block"
        toggle_text = "+" if collapsed else "-"

        return f"""
        <div style="margin: 10px 0; border: 1px solid #ddd; border-radius: 8px; overflow: hidden;">
            <div onclick="
                var content = this.nextElementSibling;
                var toggle = this.querySelector('.toggle');
                if (content.style.display === 'none') {{
                    content.style.display = 'block';
                    toggle.textContent = '-';
                }} else {{
                    content.style.display = 'none';
                    toggle.textContent = '+';
                }}
            " style="background: #f5f5f5; padding: 12px 16px; cursor: pointer; display: flex; justify-content: space-between; align-items: center;">
                <span style="font-weight: 600; font-size: 14px;">{HTMLRenderer._escape_html(title)}</span>
                <span class="toggle" style="font-family: monospace; font-size: 18px; color: #666;">{toggle_text}</span>
            </div>
            <div style="display: {collapse_state}; padding: 16px; background: #1e1e1e; overflow-x: auto;">
                <pre style="margin: 0; font-family: 'Monaco', 'Menlo', monospace; font-size: 12px; color: #d4d4d4; white-space: pre-wrap; word-wrap: break-word;">{escaped_json}</pre>
            </div>
        </div>
        """

    @staticmethod
    def render_checklist_analysis(section: str, analysis: dict) -> str:
        """Render LLM checklist analysis with styled pass/fail indicators."""
        items_html = ""
        for item in analysis.get("items", []):
            status = item.get("status", "na")
            name = HTMLRenderer._escape_html(item.get("name", ""))
            explanation = HTMLRenderer._escape_html(item.get("explanation", ""))

            status_colors = {
                "pass": ("#22c55e", "#dcfce7", "#166534"),  # green
                "fail": ("#ef4444", "#fee2e2", "#991b1b"),  # red
                "warning": ("#f59e0b", "#fef3c7", "#92400e"),  # amber
                "na": ("#6b7280", "#f3f4f6", "#374151"),  # gray
            }
            border_color, bg_color, text_color = status_colors.get(
                status, status_colors["na"]
            )

            status_icon = {"pass": "✓", "fail": "✗", "warning": "!", "na": "—"}.get(
                status, "—"
            )

            items_html += f"""
            <div style="display: flex; align-items: flex-start; gap: 12px; padding: 12px; margin: 8px 0; background: {bg_color}; border-left: 4px solid {border_color}; border-radius: 4px;">
                <span style="font-size: 16px; font-weight: bold; color: {border_color}; min-width: 20px;">{status_icon}</span>
                <div style="flex: 1;">
                    <div style="font-weight: 600; color: {text_color};">{name}</div>
                    <div style="font-size: 13px; color: #4b5563; margin-top: 4px;">{explanation}</div>
                </div>
            </div>
            """

        summary = HTMLRenderer._escape_html(analysis.get("summary", ""))

        return f"""
        <div style="margin: 20px 0; border: 1px solid #e5e7eb; border-radius: 8px; overflow: hidden;">
            <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 16px 20px;">
                <h3 style="margin: 0; font-size: 18px;">{HTMLRenderer._escape_html(section)} Analysis</h3>
            </div>
            <div style="padding: 16px 20px;">
                {items_html}
                <div style="margin-top: 16px; padding: 12px; background: #f8fafc; border-radius: 6px; border: 1px solid #e2e8f0;">
                    <strong style="color: #475569;">Summary:</strong>
                    <p style="margin: 8px 0 0 0; color: #64748b;">{summary}</p>
                </div>
            </div>
        </div>
        """

    @staticmethod
    def render_benchmark_result(
        question: str,
        expected_sql: str,
        generated_sql: str,
        passed: bool,
        details: dict,
    ) -> str:
        """Render a single benchmark result card."""
        status_color = "#22c55e" if passed else "#ef4444"
        status_text = "PASS" if passed else "FAIL"
        status_bg = "#dcfce7" if passed else "#fee2e2"

        expected_escaped = HTMLRenderer._escape_html(expected_sql or "(none)")
        generated_escaped = HTMLRenderer._escape_html(generated_sql or "(none)")
        question_escaped = HTMLRenderer._escape_html(question)

        details_html = ""
        if details:
            for key, value in details.items():
                details_html += f"<div><strong>{HTMLRenderer._escape_html(key)}:</strong> {HTMLRenderer._escape_html(str(value))}</div>"

        return f"""
        <div style="margin: 16px 0; border: 1px solid #e5e7eb; border-radius: 8px; overflow: hidden;">
            <div style="background: {status_bg}; padding: 12px 16px; display: flex; justify-content: space-between; align-items: center;">
                <span style="font-weight: 600; color: #374151;">{question_escaped}</span>
                <span style="background: {status_color}; color: white; padding: 4px 12px; border-radius: 4px; font-weight: bold; font-size: 12px;">{status_text}</span>
            </div>
            <div style="padding: 16px;">
                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 16px;">
                    <div>
                        <div style="font-weight: 600; color: #374151; margin-bottom: 8px;">Expected SQL</div>
                        <pre style="background: #1e1e1e; color: #d4d4d4; padding: 12px; border-radius: 4px; font-size: 11px; overflow-x: auto; margin: 0;">{expected_escaped}</pre>
                    </div>
                    <div>
                        <div style="font-weight: 600; color: #374151; margin-bottom: 8px;">Generated SQL</div>
                        <pre style="background: #1e1e1e; color: #d4d4d4; padding: 12px; border-radius: 4px; font-size: 11px; overflow-x: auto; margin: 0;">{generated_escaped}</pre>
                    </div>
                </div>
                {f'<div style="margin-top: 12px; padding: 12px; background: #f8fafc; border-radius: 4px; font-size: 13px;">{details_html}</div>' if details_html else ''}
            </div>
        </div>
        """

    @staticmethod
    def render_summary_table(results: list[dict]) -> str:
        """Render a summary table of all benchmark results."""
        rows_html = ""
        pass_count = 0
        total_count = len(results)

        for r in results:
            passed = r.get("passed", False)
            if passed:
                pass_count += 1
            status_color = "#22c55e" if passed else "#ef4444"
            status_text = "PASS" if passed else "FAIL"
            question = HTMLRenderer._escape_html(r.get("question", "")[:60])
            if len(r.get("question", "")) > 60:
                question += "..."
            summary = HTMLRenderer._escape_html(r.get("summary", ""))

            rows_html += f"""
            <tr style="border-bottom: 1px solid #e5e7eb;">
                <td style="padding: 12px; color: {status_color}; font-weight: bold;">{status_text}</td>
                <td style="padding: 12px;">{question}</td>
                <td style="padding: 12px; color: #6b7280; font-size: 13px;">{summary}</td>
            </tr>
            """

        pass_rate = (pass_count / total_count * 100) if total_count > 0 else 0
        header_color = (
            "#22c55e"
            if pass_rate >= 80
            else "#f59e0b" if pass_rate >= 50 else "#ef4444"
        )

        return f"""
        <div style="margin: 20px 0; border: 1px solid #e5e7eb; border-radius: 8px; overflow: hidden;">
            <div style="background: linear-gradient(135deg, #1e3a5f 0%, #2d5a87 100%); color: white; padding: 16px 20px; display: flex; justify-content: space-between; align-items: center;">
                <h3 style="margin: 0; font-size: 18px;">Benchmark Results Summary</h3>
                <div style="background: {header_color}; padding: 8px 16px; border-radius: 6px; font-weight: bold;">
                    {pass_count}/{total_count} Passed ({pass_rate:.0f}%)
                </div>
            </div>
            <table style="width: 100%; border-collapse: collapse;">
                <thead>
                    <tr style="background: #f8fafc; border-bottom: 2px solid #e5e7eb;">
                        <th style="padding: 12px; text-align: left; font-weight: 600; width: 80px;">Status</th>
                        <th style="padding: 12px; text-align: left; font-weight: 600;">Question</th>
                        <th style="padding: 12px; text-align: left; font-weight: 600;">Details</th>
                    </tr>
                </thead>
                <tbody>
                    {rows_html}
                </tbody>
            </table>
        </div>
        """

    @staticmethod
    def render_benchmark_analysis(analysis: dict) -> str:
        """Render the LLM benchmark analysis as styled HTML with collapsible sections."""
        benchmark_analyses = analysis.get("benchmark_analyses", [])
        overall_recommendations = analysis.get("overall_recommendations", [])
        summary = HTMLRenderer._escape_html(analysis.get("summary", ""))

        # Render individual benchmark analyses
        analyses_html = ""
        for i, ba in enumerate(benchmark_analyses):
            question = HTMLRenderer._escape_html(ba.get("question", ""))
            comparison = HTMLRenderer._escape_html(ba.get("comparison", ""))
            diagnosis = HTMLRenderer._escape_html(ba.get("diagnosis", ""))
            recommendations = ba.get("recommendations", [])

            recs_html = "".join(
                f'<li style="margin: 4px 0; color: #374151;">{HTMLRenderer._escape_html(r)}</li>'
                for r in recommendations
            )

            analyses_html += f"""
            <div style="margin: 12px 0; border: 1px solid #e5e7eb; border-radius: 6px; overflow: hidden;">
                <div onclick="
                    var content = this.nextElementSibling;
                    var toggle = this.querySelector('.toggle');
                    if (content.style.display === 'none') {{
                        content.style.display = 'block';
                        toggle.textContent = '−';
                    }} else {{
                        content.style.display = 'none';
                        toggle.textContent = '+';
                    }}
                " style="background: #f8fafc; padding: 12px 16px; cursor: pointer; display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #e5e7eb;">
                    <span style="font-weight: 600; color: #1e3a5f; font-size: 14px;">Q{i + 1}: {question[:80]}{'...' if len(ba.get('question', '')) > 80 else ''}</span>
                    <span class="toggle" style="font-family: monospace; font-size: 18px; color: #666;">+</span>
                </div>
                <div style="display: none; padding: 16px;">
                    <div style="margin-bottom: 16px;">
                        <div style="font-weight: 600; color: #667eea; margin-bottom: 6px; font-size: 13px;">SQL Comparison</div>
                        <div style="background: #f1f5f9; padding: 12px; border-radius: 4px; font-size: 13px; color: #475569; white-space: pre-wrap;">{comparison}</div>
                    </div>
                    <div style="margin-bottom: 16px;">
                        <div style="font-weight: 600; color: #667eea; margin-bottom: 6px; font-size: 13px;">Diagnosis</div>
                        <div style="background: #fef3c7; padding: 12px; border-radius: 4px; font-size: 13px; color: #92400e; white-space: pre-wrap;">{diagnosis}</div>
                    </div>
                    <div>
                        <div style="font-weight: 600; color: #667eea; margin-bottom: 6px; font-size: 13px;">Recommendations</div>
                        <ul style="margin: 0; padding-left: 20px; font-size: 13px;">
                            {recs_html if recs_html else '<li style="color: #6b7280;">No specific recommendations</li>'}
                        </ul>
                    </div>
                </div>
            </div>
            """

        # Render overall recommendations
        overall_recs_html = "".join(
            f"""
            <div style="display: flex; align-items: flex-start; gap: 10px; padding: 10px 12px; background: #f0fdf4; border-left: 3px solid #22c55e; border-radius: 4px; margin: 8px 0;">
                <span style="color: #22c55e; font-weight: bold;">→</span>
                <span style="color: #166534; font-size: 13px;">{HTMLRenderer._escape_html(r)}</span>
            </div>
            """
            for r in overall_recommendations
        )

        return f"""
        <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 20px 0;">
            <div style="border: 1px solid #e5e7eb; border-radius: 8px; overflow: hidden;">
                <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 16px 20px;">
                    <h3 style="margin: 0; font-size: 18px;">Benchmark Analysis & Recommendations</h3>
                    <p style="margin: 8px 0 0 0; opacity: 0.9; font-size: 14px;">LLM-powered comparison of expected vs generated SQL</p>
                </div>

                <div style="padding: 20px;">
                    <div style="background: #f8fafc; padding: 16px; border-radius: 6px; margin-bottom: 20px; border: 1px solid #e2e8f0;">
                        <div style="font-weight: 600; color: #1e3a5f; margin-bottom: 8px;">Overall Assessment</div>
                        <p style="margin: 0; color: #475569; font-size: 14px;">{summary}</p>
                    </div>

                    <div style="margin-bottom: 20px;">
                        <h4 style="color: #1e3a5f; margin: 0 0 12px 0; font-size: 16px;">Individual Benchmark Analysis</h4>
                        <p style="color: #6b7280; font-size: 13px; margin: 0 0 12px 0;">Click each benchmark to expand details</p>
                        {analyses_html if analyses_html else '<p style="color: #6b7280;">No benchmark analyses available</p>'}
                    </div>

                    <div>
                        <h4 style="color: #1e3a5f; margin: 0 0 12px 0; font-size: 16px;">Overall Recommendations</h4>
                        {overall_recs_html if overall_recs_html else '<p style="color: #6b7280;">No overall recommendations</p>'}
                    </div>
                </div>
            </div>
        </div>
        """


# =============================================================================
# LLMAnalyzer
# =============================================================================


class LLMAnalyzer:
    """Uses Foundation Model API to analyze Genie Space sections against checklists."""

    def __init__(self, dbutils, config: dict = None):
        """Initialize the LLM client."""
        if config is None:
            config = load_config()

        token = dbutils.secrets.get(
            scope=config["secret_scope"],
            key=config["secret_key"],
        )
        host = WorkspaceClient().config.host

        self.client = OpenAI(
            api_key=token,
            base_url=f"{host}/serving-endpoints",
        )
        self.endpoint = config.get("llm_endpoint", "databricks-claude-sonnet-4-5")

    def _analyze_section(
        self, section_name: str, section_data: Any, checklist: str
    ) -> dict:
        """Analyze a section against its checklist."""
        system_prompt = """You are an expert Databricks Genie Space analyst.
Analyze the provided Genie Space configuration section against the checklist criteria.

For each checklist item, determine:
- status: "pass" (meets criteria), "fail" (does not meet), "warning" (partially meets), or "na" (not applicable)
- name: short name of the checklist item
- explanation: brief explanation of your assessment

Return your analysis as JSON with this structure:
{
    "items": [
        {"name": "item name", "status": "pass|fail|warning|na", "explanation": "why"}
    ],
    "summary": "overall assessment in 1-2 sentences"
}"""

        section_json = (
            json.dumps(section_data, indent=2, default=str) if section_data else "null"
        )

        user_prompt = f"""Analyze the following '{section_name}' section from a Genie Space configuration.

## Section Data:
```json
{section_json[:15000]}
```

## Checklist to evaluate:
{checklist}

Provide your analysis as JSON."""

        response = self.client.chat.completions.create(
            model=self.endpoint,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "checklist_analysis",
                    "strict": True,
                    "schema": {
                        "type": "object",
                        "properties": {
                            "items": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "name": {"type": "string"},
                                        "status": {"type": "string"},
                                        "explanation": {"type": "string"},
                                    },
                                    "required": ["name", "status", "explanation"],
                                    "additionalProperties": False,
                                },
                            },
                            "summary": {"type": "string"},
                        },
                        "required": ["items", "summary"],
                        "additionalProperties": False,
                    },
                },
            },
            max_tokens=2000,
            temperature=0,
        )

        try:
            return json.loads(response.choices[0].message.content)
        except json.JSONDecodeError:
            return {
                "items": [],
                "summary": "Failed to parse LLM response",
            }

    def analyze_data_sources(self, data_sources: Any) -> dict:
        """Analyze the data_sources section."""
        return self._analyze_section(
            "data_sources", data_sources, CHECKLIST_DATA_SOURCES
        )

    def analyze_instructions(self, instructions: Any) -> dict:
        """Analyze the instructions section."""
        return self._analyze_section(
            "instructions", instructions, CHECKLIST_INSTRUCTIONS
        )

    def analyze_benchmarks(self, benchmarks: Any) -> dict:
        """Analyze the benchmarks section."""
        return self._analyze_section("benchmarks", benchmarks, CHECKLIST_BENCHMARKS)

    def analyze_benchmark_results(
        self,
        benchmark_results: list,
        serialized_space: dict,
    ) -> dict:
        """
        Compare expected vs generated SQL for each benchmark and provide
        actionable recommendations for improving the Genie space.

        Args:
            benchmark_results: List of BenchmarkResult with question, expected_sql, generated_sql, error
            serialized_space: Full serialized space config for context

        Returns:
            dict with benchmark_analyses, overall_recommendations, and summary
        """
        system_prompt = """You are an expert Databricks Genie Space analyst.
Your task is to analyze benchmark results where Genie generated SQL that may differ from the expected SQL.

For each benchmark:
1. Compare the expected SQL with the generated SQL semantically (not just syntactically)
2. Identify meaningful differences in logic, joins, filters, columns, or aggregations
3. Diagnose WHY Genie may have generated different SQL based on the space configuration
4. Provide SPECIFIC, ACTIONABLE recommendations to improve the Genie space

Focus on:
- Missing or unclear column descriptions that could cause wrong column selection
- Missing synonyms that could cause misinterpretation of user terminology
- Missing or unclear instructions that could guide Genie
- Example SQL queries that could demonstrate the expected pattern
- Join specifications that could clarify table relationships

Be concise but specific. Reference actual table names, column names, and instruction text where relevant."""

        # Prepare benchmark data for the prompt
        benchmark_data = []
        for r in benchmark_results:
            benchmark_data.append({
                "question": r.question,
                "expected_sql": r.expected_sql,
                "generated_sql": r.generated_sql or "(no SQL generated)",
                "passed": r.passed,
                "error": r.error,
            })

        # Truncate serialized space to avoid token limits
        space_summary = {
            "instructions": serialized_space.get("instructions", {}),
            "data_sources": self._truncate_data_sources(
                serialized_space.get("data_sources", {})
            ),
        }

        user_prompt = f"""Analyze these benchmark results and provide recommendations to improve the Genie space.

## Genie Space Configuration (summarized):
```json
{json.dumps(space_summary, indent=2, default=str)[:12000]}
```

## Benchmark Results:
```json
{json.dumps(benchmark_data, indent=2, default=str)}
```

For each benchmark, compare the expected vs generated SQL and explain:
1. What semantic differences exist (if any)
2. Why Genie may have generated different SQL
3. Specific changes to improve accuracy

Then provide overall recommendations for the space."""

        response = self.client.chat.completions.create(
            model=self.endpoint,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "benchmark_analysis",
                    "strict": True,
                    "schema": {
                        "type": "object",
                        "properties": {
                            "benchmark_analyses": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "question": {"type": "string"},
                                        "comparison": {
                                            "type": "string",
                                            "description": "Semantic comparison of expected vs generated SQL",
                                        },
                                        "diagnosis": {
                                            "type": "string",
                                            "description": "Why Genie generated different SQL",
                                        },
                                        "recommendations": {
                                            "type": "array",
                                            "items": {"type": "string"},
                                            "description": "Specific actionable changes",
                                        },
                                    },
                                    "required": [
                                        "question",
                                        "comparison",
                                        "diagnosis",
                                        "recommendations",
                                    ],
                                    "additionalProperties": False,
                                },
                            },
                            "overall_recommendations": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "High-level improvements for the space",
                            },
                            "summary": {
                                "type": "string",
                                "description": "Overall assessment of benchmark performance",
                            },
                        },
                        "required": [
                            "benchmark_analyses",
                            "overall_recommendations",
                            "summary",
                        ],
                        "additionalProperties": False,
                    },
                },
            },
            max_tokens=4000,
            temperature=0,
        )

        try:
            return json.loads(response.choices[0].message.content)
        except json.JSONDecodeError:
            return {
                "benchmark_analyses": [],
                "overall_recommendations": [],
                "summary": "Failed to parse LLM response",
            }

    def _truncate_data_sources(self, data_sources: dict) -> dict:
        """Truncate data sources to include only essential metadata."""
        if not data_sources:
            return {}

        truncated = {}

        # Include tables with just names, descriptions, and column info
        if "tables" in data_sources:
            truncated["tables"] = []
            for table in data_sources.get("tables", [])[:10]:  # Limit to 10 tables
                table_info = {
                    "name": table.get("name"),
                    "description": table.get("description"),
                }
                # Include columns with descriptions/synonyms only
                if "columns" in table:
                    table_info["columns"] = [
                        {
                            "name": c.get("name"),
                            "description": c.get("description"),
                            "synonyms": c.get("synonyms"),
                        }
                        for c in table.get("columns", [])[:20]  # Limit columns
                    ]
                truncated["tables"].append(table_info)

        return truncated

    def compare_sql_semantically(
        self,
        question: str,
        expected_sql: str,
        generated_sql: str,
    ) -> dict:
        """
        Use LLM to determine if two SQL queries are semantically equivalent.

        Returns:
            dict with 'equivalent' (bool), 'explanation' (str)
        """
        if not generated_sql or not generated_sql.strip():
            return {
                "equivalent": False,
                "explanation": "No SQL was generated",
            }

        system_prompt = """You are an expert SQL analyst. Compare two SQL queries and determine if they are semantically equivalent - meaning they would return the same results for the given question.

Consider:
- Column aliases don't matter if the underlying data is the same
- ORDER BY differences only matter if ordering is relevant to the question
- Minor syntactic differences (formatting, explicit vs implicit joins) don't matter
- Different but equivalent expressions (e.g., COALESCE vs IFNULL) are equivalent
- The queries should select the same columns and apply the same filters/logic

Be strict: if the queries would return different data, they are NOT equivalent."""

        user_prompt = f"""Question: {question}

Expected SQL:
```sql
{expected_sql}
```

Generated SQL:
```sql
{generated_sql}
```

Are these queries semantically equivalent for answering the question?"""

        response = self.client.chat.completions.create(
            model=self.endpoint,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "sql_comparison",
                    "strict": True,
                    "schema": {
                        "type": "object",
                        "properties": {
                            "equivalent": {
                                "type": "boolean",
                                "description": "True if queries are semantically equivalent",
                            },
                            "explanation": {
                                "type": "string",
                                "description": "Brief explanation of why they are or aren't equivalent",
                            },
                        },
                        "required": ["equivalent", "explanation"],
                        "additionalProperties": False,
                    },
                },
            },
            max_tokens=500,
            temperature=0,
        )

        try:
            return json.loads(response.choices[0].message.content)
        except json.JSONDecodeError:
            return {
                "equivalent": False,
                "explanation": "Failed to parse LLM comparison response",
            }


# =============================================================================
# GenieSpaceClient
# =============================================================================


@dataclass
class GenieConversationResult:
    """Result from a Genie conversation."""

    conversation_id: str
    message_id: str
    generated_sql: Optional[str] = None
    response_text: Optional[str] = None
    error: Optional[str] = None


class GenieSpaceClient:
    """Wrapper for Databricks Genie Space SDK operations."""

    def __init__(self):
        """Initialize the SDK client."""
        self.client = WorkspaceClient()

    def get_serialized_space(self, space_id: str) -> dict:
        """Fetch a Genie Space with serialized configuration."""
        space = self.client.genie.get_space(
            space_id=space_id,
            include_serialized_space=True,
        )

        if not space.serialized_space:
            raise ValueError(
                "Could not retrieve serialized_space. "
                "Ensure you have CAN EDIT permission on the Genie Space."
            )

        return {
            "title": space.title,
            "description": space.description,
            "space_id": space_id,
            "serialized_space": json.loads(space.serialized_space),
        }

    def ask_question(
        self, space_id: str, question: str, timeout_minutes: int = 5
    ) -> GenieConversationResult:
        """Ask a question to Genie and retrieve the response."""
        from datetime import timedelta

        from databricks.sdk.service.dashboards import MessageStatus

        try:
            # Use the wait pattern - start_conversation returns a waiter
            message = self.client.genie.start_conversation_and_wait(
                space_id=space_id,
                content=question,
                timeout=timedelta(minutes=timeout_minutes),
            )
        except TimeoutError:
            return GenieConversationResult(
                conversation_id="",
                message_id="",
                error="Timeout waiting for Genie response",
            )
        except Exception as e:
            return GenieConversationResult(
                conversation_id="",
                message_id="",
                error=f"Error starting conversation: {e}",
            )

        conversation_id = message.conversation_id
        message_id = message.id

        if message.status == MessageStatus.FAILED:
            error_msg = message.error.message if message.error else "Unknown error"
            return GenieConversationResult(
                conversation_id=conversation_id,
                message_id=message_id,
                error=f"Genie failed: {error_msg}",
            )

        # Extract SQL from attachments
        generated_sql = None
        response_text = None

        if message.attachments:
            for attachment in message.attachments:
                if attachment.query and attachment.query.query:
                    generated_sql = attachment.query.query
                    break

        # Try to get response text
        if message.attachments:
            for attachment in message.attachments:
                if attachment.text and attachment.text.content:
                    response_text = attachment.text.content
                    break

        return GenieConversationResult(
            conversation_id=conversation_id,
            message_id=message_id,
            generated_sql=generated_sql,
            response_text=response_text,
        )


# =============================================================================
# BenchmarkRunner
# =============================================================================


@dataclass
class BenchmarkResult:
    """Result from running a single benchmark."""

    question: str
    expected_sql: str
    generated_sql: Optional[str] = None
    passed: bool = False
    summary: str = ""
    expected_row_count: Optional[int] = None
    generated_row_count: Optional[int] = None
    error: Optional[str] = None


class BenchmarkRunner:
    """Runs benchmark tests against a Genie Space."""

    def __init__(
        self,
        genie_client: GenieSpaceClient,
        warehouse_id: Optional[str] = None,
        llm_analyzer: Optional[LLMAnalyzer] = None,
    ):
        """Initialize the benchmark runner."""
        self.genie_client = genie_client
        self.warehouse_id = warehouse_id
        self.llm_analyzer = llm_analyzer
        self.sdk_client = WorkspaceClient()

    def _execute_sql(self, sql: str) -> tuple[Optional[int], Optional[str]]:
        """Execute SQL and return (row_count, error)."""
        if not self.warehouse_id:
            return None, "No warehouse_id provided for SQL execution"

        try:
            response = self.sdk_client.statement_execution.execute_statement(
                warehouse_id=self.warehouse_id,
                statement=sql,
                wait_timeout="50s",
            )

            if response.status and response.status.state == StatementState.SUCCEEDED:
                row_count = 0
                if response.result and response.result.row_count is not None:
                    row_count = response.result.row_count
                elif (
                    response.manifest and response.manifest.total_row_count is not None
                ):
                    row_count = response.manifest.total_row_count
                return row_count, None
            else:
                error = response.status.error if response.status else "Unknown error"
                return None, f"SQL execution failed: {error}"
        except Exception as e:
            return None, f"SQL execution error: {str(e)}"

    def _compare_results(
        self, expected_sql: str, generated_sql: str
    ) -> tuple[bool, str, Optional[int], Optional[int]]:
        """Compare SQL results and return (passed, summary, expected_rows, generated_rows)."""
        if not generated_sql:
            return False, "No SQL generated", None, None

        # Execute both queries
        expected_rows, expected_error = self._execute_sql(expected_sql)
        generated_rows, generated_error = self._execute_sql(generated_sql)

        if expected_error:
            return False, f"Expected SQL error: {expected_error}", None, None

        if generated_error:
            return False, f"Generated SQL error: {generated_error}", expected_rows, None

        # Compare row counts
        if expected_rows == generated_rows:
            return (
                True,
                f"Row counts match: {expected_rows}",
                expected_rows,
                generated_rows,
            )
        else:
            return (
                False,
                f"Row count mismatch: expected {expected_rows}, got {generated_rows}",
                expected_rows,
                generated_rows,
            )

    def run_benchmark(self, space_id: str, benchmark: dict) -> BenchmarkResult:
        """Run a single benchmark test."""
        question = benchmark.get("question", [])
        if isinstance(question, list):
            question = question[0] if question else ""

        # Extract expected SQL from answer array
        # Structure: answer: [{"format": "SQL", "content": ["SELECT\n", "..."]}]
        expected_sql = ""
        answers = benchmark.get("answer", [])
        for ans in answers:
            if ans.get("format") == "SQL":
                content = ans.get("content", [])
                expected_sql = "".join(content) if isinstance(content, list) else content
                break

        if not question:
            return BenchmarkResult(
                question="(empty)",
                expected_sql=expected_sql,
                error="No question provided",
                summary="Invalid benchmark: no question",
            )

        # Ask Genie the question
        result = self.genie_client.ask_question(space_id, question)

        if result.error:
            return BenchmarkResult(
                question=question,
                expected_sql=expected_sql,
                generated_sql=result.generated_sql,
                error=result.error,
                summary=f"Genie error: {result.error}",
            )

        generated_sql = result.generated_sql

        # Use LLM semantic comparison if analyzer is available
        if self.llm_analyzer:
            comparison = self.llm_analyzer.compare_sql_semantically(
                question=question,
                expected_sql=expected_sql,
                generated_sql=generated_sql,
            )
            passed = comparison.get("equivalent", False)
            summary = comparison.get("explanation", "")
            return BenchmarkResult(
                question=question,
                expected_sql=expected_sql,
                generated_sql=generated_sql,
                passed=passed,
                summary=summary,
            )

        # Fallback: If no warehouse_id, just check if SQL was generated
        if not self.warehouse_id:
            passed = generated_sql is not None and len(generated_sql.strip()) > 0
            summary = (
                "SQL generated (not executed - no warehouse)"
                if passed
                else "No SQL generated"
            )
            return BenchmarkResult(
                question=question,
                expected_sql=expected_sql,
                generated_sql=generated_sql,
                passed=passed,
                summary=summary,
            )

        # Fallback: Compare results by executing both SQLs (row count only)
        passed, summary, expected_rows, generated_rows = self._compare_results(
            expected_sql, generated_sql
        )

        return BenchmarkResult(
            question=question,
            expected_sql=expected_sql,
            generated_sql=generated_sql,
            passed=passed,
            summary=summary,
            expected_row_count=expected_rows,
            generated_row_count=generated_rows,
        )

    def run_all_benchmarks(
        self, space_id: str, benchmarks: list[dict]
    ) -> list[BenchmarkResult]:
        """Run all benchmark tests."""
        results = []
        for i, benchmark in enumerate(benchmarks):
            question = benchmark.get("question", "")
            if isinstance(question, list):
                question = question[0] if question else ""
            print(f"Running benchmark {i + 1}/{len(benchmarks)}: {question[:50]}...")
            result = self.run_benchmark(space_id, benchmark)
            results.append(result)
        return results

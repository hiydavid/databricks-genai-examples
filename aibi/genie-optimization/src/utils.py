"""
Genie Space Optimizer Utilities

Helper classes and functions for analyzing and optimizing Databricks Genie Spaces.
"""

import copy
import json
import re
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
from openai import OpenAI
from sqlglot import exp, parse_one


def load_config(config_path: str = None) -> dict:
    """Load configuration from YAML file."""
    if config_path is None:
        # Look for config.yaml in parent directory of src/
        config_path = Path(__file__).parent.parent / "config.yaml"
    with open(config_path) as f:
        return yaml.safe_load(f)


# =============================================================================
# Helper Functions
# =============================================================================


ID_PATTERN = re.compile(r"^[0-9a-f]{32}$")


def _as_list(value: Any) -> list:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _text_from_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return " ".join(str(v).strip() for v in value if str(v).strip()).strip()
    return str(value).strip()


def _to_string_segments(value: Any) -> list[str]:
    text = _text_from_value(value)
    return [text] if text else []


def _new_id() -> str:
    return uuid.uuid4().hex


def _column_configs(table: dict) -> list[dict]:
    if isinstance(table.get("column_configs"), list):
        return table.get("column_configs", [])
    if isinstance(table.get("columns"), list):
        return table.get("columns", [])
    return []


def _column_name(column: dict) -> str:
    return str(column.get("column_name") or column.get("name") or "").strip()


def _is_meaningful_text(value: Any) -> bool:
    text = _text_from_value(value)
    if not text:
        return False
    if len(text) < 6:
        return False
    generic = {
        "table",
        "column",
        "amount",
        "metric",
        "description",
        "none",
    }
    return text.lower().strip() not in generic


def _humanize_identifier(identifier: str) -> str:
    if not identifier:
        return "dataset"
    base = identifier.split(".")[-1]
    return base.replace("_", " ")


def _guess_synonyms(column_name: str) -> list[str]:
    col = column_name.lower().strip()
    if not col:
        return []

    mapping = {
        "revenue": ["sales", "sales amount", "total revenue"],
        "amount": ["value", "total", "amount value"],
        "customer": ["client", "account"],
        "region": ["area", "territory", "market"],
        "status": ["state", "stage"],
        "date": ["day", "timestamp"],
        "count": ["total", "volume"],
    }

    suggestions = []
    spaced = col.replace("_", " ")
    if spaced != col:
        suggestions.append(spaced)

    for token, synonyms in mapping.items():
        if token in col:
            suggestions.extend(synonyms)

    unique = []
    for s in suggestions:
        s = s.strip().lower()
        if s and s != col and s not in unique:
            unique.append(s)

    return unique[:4]


def _jaccard_similarity(left: set[str], right: set[str]) -> float:
    if not left and not right:
        return 1.0
    if not left or not right:
        return 0.0
    union = left | right
    if not union:
        return 1.0
    return len(left & right) / len(union)


def _text_similarity(left: str, right: str) -> float:
    l = " ".join(left.lower().split())
    r = " ".join(right.lower().split())

    if not l and not r:
        return 1.0
    if not l or not r:
        return 0.0
    if l == r:
        return 1.0

    l_tokens = set(l.split())
    r_tokens = set(r.split())
    token_score = _jaccard_similarity(l_tokens, r_tokens)

    if l in r or r in l:
        return max(token_score, 0.7)
    return token_score


def _normalize_expression(node: Any) -> str:
    if node is None:
        return ""
    if isinstance(node, str):
        return " ".join(node.strip().lower().split())
    try:
        sql = node.sql(dialect="databricks")
    except Exception:
        sql = str(node)
    return " ".join(sql.strip().lower().split())


def _normalize_serialized_space_recursive(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _normalize_serialized_space_recursive(v) for k, v in value.items()}

    if isinstance(value, list):
        normalized = [_normalize_serialized_space_recursive(v) for v in value]
        if normalized and all(isinstance(v, dict) for v in normalized):
            if all("id" in v for v in normalized):
                return sorted(normalized, key=lambda v: str(v.get("id", "")))
            if all("identifier" in v for v in normalized):
                return sorted(
                    normalized,
                    key=lambda v: str(v.get("identifier", "")),
                )
        return normalized

    return value


# =============================================================================
# Evaluation Checklists
# =============================================================================


CHECKLIST_DATA_SOURCES = """
### Tables
- Table count is between 1 and 25 (warning if >10)
- Table descriptions are specific and non-generic
- Table selection appears focused on stated space purpose

### Columns
- Important columns have contextual descriptions
- Key business columns include synonyms
- Filterable columns enable get_example_values (v1) or enable_format_assistance (v2)
- Low-cardinality columns enable build_value_dictionary (v1) or enable_entity_matching (v2)
- Irrelevant internal columns are hidden when possible

### Metric Views
- Metric views have descriptions (if metric views exist)
"""

CHECKLIST_INSTRUCTIONS = """
### Text Instructions
- At least one text instruction exists
- Instructions are focused (not overly verbose)
- Business jargon is mapped to canonical terms

### Example Question SQLs
- At least one example question SQL exists
- Examples demonstrate complex patterns (joins, windows, CTEs, business logic)
- Examples are diverse and non-redundant
- Parameters include descriptions (if parameters exist)
- Complex examples include usage guidance

### Join Specs
- Join specs exist when multiple tables are configured
- Join specs include comment and instruction
- Join SQL clauses are single equality expressions

### SQL Snippets
- Filter snippets are defined for common filters
- Expression snippets exist when business logic is complex
- Multiple measure snippets exist for common metrics
"""

CHECKLIST_BENCHMARKS = """
### Questions
- At least 10 benchmark Q&A pairs are configured
- Benchmarks are diverse across topics and query patterns
- Expected SQL exists for each benchmark question
"""

CHECKLIST_CONFIG = """
### Sample Questions
- Sample questions are present
- Sample questions reflect core use cases of the space
- Sample question IDs are unique and valid

### Structural Hygiene
- IDs are lowercase 32-char hex
- Collections with IDs or identifiers are sorted
"""


# =============================================================================
# HTMLRenderer
# =============================================================================


class HTMLRenderer:
    """Renders HTML components for notebook display."""

    @staticmethod
    def _escape_html(text: str) -> str:
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
        items_html = ""

        status_colors = {
            "pass": ("#22c55e", "#dcfce7", "#166534"),
            "fail": ("#ef4444", "#fee2e2", "#991b1b"),
            "warning": ("#f59e0b", "#fef3c7", "#92400e"),
            "na": ("#6b7280", "#f3f4f6", "#374151"),
        }

        for item in analysis.get("items", []):
            status = item.get("status", "na")
            name = HTMLRenderer._escape_html(item.get("name", ""))
            explanation = HTMLRenderer._escape_html(item.get("explanation", ""))
            fix = HTMLRenderer._escape_html(item.get("fix", ""))

            border_color, bg_color, text_color = status_colors.get(
                status, status_colors["na"]
            )
            status_icon = {"pass": "✓", "fail": "✗", "warning": "!", "na": "—"}.get(
                status,
                "—",
            )

            fix_html = ""
            if status in {"fail", "warning"} and fix:
                fix_html = f"""
                <div style="margin-top: 8px; padding: 8px 10px; background: white; border-radius: 4px; border: 1px solid #e2e8f0; font-size: 12px; color: #334155;">
                    <strong>Recommended Fix:</strong> {fix}
                </div>
                """

            items_html += f"""
            <div style="display: flex; align-items: flex-start; gap: 12px; padding: 12px; margin: 8px 0; background: {bg_color}; border-left: 4px solid {border_color}; border-radius: 4px;">
                <span style="font-size: 16px; font-weight: bold; color: {border_color}; min-width: 20px;">{status_icon}</span>
                <div style="flex: 1;">
                    <div style="font-weight: 600; color: {text_color};">{name}</div>
                    <div style="font-size: 13px; color: #4b5563; margin-top: 4px;">{explanation}</div>
                    {fix_html}
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
    def render_priority_recommendations(all_items: list[dict]) -> str:
        candidates = []
        for item in all_items:
            if item.get("status") in {"fail", "warning"} and item.get("fix"):
                priority = 0 if item.get("status") == "fail" else 1
                candidates.append((priority, item.get("name", ""), item.get("fix", "")))

        if not candidates:
            return """
            <div style="margin: 20px 0; padding: 14px; border: 1px solid #e5e7eb; border-radius: 8px; background: #f8fafc; color: #475569;">
                No high-priority recommendations were generated.
            </div>
            """

        candidates = sorted(candidates, key=lambda x: (x[0], x[1]))[:5]

        rows = ""
        for _, name, fix in candidates:
            rows += f"""
            <li style="margin: 8px 0; color: #1f2937;">
                <strong>{HTMLRenderer._escape_html(name)}:</strong> {HTMLRenderer._escape_html(fix)}
            </li>
            """

        return f"""
        <div style="margin: 20px 0; border: 1px solid #e5e7eb; border-radius: 8px; overflow: hidden;">
            <div style="background: #1e3a5f; color: white; padding: 14px 20px;">
                <h3 style="margin: 0; font-size: 17px;">Priority Recommendations</h3>
            </div>
            <div style="padding: 12px 20px; background: #f8fafc;">
                <ol style="margin: 0; padding-left: 20px;">{rows}</ol>
            </div>
        </div>
        """

    @staticmethod
    def render_benchmark_result(
        question: str,
        expected_sql: str,
        generated_sql: str,
        verdict: str,
        score: float,
        summary: str,
        dimension_results: dict,
        details: dict,
    ) -> str:
        verdict_styles = {
            "correct": ("#22c55e", "#dcfce7", "CORRECT"),
            "partial": ("#f59e0b", "#fef3c7", "PARTIAL"),
            "incorrect": ("#ef4444", "#fee2e2", "INCORRECT"),
            "error": ("#6b7280", "#f3f4f6", "ERROR"),
        }
        status_color, status_bg, status_text = verdict_styles.get(
            verdict,
            verdict_styles["error"],
        )

        expected_escaped = HTMLRenderer._escape_html(expected_sql or "(none)")
        generated_escaped = HTMLRenderer._escape_html(generated_sql or "(none)")
        question_escaped = HTMLRenderer._escape_html(question)
        summary_escaped = HTMLRenderer._escape_html(summary or "")

        details_html = ""
        if details:
            for key, value in details.items():
                details_html += f"<div><strong>{HTMLRenderer._escape_html(key)}:</strong> {HTMLRenderer._escape_html(str(value))}</div>"

        dimensions_html = ""
        if dimension_results:
            for key, value in dimension_results.items():
                dimensions_html += f"""
                <span style="display: inline-block; margin: 2px 6px 2px 0; padding: 3px 8px; border-radius: 999px; background: #eef2ff; color: #3730a3; font-size: 11px;">
                    {HTMLRenderer._escape_html(key)}: {HTMLRenderer._escape_html(str(value))}
                </span>
                """

        return f"""
        <div style="margin: 16px 0; border: 1px solid #e5e7eb; border-radius: 8px; overflow: hidden;">
            <div style="background: {status_bg}; padding: 12px 16px; display: flex; justify-content: space-between; align-items: center; gap: 10px;">
                <span style="font-weight: 600; color: #374151;">{question_escaped}</span>
                <span style="display: inline-flex; gap: 8px; align-items: center;">
                    <span style="background: {status_color}; color: white; padding: 4px 10px; border-radius: 4px; font-weight: bold; font-size: 12px;">{status_text}</span>
                    <span style="background: #111827; color: white; padding: 4px 10px; border-radius: 4px; font-weight: bold; font-size: 12px;">Score {score:.1f}</span>
                </span>
            </div>
            <div style="padding: 16px;">
                <div style="margin-bottom: 10px; color: #475569; font-size: 13px;">{summary_escaped}</div>
                <div style="margin-bottom: 10px;">{dimensions_html}</div>
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
        rows_html = ""
        total_count = len(results)

        verdict_counts = {
            "correct": 0,
            "partial": 0,
            "incorrect": 0,
            "error": 0,
        }

        weighted_total = 0.0

        verdict_styles = {
            "correct": ("#22c55e", "CORRECT"),
            "partial": ("#f59e0b", "PARTIAL"),
            "incorrect": ("#ef4444", "INCORRECT"),
            "error": ("#6b7280", "ERROR"),
        }

        for r in results:
            verdict = str(r.get("verdict", "error")).lower()
            verdict = verdict if verdict in verdict_counts else "error"
            verdict_counts[verdict] += 1

            score = float(r.get("score", 0.0))
            weighted_total += score

            status_color, status_text = verdict_styles.get(verdict, verdict_styles["error"])
            question = HTMLRenderer._escape_html(r.get("question", "")[:72])
            if len(r.get("question", "")) > 72:
                question += "..."
            summary = HTMLRenderer._escape_html(r.get("summary", ""))

            rows_html += f"""
            <tr style="border-bottom: 1px solid #e5e7eb;">
                <td style="padding: 10px; color: {status_color}; font-weight: bold;">{status_text}</td>
                <td style="padding: 10px; color: #111827;">{question}</td>
                <td style="padding: 10px; color: #6b7280; font-size: 13px;">{summary}</td>
                <td style="padding: 10px; color: #111827; font-weight: 600;">{score:.1f}</td>
            </tr>
            """

        percent = (weighted_total / total_count * 100) if total_count else 0.0

        return f"""
        <div style="margin: 20px 0; border: 1px solid #e5e7eb; border-radius: 8px; overflow: hidden;">
            <div style="background: linear-gradient(135deg, #1e3a5f 0%, #2d5a87 100%); color: white; padding: 16px 20px; display: flex; justify-content: space-between; align-items: center;">
                <h3 style="margin: 0; font-size: 18px;">Benchmark Results Summary</h3>
                <div style="display: flex; gap: 10px; align-items: center;">
                    <span style="background: rgba(255,255,255,0.2); padding: 6px 10px; border-radius: 6px;">Correct {verdict_counts['correct']}</span>
                    <span style="background: rgba(255,255,255,0.2); padding: 6px 10px; border-radius: 6px;">Partial {verdict_counts['partial']}</span>
                    <span style="background: rgba(255,255,255,0.2); padding: 6px 10px; border-radius: 6px;">Incorrect {verdict_counts['incorrect']}</span>
                    <span style="background: rgba(255,255,255,0.2); padding: 6px 10px; border-radius: 6px;">Error {verdict_counts['error']}</span>
                    <span style="background: #111827; padding: 6px 10px; border-radius: 6px; font-weight: 700;">{weighted_total:.1f}/{total_count} ({percent:.0f}%)</span>
                </div>
            </div>
            <table style="width: 100%; border-collapse: collapse;">
                <thead>
                    <tr style="background: #f8fafc; border-bottom: 2px solid #e5e7eb;">
                        <th style="padding: 10px; text-align: left; width: 120px;">Verdict</th>
                        <th style="padding: 10px; text-align: left;">Question</th>
                        <th style="padding: 10px; text-align: left;">Summary</th>
                        <th style="padding: 10px; text-align: left; width: 80px;">Score</th>
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
        benchmark_analyses = analysis.get("benchmark_analyses", [])
        overall_recommendations = analysis.get("overall_recommendations", [])
        summary = HTMLRenderer._escape_html(analysis.get("summary", ""))

        analyses_html = ""
        for i, ba in enumerate(benchmark_analyses):
            question = HTMLRenderer._escape_html(ba.get("question", ""))
            verdict = HTMLRenderer._escape_html(ba.get("verdict", ""))
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
                    <span style="display: flex; gap: 8px; align-items: center;">
                        <span style="background: #111827; color: white; padding: 3px 8px; border-radius: 999px; font-size: 11px; text-transform: uppercase;">{verdict}</span>
                        <span class="toggle" style="font-family: monospace; font-size: 18px; color: #666;">+</span>
                    </span>
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
                    <p style="margin: 8px 0 0 0; opacity: 0.9; font-size: 14px;">Hybrid evaluation + LLM diagnosis</p>
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

    @staticmethod
    def render_optimization_plan(changes: list[dict]) -> str:
        if not changes:
            return """
            <div style="margin: 16px 0; padding: 14px; border: 1px solid #e5e7eb; border-radius: 8px; background: #f8fafc; color: #475569;">
                No optimization changes were generated.
            </div>
            """

        category_counts = {}
        for change in changes:
            category = change.get("category", "other")
            category_counts[category] = category_counts.get(category, 0) + 1

        summary_chips = "".join(
            f'<span style="display: inline-block; margin-right: 8px; margin-bottom: 8px; padding: 4px 10px; background: #eef2ff; color: #3730a3; border-radius: 999px; font-size: 12px;">{HTMLRenderer._escape_html(k)}: {v}</span>'
            for k, v in sorted(category_counts.items())
        )

        rows = ""
        for idx, change in enumerate(changes, start=1):
            action = HTMLRenderer._escape_html(change.get("action", ""))
            rationale = HTMLRenderer._escape_html(change.get("rationale", ""))
            before = HTMLRenderer._escape_html(_text_from_value(change.get("before")))
            after = HTMLRenderer._escape_html(_text_from_value(change.get("after")))
            path = HTMLRenderer._escape_html(change.get("path", ""))

            rows += f"""
            <tr style="border-bottom: 1px solid #e5e7eb; vertical-align: top;">
                <td style="padding: 10px; width: 45px; color: #1f2937;">{idx}</td>
                <td style="padding: 10px; width: 230px; color: #1f2937;"><code>{action}</code><br><span style="font-size: 11px; color: #6b7280;">{path}</span></td>
                <td style="padding: 10px; color: #334155; font-size: 12px;">{before or '(none)'}</td>
                <td style="padding: 10px; color: #0f172a; font-size: 12px;">{after or '(none)'}</td>
                <td style="padding: 10px; color: #475569; font-size: 12px;">{rationale}</td>
            </tr>
            """

        return f"""
        <div style="margin: 20px 0; border: 1px solid #e5e7eb; border-radius: 8px; overflow: hidden;">
            <div style="background: linear-gradient(135deg, #1e3a5f 0%, #2d5a87 100%); color: white; padding: 16px 20px;">
                <h3 style="margin: 0; font-size: 18px;">Proposed Optimization Changes ({len(changes)})</h3>
                <div style="margin-top: 10px;">{summary_chips}</div>
            </div>
            <div style="padding: 10px 0; overflow-x: auto;">
                <table style="width: 100%; border-collapse: collapse; min-width: 980px;">
                    <thead>
                        <tr style="background: #f8fafc; border-bottom: 2px solid #e5e7eb;">
                            <th style="padding: 10px; text-align: left;">#</th>
                            <th style="padding: 10px; text-align: left;">Change</th>
                            <th style="padding: 10px; text-align: left;">Before</th>
                            <th style="padding: 10px; text-align: left;">After</th>
                            <th style="padding: 10px; text-align: left;">Rationale</th>
                        </tr>
                    </thead>
                    <tbody>
                        {rows}
                    </tbody>
                </table>
            </div>
        </div>
        """

    @staticmethod
    def render_validation_report(validation: dict) -> str:
        valid = validation.get("valid", False)
        errors = validation.get("errors", [])
        warnings = validation.get("warnings", [])

        header_color = "#22c55e" if valid else "#ef4444"
        header_text = "VALID" if valid else "INVALID"

        errors_html = "".join(
            f'<li style="margin: 6px 0; color: #991b1b;">{HTMLRenderer._escape_html(e)}</li>'
            for e in errors
        )
        warnings_html = "".join(
            f'<li style="margin: 6px 0; color: #92400e;">{HTMLRenderer._escape_html(w)}</li>'
            for w in warnings
        )

        return f"""
        <div style="margin: 20px 0; border: 1px solid #e5e7eb; border-radius: 8px; overflow: hidden;">
            <div style="background: {header_color}; color: white; padding: 14px 20px; display: flex; justify-content: space-between; align-items: center;">
                <h3 style="margin: 0; font-size: 18px;">Serialized Space Validation</h3>
                <span style="background: rgba(255,255,255,0.2); padding: 5px 10px; border-radius: 6px; font-weight: 700;">{header_text}</span>
            </div>
            <div style="padding: 16px 20px; background: #f8fafc;">
                <div style="font-size: 13px; color: #475569; margin-bottom: 12px;">Errors: {len(errors)} | Warnings: {len(warnings)}</div>
                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 16px;">
                    <div>
                        <div style="font-weight: 700; color: #991b1b; margin-bottom: 8px;">Errors</div>
                        <ul style="margin: 0; padding-left: 18px;">{errors_html if errors_html else '<li style="color: #64748b;">None</li>'}</ul>
                    </div>
                    <div>
                        <div style="font-weight: 700; color: #92400e; margin-bottom: 8px;">Warnings</div>
                        <ul style="margin: 0; padding-left: 18px;">{warnings_html if warnings_html else '<li style="color: #64748b;">None</li>'}</ul>
                    </div>
                </div>
            </div>
        </div>
        """


# =============================================================================
# LLMAnalyzer
# =============================================================================


class LLMAnalyzer:
    """Uses Foundation Model API to analyze Genie Space sections and SQL behavior."""

    def __init__(self, dbutils, config: dict = None):
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

    def _chat_json(
        self,
        system_prompt: str,
        user_prompt: str,
        schema_name: str,
        schema: dict,
        max_tokens: int,
    ) -> dict:
        response = self.client.chat.completions.create(
            model=self.endpoint,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": schema_name,
                    "strict": True,
                    "schema": schema,
                },
            },
            max_tokens=max_tokens,
            temperature=0,
        )

        try:
            return json.loads(response.choices[0].message.content)
        except Exception:
            return {}

    def _analyze_section(
        self,
        section_name: str,
        section_data: Any,
        checklist: str,
    ) -> dict:
        system_prompt = """You are an expert Databricks Genie Space analyst.
Analyze the section against the checklist criteria.

For each checklist item, return:
- status: one of pass, fail, warning, na
- name: concise item name
- explanation: concrete evidence from the section
- fix: actionable fix for fail/warning items (empty string for pass/na)

Be specific and reference exact table names, column names, or fields."""

        section_json = (
            json.dumps(section_data, indent=2, default=str) if section_data else "null"
        )

        user_prompt = f"""Analyze the '{section_name}' section from a Genie Space configuration.

## Section Data
```json
{section_json[:18000]}
```

## Checklist
{checklist}

Return JSON only."""

        schema = {
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
                            "fix": {"type": "string"},
                        },
                        "required": ["name", "status", "explanation", "fix"],
                        "additionalProperties": False,
                    },
                },
                "summary": {"type": "string"},
            },
            "required": ["items", "summary"],
            "additionalProperties": False,
        }

        parsed = self._chat_json(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            schema_name="checklist_analysis",
            schema=schema,
            max_tokens=2500,
        )

        if not parsed:
            return {"items": [], "summary": "Failed to parse LLM response"}

        normalized_items = []
        for item in parsed.get("items", []):
            status = str(item.get("status", "na")).lower()
            if status not in {"pass", "fail", "warning", "na"}:
                status = "na"
            normalized_items.append(
                {
                    "name": item.get("name", ""),
                    "status": status,
                    "explanation": item.get("explanation", ""),
                    "fix": item.get("fix", ""),
                }
            )

        return {
            "items": normalized_items,
            "summary": parsed.get("summary", ""),
        }

    def analyze_data_sources(self, data_sources: Any) -> dict:
        return self._analyze_section("data_sources", data_sources, CHECKLIST_DATA_SOURCES)

    def analyze_instructions(self, instructions: Any) -> dict:
        return self._analyze_section("instructions", instructions, CHECKLIST_INSTRUCTIONS)

    def analyze_benchmarks(self, benchmarks: Any) -> dict:
        return self._analyze_section("benchmarks", benchmarks, CHECKLIST_BENCHMARKS)

    def analyze_config(self, config_section: Any) -> dict:
        return self._analyze_section("config", config_section, CHECKLIST_CONFIG)

    def analyze_benchmark_results(
        self,
        benchmark_results: list,
        serialized_space: dict,
    ) -> dict:
        system_prompt = """You are an expert Databricks Genie Space analyst.
Analyze benchmark results and diagnose why generated SQL diverges from expected SQL.
Return concrete, actionable recommendations tied to Genie config changes."""

        benchmark_data = []
        for r in benchmark_results:
            benchmark_data.append(
                {
                    "question": r.question,
                    "verdict": r.verdict,
                    "score": r.score,
                    "summary": r.summary,
                    "dimension_results": r.dimension_results,
                    "expected_sql": r.expected_sql,
                    "generated_sql": r.generated_sql or "(no SQL generated)",
                    "error": r.error,
                }
            )

        space_summary = {
            "config": serialized_space.get("config", {}),
            "instructions": serialized_space.get("instructions", {}),
            "data_sources": self._truncate_data_sources(
                serialized_space.get("data_sources", {}),
            ),
        }

        user_prompt = f"""Analyze these benchmark outcomes and provide recommendations.

## Space summary
```json
{json.dumps(space_summary, indent=2, default=str)[:14000]}
```

## Benchmark outcomes
```json
{json.dumps(benchmark_data, indent=2, default=str)[:18000]}
```

For each benchmark, include:
1) semantic comparison,
2) diagnosis,
3) specific fixes in the serialized space.
Also provide overall recommendations."""

        schema = {
            "type": "object",
            "properties": {
                "benchmark_analyses": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "question": {"type": "string"},
                            "verdict": {"type": "string"},
                            "comparison": {"type": "string"},
                            "diagnosis": {"type": "string"},
                            "recommendations": {
                                "type": "array",
                                "items": {"type": "string"},
                            },
                        },
                        "required": [
                            "question",
                            "verdict",
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
                },
                "summary": {"type": "string"},
            },
            "required": [
                "benchmark_analyses",
                "overall_recommendations",
                "summary",
            ],
            "additionalProperties": False,
        }

        parsed = self._chat_json(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            schema_name="benchmark_analysis",
            schema=schema,
            max_tokens=4500,
        )

        if not parsed:
            return {
                "benchmark_analyses": [],
                "overall_recommendations": [],
                "summary": "Failed to parse LLM response",
            }

        return parsed

    def _truncate_data_sources(self, data_sources: dict) -> dict:
        if not data_sources:
            return {}

        truncated = {}

        if "tables" in data_sources:
            truncated["tables"] = []
            for table in data_sources.get("tables", [])[:10]:
                table_info = {
                    "identifier": table.get("identifier") or table.get("name"),
                    "description": table.get("description"),
                }
                column_configs = _column_configs(table)
                if column_configs:
                    table_info["column_configs"] = [
                        {
                            "column_name": _column_name(c),
                            "description": c.get("description"),
                            "synonyms": c.get("synonyms"),
                            "enable_format_assistance": c.get("enable_format_assistance"),
                            "enable_entity_matching": c.get("enable_entity_matching"),
                            "get_example_values": c.get("get_example_values"),
                            "build_value_dictionary": c.get("build_value_dictionary"),
                        }
                        for c in column_configs[:25]
                    ]
                truncated["tables"].append(table_info)

        if "metric_views" in data_sources:
            truncated["metric_views"] = data_sources.get("metric_views", [])[:10]

        return truncated

    def compare_sql_semantically(
        self,
        question: str,
        expected_sql: str,
        generated_sql: str,
    ) -> dict:
        if not generated_sql or not generated_sql.strip():
            return {"equivalent": False, "explanation": "No SQL was generated"}

        system_prompt = """You are an expert SQL analyst. Compare two SQL queries.
Determine if they are semantically equivalent for the question.
Ignore aliases/formatting; focus on returned data and logic."""

        user_prompt = f"""Question: {question}

Expected SQL:
```sql
{expected_sql}
```

Generated SQL:
```sql
{generated_sql}
```

Return JSON."""

        schema = {
            "type": "object",
            "properties": {
                "equivalent": {"type": "boolean"},
                "explanation": {"type": "string"},
            },
            "required": ["equivalent", "explanation"],
            "additionalProperties": False,
        }

        parsed = self._chat_json(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            schema_name="sql_comparison",
            schema=schema,
            max_tokens=600,
        )

        if not parsed:
            return {
                "equivalent": False,
                "explanation": "Failed to parse LLM comparison response",
            }

        return parsed


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
        self.client = WorkspaceClient()

    def get_serialized_space(self, space_id: str) -> dict:
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
            "warehouse_id": space.warehouse_id,
            "serialized_space": json.loads(space.serialized_space),
        }

    def ask_question(
        self,
        space_id: str,
        question: str,
        timeout_minutes: int = 5,
    ) -> GenieConversationResult:
        from datetime import timedelta

        from databricks.sdk.service.dashboards import MessageStatus

        try:
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

        generated_sql = None
        response_text = None

        if message.attachments:
            for attachment in message.attachments:
                if attachment.query and attachment.query.query:
                    generated_sql = attachment.query.query
                    break

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

    def create_optimized_space(
        self,
        original_space_id: str,
        updated_config: dict,
        title_prefix: str = "[Optimized] ",
    ) -> dict:
        original_space = self.client.genie.get_space(space_id=original_space_id)
        if not original_space.warehouse_id:
            raise ValueError(
                f"Original space '{original_space_id}' has no warehouse_id; cannot create new space"
            )

        new_title = f"{title_prefix}{original_space.title}"
        created = self.client.genie.create_space(
            warehouse_id=original_space.warehouse_id,
            serialized_space=json.dumps(updated_config, ensure_ascii=False),
            title=new_title,
            description=original_space.description,
        )

        host = self.client.config.host.rstrip("/")
        return {
            "new_space_id": created.space_id,
            "new_space_title": new_title,
            "original_space_id": original_space_id,
            "new_space_url": f"{host}/spaces/{created.space_id}",
        }


# =============================================================================
# BenchmarkRunner
# =============================================================================


@dataclass
class BenchmarkResult:
    """Result from running a single benchmark."""

    question: str
    expected_sql: str
    generated_sql: Optional[str] = None
    verdict: str = "error"
    score: float = 0.0
    deterministic_score: float = 0.0
    dimension_results: dict[str, str] = field(default_factory=dict)
    passed: bool = False
    summary: str = ""
    expected_row_count: Optional[int] = None
    generated_row_count: Optional[int] = None
    error: Optional[str] = None


class BenchmarkRunner:
    """Runs benchmark tests against a Genie Space."""

    DIMENSIONS = [
        "tables",
        "joins",
        "where",
        "aggregations",
        "group_by",
        "order_by",
        "limit",
        "column_selection",
        "expressions",
    ]

    def __init__(
        self,
        genie_client: GenieSpaceClient,
        warehouse_id: Optional[str] = None,
        llm_analyzer: Optional[LLMAnalyzer] = None,
        judge_mode: str = "hybrid",
        timeout_minutes: int = 5,
        enable_execution_diagnostics: bool = False,
    ):
        self.genie_client = genie_client
        self.warehouse_id = warehouse_id
        self.llm_analyzer = llm_analyzer
        self.sdk_client = WorkspaceClient()
        self.judge_mode = str(judge_mode or "hybrid").lower()
        if self.judge_mode not in {"hybrid", "llm", "execution"}:
            self.judge_mode = "hybrid"
        self.timeout_minutes = int(timeout_minutes or 5)
        self.enable_execution_diagnostics = bool(enable_execution_diagnostics)

    def _execute_sql(self, sql: str) -> tuple[Optional[int], Optional[str]]:
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
            error = response.status.error if response.status else "Unknown error"
            return None, f"SQL execution failed: {error}"
        except Exception as e:
            return None, f"SQL execution error: {str(e)}"

    def _compare_results(
        self,
        expected_sql: str,
        generated_sql: str,
    ) -> tuple[bool, str, Optional[int], Optional[int], Optional[str]]:
        if not generated_sql:
            return False, "No SQL generated", None, None, "No SQL generated"

        expected_rows, expected_error = self._execute_sql(expected_sql)
        generated_rows, generated_error = self._execute_sql(generated_sql)

        if expected_error:
            return False, f"Expected SQL error: {expected_error}", None, None, expected_error

        if generated_error:
            return (
                False,
                f"Generated SQL error: {generated_error}",
                expected_rows,
                None,
                generated_error,
            )

        if expected_rows == generated_rows:
            return (
                True,
                f"Row counts match: {expected_rows}",
                expected_rows,
                generated_rows,
                None,
            )

        return (
            False,
            f"Row count mismatch: expected {expected_rows}, got {generated_rows}",
            expected_rows,
            generated_rows,
            None,
        )

    def _parse_sql(self, sql: str) -> tuple[Optional[exp.Expression], Optional[str]]:
        if not sql or not sql.strip():
            return None, "Empty SQL"

        try:
            return parse_one(sql, read="databricks"), None
        except Exception as first_error:
            try:
                return parse_one(sql), None
            except Exception as second_error:
                return None, f"{first_error}; fallback parse failed: {second_error}"

    def _extract_features(self, parsed: exp.Expression) -> dict[str, Any]:
        tables = set()
        for table in parsed.find_all(exp.Table):
            pieces = [table.catalog, table.db, table.name]
            text_pieces = [str(p) for p in pieces if p]
            identifier = ".".join(text_pieces) if text_pieces else _normalize_expression(table)
            tables.add(identifier.lower())

        joins = set()
        for join in parsed.find_all(exp.Join):
            join_kind = str(join.args.get("kind") or join.args.get("side") or "join").upper()
            join_target = _normalize_expression(join.this)
            join_on = _normalize_expression(join.args.get("on"))
            joins.add(f"{join_kind}:{join_target}:{join_on}")

        where = parsed.args.get("where")
        where_expr = where.this if hasattr(where, "this") else where
        where_sql = _normalize_expression(where_expr)

        aggregations = set()
        agg_names = {
            "count",
            "sum",
            "avg",
            "min",
            "max",
            "stddev",
            "stddev_samp",
            "stddev_pop",
            "variance",
            "var_pop",
            "var_samp",
            "approx_count_distinct",
        }
        for func in parsed.find_all(exp.Func):
            func_name = str(func.sql_name() or func.__class__.__name__).lower()
            if func_name in agg_names:
                aggregations.add(_normalize_expression(func))

        group_set = set()
        group = parsed.args.get("group")
        if group and getattr(group, "expressions", None):
            group_set = {_normalize_expression(g) for g in group.expressions}

        order_set = set()
        order = parsed.args.get("order")
        if order and getattr(order, "expressions", None):
            order_set = {_normalize_expression(o) for o in order.expressions}

        limit = parsed.args.get("limit")
        limit_expr = None
        if limit is not None:
            limit_expr = getattr(limit, "expression", limit)
        limit_sql = _normalize_expression(limit_expr)

        column_selection = set()
        expressions = set()
        select_exprs = parsed.args.get("expressions") or []
        for expression_item in select_exprs:
            expressions.add(_normalize_expression(expression_item))
            core = expression_item.this if isinstance(expression_item, exp.Alias) else expression_item
            column_selection.add(_normalize_expression(core))

        return {
            "tables": tables,
            "joins": joins,
            "where": where_sql,
            "aggregations": aggregations,
            "group_by": group_set,
            "order_by": order_set,
            "limit": limit_sql,
            "column_selection": column_selection,
            "expressions": expressions,
        }

    def _dimension_similarity(self, dimension: str, expected: Any, generated: Any) -> float:
        if dimension in {
            "tables",
            "joins",
            "aggregations",
            "group_by",
            "order_by",
            "column_selection",
            "expressions",
        }:
            return _jaccard_similarity(set(expected), set(generated))

        if dimension == "limit":
            if not expected and not generated:
                return 1.0
            return 1.0 if str(expected).strip() == str(generated).strip() else 0.0

        return _text_similarity(str(expected), str(generated))

    def _deterministic_compare(self, expected_sql: str, generated_sql: str) -> dict:
        expected_parsed, expected_error = self._parse_sql(expected_sql)
        generated_parsed, generated_error = self._parse_sql(generated_sql)

        if expected_error or generated_error:
            return {
                "parse_error": True,
                "error": f"Parse issue. expected={expected_error or 'ok'} generated={generated_error or 'ok'}",
                "score": 0.0,
                "dimension_scores": {},
                "dimension_results": {},
                "summary": "Deterministic SQL comparison unavailable due to parse failure",
                "is_ambiguous": False,
            }

        expected_features = self._extract_features(expected_parsed)
        generated_features = self._extract_features(generated_parsed)

        dimension_scores: dict[str, float] = {}
        dimension_results: dict[str, str] = {}

        for dimension in self.DIMENSIONS:
            sim = self._dimension_similarity(
                dimension,
                expected_features.get(dimension),
                generated_features.get(dimension),
            )
            dimension_scores[dimension] = sim
            if sim >= 0.9:
                dimension_results[dimension] = "match"
            elif sim >= 0.6:
                dimension_results[dimension] = "partial"
            else:
                dimension_results[dimension] = "mismatch"

        score = sum(dimension_scores.values()) / len(self.DIMENSIONS)
        is_ambiguous = any(0.6 <= s < 0.9 for s in dimension_scores.values())

        top_mismatches = [
            d for d, s in sorted(dimension_scores.items(), key=lambda kv: kv[1]) if s < 0.9
        ]
        mismatch_text = ", ".join(top_mismatches[:3]) if top_mismatches else "none"

        return {
            "parse_error": False,
            "error": None,
            "score": score,
            "dimension_scores": dimension_scores,
            "dimension_results": dimension_results,
            "summary": f"Deterministic dimension score {score:.2f}; main mismatches: {mismatch_text}.",
            "is_ambiguous": is_ambiguous,
        }

    def _score_to_verdict(self, score: float) -> str:
        if score >= 0.90:
            return "correct"
        if score >= 0.60:
            return "partial"
        return "incorrect"

    def _verdict_score(self, verdict: str) -> float:
        if verdict == "correct":
            return 1.0
        if verdict == "partial":
            return 0.5
        return 0.0

    def _extract_question_text(self, benchmark: dict) -> str:
        return _text_from_value(benchmark.get("question"))

    def _extract_expected_sql(self, benchmark: dict) -> str:
        answers = benchmark.get("answer", [])
        for answer in answers:
            if str(answer.get("format", "")).upper() == "SQL":
                return _text_from_value(answer.get("content"))
        return ""

    def run_benchmark(self, space_id: str, benchmark: dict) -> BenchmarkResult:
        question = self._extract_question_text(benchmark)
        expected_sql = self._extract_expected_sql(benchmark)

        if not question:
            return BenchmarkResult(
                question="(empty)",
                expected_sql=expected_sql,
                verdict="error",
                score=0.0,
                passed=False,
                error="No question provided",
                summary="Invalid benchmark: no question",
            )

        conversation = self.genie_client.ask_question(
            space_id,
            question,
            timeout_minutes=self.timeout_minutes,
        )

        if conversation.error:
            return BenchmarkResult(
                question=question,
                expected_sql=expected_sql,
                generated_sql=conversation.generated_sql,
                verdict="error",
                score=0.0,
                passed=False,
                error=conversation.error,
                summary=f"Genie error: {conversation.error}",
            )

        generated_sql = conversation.generated_sql
        if not generated_sql or not generated_sql.strip():
            return BenchmarkResult(
                question=question,
                expected_sql=expected_sql,
                generated_sql=generated_sql,
                verdict="error",
                score=0.0,
                passed=False,
                error="No SQL generated",
                summary="Genie did not produce SQL for this question",
            )

        deterministic = self._deterministic_compare(expected_sql, generated_sql)
        deterministic_score = float(deterministic.get("score", 0.0))
        dimension_results = dict(deterministic.get("dimension_results", {}))
        summary = deterministic.get("summary", "")

        verdict = "error"

        if self.judge_mode == "execution" and self.warehouse_id:
            rows_match, rows_summary, expected_rows, generated_rows, rows_error = self._compare_results(
                expected_sql,
                generated_sql,
            )
            if rows_error:
                verdict = "error"
                summary = f"Execution-first judge failed: {rows_summary}"
            else:
                verdict = "correct" if rows_match else "incorrect"
                summary = f"Execution-first judge: {rows_summary}"
            dimension_results = {"row_count": "match" if rows_match else "mismatch"}
            result = BenchmarkResult(
                question=question,
                expected_sql=expected_sql,
                generated_sql=generated_sql,
                verdict=verdict,
                score=self._verdict_score(verdict),
                deterministic_score=deterministic_score,
                dimension_results=dimension_results,
                passed=verdict == "correct",
                summary=summary,
                expected_row_count=expected_rows,
                generated_row_count=generated_rows,
                error=rows_error,
            )
            return result

        if deterministic.get("parse_error"):
            if self.judge_mode in {"hybrid", "llm"} and self.llm_analyzer:
                llm_cmp = self.llm_analyzer.compare_sql_semantically(
                    question=question,
                    expected_sql=expected_sql,
                    generated_sql=generated_sql,
                )
                equivalent = bool(llm_cmp.get("equivalent", False))
                verdict = "correct" if equivalent else "incorrect"
                summary = (
                    "Deterministic parse failed; LLM fallback used. "
                    f"{llm_cmp.get('explanation', '')}"
                )
                dimension_results = {"parser": "llm_fallback"}
            else:
                verdict = "error"
                summary = deterministic.get("error", "SQL parse failure")
                dimension_results = {"parser": "failed"}
        else:
            if self.judge_mode == "llm" and self.llm_analyzer:
                llm_cmp = self.llm_analyzer.compare_sql_semantically(
                    question=question,
                    expected_sql=expected_sql,
                    generated_sql=generated_sql,
                )
                equivalent = bool(llm_cmp.get("equivalent", False))
                verdict = "correct" if equivalent else "incorrect"
                summary = f"LLM judge: {llm_cmp.get('explanation', '')}"
            else:
                verdict = self._score_to_verdict(deterministic_score)

                if (
                    self.judge_mode == "hybrid"
                    and self.llm_analyzer
                    and (0.75 <= deterministic_score < 0.90 or deterministic.get("is_ambiguous"))
                ):
                    llm_cmp = self.llm_analyzer.compare_sql_semantically(
                        question=question,
                        expected_sql=expected_sql,
                        generated_sql=generated_sql,
                    )
                    equivalent = bool(llm_cmp.get("equivalent", False))
                    if equivalent and deterministic_score >= 0.75:
                        verdict = "correct"
                    elif not equivalent and verdict == "correct":
                        verdict = "partial"
                    summary = f"{summary} LLM tie-breaker: {llm_cmp.get('explanation', '')}"

        expected_rows = None
        generated_rows = None
        diagnostics_error = None
        if (
            self.enable_execution_diagnostics
            and self.warehouse_id
            and expected_sql
            and generated_sql
        ):
            rows_match, rows_summary, expected_rows, generated_rows, diagnostics_error = self._compare_results(
                expected_sql,
                generated_sql,
            )
            dimension_results["row_count"] = "match" if rows_match else "mismatch"
            summary = f"{summary} Diagnostics: {rows_summary}"

        score = self._verdict_score(verdict)

        return BenchmarkResult(
            question=question,
            expected_sql=expected_sql,
            generated_sql=generated_sql,
            verdict=verdict,
            score=score,
            deterministic_score=deterministic_score,
            dimension_results=dimension_results,
            passed=verdict == "correct",
            summary=summary,
            expected_row_count=expected_rows,
            generated_row_count=generated_rows,
            error=diagnostics_error,
        )

    def run_all_benchmarks(
        self,
        space_id: str,
        benchmarks: list[dict],
    ) -> list[BenchmarkResult]:
        results = []
        for i, benchmark in enumerate(benchmarks):
            question = self._extract_question_text(benchmark)
            print(f"Running benchmark {i + 1}/{len(benchmarks)}: {question[:70]}...")
            result = self.run_benchmark(space_id, benchmark)
            results.append(result)
        return results


# =============================================================================
# Optimization Functions
# =============================================================================


def generate_optimization_changes(
    serialized_space: dict,
    checklist_analyses: Optional[dict[str, dict]] = None,
    benchmark_results: Optional[list[BenchmarkResult]] = None,
    benchmark_analysis: Optional[dict] = None,
) -> list[dict]:
    """Generate structured config changes based on analyses and benchmark outcomes."""
    checklist_analyses = checklist_analyses or {}
    benchmark_results = benchmark_results or []
    benchmark_analysis = benchmark_analysis or {}

    version = int(serialized_space.get("version", 1) or 1)
    changes: list[dict] = []
    seen_keys: set[tuple[str, str, str]] = set()

    def add_change(change: dict):
        key = (
            str(change.get("action", "")),
            str(change.get("path", "")),
            json.dumps(change.get("target", {}), sort_keys=True),
        )
        if key in seen_keys:
            return
        seen_keys.add(key)
        changes.append(change)

    tables = serialized_space.get("data_sources", {}).get("tables", [])

    for table_idx, table in enumerate(tables):
        table_identifier = table.get("identifier") or table.get("name") or f"table_{table_idx}"
        table_desc = table.get("description")

        if not _is_meaningful_text(table_desc):
            suggested = [
                f"Core dataset for {_humanize_identifier(table_identifier)} analytics in this Genie space."
            ]
            add_change(
                {
                    "category": "data_source",
                    "action": "update_table_description",
                    "path": f"data_sources.tables[{table_idx}].description",
                    "target": {"table_identifier": table_identifier},
                    "before": table_desc,
                    "after": suggested,
                    "rationale": "Table description is missing or generic.",
                }
            )

        for col_idx, column in enumerate(_column_configs(table)):
            col_name = _column_name(column)
            if not col_name:
                continue

            desc = column.get("description")
            if not _is_meaningful_text(desc):
                add_change(
                    {
                        "category": "data_source",
                        "action": "update_column_description",
                        "path": f"data_sources.tables[{table_idx}].column_configs[{col_idx}].description",
                        "target": {
                            "table_identifier": table_identifier,
                            "column_name": col_name,
                        },
                        "before": desc,
                        "after": [
                            f"Business meaning of {col_name.replace('_', ' ')} in {_humanize_identifier(table_identifier)} queries."
                        ],
                        "rationale": "Column description is missing or not contextual.",
                    }
                )

            existing_synonyms = _as_list(column.get("synonyms"))
            if not existing_synonyms:
                suggestions = _guess_synonyms(col_name)
                if suggestions:
                    add_change(
                        {
                            "category": "data_source",
                            "action": "add_column_synonyms",
                            "path": f"data_sources.tables[{table_idx}].column_configs[{col_idx}].synonyms",
                            "target": {
                                "table_identifier": table_identifier,
                                "column_name": col_name,
                            },
                            "before": existing_synonyms,
                            "after": suggestions,
                            "rationale": "Key business column is missing synonyms.",
                        }
                    )

            col_name_lower = col_name.lower()
            looks_filterable = any(
                token in col_name_lower
                for token in ["date", "status", "region", "country", "category", "type"]
            )
            if looks_filterable:
                if version >= 2:
                    if not column.get("enable_format_assistance", False):
                        add_change(
                            {
                                "category": "data_source",
                                "action": "set_column_flag",
                                "path": f"data_sources.tables[{table_idx}].column_configs[{col_idx}].enable_format_assistance",
                                "target": {
                                    "table_identifier": table_identifier,
                                    "column_name": col_name,
                                    "field": "enable_format_assistance",
                                },
                                "before": column.get("enable_format_assistance"),
                                "after": True,
                                "rationale": "Filterable column should enable format assistance in v2 spaces.",
                            }
                        )
                    if any(token in col_name_lower for token in ["status", "region", "category", "type"]):
                        if not column.get("enable_entity_matching", False):
                            add_change(
                                {
                                    "category": "data_source",
                                    "action": "set_column_flag",
                                    "path": f"data_sources.tables[{table_idx}].column_configs[{col_idx}].enable_entity_matching",
                                    "target": {
                                        "table_identifier": table_identifier,
                                        "column_name": col_name,
                                        "field": "enable_entity_matching",
                                    },
                                    "before": column.get("enable_entity_matching"),
                                    "after": True,
                                    "rationale": "Categorical column should enable entity matching in v2 spaces.",
                                }
                            )
                else:
                    if not column.get("get_example_values", False):
                        add_change(
                            {
                                "category": "data_source",
                                "action": "set_column_flag",
                                "path": f"data_sources.tables[{table_idx}].column_configs[{col_idx}].get_example_values",
                                "target": {
                                    "table_identifier": table_identifier,
                                    "column_name": col_name,
                                    "field": "get_example_values",
                                },
                                "before": column.get("get_example_values"),
                                "after": True,
                                "rationale": "Filterable column should enable get_example_values in v1 spaces.",
                            }
                        )
                    if any(token in col_name_lower for token in ["status", "region", "category", "type"]):
                        if not column.get("build_value_dictionary", False):
                            add_change(
                                {
                                    "category": "data_source",
                                    "action": "set_column_flag",
                                    "path": f"data_sources.tables[{table_idx}].column_configs[{col_idx}].build_value_dictionary",
                                    "target": {
                                        "table_identifier": table_identifier,
                                        "column_name": col_name,
                                        "field": "build_value_dictionary",
                                    },
                                    "before": column.get("build_value_dictionary"),
                                    "after": True,
                                    "rationale": "Categorical column should enable value dictionary in v1 spaces.",
                                }
                            )

    instructions = serialized_space.get("instructions", {})

    text_instructions = instructions.get("text_instructions", [])
    if not text_instructions:
        add_change(
            {
                "category": "instruction",
                "action": "add_text_instruction",
                "path": "instructions.text_instructions",
                "target": {},
                "before": "",
                "after": "Define key business terms, preferred date semantics, and default filters before generating SQL.",
                "rationale": "At least one global text instruction should exist.",
            }
        )

    example_sqls = instructions.get("example_question_sqls", [])
    if not example_sqls:
        benchmark_seed = next(
            (
                b
                for b in benchmark_results
                if b.expected_sql and b.question
            ),
            None,
        )

        if benchmark_seed:
            add_change(
                {
                    "category": "instruction",
                    "action": "add_example_sql",
                    "path": "instructions.example_question_sqls",
                    "target": {"source": "benchmark_seed"},
                    "before": "",
                    "after": {
                        "question": benchmark_seed.question,
                        "sql": benchmark_seed.expected_sql,
                        "usage_guidance": "Use this pattern for similar analytic questions.",
                        "parameters": [],
                    },
                    "rationale": "No example SQLs exist; seed from benchmark expected SQL.",
                }
            )
        elif tables:
            first_table = tables[0]
            first_identifier = first_table.get("identifier") or first_table.get("name")
            if first_identifier:
                add_change(
                    {
                        "category": "instruction",
                        "action": "add_example_sql",
                        "path": "instructions.example_question_sqls",
                        "target": {"source": "generated_default"},
                        "before": "",
                        "after": {
                            "question": f"How many records are in {first_identifier}?",
                            "sql": f"SELECT COUNT(*) AS total_rows FROM {first_identifier}",
                            "usage_guidance": "Use this as a baseline counting pattern.",
                            "parameters": [],
                        },
                        "rationale": "No example SQLs exist; add a baseline query pattern.",
                    }
                )

    join_specs = instructions.get("join_specs", [])
    if len(tables) > 1 and not join_specs:
        left_table = tables[0]
        right_table = tables[1]
        left_identifier = left_table.get("identifier") or left_table.get("name")
        right_identifier = right_table.get("identifier") or right_table.get("name")

        if left_identifier and right_identifier:
            left_columns = {(_column_name(c)).lower() for c in _column_configs(left_table)}
            right_columns = {(_column_name(c)).lower() for c in _column_configs(right_table)}

            join_expr = None
            for left_col in left_columns:
                if left_col.endswith("_id") and left_col in right_columns:
                    left_alias = left_identifier.split(".")[-1]
                    right_alias = right_identifier.split(".")[-1]
                    join_expr = f"{left_alias}.{left_col} = {right_alias}.{left_col}"
                    break

            if not join_expr:
                join_expr = (
                    f"{left_identifier.split('.')[-1]}.id = {right_identifier.split('.')[-1]}.{left_identifier.split('.')[-1]}_id"
                )

            add_change(
                {
                    "category": "instruction",
                    "action": "add_join_spec",
                    "path": "instructions.join_specs",
                    "target": {
                        "left_identifier": left_identifier,
                        "right_identifier": right_identifier,
                    },
                    "before": "",
                    "after": {
                        "left_identifier": left_identifier,
                        "right_identifier": right_identifier,
                        "join_type": "LEFT JOIN",
                        "sql": [join_expr],
                        "comment": [
                            "Primary relationship for cross-table analytics in this space"
                        ],
                        "instruction": [
                            "Apply this join when answering questions that require both tables"
                        ],
                    },
                    "rationale": "Multiple tables exist without explicit join specs.",
                }
            )

    sql_snippets = instructions.get("sql_snippets", {})
    filters = sql_snippets.get("filters", []) if isinstance(sql_snippets, dict) else []
    expressions = (
        sql_snippets.get("expressions", []) if isinstance(sql_snippets, dict) else []
    )
    measures = sql_snippets.get("measures", []) if isinstance(sql_snippets, dict) else []

    if not filters:
        add_change(
            {
                "category": "instruction",
                "action": "add_sql_snippet",
                "path": "instructions.sql_snippets.filters",
                "target": {"snippet_type": "filters"},
                "before": "",
                "after": {
                    "display_name": "Last 30 Days",
                    "sql": "WHERE date >= DATE_ADD(CURRENT_DATE(), -30)",
                    "synonyms": ["recent", "last month", "past 30 days"],
                    "instruction": [
                        "Use when users ask for recent performance windows"
                    ],
                    "comment": ["Default recency filter snippet"],
                },
                "rationale": "No filter snippets are defined.",
            }
        )

    if not expressions:
        add_change(
            {
                "category": "instruction",
                "action": "add_sql_snippet",
                "path": "instructions.sql_snippets.expressions",
                "target": {"snippet_type": "expressions"},
                "before": "",
                "after": {
                    "alias": "record_status_group",
                    "display_name": "Record Status Group",
                    "sql": "CASE WHEN status IN ('active', 'enabled') THEN 'Active' ELSE 'Inactive' END",
                    "synonyms": ["status group", "active vs inactive"],
                    "instruction": [
                        "Use for status rollups and segmentation questions"
                    ],
                    "comment": ["Standardized status bucketing"],
                },
                "rationale": "No expression snippets are defined.",
            }
        )

    if len(measures) <= 1:
        add_change(
            {
                "category": "instruction",
                "action": "add_sql_snippet",
                "path": "instructions.sql_snippets.measures",
                "target": {"snippet_type": "measures"},
                "before": "",
                "after": {
                    "alias": "total_records",
                    "display_name": "Total Records",
                    "sql": "COUNT(*)",
                    "synonyms": ["count", "volume", "total rows"],
                    "instruction": [
                        "Use when users ask for totals without additional metrics"
                    ],
                    "comment": ["Canonical count measure"],
                },
                "rationale": "Measure coverage is sparse.",
            }
        )

    sample_questions = (
        serialized_space.get("config", {}).get("sample_questions", [])
        if isinstance(serialized_space.get("config"), dict)
        else []
    )
    if not sample_questions:
        sample_question_text = None
        if benchmark_results:
            sample_question_text = benchmark_results[0].question
        if not sample_question_text and tables:
            first_identifier = tables[0].get("identifier") or tables[0].get("name")
            sample_question_text = f"Show a quick summary from {first_identifier}" if first_identifier else None

        if sample_question_text:
            add_change(
                {
                    "category": "config",
                    "action": "add_sample_question",
                    "path": "config.sample_questions",
                    "target": {},
                    "before": "",
                    "after": sample_question_text,
                    "rationale": "Sample questions are missing.",
                }
            )

    for result in benchmark_results:
        if result.verdict in {"partial", "incorrect"} and result.expected_sql:
            add_change(
                {
                    "category": "benchmark_driven",
                    "action": "add_example_sql",
                    "path": "instructions.example_question_sqls",
                    "target": {"source": "benchmark_failure"},
                    "before": "",
                    "after": {
                        "question": result.question,
                        "sql": result.expected_sql,
                        "usage_guidance": "Prioritize this template when similar language appears.",
                        "parameters": [],
                    },
                    "rationale": f"Benchmark verdict '{result.verdict}' indicates this pattern needs stronger guidance.",
                }
            )

    for recommendation in benchmark_analysis.get("overall_recommendations", [])[:3]:
        add_change(
            {
                "category": "benchmark_driven",
                "action": "add_text_instruction",
                "path": "instructions.text_instructions",
                "target": {"source": "benchmark_analysis"},
                "before": "",
                "after": recommendation,
                "rationale": "LLM benchmark analysis recommended a global instruction update.",
            }
        )

    return changes[:40]


def apply_optimization_changes(serialized_space: dict, changes: list[dict]) -> dict:
    """Apply structured optimization changes and return a normalized serialized space."""
    updated = copy.deepcopy(serialized_space)

    def find_table(table_identifier: str) -> Optional[dict]:
        for table in updated.get("data_sources", {}).get("tables", []):
            if (table.get("identifier") or table.get("name")) == table_identifier:
                return table
        return None

    for change in changes:
        action = change.get("action")
        target = change.get("target", {})
        after = change.get("after")

        if action == "update_table_description":
            table = find_table(target.get("table_identifier"))
            if table is not None:
                table["description"] = _to_string_segments(after)

        elif action == "update_column_description":
            table = find_table(target.get("table_identifier"))
            if table is not None:
                for column in _column_configs(table):
                    if _column_name(column) == target.get("column_name"):
                        column["description"] = _to_string_segments(after)
                        break

        elif action == "add_column_synonyms":
            table = find_table(target.get("table_identifier"))
            if table is not None:
                for column in _column_configs(table):
                    if _column_name(column) == target.get("column_name"):
                        existing = [str(s).strip() for s in _as_list(column.get("synonyms"))]
                        merged = existing[:]
                        for synonym in _as_list(after):
                            synonym = str(synonym).strip()
                            if synonym and synonym not in merged:
                                merged.append(synonym)
                        column["synonyms"] = merged
                        break

        elif action == "set_column_flag":
            table = find_table(target.get("table_identifier"))
            if table is not None:
                for column in _column_configs(table):
                    if _column_name(column) == target.get("column_name"):
                        field_name = target.get("field")
                        if field_name:
                            column[field_name] = bool(after)
                        break

        elif action == "add_text_instruction":
            instructions = updated.setdefault("instructions", {})
            text_instructions = instructions.setdefault("text_instructions", [])
            text_instructions.append(
                {
                    "id": _new_id(),
                    "content": _to_string_segments(after),
                }
            )

        elif action == "add_example_sql":
            instructions = updated.setdefault("instructions", {})
            example_question_sqls = instructions.setdefault("example_question_sqls", [])
            question = _text_from_value(after.get("question")) if isinstance(after, dict) else ""
            sql = _text_from_value(after.get("sql")) if isinstance(after, dict) else ""
            usage = _text_from_value(after.get("usage_guidance")) if isinstance(after, dict) else ""
            parameters = after.get("parameters", []) if isinstance(after, dict) else []
            example_question_sqls.append(
                {
                    "id": _new_id(),
                    "question": [question] if question else [],
                    "sql": [sql] if sql else [],
                    "usage_guidance": [usage] if usage else [],
                    "parameters": parameters,
                }
            )

        elif action == "add_join_spec":
            instructions = updated.setdefault("instructions", {})
            join_specs = instructions.setdefault("join_specs", [])
            payload = after if isinstance(after, dict) else {}
            join_specs.append(
                {
                    "id": _new_id(),
                    "left": {
                        "identifier": payload.get("left_identifier", ""),
                        "alias": payload.get("left_identifier", "").split(".")[-1],
                    },
                    "right": {
                        "identifier": payload.get("right_identifier", ""),
                        "alias": payload.get("right_identifier", "").split(".")[-1],
                    },
                    "join_type": payload.get("join_type", "LEFT JOIN"),
                    "sql": _as_list(payload.get("sql")),
                    "comment": _as_list(payload.get("comment")),
                    "instruction": _as_list(payload.get("instruction")),
                }
            )

        elif action == "add_sql_snippet":
            instructions = updated.setdefault("instructions", {})
            sql_snippets = instructions.setdefault("sql_snippets", {})
            snippet_type = target.get("snippet_type")
            if snippet_type not in {"filters", "expressions", "measures"}:
                continue

            snippet_list = sql_snippets.setdefault(snippet_type, [])
            payload = after if isinstance(after, dict) else {}
            snippet = {
                "id": _new_id(),
                "display_name": payload.get("display_name", "Snippet"),
                "sql": payload.get("sql", ""),
                "synonyms": _as_list(payload.get("synonyms")),
                "instruction": _as_list(payload.get("instruction")),
                "comment": _as_list(payload.get("comment")),
            }
            if snippet_type in {"expressions", "measures"}:
                snippet["alias"] = payload.get(
                    "alias",
                    payload.get("display_name", "snippet").lower().replace(" ", "_"),
                )
            snippet_list.append(snippet)

        elif action == "add_sample_question":
            config = updated.setdefault("config", {})
            sample_questions = config.setdefault("sample_questions", [])
            sample_questions.append(
                {
                    "id": _new_id(),
                    "question": _to_string_segments(after),
                }
            )

    return normalize_serialized_space(updated)


def normalize_serialized_space(serialized_space: dict) -> dict:
    """Normalize ordering for collections keyed by id/identifier."""
    return _normalize_serialized_space_recursive(copy.deepcopy(serialized_space))


def validate_serialized_space(serialized_space: dict) -> dict:
    """Validate serialized space against key schema and rule constraints."""
    errors: list[str] = []
    warnings: list[str] = []

    def walk(value: Any, path: str):
        if isinstance(value, dict):
            for key, child in value.items():
                child_path = f"{path}.{key}" if path else key
                if key == "id":
                    if not isinstance(child, str) or not ID_PATTERN.match(child):
                        errors.append(f"{child_path} must be a 32-char lowercase hex string")
                walk(child, child_path)
        elif isinstance(value, list):
            if len(value) > 10000:
                errors.append(f"{path} exceeds max array length 10000")
            for idx, child in enumerate(value):
                walk(child, f"{path}[{idx}]")

            if value and all(isinstance(v, dict) for v in value):
                sort_key = None
                if all("id" in v for v in value):
                    sort_key = "id"
                elif all("identifier" in v for v in value):
                    sort_key = "identifier"

                if sort_key:
                    current = [str(v.get(sort_key, "")) for v in value]
                    if current != sorted(current):
                        errors.append(f"{path} must be sorted by '{sort_key}'")
        elif isinstance(value, str):
            if len(value) > 25000:
                errors.append(f"{path} exceeds max string length 25000")

    walk(serialized_space, "")

    version = int(serialized_space.get("version", 1) or 1)
    tables = serialized_space.get("data_sources", {}).get("tables", [])

    if version >= 2:
        for t_idx, table in enumerate(tables):
            for c_idx, column in enumerate(_column_configs(table)):
                if "get_example_values" in column:
                    errors.append(
                        "data_sources.tables"
                        f"[{t_idx}].column_configs[{c_idx}].get_example_values is v1-only and not allowed in v2"
                    )
                if "build_value_dictionary" in column:
                    errors.append(
                        "data_sources.tables"
                        f"[{t_idx}].column_configs[{c_idx}].build_value_dictionary is v1-only and not allowed in v2"
                    )

    sample_ids = [
        q.get("id")
        for q in serialized_space.get("config", {}).get("sample_questions", [])
        if isinstance(q, dict)
    ]
    benchmark_ids = [
        q.get("id")
        for q in serialized_space.get("benchmarks", {}).get("questions", [])
        if isinstance(q, dict)
    ]

    all_question_ids = [i for i in sample_ids + benchmark_ids if i]
    duplicate_ids = sorted({i for i in all_question_ids if all_question_ids.count(i) > 1})
    if duplicate_ids:
        errors.append(
            "Question IDs must be unique across config.sample_questions and benchmarks.questions: "
            + ", ".join(duplicate_ids[:5])
        )

    join_specs = serialized_space.get("instructions", {}).get("join_specs", [])
    for idx, spec in enumerate(join_specs):
        for clause_idx, clause in enumerate(_as_list(spec.get("sql"))):
            clause_text = _text_from_value(clause)
            if not clause_text:
                warnings.append(f"instructions.join_specs[{idx}].sql[{clause_idx}] is empty")
                continue

            if re.search(r"\b(and|or)\b", clause_text, re.IGNORECASE):
                errors.append(
                    f"instructions.join_specs[{idx}].sql[{clause_idx}] contains AND/OR; use one equality expression per element"
                )
            if clause_text.count("=") != 1:
                warnings.append(
                    f"instructions.join_specs[{idx}].sql[{clause_idx}] should contain exactly one '=' expression"
                )

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
    }

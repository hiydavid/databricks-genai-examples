"""
Fix Taxonomy: Assessment Reasons → Config Fixes

Maps Genie Benchmark API assessment_reasons to specific serialized_space
configuration changes. Based on the fix taxonomy reference document.
"""

# All 19 assessment labels
ASSESSMENT_REASONS: dict[str, dict] = {
    "LLM_JUDGE_INCORRECT_TABLE_OR_FIELD_USAGE": {
        "priority": "P1",
        "description": "Genie's generated SQL references wrong tables, columns, or fields.",
        "fixes": [
            {"action": "Add synonyms mapping user terminology to column names", "path": "data_sources.tables[].column_configs[].synonyms"},
            {"action": "Enable entity matching on low-cardinality columns", "path": "data_sources.tables[].column_configs[].enable_entity_matching"},
            {"action": "Improve table description to clarify purpose and grain (upstream in UC)", "path": "data_sources.tables[].description"},
            {"action": "Improve column description for misused columns (upstream in UC)", "path": "data_sources.tables[].column_configs[].description"},
        ],
        "diagnostic_signal": "Compare expected vs generated SQL — look at which tables/columns each references.",
    },
    "LLM_JUDGE_MISSING_OR_INCORRECT_JOIN": {
        "priority": "P1",
        "description": "Genie's generated SQL is missing necessary joins or has incorrect join conditions.",
        "fixes": [
            {"action": "Add or fix join spec with correct condition", "path": "instructions.join_specs[]"},
            {"action": "Add business context comment to join spec", "path": "instructions.join_specs[].comment"},
            {"action": "Improve table descriptions to clarify relationships", "path": "data_sources.tables[].description"},
        ],
        "diagnostic_signal": "Compare JOIN clauses in expected vs generated SQL. Identify which tables should be joined and on which keys.",
    },
    "RESULT_MISSING_COLUMNS": {
        "priority": "P2",
        "description": "Genie's generated SQL response is missing columns from the ground truth.",
        "fixes": [
            {"action": "Add synonyms if user question uses different terminology", "path": "data_sources.tables[].column_configs[].synonyms"},
            {"action": "Verify columns are not excluded (exclude: true)", "path": "data_sources.tables[].column_configs[].exclude"},
            {"action": "Improve description for the missing columns (upstream in UC)", "path": "data_sources.tables[].column_configs[].description"},
        ],
        "diagnostic_signal": "Identify which columns are in the expected output but absent from the generated SQL's SELECT.",
    },
    "RESULT_EXTRA_COLUMNS": {
        "priority": "P3",
        "description": "Genie's generated SQL response has more columns than the ground truth.",
        "fixes": [
            {"action": "Add example SQL showing correct SELECT list for this query pattern", "path": "instructions.example_question_sqls[]"},
            {"action": "Improve table description to clarify relevant columns (upstream in UC)", "path": "data_sources.tables[].description"},
        ],
        "diagnostic_signal": "Identify which extra columns Genie included — may indicate unclear table/column descriptions.",
    },
    "COLUMN_TYPE_DIFFERENCE": {
        "priority": "P3",
        "description": "The values match but the column type is different.",
        "fixes": [
            {"action": "Add CAST expression snippet for the correct conversion", "path": "instructions.sql_snippets.expressions[]"},
            {"action": "Clarify expected type/format in column description (upstream in UC)", "path": "data_sources.tables[].column_configs[].description"},
        ],
        "diagnostic_signal": "Compare column types in the result manifests. Check if a CAST is needed.",
    },
    "LLM_JUDGE_MISSING_OR_INCORRECT_AGGREGATION": {
        "priority": "P1",
        "description": "Genie's generated SQL is missing GROUP BY or has incorrect grouping.",
        "fixes": [
            {"action": "Add measure snippet defining the standard aggregation", "path": "instructions.sql_snippets.measures[]"},
            {"action": "Add example SQL with correct aggregation", "path": "instructions.example_question_sqls[]"},
        ],
        "diagnostic_signal": "Expected SQL has GROUP BY / aggregate functions that the generated SQL omits or misapplies.",
    },
    "LLM_JUDGE_INCORRECT_FUNCTION_USAGE": {
        "priority": "P1",
        "description": "Genie's generated SQL uses SQL functions incorrectly.",
        "fixes": [
            {"action": "Register UC function with clear description", "path": "instructions.sql_functions[]"},
            {"action": "Add example SQL showing correct function usage", "path": "instructions.example_question_sqls[]"},
        ],
        "diagnostic_signal": "Compare function calls in expected vs generated SQL. Check if the function is a UDF that needs registration.",
    },
    "LLM_JUDGE_INCOMPLETE_OR_PARTIAL_OUTPUT": {
        "priority": "P2",
        "description": "Genie's generated SQL returns only some of the requested data.",
        "fixes": [
            {"action": "Add example SQL demonstrating the complete pattern (CTEs, UNION, subqueries)", "path": "instructions.example_question_sqls[]"},
            {"action": "Add usage guidance clarifying completeness expectation", "path": "instructions.example_question_sqls[].usage_guidance"},
        ],
        "diagnostic_signal": "Expected SQL has multiple parts (UNION, CTE) or returns more dimensions than the generated SQL.",
    },
    "EMPTY_RESULT": {
        "priority": "P2",
        "description": "Genie's generated SQL results were empty.",
        "fixes": [
            {"action": "Add filter snippet showing correct WHERE pattern", "path": "instructions.sql_snippets.filters[]"},
            {"action": "Enable entity matching on filter columns", "path": "data_sources.tables[].column_configs[].enable_entity_matching"},
            {"action": "Enable format assistance on filter columns", "path": "data_sources.tables[].column_configs[].enable_format_assistance"},
            {"action": "Add example SQL with correct filter", "path": "instructions.example_question_sqls[]"},
        ],
        "diagnostic_signal": "Compare WHERE clauses — the generated SQL's filters are likely too restrictive or use wrong values.",
    },
    "RESULT_MISSING_ROWS": {
        "priority": "P2",
        "description": "Genie's generated SQL response is missing rows from the ground truth.",
        "fixes": [
            {"action": "Add filter snippet if WHERE is too restrictive", "path": "instructions.sql_snippets.filters[]"},
            {"action": "Add measure snippet if wrong aggregation level", "path": "instructions.sql_snippets.measures[]"},
            {"action": "Add example SQL showing correct GROUP BY / filter / LIMIT", "path": "instructions.example_question_sqls[]"},
        ],
        "diagnostic_signal": "Compare GROUP BY granularity, WHERE conditions, and LIMIT clauses.",
    },
    "RESULT_EXTRA_ROWS": {
        "priority": "P3",
        "description": "Genie's generated SQL response has more rows than the ground truth.",
        "fixes": [
            {"action": "Add filter snippet if missing WHERE clause", "path": "instructions.sql_snippets.filters[]"},
            {"action": "Add measure snippet if GROUP BY too granular", "path": "instructions.sql_snippets.measures[]"},
            {"action": "Add example SQL with correct aggregation / DISTINCT / LIMIT", "path": "instructions.example_question_sqls[]"},
        ],
        "diagnostic_signal": "Check if the generated SQL is missing a WHERE, DISTINCT, LIMIT, or has a more granular GROUP BY.",
    },
    "LLM_JUDGE_FORMATTING_ERROR": {
        "priority": "P3",
        "description": "Genie's generated SQL has incorrect formatting, ordering, or presentation.",
        "fixes": [
            {"action": "Add example SQL with correct ORDER BY / formatting", "path": "instructions.example_question_sqls[]"},
            {"action": "Add usage guidance specifying expected output format", "path": "instructions.example_question_sqls[].usage_guidance"},
        ],
        "diagnostic_signal": "Results are correct but ORDER BY, column aliases, or LIMIT differ from expected.",
    },
    "LLM_JUDGE_MISSING_OR_INCORRECT_FILTER": {
        "priority": "P1",
        "description": "Genie's generated SQL is missing a WHERE condition or has incorrect filter logic.",
        "fixes": [
            {"action": "Add filter snippet for the missing/correct pattern", "path": "instructions.sql_snippets.filters[]"},
            {"action": "Add text instruction encoding the business rule", "path": "instructions.text_instructions[].content"},
            {"action": "Enable entity matching on filter column", "path": "data_sources.tables[].column_configs[].enable_entity_matching"},
            {"action": "Enable format assistance on filter column", "path": "data_sources.tables[].column_configs[].enable_format_assistance"},
        ],
        "diagnostic_signal": "Expected SQL has a WHERE condition absent from or different in the generated SQL.",
    },
    "LLM_JUDGE_MISINTERPRETATION_OF_USER_REQUEST": {
        "priority": "P1",
        "description": "Genie fundamentally misunderstands what the user is asking for.",
        "fixes": [
            {"action": "Add text instruction mapping business jargon to data concepts", "path": "instructions.text_instructions[].content"},
            {"action": "Add example SQL clarifying the pattern for this question type", "path": "instructions.example_question_sqls[]"},
            {"action": "Add column synonyms for misunderstood terms", "path": "data_sources.tables[].column_configs[].synonyms"},
        ],
        "diagnostic_signal": "The generated SQL answers a fundamentally different question. Identify which term was misunderstood.",
    },
    "LLM_JUDGE_INSTRUCTION_COMPLIANCE_OR_MISSING_BUSINESS_LOGIC": {
        "priority": "P1",
        "description": "Genie's generated SQL fails to apply business logic or instructions.",
        "fixes": [
            {"action": "Add filter snippet for reusable business logic filters", "path": "instructions.sql_snippets.filters[]"},
            {"action": "Add expression snippet for reusable business logic calculations", "path": "instructions.sql_snippets.expressions[]"},
            {"action": "Add text instruction encoding the business rule", "path": "instructions.text_instructions[].content"},
        ],
        "diagnostic_signal": "Expected SQL applies a business rule (filter, CASE, calculation) absent from the generated SQL.",
    },
    "LLM_JUDGE_INCORRECT_METRIC_CALCULATION": {
        "priority": "P1",
        "description": "Genie's generated SQL uses incorrect logic when calculating metrics.",
        "fixes": [
            {"action": "Add measure snippet defining the correct formula", "path": "instructions.sql_snippets.measures[]"},
            {"action": "Add expression snippet if metric involves CASE logic", "path": "instructions.sql_snippets.expressions[]"},
            {"action": "Add text instruction clarifying the metric definition", "path": "instructions.text_instructions[].content"},
        ],
        "diagnostic_signal": "Compare the aggregation formulas in expected vs generated SQL.",
    },
    "SINGLE_CELL_DIFFERENCE": {
        "priority": "P3",
        "description": "Single value result was produced but differs from ground truth.",
        "fixes": [
            {"action": "Add expression snippet for the correct calculation", "path": "instructions.sql_snippets.expressions[]"},
            {"action": "Add measure snippet if it's an aggregation precision issue", "path": "instructions.sql_snippets.measures[]"},
            {"action": "Add text instruction clarifying rounding/precision rules", "path": "instructions.text_instructions[].content"},
        ],
        "diagnostic_signal": "Compare the specific differing value. Check for ROUND, CAST, or edge-case handling differences.",
    },
    "LLM_JUDGE_OTHER": {
        "priority": "P3",
        "description": "LLM judge identified an error that doesn't fall into other categories.",
        "fixes": [
            {"action": "Inspect actual vs expected SQL manually — determine root cause and map to closest label", "path": "varies"},
        ],
        "diagnostic_signal": "Compare the expected and generated SQL line by line. Map to the closest specific label's fix strategy.",
    },
    "EMPTY_GOOD_SQL": {
        "priority": None,
        "description": "The benchmark SQL returned an empty result. Flag for review.",
        "fixes": [],
        "diagnostic_signal": "Benchmark question itself is broken — review or remove it.",
    },
}


def generate_prescriptive_fix(
    label: str,
    question: str,
    expected_sql: str,
    generated_sql: str,
) -> str:
    """Generate a prescriptive fix rationale for a triggered assessment reason."""
    reason = ASSESSMENT_REASONS.get(label)
    if not reason:
        return f"Unknown assessment reason: {label}"

    lines = [
        f"**{label}** [{reason['priority'] or 'n/a'}]",
        f"Issue: {reason['description']}",
        "",
        "Recommended fixes (in priority order):",
    ]

    for i, fix in enumerate(reason["fixes"], 1):
        lines.append(f"  {i}. {fix['action']}")
        lines.append(f"     Config path: `{fix['path']}`")

    lines.append("")
    lines.append(f"Diagnostic: {reason['diagnostic_signal']}")
    lines.append("")
    lines.append(f"Question: {question}")
    lines.append(f"Expected SQL: {expected_sql}")
    lines.append(f"Generated SQL: {generated_sql}")

    return "\n".join(lines)


def _summarize_sql_difference(expected_sql: str, generated_sql: str) -> str:
    """Extract structural SQL differences without exposing verbatim queries."""
    import re

    def _has_keyword(sql: str, keyword: str) -> bool:
        return bool(re.search(rf"\b{keyword}\b", sql))

    hints = []
    exp = expected_sql.upper()
    gen = generated_sql.upper()

    exp_joins = len(re.findall(r"\bJOIN\b", exp))
    gen_joins = len(re.findall(r"\bJOIN\b", gen))
    if exp_joins != gen_joins:
        hints.append(f"expected {exp_joins} JOIN(s), generated {gen_joins}")

    if _has_keyword(exp, "GROUP BY") and not _has_keyword(gen, "GROUP BY"):
        hints.append("missing GROUP BY clause")
    elif not _has_keyword(exp, "GROUP BY") and _has_keyword(gen, "GROUP BY"):
        hints.append("unexpected GROUP BY clause")

    if _has_keyword(exp, "WHERE") and not _has_keyword(gen, "WHERE"):
        hints.append("missing WHERE clause")

    for func in ("SUM", "COUNT", "AVG", "MIN", "MAX"):
        if _has_keyword(exp, func) and not _has_keyword(gen, func):
            hints.append(f"missing {func}() aggregation")

    if _has_keyword(exp, "OVER") and not _has_keyword(gen, "OVER"):
        hints.append("missing window function (OVER clause)")
    if _has_keyword(exp, "DISTINCT") and not _has_keyword(gen, "DISTINCT"):
        hints.append("missing DISTINCT")
    if _has_keyword(exp, "WITH") and not _has_keyword(gen, "WITH"):
        hints.append("missing CTE (WITH clause)")
    if _has_keyword(exp, "UNION") and not _has_keyword(gen, "UNION"):
        hints.append("missing UNION")
    if _has_keyword(exp, "ORDER BY") and not _has_keyword(gen, "ORDER BY"):
        hints.append("missing ORDER BY")

    return "; ".join(hints) if hints else ""


def compile_fix_report(scorer_results: list[dict]) -> str:
    """Compile all triggered fixes into a prioritized report.

    Args:
        scorer_results: List of dicts with keys: label, question, expected_sql, generated_sql

    Returns:
        Structured text report sorted by priority.
    """
    if not scorer_results:
        return "No fixes needed — all benchmarks passed."

    # Separate actionable items from benchmark quality issues
    items: list[dict] = []
    benchmark_issues: list[dict] = []
    for result in scorer_results:
        label = result["label"]
        reason = ASSESSMENT_REASONS.get(label)
        if not reason:
            continue
        if label == "EMPTY_GOOD_SQL":
            benchmark_issues.append(result)
        else:
            items.append({**result, **reason})

    lines = ["# Prescriptive Fix Report", ""]

    if items:
        # Sort by priority
        items.sort(key=lambda x: x.get("priority", "P3"))

        # Collect unique fix actions with their triggering questions
        seen_fixes: dict[str, list[str]] = {}
        for item in items:
            for fix in item.get("fixes", []):
                key = f"{fix['action']} | {fix['path']}"
                question = item.get("question", "unknown")
                seen_fixes.setdefault(key, []).append(question)

        lines.append("## Recommended Fixes")
        lines.append("")
        for fix_key, questions in seen_fixes.items():
            action, path = fix_key.split(" | ", 1)
            lines.append(f"- **{action}**")
            lines.append(f"  Config path: `{path}`")
            lines.append(f"  Triggered by {len(questions)} question(s):")
            for q in questions[:5]:
                lines.append(f"    - {q}")
            lines.append("")

        # List specific failures for context
        lines.append("### Detailed failures:")
        for item in items:
            lines.append(
                f"- [{item.get('priority', '?')}] **{item.get('label', '?')}**: "
                f"{item.get('question', '?')}"
            )
            lines.append(f"  Expected: `{item.get('expected_sql', '')}`")
            lines.append(f"  Generated: `{item.get('generated_sql', '')}`")
        lines.append("")

    if benchmark_issues:
        lines.append("## Benchmark Quality Issues (not fixable via config)")
        for issue in benchmark_issues:
            lines.append(f"- {issue.get('question', '?')}: EMPTY_GOOD_SQL — review or remove")
        lines.append("")

    return "\n".join(lines)

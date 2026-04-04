"""
Fix Taxonomy: Assessment Reasons → Config Fixes

Maps Genie Benchmark API assessment_reasons to specific serialized_space
configuration changes. Based on the fix taxonomy reference document.
"""

# Fix ordering — apply left-to-right, each category builds on the previous
FIX_ORDERING = ["UC Metadata", "SQL Examples", "Instructions"]

# All 25 assessment labels
ASSESSMENT_REASONS: dict[str, dict] = {
    # =========================================================================
    # Category 1: UC Metadata
    # =========================================================================
    "LLM_JUDGE_INCORRECT_TABLE_OR_FIELD_USAGE": {
        "category": "UC Metadata",
        "priority": "P1",
        "description": "Genie's generated SQL references wrong tables, columns, or fields.",
        "primary_fixes": [
            {"action": "Improve table description to clarify purpose and grain", "path": "data_sources.tables[].description"},
            {"action": "Improve column description for misused columns", "path": "data_sources.tables[].column_configs[].description"},
            {"action": "Add synonyms mapping user terminology to column names", "path": "data_sources.tables[].column_configs[].synonyms"},
        ],
        "secondary_fixes": [
            {"action": "Exclude irrelevant columns to reduce noise", "path": "data_sources.tables[].column_configs[].exclude"},
            {"action": "Enable entity matching on low-cardinality columns", "path": "data_sources.tables[].column_configs[].enable_entity_matching"},
        ],
        "diagnostic_signal": "Compare expected vs generated SQL — look at which tables/columns each references.",
    },
    "LLM_JUDGE_MISSING_OR_INCORRECT_JOIN": {
        "category": "UC Metadata",
        "priority": "P1",
        "description": "Genie's generated SQL is missing necessary joins or has incorrect join conditions.",
        "primary_fixes": [
            {"action": "Add or fix join spec with correct condition", "path": "instructions.join_specs[]"},
            {"action": "Add business context comment to join spec", "path": "instructions.join_specs[].comment"},
        ],
        "secondary_fixes": [
            {"action": "Improve table descriptions to clarify relationships", "path": "data_sources.tables[].description"},
        ],
        "diagnostic_signal": "Compare JOIN clauses in expected vs generated SQL. Identify which tables should be joined and on which keys.",
    },
    "LLM_JUDGE_MISSING_JOIN": {
        "category": "UC Metadata",
        "priority": "P1",
        "description": "Genie's generated SQL is missing a required join between tables.",
        "primary_fixes": [
            {"action": "Add join spec for the missing relationship", "path": "instructions.join_specs[]"},
        ],
        "secondary_fixes": [
            {"action": "Improve table descriptions to indicate how tables relate", "path": "data_sources.tables[].description"},
        ],
        "diagnostic_signal": "Expected SQL joins tables that the generated SQL queries independently or omits.",
    },
    "LLM_JUDGE_WRONG_COLUMNS": {
        "category": "UC Metadata",
        "priority": "P2",
        "description": "Genie's generated SQL selects wrong columns.",
        "primary_fixes": [
            {"action": "Improve column description for misused columns", "path": "data_sources.tables[].column_configs[].description"},
            {"action": "Add synonyms if user terminology differs from column names", "path": "data_sources.tables[].column_configs[].synonyms"},
        ],
        "secondary_fixes": [
            {"action": "Exclude noise columns that Genie shouldn't consider", "path": "data_sources.tables[].column_configs[].exclude"},
        ],
        "diagnostic_signal": "Compare SELECT lists — identify which columns were swapped, added, or omitted.",
    },
    "RESULT_MISSING_COLUMNS": {
        "category": "UC Metadata",
        "priority": "P2",
        "description": "Genie's generated SQL response is missing columns from the ground truth.",
        "primary_fixes": [
            {"action": "Improve description for the missing columns", "path": "data_sources.tables[].column_configs[].description"},
            {"action": "Add synonyms if user question uses different terminology", "path": "data_sources.tables[].column_configs[].synonyms"},
        ],
        "secondary_fixes": [
            {"action": "Verify columns are not excluded (exclude: true)", "path": "data_sources.tables[].column_configs[].exclude"},
        ],
        "diagnostic_signal": "Identify which columns are in the expected output but absent from the generated SQL's SELECT.",
    },
    "RESULT_EXTRA_COLUMNS": {
        "category": "UC Metadata",
        "priority": "P3",
        "description": "Genie's generated SQL response has more columns than the ground truth.",
        "primary_fixes": [
            {"action": "Exclude noise columns Genie shouldn't select", "path": "data_sources.tables[].column_configs[].exclude"},
        ],
        "secondary_fixes": [
            {"action": "Improve table description to clarify relevant columns", "path": "data_sources.tables[].description"},
        ],
        "diagnostic_signal": "Identify which extra columns Genie included and whether they should be hidden.",
    },
    "COLUMN_TYPE_DIFFERENCE": {
        "category": "UC Metadata",
        "priority": "P3",
        "description": "The values match but the column type is different.",
        "primary_fixes": [
            {"action": "Clarify expected type/format in column description", "path": "data_sources.tables[].column_configs[].description"},
        ],
        "secondary_fixes": [
            {"action": "Add CAST expression snippet for the correct conversion", "path": "instructions.sql_snippets.expressions[]"},
        ],
        "diagnostic_signal": "Compare column types in the result manifests. Check if a CAST is needed.",
    },
    # =========================================================================
    # Category 2: SQL Examples
    # =========================================================================
    "LLM_JUDGE_WRONG_AGGREGATION": {
        "category": "SQL Examples",
        "priority": "P1",
        "description": "Genie's generated SQL uses the wrong aggregate function or GROUP BY clause.",
        "primary_fixes": [
            {"action": "Add example SQL demonstrating correct aggregation pattern", "path": "instructions.example_question_sqls[]"},
            {"action": "Add measure snippet for standard aggregations", "path": "instructions.sql_snippets.measures[]"},
        ],
        "secondary_fixes": [],
        "diagnostic_signal": "Compare GROUP BY and aggregate functions (SUM vs COUNT, etc.) in expected vs generated SQL.",
    },
    "LLM_JUDGE_MISSING_OR_INCORRECT_AGGREGATION": {
        "category": "SQL Examples",
        "priority": "P1",
        "description": "Genie's generated SQL is missing GROUP BY or has incorrect grouping.",
        "primary_fixes": [
            {"action": "Add example SQL with correct aggregation", "path": "instructions.example_question_sqls[]"},
            {"action": "Add measure snippet defining the standard aggregation", "path": "instructions.sql_snippets.measures[]"},
        ],
        "secondary_fixes": [],
        "diagnostic_signal": "Expected SQL has GROUP BY / aggregate functions that the generated SQL omits or misapplies.",
    },
    "LLM_JUDGE_INCORRECT_FUNCTION_USAGE": {
        "category": "SQL Examples",
        "priority": "P1",
        "description": "Genie's generated SQL uses SQL functions incorrectly.",
        "primary_fixes": [
            {"action": "Add example SQL showing correct function usage", "path": "instructions.example_question_sqls[]"},
            {"action": "Register UC function with clear description", "path": "instructions.sql_functions[]"},
        ],
        "secondary_fixes": [],
        "diagnostic_signal": "Compare function calls in expected vs generated SQL. Check if the function is a UDF that needs registration.",
    },
    "LLM_JUDGE_SYNTAX_ERROR": {
        "category": "SQL Examples",
        "priority": "P2",
        "description": "Genie's generated SQL contains syntax errors that prevent execution.",
        "primary_fixes": [
            {"action": "Add example SQL using correct dialect syntax", "path": "instructions.example_question_sqls[]"},
        ],
        "secondary_fixes": [
            {"action": "Register UC function if syntax error involves a UDF", "path": "instructions.sql_functions[]"},
        ],
        "diagnostic_signal": "The generated SQL fails to execute. Identify the syntax error.",
    },
    "LLM_JUDGE_INCOMPLETE_OR_PARTIAL_OUTPUT": {
        "category": "SQL Examples",
        "priority": "P2",
        "description": "Genie's generated SQL returns only some of the requested data.",
        "primary_fixes": [
            {"action": "Add example SQL demonstrating the complete pattern (CTEs, UNION, subqueries)", "path": "instructions.example_question_sqls[]"},
        ],
        "secondary_fixes": [
            {"action": "Add usage guidance clarifying completeness expectation", "path": "instructions.example_question_sqls[].usage_guidance"},
        ],
        "diagnostic_signal": "Expected SQL has multiple parts (UNION, CTE) or returns more dimensions than the generated SQL.",
    },
    "EMPTY_RESULT": {
        "category": "SQL Examples",
        "priority": "P2",
        "description": "Genie's generated SQL results were empty.",
        "primary_fixes": [
            {"action": "Add filter snippet showing correct WHERE pattern", "path": "instructions.sql_snippets.filters[]"},
            {"action": "Add example SQL with correct filter", "path": "instructions.example_question_sqls[]"},
        ],
        "secondary_fixes": [
            {"action": "Enable entity matching on filter columns", "path": "data_sources.tables[].column_configs[].enable_entity_matching"},
            {"action": "Enable format assistance on filter columns", "path": "data_sources.tables[].column_configs[].enable_format_assistance"},
        ],
        "diagnostic_signal": "Compare WHERE clauses — the generated SQL's filters are likely too restrictive or use wrong values.",
    },
    "RESULT_MISSING_ROWS": {
        "category": "SQL Examples",
        "priority": "P2",
        "description": "Genie's generated SQL response is missing rows from the ground truth.",
        "primary_fixes": [
            {"action": "Add example SQL showing correct GROUP BY / filter / LIMIT", "path": "instructions.example_question_sqls[]"},
            {"action": "Add filter snippet if WHERE is too restrictive", "path": "instructions.sql_snippets.filters[]"},
        ],
        "secondary_fixes": [
            {"action": "Add measure snippet if wrong aggregation level", "path": "instructions.sql_snippets.measures[]"},
        ],
        "diagnostic_signal": "Compare GROUP BY granularity, WHERE conditions, and LIMIT clauses.",
    },
    "RESULT_EXTRA_ROWS": {
        "category": "SQL Examples",
        "priority": "P3",
        "description": "Genie's generated SQL response has more rows than the ground truth.",
        "primary_fixes": [
            {"action": "Add example SQL with correct aggregation / DISTINCT / LIMIT", "path": "instructions.example_question_sqls[]"},
            {"action": "Add filter snippet if missing WHERE clause", "path": "instructions.sql_snippets.filters[]"},
        ],
        "secondary_fixes": [
            {"action": "Add measure snippet if GROUP BY too granular", "path": "instructions.sql_snippets.measures[]"},
        ],
        "diagnostic_signal": "Check if the generated SQL is missing a WHERE, DISTINCT, LIMIT, or has a more granular GROUP BY.",
    },
    "LLM_JUDGE_FORMATTING_ERROR": {
        "category": "SQL Examples",
        "priority": "P3",
        "description": "Genie's generated SQL has incorrect formatting, ordering, or presentation.",
        "primary_fixes": [
            {"action": "Add example SQL with correct ORDER BY / formatting", "path": "instructions.example_question_sqls[]"},
        ],
        "secondary_fixes": [
            {"action": "Add usage guidance specifying expected output format", "path": "instructions.example_question_sqls[].usage_guidance"},
        ],
        "diagnostic_signal": "Results are correct but ORDER BY, column aliases, or LIMIT differ from expected.",
    },
    # =========================================================================
    # Category 3: Instructions
    # =========================================================================
    "LLM_JUDGE_WRONG_FILTER": {
        "category": "Instructions",
        "priority": "P1",
        "description": "Genie's generated SQL has an incorrect WHERE clause.",
        "primary_fixes": [
            {"action": "Add filter snippet for the correct pattern", "path": "instructions.sql_snippets.filters[]"},
            {"action": "Add text instruction explaining the business rule", "path": "instructions.text_instructions[].content"},
        ],
        "secondary_fixes": [
            {"action": "Enable entity matching on filter column", "path": "data_sources.tables[].column_configs[].enable_entity_matching"},
            {"action": "Enable format assistance on filter column", "path": "data_sources.tables[].column_configs[].enable_format_assistance"},
        ],
        "diagnostic_signal": "Compare WHERE clauses — identify the specific condition that differs.",
    },
    "LLM_JUDGE_MISSING_OR_INCORRECT_FILTER": {
        "category": "Instructions",
        "priority": "P1",
        "description": "Genie's generated SQL is missing a WHERE condition or has incorrect filter logic.",
        "primary_fixes": [
            {"action": "Add filter snippet for the missing/correct pattern", "path": "instructions.sql_snippets.filters[]"},
            {"action": "Add text instruction encoding the business rule", "path": "instructions.text_instructions[].content"},
        ],
        "secondary_fixes": [
            {"action": "Enable entity matching on filter column", "path": "data_sources.tables[].column_configs[].enable_entity_matching"},
            {"action": "Enable format assistance on filter column", "path": "data_sources.tables[].column_configs[].enable_format_assistance"},
        ],
        "diagnostic_signal": "Expected SQL has a WHERE condition absent from or different in the generated SQL.",
    },
    "LLM_JUDGE_MISINTERPRETATION_OF_USER_REQUEST": {
        "category": "Instructions",
        "priority": "P1",
        "description": "Genie fundamentally misunderstands what the user is asking for.",
        "primary_fixes": [
            {"action": "Add text instruction mapping business jargon to data concepts", "path": "instructions.text_instructions[].content"},
            {"action": "Add column synonyms for misunderstood terms", "path": "data_sources.tables[].column_configs[].synonyms"},
        ],
        "secondary_fixes": [
            {"action": "Add example SQL clarifying the pattern for this question type", "path": "instructions.example_question_sqls[]"},
        ],
        "diagnostic_signal": "The generated SQL answers a fundamentally different question. Identify which term was misunderstood.",
    },
    "LLM_JUDGE_INSTRUCTION_COMPLIANCE_OR_MISSING_BUSINESS_LOGIC": {
        "category": "Instructions",
        "priority": "P1",
        "description": "Genie's generated SQL fails to apply business logic or instructions.",
        "primary_fixes": [
            {"action": "Add text instruction encoding the business rule", "path": "instructions.text_instructions[].content"},
            {"action": "Add filter snippet for reusable business logic filters", "path": "instructions.sql_snippets.filters[]"},
            {"action": "Add expression snippet for reusable business logic calculations", "path": "instructions.sql_snippets.expressions[]"},
        ],
        "secondary_fixes": [],
        "diagnostic_signal": "Expected SQL applies a business rule (filter, CASE, calculation) absent from the generated SQL.",
    },
    "LLM_JUDGE_INCORRECT_METRIC_CALCULATION": {
        "category": "Instructions",
        "priority": "P1",
        "description": "Genie's generated SQL uses incorrect logic when calculating metrics.",
        "primary_fixes": [
            {"action": "Add measure snippet defining the correct formula", "path": "instructions.sql_snippets.measures[]"},
            {"action": "Add text instruction clarifying the metric definition", "path": "instructions.text_instructions[].content"},
        ],
        "secondary_fixes": [
            {"action": "Add expression snippet if metric involves CASE logic", "path": "instructions.sql_snippets.expressions[]"},
        ],
        "diagnostic_signal": "Compare the aggregation formulas in expected vs generated SQL.",
    },
    "LLM_JUDGE_SEMANTIC_ERROR": {
        "category": "Instructions",
        "priority": "P2",
        "description": "Genie's generated SQL is syntactically valid but logically wrong.",
        "primary_fixes": [
            {"action": "Add text instruction clarifying the correct interpretation", "path": "instructions.text_instructions[].content"},
            {"action": "Add example SQL demonstrating the correct logic", "path": "instructions.example_question_sqls[]"},
        ],
        "secondary_fixes": [],
        "diagnostic_signal": "Both SQLs execute but produce different results. Analyze the logical difference.",
    },
    "SINGLE_CELL_DIFFERENCE": {
        "category": "Instructions",
        "priority": "P3",
        "description": "Single value result was produced but differs from ground truth.",
        "primary_fixes": [
            {"action": "Add text instruction clarifying rounding/precision rules", "path": "instructions.text_instructions[].content"},
            {"action": "Add expression snippet for the correct calculation", "path": "instructions.sql_snippets.expressions[]"},
        ],
        "secondary_fixes": [
            {"action": "Add measure snippet if it's an aggregation precision issue", "path": "instructions.sql_snippets.measures[]"},
        ],
        "diagnostic_signal": "Compare the specific differing value. Check for ROUND, CAST, or edge-case handling differences.",
    },
    "LLM_JUDGE_OTHER": {
        "category": "Instructions",
        "priority": "P3",
        "description": "LLM judge identified an error that doesn't fall into other categories.",
        "primary_fixes": [
            {"action": "Inspect actual vs expected SQL manually — determine root cause and map to closest label", "path": "varies"},
        ],
        "secondary_fixes": [],
        "diagnostic_signal": "Compare the expected and generated SQL line by line. Map to the closest specific label's fix strategy.",
    },
    # =========================================================================
    # Benchmark Quality (not fixable via config)
    # =========================================================================
    "EMPTY_GOOD_SQL": {
        "category": "Benchmark Quality",
        "priority": None,
        "description": "The benchmark SQL returned an empty result. Flag for review.",
        "primary_fixes": [],
        "secondary_fixes": [],
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
        f"**{label}** [{reason['category']} / {reason['priority']}]",
        f"Issue: {reason['description']}",
        "",
        "Recommended fixes (in priority order):",
    ]

    for i, fix in enumerate(reason["primary_fixes"], 1):
        lines.append(f"  {i}. {fix['action']}")
        lines.append(f"     Config path: `{fix['path']}`")

    if reason["secondary_fixes"]:
        lines.append("")
        lines.append("Secondary fixes:")
        for fix in reason["secondary_fixes"]:
            lines.append(f"  - {fix['action']} (`{fix['path']}`)")

    lines.append("")
    lines.append(f"Diagnostic: {reason['diagnostic_signal']}")
    lines.append("")
    lines.append(f"Question: {question}")
    lines.append(f"Expected SQL: {expected_sql[:500]}")
    lines.append(f"Generated SQL: {generated_sql[:500]}")

    return "\n".join(lines)


def compile_fix_report(scorer_results: list[dict]) -> str:
    """Compile all triggered fixes into a prioritized report grouped by category.

    Args:
        scorer_results: List of dicts with keys: label, question, expected_sql, generated_sql

    Returns:
        Structured text report grouped by category and priority.
    """
    if not scorer_results:
        return "No fixes needed — all benchmarks passed."

    # Group by category
    by_category: dict[str, list[dict]] = {}
    for result in scorer_results:
        label = result["label"]
        reason = ASSESSMENT_REASONS.get(label)
        if not reason or reason["category"] == "Benchmark Quality":
            continue
        category = reason["category"]
        by_category.setdefault(category, []).append({**result, **reason})

    lines = ["# Prescriptive Fix Report", ""]

    # Deduplicate fix actions across questions
    for category in FIX_ORDERING:
        items = by_category.get(category, [])
        if not items:
            continue

        # Sort by priority
        items.sort(key=lambda x: x.get("priority", "P3"))

        lines.append(f"## {category}")
        lines.append("")

        # Collect unique fix actions with their triggering questions
        seen_fixes: dict[str, list[str]] = {}
        for item in items:
            for fix in item.get("primary_fixes", []):
                key = f"{fix['action']} | {fix['path']}"
                question = item.get("question", "unknown")
                seen_fixes.setdefault(key, []).append(question)

        for fix_key, questions in seen_fixes.items():
            action, path = fix_key.split(" | ", 1)
            lines.append(f"- **{action}**")
            lines.append(f"  Config path: `{path}`")
            lines.append(f"  Triggered by {len(questions)} question(s):")
            for q in questions[:5]:
                lines.append(f"    - {q[:120]}")
            lines.append("")

        # List specific failures for context
        lines.append("### Detailed failures:")
        for item in items:
            lines.append(
                f"- [{item.get('priority', '?')}] **{item.get('label', '?')}**: "
                f"{item.get('question', '?')[:100]}"
            )
            lines.append(f"  Expected: `{item.get('expected_sql', '')[:200]}`")
            lines.append(f"  Generated: `{item.get('generated_sql', '')[:200]}`")
        lines.append("")

    # Flag any benchmark quality issues
    benchmark_issues = [
        r for r in scorer_results
        if ASSESSMENT_REASONS.get(r.get("label", ""), {}).get("category") == "Benchmark Quality"
    ]
    if benchmark_issues:
        lines.append("## Benchmark Quality Issues (not fixable via config)")
        for issue in benchmark_issues:
            lines.append(f"- {issue.get('question', '?')}: EMPTY_GOOD_SQL — review or remove")
        lines.append("")

    return "\n".join(lines)

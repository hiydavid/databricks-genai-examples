"""
Deterministic scorers for Genie Benchmark evaluation.

Three scorers matching how the Genie Eval API judges:
- overall_assessment: top-level GOOD vs BAD/NEEDS_REVIEW verdict
- result_comparison: deterministic checks against ground truth results
- llm_judge: LLM-based semantic analysis of SQL correctness

Each returns {"name": ..., "value": "yes"/"no", "rationale": ...}.
"""

from fix_taxonomy import generate_prescriptive_fix

DETERMINISTIC_REASONS = {
    "EMPTY_RESULT",
    "RESULT_MISSING_ROWS",
    "RESULT_EXTRA_ROWS",
    "RESULT_MISSING_COLUMNS",
    "RESULT_EXTRA_COLUMNS",
    "SINGLE_CELL_DIFFERENCE",
    "EMPTY_GOOD_SQL",
    "COLUMN_TYPE_DIFFERENCE",
}

LLM_JUDGE_REASONS = {
    "LLM_JUDGE_MISSING_OR_INCORRECT_FILTER",
    "LLM_JUDGE_INCOMPLETE_OR_PARTIAL_OUTPUT",
    "LLM_JUDGE_MISINTERPRETATION_OF_USER_REQUEST",
    "LLM_JUDGE_INSTRUCTION_COMPLIANCE_OR_MISSING_BUSINESS_LOGIC",
    "LLM_JUDGE_INCORRECT_METRIC_CALCULATION",
    "LLM_JUDGE_INCORRECT_TABLE_OR_FIELD_USAGE",
    "LLM_JUDGE_INCORRECT_FUNCTION_USAGE",
    "LLM_JUDGE_MISSING_OR_INCORRECT_JOIN",
    "LLM_JUDGE_MISSING_OR_INCORRECT_AGGREGATION",
    "LLM_JUDGE_FORMATTING_ERROR",
    "LLM_JUDGE_OTHER",
}


def _build_rationale(
    triggered_reasons: list[str],
    question: str,
    expected_sql: str,
    generated_sql: str,
) -> str:
    """Build combined prescriptive fix rationale for triggered reasons."""
    fix_sections = []
    for reason in triggered_reasons:
        fix_sections.append(
            generate_prescriptive_fix(
                label=reason,
                question=question,
                expected_sql=expected_sql,
                generated_sql=generated_sql,
            )
        )
    return "\n---\n".join(fix_sections)


def score_overall_assessment(
    question: str,
    generated_sql: str,
    expected_sql: str,
    assessment: str,
    reasons: list[str],
) -> dict:
    """Top-level Genie API verdict: GOOD vs BAD/NEEDS_REVIEW."""
    if assessment == "GOOD":
        return {"name": "overall_assessment", "value": "yes", "rationale": "Genie assessment: GOOD"}

    return {
        "name": "overall_assessment",
        "value": "no",
        "rationale": f"Genie assessment: {assessment}. Reasons: {', '.join(reasons)}",
    }


def score_result_comparison(
    question: str,
    generated_sql: str,
    expected_sql: str,
    assessment: str,
    reasons: list[str],
) -> dict:
    """Deterministic result comparison: checks ground truth vs generated results."""
    triggered = [r for r in reasons if r in DETERMINISTIC_REASONS]

    if assessment == "GOOD" or not triggered:
        return {"name": "result_comparison", "value": "yes", "rationale": "No deterministic result differences"}

    rationale = _build_rationale(triggered, question, expected_sql, generated_sql)
    return {
        "name": "result_comparison",
        "value": "no",
        "rationale": f"Deterministic issues: {', '.join(triggered)}\n\n{rationale}",
    }


def score_llm_judge(
    question: str,
    generated_sql: str,
    expected_sql: str,
    assessment: str,
    reasons: list[str],
) -> dict:
    """LLM judge: semantic analysis of SQL correctness."""
    triggered = [r for r in reasons if r in LLM_JUDGE_REASONS]

    if assessment == "GOOD" or not triggered:
        return {"name": "llm_judge", "value": "yes", "rationale": "No LLM judge issues"}

    rationale = _build_rationale(triggered, question, expected_sql, generated_sql)
    return {
        "name": "llm_judge",
        "value": "no",
        "rationale": f"LLM judge issues: {', '.join(triggered)}\n\n{rationale}",
    }

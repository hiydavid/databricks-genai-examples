"""
Benchmark Q&A Validator: LLM-based semantic validation of benchmark question-answer pairs.

Reviews each benchmark to determine whether the ground truth SQL actually answers
what the question asks. Flags issues like extra/missing columns, incorrect
aggregations, or SQL logic that doesn't match the question intent.

This is a pre-evaluation quality gate: if benchmarks themselves are bad, eval
results cannot be trusted.
"""

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import mlflow
from databricks.sdk import WorkspaceClient
from mlflow.entities import SpanType
from openai import OpenAI

mlflow.openai.autolog()

logger = logging.getLogger(__name__)

VALIDATION_SYSTEM_PROMPT = """\
You are a SQL benchmark quality reviewer. You receive a natural language \
question and a ground truth SQL query that is supposed to answer that question. \
Your job is to determine whether the SQL actually answers what the question asks.

Evaluate the following aspects:
1. **Column alignment**: Does the SQL SELECT exactly the columns the question \
asks for? Flag extra columns the question didn't request and missing columns \
it did request.
2. **Aggregation correctness**: If the question asks for totals, averages, \
counts, etc., does the SQL use the correct aggregate functions?
3. **Filter correctness**: If the question specifies conditions (time periods, \
categories, thresholds), does the SQL WHERE clause reflect them?
4. **Grouping correctness**: If the question asks for results "by" some \
dimension, does the SQL GROUP BY match?
5. **Logic alignment**: Does the overall SQL logic (JOINs, subqueries, CASE \
expressions, window functions) match the question's intent?

Important guidelines:
- You are NOT checking SQL syntax or whether it would execute successfully.
- You are checking whether the SQL, if executed, would answer the question.
- Minor stylistic differences (column aliases, ORDER BY for unordered questions) \
are NOT issues.
- Extra columns that provide useful context (like IDs alongside names) are \
minor issues, not major ones.
- If the question is ambiguous and the SQL is a reasonable interpretation, \
mark it as PASS with a note.

Return a JSON object with exactly these fields:
{
    "verdict": "PASS" | "FAIL" | "WARN",
    "confidence": "high" | "medium" | "low",
    "issues": ["list of specific issues found, empty if PASS"],
    "summary": "one-sentence explanation of the verdict"
}

Definitions:
- PASS: The SQL correctly answers the question.
- WARN: The SQL mostly answers the question but has minor issues \
(e.g., an extra column, ambiguous interpretation).
- FAIL: The SQL does NOT correctly answer the question \
(e.g., wrong aggregation, missing critical filter, answers a different question).

Return ONLY the JSON object — no markdown, no explanation, no wrapping.\
"""


def _build_client() -> OpenAI:
    """Build an OpenAI-compatible client authenticated against the Databricks workspace."""
    ws = WorkspaceClient()
    token = ws.config.authenticate().get("Authorization", "").removeprefix("Bearer ")
    return OpenAI(
        base_url=f"{ws.config.host.rstrip('/')}/serving-endpoints",
        api_key=token,
    )


def _call_validation_llm(client: OpenAI, llm_endpoint: str, user_message: str) -> dict:
    """Send a chat completion request for benchmark validation and return parsed JSON."""
    response = client.chat.completions.create(
        model=llm_endpoint,
        messages=[
            {"role": "system", "content": VALIDATION_SYSTEM_PROMPT},
            {"role": "user", "content": user_message},
        ],
        temperature=0,
        max_tokens=4000,
    )
    content = response.choices[0].message.content.strip()
    if content.startswith("```"):
        content = content.split("\n", 1)[1].rsplit("```", 1)[0].strip()
    return json.loads(content)


def _extract_benchmark_sql(benchmark: dict) -> str:
    """Extract the SQL string from a benchmark question's answer array.

    The API uses ``content`` (array of strings) for the SQL body, e.g.:
        {"format": "SQL", "content": ["SELECT ...", "FROM ..."]}
    """
    from validation import _text_from_value

    for ans in benchmark.get("answer", []):
        if ans.get("format") != "SQL":
            continue
        # "content" is the documented field (array of strings)
        if ans.get("content"):
            return _text_from_value(ans["content"])
        # fall back to "response" just in case
        if ans.get("response"):
            return _text_from_value(ans["response"])
    return ""


def _validate_single_benchmark(
    client: OpenAI,
    llm_endpoint: str,
    question_text: str,
    sql_text: str,
    benchmark_id: str,
) -> dict:
    """Validate a single benchmark Q&A pair via LLM."""
    user_message = (
        f"Question: {question_text}\n\n"
        f"Ground Truth SQL:\n```sql\n{sql_text}\n```"
    )
    try:
        result = _call_validation_llm(client, llm_endpoint, user_message)
        result["benchmark_id"] = benchmark_id
        result["question"] = question_text
        return result
    except Exception as e:
        logger.warning("Failed to validate benchmark %s: %s", benchmark_id, e)
        return {
            "benchmark_id": benchmark_id,
            "question": question_text,
            "verdict": "ERROR",
            "issues": [f"Validation failed: {e}"],
            "confidence": "low",
            "summary": f"LLM validation error: {e}",
        }


def compile_validation_report(results: list[dict]) -> str:
    """Compile validation results into a human-readable report."""
    if not results:
        return "No benchmarks to validate."

    verdicts = [r.get("verdict", "ERROR") for r in results]
    total = len(results)
    pass_count = verdicts.count("PASS")
    warn_count = verdicts.count("WARN")
    fail_count = verdicts.count("FAIL")
    error_count = verdicts.count("ERROR")

    lines = [
        "# Benchmark Q&A Validation Report",
        "",
        f"Summary: {pass_count} PASS, {warn_count} WARN, {fail_count} FAIL"
        + (f", {error_count} ERROR" if error_count else "")
        + f" out of {total} benchmarks",
        "",
    ]

    for verdict_label in ("FAIL", "WARN", "ERROR"):
        items = [r for r in results if r.get("verdict") == verdict_label]
        if not items:
            continue

        lines.append(f"## {verdict_label} ({len(items)})")
        lines.append("")
        for i, item in enumerate(items, 1):
            confidence = item.get("confidence", "unknown")
            question = item.get("question", "?")[:120]
            lines.append(
                f'  {i}. [{verdict_label}] "{question}"  ({confidence} confidence)'
            )
            for issue in item.get("issues", []):
                lines.append(f"     - {issue}")
            summary_text = item.get("summary", "")
            if summary_text:
                lines.append(f"     Summary: {summary_text}")
            lines.append("")

    if pass_count > 0:
        lines.append(f"## PASS ({pass_count}) — not shown individually")
        lines.append("")

    return "\n".join(lines)


@mlflow.trace(name="validate_benchmarks", span_type=SpanType.CHAIN)
def validate_benchmarks(
    benchmark_questions: list[dict],
    llm_endpoint: str = "databricks-claude-sonnet-4-6",
    max_workers: int = 5,
) -> dict:
    """Validate all benchmark Q&A pairs for semantic correctness.

    Args:
        benchmark_questions: List of benchmark question dicts from
            serialized_space["benchmarks"]["questions"].
        llm_endpoint: Databricks Foundation Model endpoint name.
        max_workers: Max parallel LLM calls.

    Returns:
        Dict with "results" (per-benchmark), "summary" (counts), and "report" (text).
    """
    from validation import _text_from_value

    client = _build_client()

    def _validate_one(bq):
        question_text = _text_from_value(bq.get("question"))
        sql_text = _extract_benchmark_sql(bq)
        benchmark_id = bq.get("id", "unknown")

        if not sql_text:
            return {
                "benchmark_id": benchmark_id,
                "question": question_text,
                "verdict": "WARN",
                "confidence": "high",
                "issues": ["No SQL found in benchmark answer"],
                "summary": "Benchmark has no ground truth SQL.",
            }

        if not question_text:
            return {
                "benchmark_id": benchmark_id,
                "question": "",
                "verdict": "WARN",
                "confidence": "high",
                "issues": ["No question text found in benchmark"],
                "summary": "Benchmark has no question text.",
            }

        return _validate_single_benchmark(
            client, llm_endpoint, question_text, sql_text, benchmark_id
        )

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_validate_one, bq): bq for bq in benchmark_questions}
        for future in as_completed(futures):
            results.append(future.result())

    # Sort results by original benchmark order
    id_order = {bq.get("id", ""): i for i, bq in enumerate(benchmark_questions)}
    results.sort(key=lambda r: id_order.get(r.get("benchmark_id", ""), 999))

    verdicts = [r.get("verdict", "ERROR") for r in results]
    summary = {
        "total": len(results),
        "pass": verdicts.count("PASS"),
        "warn": verdicts.count("WARN"),
        "fail": verdicts.count("FAIL"),
        "error": verdicts.count("ERROR"),
    }

    return {
        "results": results,
        "summary": summary,
        "report": compile_validation_report(results),
    }

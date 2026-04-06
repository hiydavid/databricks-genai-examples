"""
LLM-powered Genie Space config optimization.

Takes the current serialized_space and a prescriptive fix report, calls an LLM
to produce an updated configuration that incorporates the fixes.
"""

import json
import logging

import mlflow
from databricks.sdk import WorkspaceClient
from mlflow.entities import SpanType
from openai import OpenAI

from validation import validate_no_benchmark_overlap

mlflow.openai.autolog()

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """\
You are a Databricks Genie Space configuration optimizer. You receive a \
serialized_space JSON and a prescriptive fix report compiled from benchmark \
evaluation results. Your job is to produce an updated serialized_space JSON \
that incorporates all applicable fixes.

Rules you MUST follow:
1. Preserve ALL existing IDs — never change or remove them.
2. New items (instructions, snippets, sample questions) need 32-character \
   lowercase hexadecimal IDs (UUID format without hyphens).
3. Apply fixes in priority order: UC Metadata fixes first, then SQL Examples, \
   then Instructions.
4. Only modify what the fixes prescribe. Do not remove, rename, or restructure \
   unrelated parts of the config.
5. Maintain "version": 2.
6. Keep tables and metric_views sorted alphabetically by their "identifier" field.
7. Keep column_configs sorted alphabetically by "column_name".
8. Keep all instruction collections (text_instructions, example_question_sqls, \
   join_specs, sql_functions, sql_snippets.*) sorted by "id".
9. Maximum 1 text_instruction entry — if one already exists, append new guidance \
   to its "content" array rather than creating a second entry.
10. join_specs "sql" field must have exactly 2 elements: \
    [join_condition_string, relationship_type_annotation]. \
    The join_condition_string must be a SINGLE equality expression \
    (e.g. "t1.col = t2.col") — no AND, OR, or multiple conditions. \
    If a join needs multiple conditions, create separate join_spec entries.
11. Do not modify the "benchmarks" section.
12. These fields MUST always be arrays of strings, never bare strings: \
    description, question, content, sql, synonyms, comment, instruction, \
    usage_guidance. For example: "description": ["Some text"], not \
    "description": "Some text".
13. Return ONLY the complete, valid serialized_space JSON object — no markdown, \
    no explanation, no wrapping.
14. example_question_sqls MUST teach generalizable SQL patterns, not answer \
    specific questions. Each example should demonstrate a reusable technique \
    (e.g., window functions for ranking, CASE expressions for categorization, \
    date arithmetic for period comparisons). The "question" field should be a \
    generic template (e.g., "What is the top N [metric] by [dimension]?") not \
    a specific business question.
15. NEVER create an example_question_sql whose "question" field or "sql" \
    field closely matches or is identical to any specific benchmark question \
    or SQL from the fix report. Generalize both the question AND the SQL: \
    if a benchmark fails on "What were Q1 2024 sales by region?", create \
    an example like "What is [metric] by [dimension] for a given time \
    period?" with SQL showing the DATE_TRUNC + GROUP BY pattern using \
    generic column references, not the exact columns from the benchmark.
16. Prefer sql_snippets (measures, filters, expressions) and text_instructions \
    over example_question_sqls when the fix only requires teaching a specific \
    calculation, filter pattern, or business rule. Use example_question_sqls \
    only when the fix requires demonstrating a multi-step SQL pattern that \
    cannot be captured in a snippet.
Valid serialized_space schema (use ONLY these field names):

  data_sources.tables[]: {identifier, description, column_configs}
  column_configs[]: {column_name, description, synonyms, \
    enable_entity_matching, enable_format_assistance, exclude}
  instructions.text_instructions[]: {id, content}
  instructions.example_question_sqls[]: {id, question, sql, \
    parameters[{name, type_hint}], usage_guidance}
  instructions.join_specs[]: {id, left{identifier,alias}, \
    right{identifier,alias}, sql, comment, instruction}
  instructions.sql_functions[]: {id, identifier}
  instructions.sql_snippets.filters[]: {id, sql, display_name, \
    synonyms, comment, instruction}
  instructions.sql_snippets.expressions[]: {id, alias, sql, \
    display_name, synonyms, comment, instruction}
  instructions.sql_snippets.measures[]: {id, alias, sql, \
    display_name, synonyms, comment, instruction}
"""


def _build_client() -> OpenAI:
    """Build an OpenAI-compatible client authenticated against the Databricks workspace."""
    ws = WorkspaceClient()
    token = ws.config.authenticate().get("Authorization", "").removeprefix("Bearer ")
    return OpenAI(
        base_url=f"{ws.config.host.rstrip('/')}/serving-endpoints",
        api_key=token,
    )


def _call_llm(client: OpenAI, llm_endpoint: str, user_message: str) -> dict | list:
    """Send a chat completion request and return the parsed JSON response."""
    response = client.chat.completions.create(
        model=llm_endpoint,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_message},
        ],
        temperature=0,
        max_tokens=20000,
    )
    content = response.choices[0].message.content.strip()
    # Strip markdown fences if the model wraps the JSON
    if content.startswith("```"):
        content = content.split("\n", 1)[1].rsplit("```", 1)[0].strip()
    try:
        return json.loads(content)
    except json.JSONDecodeError as e:
        raise ValueError(f"LLM returned invalid JSON: {content[:500]}") from e


MAX_OVERLAP_RETRIES = 2


def _build_overlap_correction_prompt(result: dict, violations: list[dict]) -> str:
    """Build a correction prompt describing benchmark overlap violations."""
    lines = [
        "Your previous output has example_question_sqls that are too similar to "
        "benchmark questions. This causes overfitting — examples must teach "
        "generalizable SQL patterns, not echo specific benchmark questions.",
        "",
        "Violations found:",
    ]
    for v in violations:
        if v["type"] == "question_overlap":
            lines.append(
                f"- example_question_sqls (id={v['example_id']}): question text "
                f"has {v['similarity']:.0%} Jaccard similarity with benchmark "
                f"question #{v['benchmark_idx']}. "
                f'Example: "{v["example_text"]}" vs '
                f'Benchmark: "{v["benchmark_text"]}"'
            )
        elif v["type"] == "sql_overlap":
            lines.append(
                f"- example_question_sqls (id={v['example_id']}): SQL has "
                f"{v['similarity']:.0%} Jaccard similarity with benchmark "
                f"question #{v['benchmark_idx']} SQL."
            )

    lines.extend([
        "",
        "For each violation, generalize the example:",
        "- Replace specific values/entities with generic placeholders "
        "(e.g., 'Q1 2024 sales by region' -> '[metric] by [dimension] for a time period')",
        "- Keep the SQL pattern but use different column/table references if possible",
        "- Ensure the example teaches the reusable SQL technique, not the specific answer",
        "",
        "Here is the current config to fix:",
        "```json",
        json.dumps(
            {k: v for k, v in result.items() if k != "benchmarks"},
            indent=2,
            ensure_ascii=False,
        ),
        "```",
        "",
        "Return the complete, corrected serialized_space JSON.",
    ])
    return "\n".join(lines)


@mlflow.trace(name="optimize_config", span_type=SpanType.CHAIN)
def optimize_config(
    serialized_space: dict,
    fix_report: str,
    llm_endpoint: str = "databricks-claude-sonnet-4-6",
) -> dict:
    """Use an LLM to produce an optimized serialized_space incorporating fixes.

    Args:
        serialized_space: The current Genie Space configuration.
        fix_report: Prescriptive fix report from compile_fix_report().
        llm_endpoint: Databricks Foundation Model endpoint name.

    Returns:
        The updated serialized_space dict with version and benchmarks preserved.
    """
    client = _build_client()

    # Strip benchmarks from what the LLM sees to prevent overfitting
    config_for_llm = {k: v for k, v in serialized_space.items() if k != "benchmarks"}
    config_json = json.dumps(config_for_llm, indent=2, ensure_ascii=False)

    user_message = (
        f"Current serialized_space:\n```json\n{config_json}\n```\n\n"
        f"Prescriptive fix report:\n{fix_report}\n\n"
        "Produce the updated serialized_space JSON incorporating all applicable fixes."
    )

    result = _call_llm(client, llm_endpoint, user_message)

    # Hard restore of invariants that must never be LLM-modified
    result["version"] = serialized_space["version"]
    result["benchmarks"] = serialized_space["benchmarks"]

    # Retry loop: check for benchmark overlap and ask LLM to self-fix
    for attempt in range(1, MAX_OVERLAP_RETRIES + 1):
        overlap = validate_no_benchmark_overlap(result)
        if not overlap["has_overlap"]:
            break

        violation_count = len(overlap["violations"])
        logger.warning(
            "Overlap check failed (attempt %d/%d): %d violation(s) found",
            attempt,
            MAX_OVERLAP_RETRIES,
            violation_count,
        )

        correction_prompt = _build_overlap_correction_prompt(
            result, overlap["violations"]
        )
        result = _call_llm(client, llm_endpoint, correction_prompt)

        # Restore invariants again after each retry
        result["version"] = serialized_space["version"]
        result["benchmarks"] = serialized_space["benchmarks"]
    else:
        # Exhausted retries — check one final time
        final_overlap = validate_no_benchmark_overlap(result)
        if final_overlap["has_overlap"]:
            logger.warning(
                "Overlap violations persist after %d retries; "
                "returning result as-is (step 10 will report warnings)",
                MAX_OVERLAP_RETRIES,
            )

    return result

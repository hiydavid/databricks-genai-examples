"""
LLM-powered Genie Space config optimization.

Makes up to 3 sequential LLM calls — one per config section — to produce an updated
configuration. Each call receives the FULL prescriptive fix report for cross-section
context, but only returns its own section's JSON to keep output tokens small.

Section mapping:
  Call 1 (UC Metadata)   → data_sources
  Call 2 (SQL Examples)  → instructions.example_question_sqls
  Call 3 (Instructions)  → remaining instructions + config.sample_questions
"""

import copy
import json

import mlflow
from databricks.sdk import WorkspaceClient
from mlflow.entities import SpanType
from openai import OpenAI

mlflow.openai.autolog()

SYSTEM_PROMPT = """\
You are a Databricks Genie Space configuration optimizer. You receive a \
serialized_space JSON (or a section of it) and a prescriptive fix report compiled \
from benchmark evaluation results. Your job is to produce an updated version of \
the JSON you are asked to modify that incorporates all applicable fixes.

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
13. Return ONLY the valid JSON for the section you are asked to modify — no markdown, \
    no explanation, no wrapping.
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


@mlflow.trace(name="optimize_data_sources", span_type=SpanType.CHAIN)
def _optimize_data_sources(
    data_sources: dict,
    fix_report: str,
    client: OpenAI,
    llm_endpoint: str,
) -> dict:
    """Call 1: Update data_sources (tables + metric_views) per UC Metadata fixes."""
    user_message = (
        'You are modifying ONLY the "data_sources" section of a Genie Space config.\n\n'
        f"Current data_sources:\n```json\n{json.dumps(data_sources, indent=2, ensure_ascii=False)}\n```\n\n"
        f"Prescriptive fix report (apply UC Metadata fixes to data_sources; "
        f"other sections listed for context):\n{fix_report}\n\n"
        "Return ONLY the updated data_sources JSON object "
        "(with keys tables and/or metric_views) — no other top-level keys."
    )
    result = _call_llm(client, llm_endpoint, user_message)
    # Defensive unwrap: if the LLM returned {"data_sources": {...}} instead of {...}
    if isinstance(result, dict) and list(result.keys()) == ["data_sources"]:
        result = result["data_sources"]
    return result


@mlflow.trace(name="optimize_sql_examples", span_type=SpanType.CHAIN)
def _optimize_sql_examples(
    sql_examples: list,
    data_sources: dict,
    fix_report: str,
    client: OpenAI,
    llm_endpoint: str,
) -> list:
    """Call 2: Update instructions.example_question_sqls per SQL Examples fixes.

    Passes data_sources as read-only context so the LLM can write correct SQL
    without modifying it.
    """
    user_message = (
        'You are modifying ONLY the "instructions.example_question_sqls" list '
        "of a Genie Space config.\n\n"
        "Read-only context — DO NOT modify, provided for writing correct SQL only:\n"
        f"```json\n{json.dumps({'data_sources': data_sources}, indent=2, ensure_ascii=False)}\n```\n\n"
        f"Current example_question_sqls:\n```json\n{json.dumps(sql_examples, indent=2, ensure_ascii=False)}\n```\n\n"
        f"Prescriptive fix report (apply SQL Examples fixes to example_question_sqls; "
        f"other sections listed for context):\n{fix_report}\n\n"
        "Return ONLY the updated example_question_sqls JSON array — no wrapping object."
    )
    result = _call_llm(client, llm_endpoint, user_message)
    # Defensive unwrap: if the LLM returned {"example_question_sqls": [...]}
    if isinstance(result, dict) and "example_question_sqls" in result:
        result = result["example_question_sqls"]
    return result


@mlflow.trace(name="optimize_instructions", span_type=SpanType.CHAIN)
def _optimize_instructions_rest(
    instructions_rest: dict,
    sample_questions: list,
    data_sources: dict,
    example_question_sqls: list,
    fix_report: str,
    client: OpenAI,
    llm_endpoint: str,
) -> tuple[dict, list]:
    """Call 3: Update remaining instructions + config.sample_questions per Instructions fixes.

    instructions_rest contains all instructions keys except example_question_sqls
    (text_instructions, join_specs, sql_snippets, sql_functions), which was
    already handled in Call 2.

    Passes data_sources and example_question_sqls as read-only context so the LLM
    can write correct SQL and coordinate with Call 2's output.
    """
    payload = {
        "instructions_rest": instructions_rest,
        "sample_questions": sample_questions,
    }
    read_only_context = {
        "data_sources": data_sources,
        "example_question_sqls": example_question_sqls,
    }
    user_message = (
        "You are modifying the remaining instructions fields "
        "(text_instructions, join_specs, sql_snippets, sql_functions) "
        'and "config.sample_questions" of a Genie Space config. '
        "Do NOT touch example_question_sqls — it was already updated.\n\n"
        "Read-only context — DO NOT modify, provided for writing correct SQL only:\n"
        f"```json\n{json.dumps(read_only_context, indent=2, ensure_ascii=False)}\n```\n\n"
        f"Current sections to update:\n```json\n{json.dumps(payload, indent=2, ensure_ascii=False)}\n```\n\n"
        f"Prescriptive fix report (apply Instructions fixes to these sections; "
        f"other sections listed for context):\n{fix_report}\n\n"
        'Return ONLY a JSON object with exactly two keys: "instructions_rest" and "sample_questions".'
    )
    result = _call_llm(client, llm_endpoint, user_message)
    updated_rest = result.get("instructions_rest", instructions_rest)
    updated_samples = result.get("sample_questions", sample_questions)
    return updated_rest, updated_samples


@mlflow.trace(name="optimize_config", span_type=SpanType.CHAIN)
def optimize_config(
    serialized_space: dict,
    fix_report: str,
    llm_endpoint: str = "databricks-claude-sonnet-4-6",
) -> dict:
    """Sequentially optimize each config section using targeted per-section LLM calls.

    Makes up to 3 LLM calls, one per fix category. Each call receives the FULL
    prescriptive fix report for cross-section context, but only returns its own
    section's JSON. Preserves version and benchmarks unchanged.

    Args:
        serialized_space: The current Genie Space configuration.
        fix_report: Full prescriptive fix report from compile_fix_report().
        llm_endpoint: Databricks Foundation Model endpoint name.

    Returns:
        The updated serialized_space dict with version and benchmarks preserved.
    """
    client = _build_client()
    result = copy.deepcopy(serialized_space)

    # --- Call 1: UC Metadata → data_sources ---
    print("  [1/3] Optimizing data_sources (UC Metadata)...")
    result["data_sources"] = _optimize_data_sources(
        result["data_sources"],
        fix_report,
        client,
        llm_endpoint,
    )
    print("  [1/3] Done.")

    # --- Call 2: SQL Examples → instructions.example_question_sqls ---
    # Pass the (potentially just-updated) data_sources as read-only context.
    print("  [2/3] Optimizing example_question_sqls (SQL Examples)...")
    result["instructions"]["example_question_sqls"] = _optimize_sql_examples(
        result["instructions"].get("example_question_sqls", []),
        result["data_sources"],
        fix_report,
        client,
        llm_endpoint,
    )
    print("  [2/3] Done.")

    # --- Call 3: Instructions → remaining instructions + config.sample_questions ---
    # Pass both data_sources and updated example_question_sqls as read-only context.
    print("  [3/3] Optimizing instructions + sample_questions (Instructions)...")
    instructions_rest = {
        k: v
        for k, v in result["instructions"].items()
        if k != "example_question_sqls"
    }
    sample_questions = result.get("config", {}).get("sample_questions", [])
    updated_rest, updated_samples = _optimize_instructions_rest(
        instructions_rest,
        sample_questions,
        result["data_sources"],
        result["instructions"].get("example_question_sqls", []),
        fix_report,
        client,
        llm_endpoint,
    )
    # Only merge back expected keys — prevent the LLM from injecting
    # example_question_sqls or other unexpected keys via .update().
    _EXPECTED_REST_KEYS = {"text_instructions", "join_specs", "sql_snippets", "sql_functions"}
    result["instructions"].update(
        {k: v for k, v in updated_rest.items() if k in _EXPECTED_REST_KEYS}
    )
    result.setdefault("config", {})["sample_questions"] = updated_samples
    print("  [3/3] Done.")

    # Hard restore of invariants that must never be LLM-modified
    result["version"] = serialized_space["version"]
    result["benchmarks"] = serialized_space["benchmarks"]

    return result

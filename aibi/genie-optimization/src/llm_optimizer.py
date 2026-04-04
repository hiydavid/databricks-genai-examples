"""
LLM-powered Genie Space config optimization.

Takes the current serialized_space and a prescriptive fix report, calls an LLM
to produce an updated configuration that incorporates the fixes.
"""

import json

from databricks.sdk import WorkspaceClient
from openai import OpenAI

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
    [join_condition_string, relationship_type_annotation].
11. Do not modify the "benchmarks" section.
12. These fields MUST always be arrays of strings, never bare strings: \
    description, question, content, sql, synonyms, comment, instruction, \
    usage_guidance. For example: "description": ["Some text"], not \
    "description": "Some text".
13. Return ONLY the complete, valid serialized_space JSON object — no markdown, \
    no explanation, no wrapping.
"""


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
        The updated serialized_space dict.
    """
    ws = WorkspaceClient()
    token = ws.config.authenticate().get("Authorization", "").removeprefix("Bearer ")
    client = OpenAI(
        base_url=f"{ws.config.host.rstrip('/')}/serving-endpoints",
        api_key=token,
    )

    config_json = json.dumps(serialized_space, indent=2, ensure_ascii=False)

    user_message = (
        f"Current serialized_space:\n```json\n{config_json}\n```\n\n"
        f"Prescriptive fix report:\n{fix_report}\n\n"
        "Produce the updated serialized_space JSON incorporating all applicable fixes."
    )

    response = client.chat.completions.create(
        model=llm_endpoint,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_message},
        ],
        temperature=0,
        max_tokens=16384,
    )

    content = response.choices[0].message.content.strip()
    # Strip markdown fences if the model wraps the JSON
    if content.startswith("```"):
        content = content.split("\n", 1)[1].rsplit("```", 1)[0].strip()
    return json.loads(content)

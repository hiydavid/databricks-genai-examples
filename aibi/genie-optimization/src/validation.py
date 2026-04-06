"""
Validation and normalization for Genie Space serialized_space configurations.
"""

import copy
import re
from collections import Counter
from typing import Any

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


def _column_configs(table: dict) -> list[dict]:
    if isinstance(table.get("column_configs"), list):
        return table["column_configs"]
    if isinstance(table.get("columns"), list):
        return table["columns"]
    return []


# =============================================================================
# Normalization
# =============================================================================


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
            if all("column_name" in v for v in normalized):
                return sorted(
                    normalized,
                    key=lambda v: str(v.get("column_name", "")),
                )
        return normalized

    return value


# Fields the Genie API requires as arrays of strings, per
# https://docs.databricks.com/aws/en/genie/conversation-api#understanding-the-serialized_space-field
_ARRAY_FIELDS = {
    "description",
    "question",
    "content",
    "sql",
    "synonyms",
    "comment",
    "instruction",
    "usage_guidance",
}


def _wrap_string_fields(value: Any) -> Any:
    """Coerce bare-string fields to single-element arrays where the API expects arrays.

    The Genie API expects certain fields (description, question, sql, etc.) as
    arrays of strings, but LLMs often emit plain strings.
    """
    if isinstance(value, dict):
        result = {}
        for k, v in value.items():
            if k in _ARRAY_FIELDS and isinstance(v, str):
                result[k] = [v]
            else:
                result[k] = _wrap_string_fields(v)
        return result
    if isinstance(value, list):
        return [_wrap_string_fields(item) for item in value]
    return value


def _fix_snippet_fields(serialized_space: dict) -> dict:
    """Fix common LLM mistakes in sql_snippets: wrong field names and wrapped scalars."""
    snippets = serialized_space.get("instructions", {}).get("sql_snippets", {})
    for category in ("filters", "expressions", "measures"):
        for item in snippets.get(category, []):
            # Rename 'name' → 'display_name'
            if "name" in item and "display_name" not in item:
                item["display_name"] = item.pop("name")
            # Unwrap scalar fields that the API expects as plain strings
            for field in ("display_name", "alias"):
                val = item.get(field)
                if isinstance(val, list) and len(val) == 1:
                    item[field] = val[0]
    return serialized_space


def normalize_serialized_space(serialized_space: dict) -> dict:
    """Normalize ordering and field formats for serialized_space."""
    normalized = copy.deepcopy(serialized_space)
    normalized = _fix_snippet_fields(normalized)
    normalized = _wrap_string_fields(normalized)
    return _normalize_serialized_space_recursive(normalized)


# =============================================================================
# Validation
# =============================================================================


def _tokenize(text: str) -> set[str]:
    """Lowercase and split into word tokens for similarity comparison."""
    return set(re.findall(r"\b\w+\b", text.lower()))


def _jaccard_similarity(a: str, b: str) -> float:
    """Compute Jaccard similarity between two text strings."""
    tokens_a = _tokenize(a)
    tokens_b = _tokenize(b)
    if not tokens_a or not tokens_b:
        return 0.0
    return len(tokens_a & tokens_b) / len(tokens_a | tokens_b)


OVERLAP_SIMILARITY_THRESHOLD = 0.9


def validate_no_benchmark_overlap(serialized_space: dict) -> dict:
    """Check that example_question_sqls don't near-exactly copy benchmark questions.

    Returns dict with 'has_overlap' (bool) and 'violations' (list of dicts).
    """
    benchmarks = serialized_space.get("benchmarks", {}).get("questions", [])
    examples = serialized_space.get("instructions", {}).get("example_question_sqls", [])

    if not benchmarks or not examples:
        return {"has_overlap": False, "violations": []}

    benchmark_texts = [
        (_text_from_value(bq.get("question")), _text_from_value(bq.get("sql")))
        for bq in benchmarks
    ]

    violations = []
    for ex_idx, example in enumerate(examples):
        ex_question = _text_from_value(example.get("question"))
        ex_sql = _text_from_value(example.get("sql"))

        for bq_idx, (bq_text, bq_sql) in enumerate(benchmark_texts):
            q_sim = _jaccard_similarity(ex_question, bq_text)
            if q_sim >= OVERLAP_SIMILARITY_THRESHOLD:
                violations.append({
                    "example_idx": ex_idx,
                    "example_id": example.get("id", "?"),
                    "benchmark_idx": bq_idx,
                    "type": "question_overlap",
                    "similarity": round(q_sim, 3),
                    "example_text": ex_question[:80],
                    "benchmark_text": bq_text[:80],
                })

            sql_sim = _jaccard_similarity(ex_sql, bq_sql) if ex_sql and bq_sql else 0.0
            if sql_sim >= OVERLAP_SIMILARITY_THRESHOLD:
                violations.append({
                    "example_idx": ex_idx,
                    "example_id": example.get("id", "?"),
                    "benchmark_idx": bq_idx,
                    "type": "sql_overlap",
                    "similarity": round(sql_sim, 3),
                })

    return {"has_overlap": len(violations) > 0, "violations": violations}


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
                elif all("column_name" in v for v in value):
                    sort_key = "column_name"

                if sort_key:
                    current = [str(v.get(sort_key, "")) for v in value]
                    if current != sorted(current):
                        errors.append(f"{path} must be sorted by '{sort_key}'")
        elif isinstance(value, str):
            if len(value) > 25000:
                errors.append(f"{path} exceeds max string length 25000")

    walk(serialized_space, "")

    # Version check
    version = int(serialized_space.get("version", 1) or 1)
    tables = serialized_space.get("data_sources", {}).get("tables", [])

    # v2 column config restrictions
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

    # Table identifier format: three-level namespace
    for t_idx, table in enumerate(tables):
        identifier = table.get("identifier", "")
        if identifier and len(identifier.split(".")) != 3:
            errors.append(
                f"data_sources.tables[{t_idx}].identifier '{identifier}' "
                "must be three-level namespace (catalog.schema.table)"
            )

    # Text instructions: max 1 allowed
    text_instructions = serialized_space.get("instructions", {}).get("text_instructions", [])
    if len(text_instructions) > 1:
        errors.append(
            f"instructions.text_instructions has {len(text_instructions)} entries; max 1 allowed"
        )

    # Question ID uniqueness
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
    id_counts = Counter(all_question_ids)
    duplicate_ids = sorted(id_ for id_, cnt in id_counts.items() if cnt > 1)
    if duplicate_ids:
        errors.append(
            "Question IDs must be unique across config.sample_questions and benchmarks.questions: "
            + ", ".join(duplicate_ids[:5])
        )

    # Join specs validation
    join_specs = serialized_space.get("instructions", {}).get("join_specs", [])
    for idx, spec in enumerate(join_specs):
        sql_elements = _as_list(spec.get("sql"))
        if len(sql_elements) != 2:
            errors.append(
                f"instructions.join_specs[{idx}].sql must have exactly 2 elements "
                f"(join condition + relationship type), found {len(sql_elements)}"
            )
        for clause_idx, clause in enumerate(sql_elements):
            clause_text = _text_from_value(clause)
            if not clause_text:
                warnings.append(f"instructions.join_specs[{idx}].sql[{clause_idx}] is empty")
                continue

            if clause_idx == 0:
                if re.search(r"\b(and|or)\b", clause_text, re.IGNORECASE):
                    errors.append(
                        f"instructions.join_specs[{idx}].sql[{clause_idx}] contains AND/OR; "
                        "use one equality expression per element"
                    )
                if clause_text.count("=") != 1:
                    warnings.append(
                        f"instructions.join_specs[{idx}].sql[{clause_idx}] "
                        "should contain exactly one '=' expression"
                    )

    # Instruction ID uniqueness across all instruction types
    instruction_ids = []
    instr = serialized_space.get("instructions", {})
    for collection_name in (
        "text_instructions", "example_question_sqls", "sql_functions", "join_specs",
    ):
        for item in instr.get(collection_name, []):
            if isinstance(item, dict) and item.get("id"):
                instruction_ids.append(item["id"])
    for snippet_cat in ("filters", "expressions", "measures"):
        for item in instr.get("sql_snippets", {}).get(snippet_cat, []):
            if isinstance(item, dict) and item.get("id"):
                instruction_ids.append(item["id"])
    instr_id_counts = Counter(instruction_ids)
    dup_instr_ids = sorted(id_ for id_, cnt in instr_id_counts.items() if cnt > 1)
    if dup_instr_ids:
        errors.append(
            "Instruction IDs must be unique across all instruction types: "
            + ", ".join(dup_instr_ids[:5])
        )

    # Column config uniqueness: (table_identifier, column_name) must be unique
    col_keys: list[str] = []
    for table in tables:
        table_id = table.get("identifier", "")
        for col in _column_configs(table):
            col_name = col.get("column_name", "")
            if col_name:
                col_keys.append(f"{table_id}.{col_name}")
    col_key_counts = Counter(col_keys)
    dup_cols = sorted(k for k, cnt in col_key_counts.items() if cnt > 1)
    if dup_cols:
        errors.append(
            "Column configs must be unique per (table, column_name): "
            + ", ".join(dup_cols[:5])
        )

    # Join spec relationship type annotation format
    _VALID_RT = {
        "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_ONE--",
        "--rt=FROM_RELATIONSHIP_TYPE_ONE_TO_MANY--",
        "--rt=FROM_RELATIONSHIP_TYPE_ONE_TO_ONE--",
        "--rt=FROM_RELATIONSHIP_TYPE_MANY_TO_MANY--",
    }
    for idx, spec in enumerate(join_specs):
        sql_elements = _as_list(spec.get("sql"))
        if len(sql_elements) >= 2:
            rt = _text_from_value(sql_elements[1])
            if rt and rt not in _VALID_RT:
                errors.append(
                    f"instructions.join_specs[{idx}].sql[1] has invalid relationship type "
                    f"'{rt}'; must be one of: {', '.join(sorted(_VALID_RT))}"
                )

    # SQL snippet sql fields must not be empty
    for snippet_cat in ("filters", "expressions", "measures"):
        for s_idx, item in enumerate(instr.get("sql_snippets", {}).get(snippet_cat, [])):
            sql_text = _text_from_value(item.get("sql"))
            if not sql_text:
                errors.append(
                    f"instructions.sql_snippets.{snippet_cat}[{s_idx}].sql must not be empty"
                )

    # Benchmark answer format validation
    for b_idx, bq in enumerate(
        serialized_space.get("benchmarks", {}).get("questions", [])
    ):
        answers = bq.get("answer", [])
        if len(answers) != 1:
            errors.append(
                f"benchmarks.questions[{b_idx}] must have exactly 1 answer, "
                f"found {len(answers)}"
            )
        for a_idx, ans in enumerate(answers):
            if ans.get("format") != "SQL":
                errors.append(
                    f"benchmarks.questions[{b_idx}].answer[{a_idx}].format "
                    f"must be 'SQL', found '{ans.get('format')}'"
                )

    # Benchmark overlap check
    overlap = validate_no_benchmark_overlap(serialized_space)
    if overlap["has_overlap"]:
        for v in overlap["violations"]:
            if v["type"] == "question_overlap":
                warnings.append(
                    f"instructions.example_question_sqls[{v['example_idx']}] "
                    f"question has {v['similarity']:.0%} similarity with benchmark "
                    f"question #{v['benchmark_idx']} — possible overfitting. "
                    f'Example: "{v["example_text"]}" vs '
                    f'Benchmark: "{v["benchmark_text"]}"'
                )
            elif v["type"] == "sql_overlap":
                warnings.append(
                    f"instructions.example_question_sqls[{v['example_idx']}] "
                    f"SQL has {v['similarity']:.0%} similarity with benchmark "
                    f"question #{v['benchmark_idx']} SQL — possible overfitting"
                )

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
    }

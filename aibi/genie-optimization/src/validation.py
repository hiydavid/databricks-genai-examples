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


def normalize_serialized_space(serialized_space: dict) -> dict:
    """Normalize ordering and field formats for serialized_space."""
    normalized = copy.deepcopy(serialized_space)
    normalized = _wrap_string_fields(normalized)
    return _normalize_serialized_space_recursive(normalized)


# =============================================================================
# Validation
# =============================================================================


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

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
    }

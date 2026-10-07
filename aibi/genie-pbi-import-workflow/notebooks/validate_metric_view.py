# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# dependencies = [
#   "databricks-sdk>=0.102.0",
#   "pyyaml",
# ]
# ///
# DBTITLE 1,Validate Metric View
# MAGIC %md
# MAGIC # Validate Metric View — Genie PBI Import Workflow
# MAGIC
# MAGIC Deterministic check of what `import_metric_view` (Genie Code) produced: the object exists and is a metric view, every source and join table it references exists, and every measure returns a result. Writes a `validation` artifact, then fails the task if any check failed so the Genie Agent is not built on a broken metric view.

# COMMAND ----------

# DBTITLE 1,Parameters
import json

dbutils.widgets.text("run_id", "")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("pbit_filename", "")
dbutils.widgets.text("metric_view_name", "")

run_id = dbutils.widgets.get("run_id").strip()
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
pbit_filename = dbutils.widgets.get("pbit_filename").strip()
metric_view_name = dbutils.widgets.get("metric_view_name").strip()

if not all((catalog, schema, pbit_filename)):
    dbutils.notebook.exit(json.dumps({"status": "DRY_RUN", "run_id": run_id}))
if not run_id:
    raise ValueError("run_id is required for a configured run; use the job run ID")
if not metric_view_name:
    raise ValueError("metric_view_name is required")


def quote(*parts):
    return ".".join(f"`{p.replace('`', '``')}`" for p in parts)


metric_view_fqn = f"{catalog}.{schema}.{metric_view_name}"
metric_view_sql = quote(catalog, schema, metric_view_name)
artifacts_table = quote(catalog, schema, "genie_pbi_import_workflow_artifacts")

print("=" * 60)
print("[TASK VALIDATE] Validate Metric View")
print("=" * 60)
print(f"  run_id:      {run_id}")
print(f"  metric view: {metric_view_fqn}")

# COMMAND ----------

# DBTITLE 1,Check the object is a metric view and parse its definition
import yaml

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
errors = []
definition = {}
exists = is_metric_view = False

try:
    info = w.tables.get(full_name=metric_view_fqn)
    exists = True
    is_metric_view = info.table_type is not None and info.table_type.value == "METRIC_VIEW"
    if not is_metric_view:
        errors.append(f"{metric_view_fqn} is a {info.table_type.value if info.table_type else 'unknown'} object, not a METRIC_VIEW")
    else:
        text = info.view_definition or ""
        # Tolerate a definition returned with its CREATE ... AS $$ ... $$ wrapper.
        if "$$" in text:
            text = text.split("$$")[1]
        definition = yaml.safe_load(text)
        if not isinstance(definition, dict):
            raise ValueError("metric view definition is not a YAML mapping")
except Exception as e:
    errors.append(f"Could not read metric view {metric_view_fqn}: {e}")
    definition = {}

dimensions = [d for d in definition.get("dimensions") or [] if isinstance(d, dict)]
measures = [m for m in definition.get("measures") or [] if isinstance(m, dict)]
if is_metric_view and definition and not measures:
    errors.append("Metric view defines no measures")
print(f"  exists: {exists}; is_metric_view: {is_metric_view}; "
      f"{len(dimensions)} dimensions, {len(measures)} measures")

# COMMAND ----------

# DBTITLE 1,Check the source and join tables exist
# `source` is either a table name or a SQL query; only table names are checked here.
# A query source is exercised by the smoke tests below.


def table_sources(node, role):
    source = node.get("source")
    if isinstance(source, str) and source.strip() and not source.strip().upper().startswith(("SELECT", "WITH")):
        yield source.strip(), role
    for join in node.get("joins") or []:
        if isinstance(join, dict):
            yield from table_sources(join, f"join:{join.get('name', '?')}")


def split_name(name):
    """Split a SQL name on dots outside backticks, so `my.schema` stays one part."""
    parts, current, quoted, i = [], "", False, 0
    while i < len(name):
        ch = name[i]
        if ch == "`":
            if quoted and name[i + 1:i + 2] == "`":  # `` is an escaped backtick
                current += "`"
                i += 1
            else:
                quoted = not quoted
        elif ch == "." and not quoted:
            parts.append(current.strip())
            current = ""
        else:
            current += ch
        i += 1
    parts.append(current.strip())
    return parts


source_tables = []
for table, role in table_sources(definition, "source"):
    parts = split_name(table)
    if len(parts) > 3 or not all(parts):
        source_tables.append({"table": table, "role": role, "exists": False,
                              "error": "not a valid [catalog.][schema.]table name"})
        continue
    # Unqualified names resolve against the metric view's own catalog and schema.
    full_name = ".".join([catalog, schema][:3 - len(parts)] + parts)
    result = {"table": full_name, "role": role}
    try:
        result["exists"] = bool(w.tables.exists(full_name=full_name).table_exists)
    except Exception as e:
        result["exists"] = False
        result["error"] = str(e)[:200]
    source_tables.append(result)

missing_sources = [s for s in source_tables if not s["exists"]]
for s in missing_sources:
    errors.append(f"Source table {s['table']} ({s['role']}) not found" + (f": {s['error']}" if "error" in s else ""))
print(f"  source tables: {len(source_tables)} checked, {len(missing_sources)} missing")

# COMMAND ----------

# DBTITLE 1,Smoke-test every measure
# One query per measure, so a single untranslatable DAX expression is reported by name
# instead of failing one combined query.
smoke_tests = []
for m in measures:
    name = str(m.get("name", ""))
    try:
        spark.sql(f"SELECT MEASURE({quote(name)}) AS value FROM {metric_view_sql}").collect()
        smoke_tests.append({"measure": name, "status": "PASS"})
    except Exception as e:
        smoke_tests.append({"measure": name, "status": "FAIL", "error": str(e)[:300]})

# And one grouped query, to check the dimensions join and group correctly.
if measures and dimensions:
    dim, measure = str(dimensions[0].get("name", "")), str(measures[0].get("name", ""))
    try:
        rows = spark.sql(f"""
            SELECT {quote(dim)}, MEASURE({quote(measure)}) AS value
            FROM {metric_view_sql}
            GROUP BY ALL
            LIMIT 5
        """).collect()
        smoke_tests.append({"dimension": dim, "measure": measure, "status": "PASS", "rows": len(rows)})
    except Exception as e:
        smoke_tests.append({"dimension": dim, "measure": measure, "status": "FAIL", "error": str(e)[:300]})

failed_tests = [t for t in smoke_tests if t["status"] != "PASS"]
for t in failed_tests:
    label = f"{t['dimension']} / {t['measure']}" if "dimension" in t else t["measure"]
    errors.append(f"Smoke test failed for {label}: {t['error']}")
print(f"  smoke tests: {len(smoke_tests)} run, {len(failed_tests)} failed")

# COMMAND ----------

# DBTITLE 1,Write the validation artifact
overall_status = "FAIL" if errors else "PASS"
validation = {
    "metric_view_fqn": metric_view_fqn,
    "exists": exists,
    "is_metric_view": is_metric_view,
    "dimension_count": len(dimensions),
    "measure_count": len(measures),
    # display_name keeps the original Power BI label when /importBI renames a measure.
    "dimensions": [{"name": d.get("name"), "display_name": d.get("display_name")} for d in dimensions],
    "measures": [{"name": m.get("name"), "display_name": m.get("display_name")} for m in measures],
    "source_table_validation": {
        "status": "FAIL" if missing_sources else "PASS",
        "tables_checked": len(source_tables),
        "tables_missing": len(missing_sources),
        "details": source_tables,
    },
    "smoke_tests": smoke_tests,
    "overall_status": overall_status,
    "errors": errors,
}
spark.sql(f"""
    INSERT INTO {artifacts_table} (run_id, task_name, artifact_type, payload, created_at)
    VALUES (:run_id, :task_name, :artifact_type, :payload, current_timestamp())
""", args={
    "run_id": run_id,
    "task_name": "validate_metric_view",
    "artifact_type": "validation",
    "payload": json.dumps(validation),
})
print(f"\n  ✓ Wrote validation artifact to {artifacts_table}")

# COMMAND ----------

# DBTITLE 1,Exit
print("\n" + "=" * 60)
print(f"[TASK VALIDATE] {overall_status}")
print("=" * 60)

# Failing the task stops create_genie_agent; the audit still runs (run_if: ALL_DONE).
if errors:
    raise RuntimeError("Metric view validation failed: " + "; ".join(errors))
exit_payload = json.dumps({"status": "SUCCESS", "run_id": run_id, "overall_status": overall_status})
print(f"\nExiting with: {exit_payload}")
dbutils.notebook.exit(exit_payload)

# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# dependencies = [
#   "databricks-sdk>=0.102.0",
# ]
# ///
# DBTITLE 1,Audit & Report
# MAGIC %md
# MAGIC # Audit & Report — Genie PBI Import Workflow
# MAGIC
# MAGIC Final task in the DAG (`run_if: ALL_DONE`). Reads all artifacts from the run, checks the required handoffs, confirms the Genie Agent reported by `create_genie_agent` exists and uses the metric view, compares the Power BI measures with what was created, and writes an `audit_summary` artifact.

# COMMAND ----------

# DBTITLE 1,Parameters
import json

dbutils.widgets.text("run_id", "")
dbutils.widgets.text("metric_view_catalog", "")
dbutils.widgets.text("metric_view_schema", "")
dbutils.widgets.text("pbit_filename", "")
dbutils.widgets.text("metric_view_name", "")
dbutils.widgets.text("create_agent", "true")

run_id = dbutils.widgets.get("run_id").strip()
metric_view_catalog = dbutils.widgets.get("metric_view_catalog").strip()
metric_view_schema = dbutils.widgets.get("metric_view_schema").strip()
pbit_filename = dbutils.widgets.get("pbit_filename").strip()
metric_view_name = dbutils.widgets.get("metric_view_name").strip()
create_agent = dbutils.widgets.get("create_agent").strip() == "true"

if not all((metric_view_catalog, metric_view_schema, pbit_filename)):
    dbutils.notebook.exit(json.dumps({"status": "DRY_RUN", "run_id": run_id}))
if not run_id:
    raise ValueError("run_id is required for a configured run; use the job run ID")

metric_view_fqn = f"{metric_view_catalog}.{metric_view_schema}.{metric_view_name}"


def normalize_name(name):
    """Compare UC names the way UC does: case-insensitive, with or without backticks."""
    return str(name or "").replace("`", "").strip().lower()


artifacts_table = ".".join(
    f"`{part.replace('`', '``')}`"
    for part in (metric_view_catalog, metric_view_schema, "genie_pbi_import_workflow_artifacts")
)

print("=" * 60)
print("[TASK AUDIT] Audit & Report")
print("=" * 60)
print(f"  run_id:       {run_id}")
print(f"  metric view:  {metric_view_fqn}")
print(f"  create_agent: {create_agent}")

# COMMAND ----------

# DBTITLE 1,Load all artifacts for this run
artifacts = {}
parse_errors = {}

# Setup may have failed before creating the table; the audit still needs somewhere to write.
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {artifacts_table} (
        run_id STRING,
        task_name STRING,
        artifact_type STRING,
        payload STRING,
        created_at TIMESTAMP
    ) USING DELTA
""")
rows = spark.sql(f"""
    SELECT artifact_type, payload, created_at
    FROM {artifacts_table}
    WHERE run_id = :run_id
    ORDER BY created_at
""", args={"run_id": run_id}).collect()

# Later rows win, so a repaired task's artifact replaces its earlier one.
for row in rows:
    artifact_type = row["artifact_type"]
    # A repaired audit must recompute its summary, not reuse its prior output.
    if artifact_type == "audit_summary":
        continue
    try:
        payload = json.loads(row["payload"])
        if not isinstance(payload, dict):
            raise ValueError("payload must be a JSON object")
        artifacts[artifact_type] = payload
        parse_errors.pop(artifact_type, None)
    except (ValueError, TypeError) as e:
        artifacts[artifact_type] = {}
        parse_errors[artifact_type] = f"Invalid {artifact_type} artifact: {e}"

print(f"\n  ✓ Found {len(artifacts)} artifact(s): {', '.join(artifacts) or '(none)'}")

# COMMAND ----------

# DBTITLE 1,Validate the audit inputs
errors = list(parse_errors.values())
for artifact_type in ("setup_config", "import_result", "validation"):
    if artifact_type not in artifacts:
        errors.append(f"Missing required artifact: {artifact_type}")

setup = artifacts.get("setup_config", {})
imported = artifacts.get("import_result", {})
validation = artifacts.get("validation", {})
agent = artifacts.get("agent_result", {})

if setup and setup.get("metric_view_fqn") != metric_view_fqn:
    errors.append("setup_config does not match this run's metric view")
if imported:
    if imported.get("status") not in ("SUCCESS", "PARTIAL"):
        errors.append(f"import_result reports status {imported.get('status')!r}")
    # Written by a Genie Code task, so tolerate backticks and casing.
    if normalize_name(imported.get("metric_view_fqn")) != normalize_name(metric_view_fqn):
        errors.append("import_result names a different metric view")
if validation and validation.get("overall_status") != "PASS":
    errors.append("Metric view validation did not pass")

# COMMAND ----------

# DBTITLE 1,Verify the Genie Agent against the Genie API
# create_genie_agent is a Genie Code task, so its reported space_id is re-read here
# rather than trusted. Skipped when create_agent is false, and when validation did not
# pass: create_genie_agent never runs then, and the validation error is already reported.


def space_data_sources(serialized_space):
    """Return every data source identifier listed under the space's data_sources."""
    def identifiers(node):
        if isinstance(node, dict):
            if isinstance(node.get("identifier"), str):
                yield node["identifier"]
            for value in node.values():
                yield from identifiers(value)
        elif isinstance(node, list):
            for item in node:
                yield from identifiers(item)

    space_json = json.loads(serialized_space or "{}")
    return {normalize_name(i) for i in identifiers(space_json.get("data_sources") or {})}


agent_space_id = None
agent_expected = create_agent and validation.get("overall_status") == "PASS"
if agent_expected:
    if "agent_result" not in artifacts:
        errors.append("Missing required artifact: agent_result (create_agent is true)")
    elif agent.get("status") != "SUCCESS" or agent.get("error"):
        errors.append(f"agent_result reports status {agent.get('status')!r}: {agent.get('error') or 'no error given'}")
    elif not isinstance(agent.get("space_id"), str) or not agent["space_id"].strip():
        errors.append("agent_result has no space_id")
    else:
        try:
            from databricks.sdk import WorkspaceClient

            w = WorkspaceClient()
            space = w.genie.get_space(space_id=agent["space_id"], include_serialized_space=True)
            # The metric view must be the space's only data source, matched by exact name.
            sources = space_data_sources(space.serialized_space)
            if sources != {normalize_name(metric_view_fqn)}:
                errors.append(f"Genie space {agent['space_id']} does not use {metric_view_fqn} as its "
                              f"only data source (found: {', '.join(sorted(sources)) or 'none'})")
            else:
                agent_space_id = agent["space_id"]
        except Exception as e:
            errors.append(f"Could not read Genie space {agent['space_id']}: {e}")

# COMMAND ----------

# DBTITLE 1,Compare Power BI measures with the metric view
# Informational, not an audit error: /importBI may rename measures or skip DAX it cannot
# translate (import_result.measures_not_translated says why). Matches on name or
# display_name, case-insensitively.
created_names = {
    str(label).lower()
    for m in validation.get("measures") or [] if isinstance(m, dict)
    for label in (m.get("name"), m.get("display_name")) if label
}
pbi_measures = [m.get("name") for m in setup.get("pbi_measures") or [] if isinstance(m, dict)]
unmatched_measures = [name for name in pbi_measures if str(name).lower() not in created_names]

# COMMAND ----------

# DBTITLE 1,Compile audit report
print("\n" + "═" * 60)
print("AUDIT REPORT")
print("═" * 60)
print(f"\nPower BI model ({setup.get('pbit_filename', pbit_filename)})")
print(f"  Tables:             {len(setup.get('pbi_tables') or [])}")
print(f"  Measures:           {len(pbi_measures)}")
source_check = setup.get("source_table_validation") or {}
print(f"  UC sources missing: {source_check.get('tables_missing_from_uc', 'n/a')}")
print(f"\nImport ({imported.get('status', 'no artifact')})")
print(f"  Measures created:   {imported.get('measures_created', 'n/a')}")
print(f"  Dimensions created: {imported.get('dimensions_created', 'n/a')}")
if imported.get("notes"):
    print(f"  Notes:              {imported['notes']}")
print(f"\nValidation ({validation.get('overall_status', 'no artifact')})")
print(f"  Measures:           {validation.get('measure_count', 'n/a')}")
print(f"  Dimensions:         {validation.get('dimension_count', 'n/a')}")
if unmatched_measures:
    print(f"  PBI measures not found in the metric view: {', '.join(map(str, unmatched_measures))}")
print(f"\nGenie Agent")
if not create_agent:
    agent_status = "not requested"
elif not agent_expected:
    agent_status = "skipped (validation did not pass)"
else:
    agent_status = agent_space_id or "n/a"
print(f"  Space ID:           {agent_status}")
print("\n" + "═" * 60)

# COMMAND ----------

# DBTITLE 1,Write the audit summary
audit_complete = not errors
summary = {
    "status": "SUCCESS" if audit_complete else "INCOMPLETE",
    "run_id": run_id,
    "pbit_filename": pbit_filename,
    "metric_view_fqn": metric_view_fqn,
    "metric_view_valid": validation.get("overall_status") == "PASS",
    "pbi_measures_found": len(pbi_measures),
    "measures_created": validation.get("measure_count"),
    "dimensions_created": validation.get("dimension_count"),
    "pbi_measures_unmatched": unmatched_measures,
    "create_agent": create_agent,
    "agent_space_id": agent_space_id,
    "audit_complete": audit_complete,
    "errors": errors,
    "artifacts_collected": list(artifacts),
}
spark.sql(f"""
    INSERT INTO {artifacts_table} (run_id, task_name, artifact_type, payload, created_at)
    VALUES (:run_id, :task_name, :artifact_type, :payload, current_timestamp())
""", args={
    "run_id": run_id,
    "task_name": "audit_and_report",
    "artifact_type": "audit_summary",
    "payload": json.dumps(summary),
})
print(f"\n  ✓ Wrote audit_summary artifact to {artifacts_table}")

# COMMAND ----------

# DBTITLE 1,Exit
print("\n" + "=" * 60)
print(f"[TASK AUDIT] {summary['status']}")
print("=" * 60)

if errors:
    raise RuntimeError("Audit incomplete: " + "; ".join(errors))
exit_payload = json.dumps(summary)
print(f"\nExiting with: {exit_payload}")
dbutils.notebook.exit(exit_payload)

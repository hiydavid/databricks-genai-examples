# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# dependencies = [
#   "databricks-sdk>=0.102.0",
# ]
# ///
# DBTITLE 1,Setup & Preflight
# MAGIC %md
# MAGIC # Setup & Preflight — Genie PBI Import Workflow
# MAGIC
# MAGIC First task in the DAG. Validates the job parameters, creates the artifacts table, parses the `.pbit` semantic model for reference, checks which Power BI source tables resolve in Unity Catalog, and writes a `setup_config` artifact.

# COMMAND ----------

# DBTITLE 1,Parameters
import json

dbutils.widgets.text("run_id", "")
dbutils.widgets.text("pbit_volume_path", "")
dbutils.widgets.text("metric_view_catalog", "")
dbutils.widgets.text("metric_view_schema", "")
dbutils.widgets.text("pbit_filename", "")
dbutils.widgets.text("metric_view_name", "")
dbutils.widgets.text("create_agent", "true")
dbutils.widgets.text("agent_name", "")
dbutils.widgets.text("warehouse_id", "")

run_id = dbutils.widgets.get("run_id").strip()
pbit_volume_path = dbutils.widgets.get("pbit_volume_path").strip()
metric_view_catalog = dbutils.widgets.get("metric_view_catalog").strip()
metric_view_schema = dbutils.widgets.get("metric_view_schema").strip()
pbit_filename = dbutils.widgets.get("pbit_filename").strip()
metric_view_name = dbutils.widgets.get("metric_view_name").strip()
create_agent = dbutils.widgets.get("create_agent").strip()
agent_name = dbutils.widgets.get("agent_name").strip()
warehouse_id = dbutils.widgets.get("warehouse_id").strip()

# Keep dry runs free of API calls and Delta reads/writes, including partial config.
if not all((metric_view_catalog, metric_view_schema, pbit_filename)):
    dbutils.notebook.exit(json.dumps({"status": "DRY_RUN", "run_id": run_id}))
if not run_id:
    raise ValueError("run_id is required for a configured run; use the job run ID")
# The Genie Code tasks read these from their prompts; reject bad values here, before
# they can act on something they have to guess at.
if not pbit_volume_path or not metric_view_name:
    raise ValueError("pbit_volume_path and metric_view_name are required")
if not pbit_volume_path.startswith("/Volumes/"):
    raise ValueError("pbit_volume_path must be a full Unity Catalog Volume path starting with /Volumes/")
if not pbit_filename.lower().endswith(".pbit"):
    raise ValueError(f"pbit_filename must be a .pbit template file; got {pbit_filename!r}")
if "/" in pbit_filename or "\\" in pbit_filename:
    raise ValueError("pbit_filename must be a file name, not a path")
if create_agent not in ("true", "false"):
    raise ValueError(f"create_agent must be 'true' or 'false'; got {create_agent!r}")
if create_agent == "true" and not (agent_name and warehouse_id):
    raise ValueError("agent_name and warehouse_id are required when create_agent is 'true'")

pbit_path = f"{pbit_volume_path.rstrip('/')}/{pbit_filename}"

print("=" * 60)
print("[TASK SETUP] Setup & Preflight")
print("=" * 60)
print(f"  run_id:           {run_id}")
print(f"  MV catalog:       {metric_view_catalog}")
print(f"  MV schema:        {metric_view_schema}")
print(f"  pbit file:        {pbit_path}")
print(f"  metric_view_name: {metric_view_name}")
print(f"  create_agent:     {create_agent}")

# COMMAND ----------

# DBTITLE 1,Create the artifacts table
# Created first so the audit task can still record a summary if a later check fails.
artifacts_table = ".".join(
    f"`{part.replace('`', '``')}`"
    for part in (metric_view_catalog, metric_view_schema, "genie_pbi_import_workflow_artifacts")
)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {artifacts_table} (
        run_id STRING,
        task_name STRING,
        artifact_type STRING,
        payload STRING,
        created_at TIMESTAMP
    ) USING DELTA
    COMMENT 'Inter-task message bus for the Genie PBI import workflow'
""")
print(f"\n  ✓ Artifacts table ready: {artifacts_table}")

# COMMAND ----------

# DBTITLE 1,Parse the Power BI semantic model
import os
import zipfile

if not os.path.exists(pbit_path):
    raise FileNotFoundError(f"PBIT file not found: {pbit_path}")

# A .pbit is a zip archive; DataModelSchema holds the semantic model as UTF-16LE JSON.
with zipfile.ZipFile(pbit_path) as z:
    if "DataModelSchema" not in z.namelist():
        raise ValueError(f"{pbit_filename} has no DataModelSchema entry; is it a valid .pbit?")
    model = json.loads(z.read("DataModelSchema").decode("utf-16-le").lstrip("﻿")).get("model", {})

pbi_tables = model.get("tables", [])
measures = [
    {"table": t["name"], "name": m["name"]}
    for t in pbi_tables for m in t.get("measures", [])
]
print(f"\n  ✓ Parsed model: {len(pbi_tables)} tables, {len(measures)} measures")

# COMMAND ----------

# DBTITLE 1,Check which source tables resolve in Unity Catalog
# Informational only: /importBI resolves sources itself, and the validate task checks the
# tables the metric view actually references. Missing tables here are an early warning.
import re

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# The Databricks Power BI connector navigates catalog → schema → table in M, e.g.
# Source{[Name="main",Kind="Database"]}[Data]{[Name="sales",Kind="Schema"]}[Data]...
NAVIGATION = re.compile(r'\[Name\s*=\s*"([^"]+)"\s*,\s*Kind\s*=\s*"(Database|Schema|Table|View)"\]')


def source_reference(table):
    """Return (uc_table, match) for a PBI table, or (None, kind) if it has no UC source."""
    partitions = table.get("partitions", [])
    if not partitions:
        return None, "no_source"
    if any(p.get("source", {}).get("type") == "calculated" for p in partitions):
        return None, "calculated"
    for p in partitions:
        expression = p.get("source", {}).get("expression", "")
        if isinstance(expression, list):
            expression = "\n".join(expression)
        parts = {kind: name for name, kind in NAVIGATION.findall(expression)}
        leaf = parts.get("Table") or parts.get("View")
        if parts.get("Database") and parts.get("Schema") and leaf:
            return f"{parts['Database']}.{parts['Schema']}.{leaf}", "m_navigation"
    # Fall back to a naming convention when the M query doesn't name a UC table
    # (native SQL queries, other connectors). This is a guess; treat misses as hints.
    return (f"{metric_view_catalog}.{metric_view_schema}."
            f"{table['name'].lower().replace(' ', '_')}"), "name_convention"


source_tables = []
for t in pbi_tables:
    uc_table, match = source_reference(t)
    result = {"pbi_table": t["name"], "uc_table": uc_table, "match": match}
    if uc_table:
        try:
            result["uc_exists"] = bool(w.tables.exists(full_name=uc_table).table_exists)
        except Exception as e:
            result["uc_exists"] = False
            result["error"] = str(e)[:200]
    source_tables.append(result)

found = [r for r in source_tables if r.get("uc_exists") is True]
missing = [r for r in source_tables if r.get("uc_exists") is False]
print(f"  UC source tables: {len(found)} found, {len(missing)} missing, "
      f"{len(source_tables) - len(found) - len(missing)} without a UC source")
for r in missing:
    print(f"  ! PBI table '{r['pbi_table']}' → '{r['uc_table']}' not found ({r['match']})")

# COMMAND ----------

# DBTITLE 1,Write the setup_config artifact
setup_config = {
    "run_id": run_id,
    "metric_view_catalog": metric_view_catalog,
    "metric_view_schema": metric_view_schema,
    "pbit_filename": pbit_filename,
    "pbit_volume_path": pbit_volume_path.rstrip("/"),
    "pbit_path": pbit_path,
    "metric_view_name": metric_view_name,
    "metric_view_fqn": f"{metric_view_catalog}.{metric_view_schema}.{metric_view_name}",
    "create_agent": create_agent == "true",
    "pbi_tables": [t["name"] for t in pbi_tables],
    "pbi_measures": measures,
    "source_table_validation": {
        "tables_found_in_uc": len(found),
        "tables_missing_from_uc": len(missing),
        "details": source_tables,
    },
}

# Bind JSON as a value: SQL string parsing must not consume its escape characters.
spark.sql(f"""
    INSERT INTO {artifacts_table} (run_id, task_name, artifact_type, payload, created_at)
    VALUES (:run_id, :task_name, :artifact_type, :payload, current_timestamp())
""", args={
    "run_id": run_id,
    "task_name": "setup_and_preflight",
    "artifact_type": "setup_config",
    "payload": json.dumps(setup_config),
})
print(f"\n  ✓ Wrote setup_config artifact to {artifacts_table}")

# COMMAND ----------

# DBTITLE 1,Exit
exit_payload = json.dumps({"status": "SUCCESS", "run_id": run_id, "measures_found": len(measures)})
print(f"\nExiting with: {exit_payload}")
dbutils.notebook.exit(exit_payload)

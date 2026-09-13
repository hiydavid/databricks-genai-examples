# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Intake and Snapshot Prototype
# MAGIC %md
# MAGIC # Intake & Snapshot — GSO Prototype
# MAGIC
# MAGIC Simplified prototype of the `intake_and_snapshot` task. Fetches Genie Space config via SDK, writes run manifest and config snapshot to a Delta artifacts table.

# COMMAND ----------

# DBTITLE 1,Parameters
# -- Parameters --
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("space_id", "")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("warehouse_id", "")
dbutils.widgets.text("triggered_by", "")

run_id = dbutils.widgets.get("run_id").strip()
space_id = dbutils.widgets.get("space_id").strip()
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
warehouse_id = dbutils.widgets.get("warehouse_id").strip()
triggered_by = dbutils.widgets.get("triggered_by").strip()

print("=" * 60)
print("[TASK INTAKE] Intake & Snapshot — Prototype")
print("=" * 60)
print(f"  run_id:      {run_id or '(empty — dry run)'}")
print(f"  space_id:    {space_id or '(empty — will skip API call)'}")
print(f"  catalog:     {catalog or '(empty)'}")
print(f"  schema:      {schema or '(empty)'}")
print(f"  warehouse_id:{warehouse_id or '(empty)'}")

# COMMAND ----------

# DBTITLE 1,Fetch Genie Space config
import json
from datetime import datetime
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

space_config = None
space_config_json = "{}"

if space_id:
    print(f"\nFetching Genie Space config for space_id={space_id} ...")
    try:
        space = w.genie.get_space(space_id=space_id, include_serialized_space=True)
        space_config = {
            "space_id": space_id,
            "title": space.title,
            "description": space.description,
            "serialized_space": space.serialized_space,
        }
        space_config_json = json.dumps(space_config, default=str)
        print(f"  ✓ Space title: {space_config['title']}")
        print(f"  ✓ Serialized space captured: {len(space.serialized_space or '')} chars")
    except Exception as e:
        print(f"  ⚠ Failed to fetch space config: {e}")
        space_config_json = json.dumps({"error": str(e)})
else:
    print("\n  ⏭ No space_id provided — skipping Genie API call (dry run)")
    space_config_json = json.dumps({"dry_run": True})

# COMMAND ----------

# DBTITLE 1,Write artifacts to Delta
if catalog and schema:
    artifacts_table = f"`{catalog}`.`{schema}`.gso_prototype_artifacts"

    # Create table if not exists
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {artifacts_table} (
            run_id STRING,
            artifact_type STRING,
            payload STRING,
            created_at TIMESTAMP
        ) USING DELTA
    """)

    # Build the run manifest payload
    run_manifest = json.dumps({
        "run_id": run_id,
        "space_id": space_id,
        "catalog": catalog,
        "schema": schema,
        "triggered_by": triggered_by,
    })

    # Insert run_manifest artifact
    spark.sql(f"""
        INSERT INTO {artifacts_table}
        VALUES (
            '{run_id}',
            'run_manifest',
            '{run_manifest.replace("'", "''")  }',
            current_timestamp()
        )
    """)
    print(f"\n  ✓ Wrote run_manifest artifact to {artifacts_table}")

    # Insert space_config_snapshot artifact
    safe_config = space_config_json.replace("'", "''")
    spark.sql(f"""
        INSERT INTO {artifacts_table}
        VALUES (
            '{run_id}',
            'space_config_snapshot',
            '{safe_config}',
            current_timestamp()
        )
    """)
    print(f"  ✓ Wrote space_config_snapshot artifact to {artifacts_table}")
else:
    print("\n  ⏭ No catalog/schema provided — skipping Delta writes (dry run)")

# COMMAND ----------

# DBTITLE 1,Summary and exit
print("\n" + "=" * 60)
print("[TASK INTAKE] Summary")
print("=" * 60)
print(f"  Space ID:      {space_id or '(dry run)'}")
print(f"  Artifacts:     {'written to Delta' if (catalog and schema) else 'skipped (dry run)'}")
print("=" * 60)

exit_payload = json.dumps({"status": "SUCCESS", "run_id": run_id})
print(f"\nExiting with: {exit_payload}")
dbutils.notebook.exit(exit_payload)
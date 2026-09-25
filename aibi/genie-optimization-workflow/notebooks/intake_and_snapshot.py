# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# dependencies = [
#   "databricks-sdk>=0.102.0",
# ]
# ///
# DBTITLE 1,Intake and Snapshot Prototype
# MAGIC %md
# MAGIC # Intake & Snapshot — GSO Prototype
# MAGIC
# MAGIC Simplified prototype of the `intake_and_snapshot` task. Fetches Genie Space config via SDK, writes run manifest and config snapshot to a Delta artifacts table.

# COMMAND ----------

# DBTITLE 1,Parameters
import json

# -- Parameters --
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("space_id", "")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("warehouse_id", "")
dbutils.widgets.text("triggered_by", "")
dbutils.widgets.text("job_run_id", "")

run_id = dbutils.widgets.get("run_id").strip()
space_id = dbutils.widgets.get("space_id").strip()
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
warehouse_id = dbutils.widgets.get("warehouse_id").strip()
triggered_by = dbutils.widgets.get("triggered_by").strip()
job_run_id = dbutils.widgets.get("job_run_id").strip()

# Keep dry runs free of API calls and Delta reads/writes, including partial config.
if not all((space_id, catalog, schema)):
    dbutils.notebook.exit(json.dumps({"status": "DRY_RUN", "run_id": run_id}))
if not run_id:
    raise ValueError("run_id is required for a configured run; use the job run ID")

print("=" * 60)
print("[TASK INTAKE] Intake & Snapshot — Prototype")
print("=" * 60)
print(f"  run_id:       {run_id}")
print(f"  space_id:     {space_id}")
print(f"  catalog:      {catalog}")
print(f"  schema:       {schema}")
print(f"  warehouse_id: {warehouse_id or '(empty)'}")

# COMMAND ----------

# DBTITLE 1,Fetch Genie Space config
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

print(f"\nFetching Genie Space config for space_id={space_id} ...")
# A failed or missing snapshot must stop the job before QC mutates the space.
space = w.genie.get_space(space_id=space_id, include_serialized_space=True)
if not space.serialized_space or not isinstance(json.loads(space.serialized_space), dict):
    raise ValueError("Genie did not return a valid serialized space snapshot")
space_config = {
    "space_id": space_id,
    "title": space.title,
    "description": space.description,
    "serialized_space": space.serialized_space,
}
print(f"  ✓ Space title: {space_config['title']}")
print(f"  ✓ Serialized space captured: {len(space.serialized_space)} chars")

# COMMAND ----------

# DBTITLE 1,Resolve who triggered the run
# Resolve at run time rather than baking a user into the job definition: the job's
# run-as identity (current_user.me()) is not the person who clicked Run now.
trigger_type = None
if not triggered_by and job_run_id:
    try:
        job_run = w.jobs.get_run(run_id=int(job_run_id))
        triggered_by = job_run.creator_user_name or ""
        trigger_type = job_run.trigger.value if job_run.trigger else None
    except Exception as e:
        # Provenance is informational; don't fail the run over it.
        print(f"  ! Could not look up job run {job_run_id}: {e}")
print(f"  triggered_by: {triggered_by or '(unknown)'} (trigger: {trigger_type or 'n/a'})")

# COMMAND ----------

# DBTITLE 1,Write artifacts to Delta
artifacts_table = ".".join(
    f"`{part.replace('`', '``')}`" for part in (catalog, schema, "gso_prototype_artifacts")
)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {artifacts_table} (
        run_id STRING,
        artifact_type STRING,
        payload STRING,
        created_at TIMESTAMP
    ) USING DELTA
""")

run_manifest = {
    "run_id": run_id,
    "space_id": space_id,
    "catalog": catalog,
    "schema": schema,
    "triggered_by": triggered_by or None,
    "trigger_type": trigger_type,
}

# Bind JSON as a value: SQL string parsing must not consume its escape characters.
for artifact_type, payload in (
    ("run_manifest", run_manifest),
    ("space_config_snapshot", space_config),
):
    spark.sql(f"""
        INSERT INTO {artifacts_table} (run_id, artifact_type, payload, created_at)
        VALUES (:run_id, :artifact_type, :payload, current_timestamp())
    """, args={
        "run_id": run_id,
        "artifact_type": artifact_type,
        "payload": json.dumps(payload),
    })
    print(f"  ✓ Wrote {artifact_type} artifact to {artifacts_table}")

# COMMAND ----------

# DBTITLE 1,Summary and exit
print("\n" + "=" * 60)
print("[TASK INTAKE] Summary")
print("=" * 60)
print(f"  Space ID:      {space_id}")
print(f"  Artifacts:     written to {artifacts_table}")
print("=" * 60)

exit_payload = json.dumps({"status": "SUCCESS", "run_id": run_id})
print(f"\nExiting with: {exit_payload}")
dbutils.notebook.exit(exit_payload)

# Databricks notebook source
# DBTITLE 1,Publish & Audit — GSO Prototype
# MAGIC %md
# MAGIC # Publish & Audit — GSO Prototype
# MAGIC
# MAGIC Final task in the DAG. Reads all artifacts from the run, compiles a summary audit report, and writes the final run status to Delta.

# COMMAND ----------

# DBTITLE 1,Parameters
import json
import math

dbutils.widgets.text("run_id", "")
dbutils.widgets.text("space_id", "")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("target_accuracy", "0.90")
dbutils.widgets.text("max_rounds", "3")

run_id = dbutils.widgets.get("run_id").strip()
space_id = dbutils.widgets.get("space_id").strip()
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()

if not all((space_id, catalog, schema)):
    dbutils.notebook.exit(json.dumps({"status": "DRY_RUN", "run_id": run_id}))
if not run_id:
    raise ValueError("run_id is required for a configured run; use the job run ID")

target_accuracy = float(dbutils.widgets.get("target_accuracy") or "0.90")
max_rounds = int(dbutils.widgets.get("max_rounds") or "3")
if not math.isfinite(target_accuracy) or not 0 <= target_accuracy <= 1:
    raise ValueError("target_accuracy must be between 0 and 1")

print("=" * 60)
print("[TASK PUBLISH] Publish & Audit — Prototype")
print("=" * 60)
print(f"  run_id:          {run_id or '(empty — dry run)'}")
print(f"  space_id:        {space_id or '(empty)'}")
print(f"  target_accuracy: {target_accuracy}")

# COMMAND ----------

# DBTITLE 1,Load all artifacts for this run
artifacts = {}
parse_errors = {}
artifacts_table = ".".join(
    f"`{part.replace('`', '``')}`" for part in (catalog, schema, "gso_prototype_artifacts")
)
print(f"\nLoading artifacts from {artifacts_table} for run_id={run_id} ...")

rows = spark.sql(f"""
    SELECT artifact_type, payload, created_at
    FROM {artifacts_table}
    WHERE run_id = :run_id
    ORDER BY created_at
""", args={"run_id": run_id}).collect()

for row in rows:
    artifact_type = row["artifact_type"]
    # A repaired audit must capture a fresh snapshot, not reuse its prior output.
    if artifact_type in ("space_config_post_opt", "run_summary"):
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

print(f"  ✓ Found {len(artifacts)} artifact(s): {', '.join(artifacts.keys())}")

# COMMAND ----------

# DBTITLE 1,Validate the audit inputs
errors = list(parse_errors.values())
required_artifacts = ("run_manifest", "space_config_snapshot", "benchmark_qc",
                      "baseline_run", "optimization_result")
for artifact_type in required_artifacts:
    if artifact_type not in artifacts:
        errors.append(f"Missing required artifact: {artifact_type}")


def valid_accuracy(value):
    return type(value) in (int, float) and math.isfinite(value) and 0 <= value <= 1


def valid_snapshot(snapshot):
    try:
        return snapshot.get("space_id") == space_id and isinstance(
            json.loads(snapshot.get("serialized_space")), dict
        )
    except (ValueError, TypeError):
        return False


manifest = artifacts.get("run_manifest", {})
qc = artifacts.get("benchmark_qc", {})
baseline = artifacts.get("baseline_run", {})
opt = artifacts.get("optimization_result", {})

if manifest.get("run_id") != run_id or manifest.get("space_id") != space_id:
    errors.append("Run manifest does not match this run and space")
if not valid_snapshot(artifacts.get("space_config_snapshot", {})):
    errors.append("Missing or invalid pre-optimization space snapshot")
approved_ids = qc.get("approved_benchmark_question_ids")
if (not isinstance(approved_ids, list) or not approved_ids
        or any(not isinstance(i, str) or not i.strip() for i in approved_ids)):
    errors.append("QC artifact must contain a nonempty list of approved benchmark IDs")
if baseline.get("eval_run_status") != "DONE" or not baseline.get("eval_run_id"):
    errors.append("Baseline evaluation did not complete successfully")
if baseline.get("status") != "SUCCESS" or baseline.get("error"):
    errors.append("Baseline artifact does not report success")
if opt.get("status") != "SUCCESS" or opt.get("error"):
    errors.append("Optimization artifact does not report success")

baseline_accuracy = baseline.get("accuracy")
final_accuracy = opt.get("final_accuracy")
if not valid_accuracy(baseline_accuracy):
    errors.append("Missing or invalid baseline accuracy")
    baseline_accuracy = None
if not valid_accuracy(final_accuracy):
    errors.append("Missing or invalid final accuracy in optimization_result")
    final_accuracy = None
if not valid_accuracy(opt.get("starting_accuracy")):
    errors.append("Missing or invalid starting accuracy in optimization_result")
elif baseline_accuracy is not None and not math.isclose(opt["starting_accuracy"], baseline_accuracy):
    errors.append("Optimization starting accuracy does not match the baseline")
rounds = opt.get("rounds_executed")
if type(rounds) is not int or not 0 <= rounds <= max_rounds:
    errors.append("Missing or invalid rounds_executed in optimization_result")
changes = opt.get("changes_per_round")
if (not isinstance(changes, list)
        or any(not isinstance(r, dict) or not isinstance(r.get("summary"), str) for r in changes)):
    errors.append("Missing or invalid changes_per_round in optimization_result")
    changes = []
if not isinstance(opt.get("remaining_failures"), list):
    errors.append("Missing or invalid remaining_failures in optimization_result")

# COMMAND ----------

# DBTITLE 1,Compile audit report
print("\n" + "═" * 60)
print("AUDIT REPORT")
print("═" * 60)

# --- Run manifest ---
print(f"\n📋 Run Manifest")
print(f"  Run ID:     {manifest.get('run_id', run_id)}")
print(f"  Space ID:   {manifest.get('space_id', space_id)}")

# --- Benchmark QC ---
if qc:
    print(f"\n🔍 Benchmark QC")
    print(f"  Total benchmarks:     {qc.get('total', 'n/a')}")
    print(f"  Valid:                {qc.get('valid_count', 'n/a')}")
    print(f"  Repaired:             {qc.get('repaired_count', 'n/a')}")
    print(f"  Excluded:             {qc.get('excluded_count', 'n/a')}")
    print(f"  Corpus sufficient:    {qc.get('is_sufficient', 'n/a')}")
else:
    print(f"\n🔍 Benchmark QC: no artifact found")

# --- Baseline run ---
if baseline:
    print(f"\n📊 Baseline Evaluation")
    print(f"  Eval run:  {baseline.get('eval_run_id', 'n/a')}")
    print(f"  Status:    {baseline.get('eval_run_status', 'n/a')}")
    print(f"  Accuracy:  {baseline_accuracy:.1%}" if isinstance(baseline_accuracy, (int, float)) else "  Accuracy:  n/a")
else:
    print(f"\n📊 Baseline Evaluation: no artifact found")

# --- Optimization result ---
if opt:
    print(f"\n⚙️ Optimization")
    print(f"  Starting accuracy:  {opt.get('starting_accuracy', 'n/a')}")
    print(f"  Final accuracy:     {opt.get('final_accuracy', 'n/a')}")
    print(f"  Rounds executed:    {opt.get('rounds_executed', 'n/a')} of {max_rounds}")
    if changes:
        print(f"  Changes per round:")
        for rnd in changes:
            print(f"    Round {rnd.get('round', '?')}: {rnd.get('summary', 'n/a')}")
else:
    print(f"\n⚙️ Optimization: no artifact found")

print("\n" + "═" * 60)

# COMMAND ----------

# DBTITLE 1,Capture post-optimization space config snapshot
# Mirror of the intake task's snapshot: captures the Genie Space config after the
# optimize task has finished mutating it, giving the run a before/after audit trail.
# Note: UC-level changes the optimizer may have made (table/column comments) are
# not part of get_space output and are therefore not captured here.

try:
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    print(f"\nCapturing post-optimization snapshot for space_id={space_id} ...")
    space = w.genie.get_space(space_id=space_id, include_serialized_space=True)
    post_config = {
        "space_id": space_id,
        "title": space.title,
        "description": space.description,
        "serialized_space": space.serialized_space,
    }
    if not valid_snapshot(post_config):
        raise ValueError("Genie did not return a valid serialized space snapshot")
    spark.sql(f"""
        INSERT INTO {artifacts_table} (run_id, artifact_type, payload, created_at)
        VALUES (:run_id, :artifact_type, :payload, current_timestamp())
    """, args={
        "run_id": run_id,
        "artifact_type": "space_config_post_opt",
        "payload": json.dumps(post_config),
    })
    artifacts["space_config_post_opt"] = post_config
    print(f"  ✓ Wrote space_config_post_opt artifact to {artifacts_table}")
except Exception as e:
    errors.append(f"Failed to capture post-optimization snapshot: {e}")

# COMMAND ----------

# DBTITLE 1,Write final run status to Delta
audit_complete = not errors
# Unknown is distinct from a completed optimization that missed its target.
target_met = final_accuracy >= target_accuracy if audit_complete else None
final_status = {
    "status": "SUCCESS" if audit_complete else "INCOMPLETE",
    "run_id": run_id,
    "space_id": space_id,
    "baseline_accuracy": baseline_accuracy,
    "final_accuracy": final_accuracy,
    "target_accuracy": target_accuracy,
    "target_met": target_met,
    "audit_complete": audit_complete,
    "errors": errors,
    "artifacts_collected": list(artifacts.keys()),
}
spark.sql(f"""
    INSERT INTO {artifacts_table} (run_id, artifact_type, payload, created_at)
    VALUES (:run_id, :artifact_type, :payload, current_timestamp())
""", args={"run_id": run_id, "artifact_type": "run_summary", "payload": json.dumps(final_status)})
print(f"\n  ✓ Wrote run_summary artifact to {artifacts_table}")
print(f"  Audit complete: {audit_complete}; target met: {target_met}")

# COMMAND ----------

# DBTITLE 1,Exit
print("\n" + "=" * 60)
print(f"[TASK PUBLISH] {final_status['status']}")
print("=" * 60)

if errors:
    raise RuntimeError("Audit incomplete: " + "; ".join(errors))
exit_payload = json.dumps(final_status)
print(f"\nExiting with: {exit_payload}")
dbutils.notebook.exit(exit_payload)

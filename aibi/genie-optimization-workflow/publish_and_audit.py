# Databricks notebook source
# DBTITLE 1,Publish & Audit — GSO Prototype
# MAGIC %md
# MAGIC # Publish & Audit — GSO Prototype
# MAGIC
# MAGIC Final task in the DAG. Reads all artifacts from the run, compiles a summary audit report, and writes the final run status to Delta.

# COMMAND ----------

# DBTITLE 1,Parameters
import json
from datetime import datetime

dbutils.widgets.text("run_id", "")
dbutils.widgets.text("space_id", "")
dbutils.widgets.text("domain", "default")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("target_accuracy", "0.90")
dbutils.widgets.text("max_rounds", "3")

run_id = dbutils.widgets.get("run_id").strip()
space_id = dbutils.widgets.get("space_id").strip()
domain = dbutils.widgets.get("domain").strip() or "default"
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
target_accuracy = float(dbutils.widgets.get("target_accuracy") or "0.90")
max_rounds = int(dbutils.widgets.get("max_rounds") or "3")

print("=" * 60)
print("[TASK PUBLISH] Publish & Audit — Prototype")
print("=" * 60)
print(f"  run_id:          {run_id or '(empty — dry run)'}")
print(f"  space_id:        {space_id or '(empty)'}")
print(f"  target_accuracy: {target_accuracy}")

# COMMAND ----------

# DBTITLE 1,Load all artifacts for this run
artifacts = {}

if catalog and schema:
    artifacts_table = f"`{catalog}`.`{schema}`.gso_prototype_artifacts"
    print(f"\nLoading artifacts from {artifacts_table} for run_id={run_id} ...")

    rows = spark.sql(f"""
        SELECT artifact_type, payload, created_at
        FROM {artifacts_table}
        WHERE run_id = '{run_id}'
        ORDER BY created_at
    """).collect()

    for row in rows:
        artifact_type = row["artifact_type"]
        try:
            artifacts[artifact_type] = json.loads(row["payload"])
        except (json.JSONDecodeError, TypeError):
            artifacts[artifact_type] = {"raw": row["payload"]}

    print(f"  ✓ Found {len(artifacts)} artifact(s): {', '.join(artifacts.keys())}")
else:
    print("\n  ⏭ No catalog/schema — skipping artifact load (dry run)")

# COMMAND ----------

# DBTITLE 1,Compile audit report
print("\n" + "═" * 60)
print("AUDIT REPORT")
print("═" * 60)

# --- Run manifest ---
manifest = artifacts.get("run_manifest", {})
print(f"\n📋 Run Manifest")
print(f"  Run ID:     {manifest.get('run_id', run_id)}")
print(f"  Space ID:   {manifest.get('space_id', space_id)}")
print(f"  Domain:     {manifest.get('domain', domain)}")
print(f"  Apply mode: {manifest.get('apply_mode', 'n/a')}")

# --- Benchmark QC ---
qc = artifacts.get("benchmark_qc", {})
if qc:
    print(f"\n🔍 Benchmark QC")
    print(f"  Total benchmarks:     {qc.get('total', 'n/a')}")
    print(f"  Valid:                {qc.get('valid_count', 'n/a')}")
    print(f"  Repaired:             {qc.get('repaired_count', 'n/a')}")
    print(f"  Excluded:             {qc.get('excluded_count', 'n/a')}")
    print(f"  Corpus sufficient:    {qc.get('is_sufficient', 'n/a')}")
else:
    print(f"\n🔍 Benchmark QC: no artifact found")

# --- Baseline eval ---
baseline = artifacts.get("baseline_eval", {})
baseline_accuracy = baseline.get("accuracy", 0.0)
if baseline:
    print(f"\n📊 Baseline Evaluation")
    print(f"  Accuracy: {baseline_accuracy:.1%} ({baseline.get('correct', 0)}/{baseline.get('total', 0)})")
else:
    print(f"\n📊 Baseline Evaluation: no artifact found")

# --- Optimization result ---
opt = artifacts.get("optimization_result", {})
final_accuracy = opt.get("final_accuracy", baseline_accuracy)
if opt:
    print(f"\n⚙️ Optimization")
    print(f"  Starting accuracy:  {opt.get('starting_accuracy', 'n/a')}")
    print(f"  Final accuracy:     {opt.get('final_accuracy', 'n/a')}")
    print(f"  Rounds executed:    {opt.get('rounds_executed', 'n/a')} of {max_rounds}")
    target_met = final_accuracy >= target_accuracy if isinstance(final_accuracy, (int, float)) else False
    print(f"  Target met:         {'Yes ✓' if target_met else 'No ✗'}")
    if opt.get("changes_per_round"):
        print(f"  Changes per round:")
        for rnd in opt["changes_per_round"]:
            print(f"    Round {rnd.get('round', '?')}: {rnd.get('summary', 'n/a')}")
else:
    print(f"\n⚙️ Optimization: no artifact found")

print("\n" + "═" * 60)

# COMMAND ----------

# DBTITLE 1,Write final run status to Delta
if catalog and schema:
    final_status = {
        "run_id": run_id,
        "space_id": space_id,
        "domain": domain,
        "baseline_accuracy": baseline_accuracy,
        "final_accuracy": final_accuracy,
        "target_accuracy": target_accuracy,
        "target_met": final_accuracy >= target_accuracy if isinstance(final_accuracy, (int, float)) else False,
        "artifacts_collected": list(artifacts.keys()),
    }

    safe_payload = json.dumps(final_status, default=str).replace("'", "''")
    spark.sql(f"""
        INSERT INTO {artifacts_table}
        VALUES (
            '{run_id}',
            'run_summary',
            '{safe_payload}',
            current_timestamp()
        )
    """)
    print(f"\n  ✓ Wrote run_summary artifact to {artifacts_table}")
else:
    final_status = {"status": "DRY_RUN"}
    print("\n  ⏭ No catalog/schema — skipping Delta write (dry run)")

# COMMAND ----------

# DBTITLE 1,Exit
print("\n" + "=" * 60)
print("[TASK PUBLISH] Complete")
print("=" * 60)

exit_payload = json.dumps({
    "status": "SUCCESS",
    "run_id": run_id,
    "baseline_accuracy": baseline_accuracy,
    "final_accuracy": final_accuracy,
    "target_met": final_accuracy >= target_accuracy if isinstance(final_accuracy, (int, float)) else False,
}, default=str)
print(f"\nExiting with: {exit_payload}")
dbutils.notebook.exit(exit_payload)
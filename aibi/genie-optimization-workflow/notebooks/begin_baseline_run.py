# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# dependencies = [
#   "databricks-sdk>=0.102.0",
# ]
# ///
# DBTITLE 1,Begin Baseline Run — GSO Prototype
# MAGIC %md
# MAGIC # Begin Baseline Run — GSO Prototype
# MAGIC
# MAGIC Starts a Genie benchmark eval run against the space, polls it to
# MAGIC completion, and records the final status and accuracy counts in the
# MAGIC artifacts table. The `optimize` Genie Code task reads the per-question
# MAGIC results using the recorded `eval_run_id`.

# COMMAND ----------

# DBTITLE 1,Parameters
import json
import time

dbutils.widgets.text("run_id", "")
dbutils.widgets.text("space_id", "")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")

run_id = dbutils.widgets.get("run_id").strip()
space_id = dbutils.widgets.get("space_id").strip()
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()

if not all((space_id, catalog, schema)):
    dbutils.notebook.exit(json.dumps({"status": "DRY_RUN", "run_id": run_id}))
if not run_id:
    raise ValueError("run_id is required for a configured run; use the job run ID")

print("=" * 60)
print("[TASK BEGIN_BASELINE_RUN] Begin Baseline Run — Prototype")
print("=" * 60)
print(f"  run_id:   {run_id}")
print(f"  space_id: {space_id}")
print(f"  catalog:  {catalog}")
print(f"  schema:   {schema}")

# COMMAND ----------

# DBTITLE 1,Read approved benchmark IDs from the benchmark_qc artifact
artifacts_table = ".".join(
    f"`{part.replace('`', '``')}`" for part in (catalog, schema, "gso_prototype_artifacts")
)
approved_ids = None
error = None

try:
    qc_rows = spark.sql(f"""
        SELECT payload FROM {artifacts_table}
        WHERE run_id = :run_id AND artifact_type = 'benchmark_qc'
        ORDER BY created_at DESC LIMIT 1
    """, args={"run_id": run_id}).collect()
    if not qc_rows:
        raise ValueError("Missing benchmark_qc artifact")
    qc = json.loads(qc_rows[0]["payload"])
    ids = qc.get("approved_benchmark_question_ids") if isinstance(qc, dict) else None
    if not isinstance(ids, list) or any(not isinstance(i, str) or not i.strip() for i in ids):
        raise ValueError("benchmark_qc must provide an approved_benchmark_question_ids list of IDs")
    approved_ids = list(dict.fromkeys(ids))
    if not approved_ids:
        raise ValueError("No valid benchmarks: benchmark_qc approved zero questions")
    print(f"benchmark_qc approved {len(approved_ids)} benchmark questions for evaluation")
except Exception as e:
    # Never broaden a failed QC handoff into an evaluation of every question.
    error = f"Cannot use benchmark_qc approvals: {e}"

# COMMAND ----------

# DBTITLE 1,Start the eval run
TERMINAL_STATUSES = {"DONE", "EVALUATION_FAILED", "EVALUATION_CANCELLED", "EVALUATION_TIMEOUT"}
POLL_INTERVAL_SECONDS = 15
POLL_MAX_ATTEMPTS = 220  # ~55 min, under the 3600s task timeout

eval_run = None
eval_run_id = None
status = "NOT_STARTED"

if error is None:
    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        print(f"\nStarting benchmark eval run against Genie Space {space_id} ...")
        eval_run = w.genie.genie_create_eval_run(space_id=space_id, benchmark_question_ids=approved_ids)
        eval_run_id = eval_run.eval_run_id
        if not eval_run_id:
            raise ValueError("Genie did not return an eval_run_id")
        status = eval_run.eval_run_status.value if eval_run.eval_run_status else "RUNNING"
        print(f"  ✓ Eval run started: {eval_run_id} (status: {status})")
    except Exception as e:
        error = f"Could not start baseline evaluation: {e}"

# COMMAND ----------

# DBTITLE 1,Poll the eval run to completion
if eval_run_id and error is None:
    print(f"\nPolling eval run {eval_run_id} (every {POLL_INTERVAL_SECONDS}s) ...")
    try:
        for attempt in range(POLL_MAX_ATTEMPTS):
            if status in TERMINAL_STATUSES:
                break
            eval_run = w.genie.genie_get_eval_run(space_id=space_id, eval_run_id=eval_run_id)
            status = eval_run.eval_run_status.value if eval_run.eval_run_status else "UNKNOWN"
            if status in TERMINAL_STATUSES:
                break
            if attempt % 4 == 0:
                done = eval_run.num_done or 0
                total = eval_run.num_questions or 0
                print(f"  ... status={status} ({done}/{total} questions done)")
            time.sleep(POLL_INTERVAL_SECONDS)
        if status not in TERMINAL_STATUSES:
            error = f"Baseline polling budget exhausted; last status: {status}"
        elif status != "DONE":
            error = f"Baseline evaluation ended with status: {status}"
    except Exception as e:
        error = f"Could not poll baseline evaluation {eval_run_id}: {e}"

# COMMAND ----------

# DBTITLE 1,Record the eval run
num_questions = eval_run.num_questions if eval_run else None
num_correct = eval_run.num_correct if eval_run else None
num_needs_review = eval_run.num_needs_review if eval_run else None
accuracy = None
if error is None:
    if (type(num_questions) is not int or num_questions <= 0
            or type(num_correct) is not int or not 0 <= num_correct <= num_questions):
        error = "Completed baseline evaluation has missing or invalid accuracy counts"
    else:
        accuracy = num_correct / num_questions

payload = {
    "status": "FAILED" if error else "SUCCESS",
    "eval_run_id": eval_run_id,
    "eval_run_status": status,
    "space_id": space_id,
    "benchmark_question_ids": approved_ids,
    "num_questions": num_questions,
    "num_correct": num_correct,
    "num_needs_review": num_needs_review,
    "accuracy": accuracy,
    "error": error,
}
spark.sql(f"""
    INSERT INTO {artifacts_table} (run_id, artifact_type, payload, created_at)
    VALUES (:run_id, :artifact_type, :payload, current_timestamp())
""", args={"run_id": run_id, "artifact_type": "baseline_run", "payload": json.dumps(payload)})
print(f"\n  ✓ Wrote baseline_run artifact to {artifacts_table}")

# COMMAND ----------

# DBTITLE 1,Exit
# A JSON status alone does not fail a Databricks task. Raise after saving diagnostics.
if error:
    raise RuntimeError(error)
exit_payload = json.dumps({"run_id": run_id, **payload})
print(f"\nExiting with: {exit_payload}")
dbutils.notebook.exit(exit_payload)

# Databricks notebook source
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
from databricks.sdk import WorkspaceClient

dbutils.widgets.text("run_id", "")
dbutils.widgets.text("space_id", "")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")

run_id = dbutils.widgets.get("run_id").strip()
space_id = dbutils.widgets.get("space_id").strip()
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()

print("=" * 60)
print("[TASK BEGIN_BASELINE_RUN] Begin Baseline Run — Prototype")
print("=" * 60)
print(f"  run_id:   {run_id or '(empty — dry run)'}")
print(f"  space_id: {space_id or '(empty)'}")
print(f"  catalog:  {catalog or '(empty)'}")
print(f"  schema:   {schema or '(empty)'}")

# COMMAND ----------

# DBTITLE 1,Read approved benchmark IDs from the benchmark_qc artifact
approved_ids = None

if space_id and catalog and schema:
    try:
        artifacts_table = f"`{catalog}`.`{schema}`.gso_prototype_artifacts"
        qc_row = spark.sql(f"""
            SELECT payload FROM {artifacts_table}
            WHERE run_id = '{run_id}' AND artifact_type = 'benchmark_qc'
            ORDER BY created_at DESC LIMIT 1
        """).collect()
        if qc_row:
            ids = (json.loads(qc_row[0]["payload"]) or {}).get("approved_benchmark_question_ids")
            if isinstance(ids, list) and ids:
                approved_ids = ids
                print(f"benchmark_qc approved {len(ids)} benchmark questions for evaluation")
            else:
                print("benchmark_qc artifact has no approved ID list — will evaluate all benchmark questions")
        else:
            print("No benchmark_qc artifact found — will evaluate all benchmark questions")
    except Exception as e:
        print(f"⚠ Could not read benchmark_qc artifact ({e}) — will evaluate all benchmark questions")

# COMMAND ----------

# DBTITLE 1,Start the eval run
w = WorkspaceClient()

TERMINAL_STATUSES = {"DONE", "EVALUATION_FAILED", "EVALUATION_CANCELLED", "EVALUATION_TIMEOUT"}
POLL_INTERVAL_SECONDS = 15
POLL_MAX_ATTEMPTS = 220  # ~55 min, under the 3600s task timeout

eval_run = None
eval_run_id = None
status = "SKIPPED"

if space_id:
    print(f"\nStarting benchmark eval run against Genie Space {space_id} ...")
    eval_run = w.genie.genie_create_eval_run(space_id=space_id, benchmark_question_ids=approved_ids)
    eval_run_id = eval_run.eval_run_id
    status = eval_run.eval_run_status.value if eval_run.eval_run_status else "RUNNING"
    print(f"  ✓ Eval run started: {eval_run_id} (status: {status})")
else:
    print("\n  ⏭ No space_id — skipping (dry run)")

# COMMAND ----------

# DBTITLE 1,Poll the eval run to completion
if eval_run_id:
    print(f"\nPolling eval run {eval_run_id} (every {POLL_INTERVAL_SECONDS}s) ...")
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

    print(f"\n  Eval run finished with status: {status}")
    if status not in TERMINAL_STATUSES:
        print("  ⚠ Polling budget exhausted before the run reached a terminal status")

# COMMAND ----------

# DBTITLE 1,Record the eval run
if catalog and schema:
    artifacts_table = f"`{catalog}`.`{schema}`.gso_prototype_artifacts"

    num_questions = eval_run.num_questions if eval_run else None
    num_correct = eval_run.num_correct if eval_run else None
    num_needs_review = eval_run.num_needs_review if eval_run else None
    accuracy = (num_correct / num_questions) if num_questions else None

    payload = json.dumps({
        "eval_run_id": eval_run_id,
        "eval_run_status": status,
        "space_id": space_id,
        "benchmark_question_ids": approved_ids,
        "num_questions": num_questions,
        "num_correct": num_correct,
        "num_needs_review": num_needs_review,
        "accuracy": accuracy,
    }, default=str)

    safe_payload = payload.replace("'", "''")
    spark.sql(f"""
        INSERT INTO {artifacts_table}
        VALUES (
            '{run_id}',
            'baseline_run',
            '{safe_payload}',
            current_timestamp()
        )
    """)
    print(f"\n  ✓ Wrote baseline_run artifact to {artifacts_table}")
else:
    print("\n  ⏭ No catalog/schema — skipping Delta write (dry run)")

# COMMAND ----------

# DBTITLE 1,Exit
exit_payload = json.dumps({
    "status": "SUCCESS",
    "run_id": run_id,
    "eval_run_id": eval_run_id,
    "eval_run_status": status,
    "accuracy": (eval_run.num_correct / eval_run.num_questions) if (eval_run and eval_run.num_questions) else None,
    "correct": eval_run.num_correct if eval_run else None,
    "total": eval_run.num_questions if eval_run else None,
})
print(f"\nExiting with: {exit_payload}")
dbutils.notebook.exit(exit_payload)

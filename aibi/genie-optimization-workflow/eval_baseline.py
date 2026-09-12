# Databricks notebook source
# DBTITLE 1,Eval Baseline — GSO Prototype
# MAGIC %md
# MAGIC # Eval Baseline — GSO Prototype
# MAGIC
# MAGIC Runs the Genie benchmark evaluation against the validated benchmark corpus.
# MAGIC Persists baseline accuracy to Delta so the `optimize` Genie Code task can read it.

# COMMAND ----------

# DBTITLE 1,Parameters
import json
import time
from databricks.sdk import WorkspaceClient

dbutils.widgets.text("run_id", "")
dbutils.widgets.text("space_id", "")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("warehouse_id", "")

run_id = dbutils.widgets.get("run_id").strip()
space_id = dbutils.widgets.get("space_id").strip()
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
warehouse_id = dbutils.widgets.get("warehouse_id").strip()

print("=" * 60)
print("[TASK EVAL_BASELINE] Baseline Evaluation — Prototype")
print("=" * 60)
print(f"  run_id:       {run_id or '(empty — dry run)'}")
print(f"  space_id:     {space_id or '(empty)'}")
print(f"  catalog:      {catalog or '(empty)'}")
print(f"  schema:       {schema or '(empty)'}")
print(f"  warehouse_id: {warehouse_id or '(empty)'}")

# COMMAND ----------

# DBTITLE 1,Load benchmarks from Delta
benchmarks = []

if catalog and schema:
    benchmarks_table = f"`{catalog}`.`{schema}`.genie_benchmarks"
    print(f"\nLoading benchmarks from {benchmarks_table} ...")
    try:
        benchmarks_df = spark.sql(f"SELECT * FROM {benchmarks_table}")
        benchmarks = [row.asDict() for row in benchmarks_df.collect()]
        print(f"  ✓ Loaded {len(benchmarks)} benchmarks")
    except Exception as e:
        print(f"  ⚠ Could not load benchmarks: {e}")
        print("  Falling back to empty benchmark set (dry run)")
else:
    print("\n  ⏭ No catalog/schema — skipping benchmark load (dry run)")

# COMMAND ----------

# DBTITLE 1,Run Genie benchmark evaluation
w = WorkspaceClient()

results = []
overall_accuracy = 0.0
correct = 0
total = len(benchmarks)

if space_id and benchmarks:
    print(f"\nRunning baseline evaluation against Genie Space {space_id} ...")
    print(f"  Evaluating {total} benchmark questions ...\n")

    for i, bm in enumerate(benchmarks):
        question = bm.get("question", "")
        expected_sql = bm.get("expected_sql", "")
        if isinstance(question, list):
            question = question[0] if question else ""

        result = {"question": question, "expected_sql": expected_sql, "status": "error", "genie_sql": None}

        try:
            # Start a conversation and ask the question
            conversation = w.genie.start_conversation(space_id=space_id, content=question)
            conversation_id = conversation.conversation_id
            message_id = conversation.message_id

            # Poll for completion (up to 120s)
            for _ in range(60):
                msg = w.genie.get_message(space_id=space_id, conversation_id=conversation_id, message_id=message_id)
                if msg.status and msg.status.value in ("COMPLETED", "FAILED"):
                    break
                time.sleep(2)

            if msg.status and msg.status.value == "COMPLETED":
                # Extract the generated SQL from attachments
                genie_sql = None
                if msg.attachments:
                    for att in msg.attachments:
                        if att.query and att.query.query:
                            genie_sql = att.query.query
                            break

                result["genie_sql"] = genie_sql
                # Simple match: normalize and compare SQL
                if genie_sql and expected_sql:
                    norm_genie = " ".join(genie_sql.strip().lower().split())
                    norm_expected = " ".join(expected_sql.strip().lower().split())
                    if norm_genie == norm_expected:
                        result["status"] = "exact_match"
                        correct += 1
                    else:
                        # TODO: semantic comparison (run both, compare results)
                        result["status"] = "sql_mismatch"
                else:
                    result["status"] = "no_sql_generated"
            else:
                result["status"] = "failed"

        except Exception as e:
            result["status"] = f"error: {str(e)[:100]}"

        results.append(result)
        status_icon = "✓" if result["status"] == "exact_match" else "✗"
        print(f"  [{i+1}/{total}] {status_icon} {question[:60]}... → {result['status']}")

    overall_accuracy = correct / total if total > 0 else 0.0
    print(f"\n  Baseline accuracy: {overall_accuracy:.1%} ({correct}/{total})")
else:
    print("\n  ⏭ No space_id or no benchmarks — skipping evaluation (dry run)")
    overall_accuracy = 0.0

# COMMAND ----------

# DBTITLE 1,Persist baseline results to Delta
if catalog and schema:
    artifacts_table = f"`{catalog}`.`{schema}`.gso_prototype_artifacts"

    baseline_payload = json.dumps({
        "accuracy": overall_accuracy,
        "correct": correct,
        "total": total,
        "results": results,
    }, default=str)

    safe_payload = baseline_payload.replace("'", "''")
    spark.sql(f"""
        INSERT INTO {artifacts_table}
        VALUES (
            '{run_id}',
            'baseline_eval',
            '{safe_payload}',
            current_timestamp()
        )
    """)
    print(f"\n  ✓ Wrote baseline_eval artifact to {artifacts_table}")
else:
    print("\n  ⏭ No catalog/schema — skipping Delta write (dry run)")

# COMMAND ----------

# DBTITLE 1,Summary and exit
print("\n" + "=" * 60)
print("[TASK EVAL_BASELINE] Summary")
print("=" * 60)
print(f"  Benchmarks evaluated: {total}")
print(f"  Correct (exact match): {correct}")
print(f"  Baseline accuracy:    {overall_accuracy:.1%}")
print(f"  Results persisted:    {'Yes' if (catalog and schema) else 'No (dry run)'}")
print("=" * 60)

exit_payload = json.dumps({
    "status": "SUCCESS",
    "run_id": run_id,
    "accuracy": overall_accuracy,
    "correct": correct,
    "total": total,
})
print(f"\nExiting with: {exit_payload}")
dbutils.notebook.exit(exit_payload)
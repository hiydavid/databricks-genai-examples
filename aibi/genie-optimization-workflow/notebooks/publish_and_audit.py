# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# dependencies = [
#   "databricks-sdk>=0.102.0",
# ]
# ///
# DBTITLE 1,Publish & Audit — Genie Agent Optimization Workflow
# MAGIC %md
# MAGIC # Publish & Audit — Genie Agent Optimization Workflow
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
if max_rounds < 0:
    raise ValueError("max_rounds must be a nonnegative integer")

print("=" * 60)
print("[TASK PUBLISH] Publish & Audit — Prototype")
print("=" * 60)
print(f"  run_id:          {run_id}")
print(f"  space_id:        {space_id}")
print(f"  target_accuracy: {target_accuracy}")

# COMMAND ----------

# DBTITLE 1,Load all artifacts for this run
artifacts = {}
parse_errors = {}
artifacts_table = ".".join(
    f"`{part.replace('`', '``')}`" for part in (catalog, schema, "genie_agent_optimization_workflow_artifacts")
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
    approved_ids = []
# Benchmarks QC wrote itself (repair_and_augment); absent means none were added.
generated_ids = qc.get("generated_benchmark_question_ids", [])
if (not isinstance(generated_ids, list)
        or any(not isinstance(i, str) or i not in approved_ids for i in generated_ids)):
    errors.append("QC generated_benchmark_question_ids must be a list of approved benchmark IDs")
    generated_ids = []
elif approved_ids and set(approved_ids) <= set(generated_ids):
    errors.append("QC approved only generated benchmarks; no human-authored baseline remains")
generated_ids = set(generated_ids)
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
final_eval_run_id = opt.get("final_eval_run_id")
if not isinstance(final_eval_run_id, str) or not final_eval_run_id.strip():
    errors.append("Missing or invalid final_eval_run_id in optimization_result")
    final_eval_run_id = None
elif (type(rounds) is int and rounds > 0
        and final_eval_run_id == baseline.get("eval_run_id")):
    errors.append("final_eval_run_id is the baseline eval run, but optimization rounds ran")

# COMMAND ----------

# DBTITLE 1,Verify the final evaluation against the Genie API
# The optimizer reports its own final accuracy; re-read the eval run it names and use
# the API counts as the final accuracy. The reported value only has to agree to within
# half a question, so a rounded number (e.g. 0.89 for 17/19) is accepted.
if final_eval_run_id and final_accuracy is not None:
    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        final_eval = w.genie.genie_get_eval_run(space_id=space_id, eval_run_id=final_eval_run_id)
        final_eval_status = final_eval.eval_run_status.value if final_eval.eval_run_status else None
        total, correct = final_eval.num_questions, final_eval.num_correct
        if final_eval_status != "DONE":
            errors.append(f"Final evaluation {final_eval_run_id} has status {final_eval_status}")
        elif (type(total) is not int or total <= 0
                or type(correct) is not int or not 0 <= correct <= total):
            errors.append(f"Final evaluation {final_eval_run_id} has invalid accuracy counts")
        elif abs(correct / total - final_accuracy) > 0.5 / total:
            errors.append("Reported final accuracy does not match the final evaluation")
        # Same question count as the baseline; the API does not return question IDs.
        elif total != baseline.get("num_questions"):
            errors.append("Final evaluation did not cover the baseline's benchmark questions")
        else:
            final_accuracy = correct / total
    except Exception as e:
        errors.append(f"Could not read final evaluation {final_eval_run_id}: {e}")

# COMMAND ----------

# DBTITLE 1,Score the human-authored benchmarks separately
# When QC generated benchmarks, the optimizer is partly graded against gold SQL that
# Genie wrote. Accuracy on the human-authored subset shows whether gains hold up on
# questions the optimizer did not help write. GOOD counts as correct, as in num_correct.
human_ids = set(approved_ids) - generated_ids


def human_authored_accuracy(eval_run_id):
    correct = total = 0
    page_token = None
    while True:
        page = w.genie.genie_list_eval_results(
            space_id=space_id, eval_run_id=eval_run_id, page_token=page_token
        )
        for result in page.eval_results or []:
            if result.benchmark_question_id not in human_ids:
                continue
            details = w.genie.genie_get_eval_result_details(
                space_id=space_id, eval_run_id=eval_run_id, result_id=result.result_id
            )
            total += 1
            correct += details.assessment is not None and details.assessment.value == "GOOD"
        page_token = page.next_page_token
        if not page_token:
            break
    if total != len(human_ids):
        raise ValueError(f"expected {len(human_ids)} human-authored results, found {total}")
    return correct / total


human_baseline_accuracy = human_final_accuracy = None
if generated_ids and human_ids:
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    for label, eval_run_id in (("baseline", baseline.get("eval_run_id")), ("final", final_eval_run_id)):
        if not eval_run_id:
            continue  # already reported as an audit error above
        try:
            accuracy = human_authored_accuracy(eval_run_id)
        except Exception as e:
            errors.append(f"Could not score human-authored benchmarks in {label} eval {eval_run_id}: {e}")
            continue
        if label == "baseline":
            human_baseline_accuracy = accuracy
        else:
            human_final_accuracy = accuracy

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
    print(f"  Generated:            {len(generated_ids)}")
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
    print(f"  Final accuracy:     {final_accuracy if final_accuracy is not None else 'n/a'}")
    print(f"  Rounds executed:    {opt.get('rounds_executed', 'n/a')} of {max_rounds}")
    if generated_ids:
        print(f"  Human-authored only ({len(human_ids)} questions):")
        print(f"    Baseline:         {human_baseline_accuracy if human_baseline_accuracy is not None else 'n/a'}")
        print(f"    Final:            {human_final_accuracy if human_final_accuracy is not None else 'n/a'}")
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
    # None when QC generated no benchmarks: the overall accuracies are already human-authored.
    "generated_benchmark_count": len(generated_ids),
    "human_authored_baseline_accuracy": human_baseline_accuracy,
    "human_authored_final_accuracy": human_final_accuracy,
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

# Databricks notebook source

# MAGIC %md
# MAGIC # Genie Space Optimizer
# MAGIC
# MAGIC Evaluates a Genie Space using the Benchmark Evaluation API, tracks results
# MAGIC in MLflow, and produces an optimized configuration using prescriptive fixes.
# MAGIC
# MAGIC **Flow:**
# MAGIC 1. Get serialized Genie config
# MAGIC 2. Check benchmark questions (minimum 10)
# MAGIC 3. Validate benchmark Q&A pairs (LLM review)
# MAGIC 4. Create (or reuse) Genie benchmark eval run
# MAGIC 5. List eval results
# MAGIC 6. Score results and log to MLflow
# MAGIC 7. Compile prescriptive fixes from failures
# MAGIC 8. LLM optimization call
# MAGIC 9. Validate optimized config
# MAGIC 10. Create optimized space
# MAGIC 11. Re-evaluate optimized space and compare

# COMMAND ----------

# MAGIC %pip install databricks-sdk==0.102.0 mlflow[databricks]==3.10.1 openai==2.30.0 -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC Edit the values below before running.

# COMMAND ----------

SPACE_ID = ""  # Required: Genie Space ID to optimize
EXPERIMENT_ID = ""  # Required: MLflow Experiment ID (create one in the MLflow UI first)
EXISTING_EVAL_RUN_ID = ""  # Optional: reuse a completed eval run instead of creating a new one
LLM_ENDPOINT = (
    "databricks-claude-sonnet-4-6"  # Foundation model for generating optimized config
)
POLL_INTERVAL_SECONDS = 30  # Seconds between eval run status polls
POLL_TIMEOUT_SECONDS = 1800  # Max seconds to wait for eval run completion

# COMMAND ----------

import mlflow
from scorers import score_llm_judge, score_overall_assessment, score_result_comparison

if not EXPERIMENT_ID:
    raise ValueError(
        "EXPERIMENT_ID is required — create an MLflow Experiment in the UI first, "
        "then set EXPERIMENT_ID in the Configuration cell above."
    )

experiment = mlflow.get_experiment(EXPERIMENT_ID)
if experiment is None or experiment.lifecycle_stage == "deleted":
    raise ValueError(
        f"MLflow Experiment '{EXPERIMENT_ID}' does not exist or has been deleted. "
        "Create one in the MLflow UI first."
    )

mlflow.set_experiment(experiment_id=EXPERIMENT_ID)

print(f"MLflow experiment: {experiment.name} (ID: {EXPERIMENT_ID})")
print(
    "Scorers: overall_assessment, result_comparison (deterministic), llm_judge (semantic)"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Get Serialized Genie Config

# COMMAND ----------

import json

from genie_client import GenieClient
from validation import _text_from_value

if not SPACE_ID:
    raise ValueError("SPACE_ID is required — set it in the Configuration cell above")

genie = GenieClient()
space_info = genie.get_space_config(SPACE_ID)
serialized_space = space_info["serialized_space"]

tables = serialized_space.get("data_sources", {}).get("tables", [])
benchmarks_section = serialized_space.get("benchmarks", {})
benchmark_questions = benchmarks_section.get("questions", [])
instructions = serialized_space.get("instructions", {})

print(f"Space: {space_info['title']}")
print(f"Space ID: {SPACE_ID}")
print(f"Tables: {len(tables)}")
print(f"Benchmark questions: {len(benchmark_questions)}")
print(f"Text instructions: {len(instructions.get('text_instructions', []))}")
print(f"Example SQLs: {len(instructions.get('example_question_sqls', []))}")
print(f"Join specs: {len(instructions.get('join_specs', []))}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Check Benchmark Questions (Minimum 10)

# COMMAND ----------

MIN_BENCHMARKS = 10

if len(benchmark_questions) < MIN_BENCHMARKS:
    msg = (
        f"Need at least {MIN_BENCHMARKS} benchmark questions, "
        f"found {len(benchmark_questions)}. "
        "Add more benchmark Q&A pairs to the Genie Space before running optimization."
    )
    raise ValueError(msg)

print(
    f"✓ {len(benchmark_questions)} benchmark questions available (minimum: {MIN_BENCHMARKS})"
)

# Display benchmark questions
for i, q in enumerate(benchmark_questions, 1):
    print(f"  {i}. {_text_from_value(q.get('question'))[:120]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Validate Benchmark Q&A Pairs
# MAGIC
# MAGIC Uses an LLM to review each benchmark question-answer pair and determine
# MAGIC whether the ground truth SQL actually answers what the question asks.
# MAGIC This catches issues like extra/missing columns, wrong aggregations, or
# MAGIC SQL that answers a different question entirely.

# COMMAND ----------

from benchmark_validator import validate_benchmarks

print("Validating benchmark Q&A pairs...")
qa_validation = validate_benchmarks(
    benchmark_questions=benchmark_questions,
    llm_endpoint=LLM_ENDPOINT,
)

summary = qa_validation["summary"]
print(f"\n✓ Benchmark Q&A validation complete:")
print(f"  PASS: {summary['pass']}/{summary['total']}")
print(f"  WARN: {summary['warn']}/{summary['total']}")
print(f"  FAIL: {summary['fail']}/{summary['total']}")
if summary["error"] > 0:
    print(f"  ERROR: {summary['error']}/{summary['total']}")

if summary["fail"] > 0 or summary["warn"] > 0:
    print()
    print(qa_validation["report"])
    print()
    print(
        "⚠ Some benchmark Q&A pairs may have issues. Review the report above."
    )
    print(
        "  The pipeline will continue, but eval results for flagged benchmarks "
        "may not be meaningful."
    )
    print(
        "  To fix: edit the benchmark questions/SQL in the Genie Space UI, "
        "then re-run."
    )
else:
    print("  All benchmark Q&A pairs look correct.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create (or Reuse) Genie Benchmark Eval Run

# COMMAND ----------

if EXISTING_EVAL_RUN_ID:
    eval_run_id = EXISTING_EVAL_RUN_ID
    print(f"Reusing existing eval run: {eval_run_id}")
    completed_run = genie.get_eval_run(SPACE_ID, eval_run_id)
    status = completed_run.get("eval_run_status", "")
    if status != "DONE":
        raise ValueError(
            f"Existing eval run {eval_run_id} has status '{status}', expected 'DONE'"
        )
    print(f"✓ Eval run status: {status}")
else:
    eval_run = genie.create_eval_run(SPACE_ID)
    eval_run_id = eval_run.get("eval_run_id")
    if not eval_run_id:
        raise ValueError(f"Could not extract eval_run_id from response: {eval_run}")
    print(f"Eval run created: {eval_run_id}")
    print(f"Status: {eval_run.get('eval_run_status', 'unknown')}")

    print(
        f"Polling eval run (interval={POLL_INTERVAL_SECONDS}s, timeout={POLL_TIMEOUT_SECONDS}s)..."
    )
    completed_run = genie.poll_eval_run(
        SPACE_ID,
        eval_run_id,
        poll_interval=POLL_INTERVAL_SECONDS,
        timeout=POLL_TIMEOUT_SECONDS,
    )
    print(f"✓ Eval run completed: {completed_run.get('eval_run_status', 'DONE')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: List Eval Results

# COMMAND ----------

eval_results_list = genie.list_eval_results(SPACE_ID, eval_run_id)
print(f"✓ {len(eval_results_list)} eval results ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Score Results and Log to MLflow
# MAGIC
# MAGIC For each result, fetch details from the Genie API and score with
# MAGIC deterministic scorers. Results are logged to MLflow as metrics and a
# MAGIC results table for experiment tracking.

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor

import pandas as pd

# Build lookup: benchmark_question_id → question text
benchmark_question_lookup = {}
for q in benchmark_questions:
    qid = q.get("id", "")
    text = _text_from_value(q.get("question"))
    benchmark_question_lookup[qid] = text


# Fetch detailed results for each benchmark question (parallelized)
def _fetch_detail(result):
    result_id = result.get("result_id")
    if not result_id:
        return None
    return genie.get_eval_result_details(SPACE_ID, eval_run_id, result_id)


with ThreadPoolExecutor(max_workers=10) as executor:
    all_details = [
        d for d in executor.map(_fetch_detail, eval_results_list) if d is not None
    ]

print(f"Fetched {len(all_details)} detailed results")


# Helper: extract SQL from actual_response / expected_response
def _extract_sql(response_list):
    """Extract SQL string from the API's response array."""
    if not response_list:
        return ""
    for entry in response_list:
        if entry.get("response_type") == "SQL" and entry.get("response"):
            return entry["response"]
        if entry.get("response"):
            return entry["response"]
    return ""


def _score_eval_details(details, question_lookup, run_name=None):
    """Score eval details and log to MLflow. Returns (results_df, metrics, run_id)."""
    scorer_names = ["overall_assessment", "result_comparison", "llm_judge"]
    eval_rows = []
    for d in details:
        question_id = d.get("benchmark_question_id", "")
        question = question_lookup.get(question_id, question_id)
        generated_sql = _extract_sql(d.get("actual_response"))
        expected_sql = _extract_sql(d.get("expected_response"))
        assessment = d.get("assessment", "")
        reasons = d.get("assessment_reasons", [])

        row = {
            "question": question,
            "generated_sql": generated_sql,
            "expected_sql": expected_sql,
            "assessment": assessment,
            "assessment_reasons": ", ".join(reasons),
        }
        for scorer_fn in [score_overall_assessment, score_result_comparison, score_llm_judge]:
            result = scorer_fn(question, generated_sql, expected_sql, assessment, reasons)
            row[result["name"]] = result["value"]
            row[f"{result['name']}/rationale"] = result["rationale"]

        eval_rows.append(row)

    results_df = pd.DataFrame(eval_rows)
    metrics = {}
    for name in scorer_names:
        pass_count = (results_df[name] == "yes").sum()
        metrics[f"{name}/pass_rate"] = pass_count / len(results_df)

    with mlflow.start_run(run_name=run_name) as run:
        mlflow.log_metrics(metrics)
        mlflow.log_table(results_df, artifact_file="eval_results.json")
        run_id = run.info.run_id

    return results_df, metrics, run_id


# Score baseline results
results_df, baseline_metrics, mlflow_run_id = _score_eval_details(
    all_details, benchmark_question_lookup, run_name="baseline",
)

print(f"\n✓ MLflow Run ID: {mlflow_run_id}")
print(f"  View in MLflow experiment: {experiment.name}")
for name, value in sorted(baseline_metrics.items()):
    print(f"  {name}: {value:.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Compile Prescriptive Fixes from Failures

# COMMAND ----------

from fix_taxonomy import compile_fix_report

# Collect all triggered assessment reasons from failed benchmarks
failed_details = [d for d in all_details if d.get("assessment") != "GOOD"]

scorer_results = []
for d in failed_details:
    question_id = d.get("benchmark_question_id", "")
    question = benchmark_question_lookup.get(question_id, question_id)
    for reason in d.get("assessment_reasons", []):
        scorer_results.append(
            {
                "label": reason,
                "question": question,
                "expected_sql": _extract_sql(d.get("expected_response")),
                "generated_sql": _extract_sql(d.get("actual_response")),
            }
        )

fix_report = compile_fix_report(scorer_results)

correct_count = len(all_details) - len(failed_details)
print(
    f"Results: {correct_count}/{len(all_details)} correct, {len(failed_details)} failures"
)
print(f"Total assessment reasons triggered: {len(scorer_results)}")
print()
print(fix_report)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: LLM Optimization Call

# COMMAND ----------

if not failed_details:
    print("✓ All benchmarks passed — no optimization needed.")
    optimized_space = serialized_space
else:
    from llm_optimizer import optimize_config

    print(f"Calling {LLM_ENDPOINT} to generate optimized config...")

    optimized_space = optimize_config(
        serialized_space=serialized_space,
        fix_report=fix_report,
        llm_endpoint=LLM_ENDPOINT,
    )
    print("✓ Optimized config generated")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Validate Optimized Config

# COMMAND ----------

from validation import normalize_serialized_space, validate_serialized_space

# Normalize ordering
optimized_space = normalize_serialized_space(optimized_space)

# Validate
validation = validate_serialized_space(optimized_space)

if validation["valid"]:
    print("✓ Optimized config is valid")
else:
    print(f"✗ Validation errors ({len(validation['errors'])}):")
    for err in validation["errors"]:
        print(f"  - {err}")

if validation["warnings"]:
    print(f"\nWarnings ({len(validation['warnings'])}):")
    for warn in validation["warnings"]:
        print(f"  - {warn}")

overlap_warnings = [w for w in validation["warnings"] if "similarity" in w]
if not overlap_warnings and failed_details:
    print("  (benchmark overlap was checked and resolved during LLM optimization)")

# Show diff summary
def _count(space, *keys):
    val = space
    for k in keys:
        val = val.get(k, {}) if isinstance(val, dict) else {}
    return len(val) if isinstance(val, list) else 0

def _count_columns_with(space, field):
    total = 0
    for table in space.get("data_sources", {}).get("tables", []):
        for col in table.get("column_configs", []):
            if col.get(field):
                total += 1
    return total

diff_items = [
    ("Tables", _count(serialized_space, "data_sources", "tables"), _count(optimized_space, "data_sources", "tables")),
    ("Example SQLs", _count(serialized_space, "instructions", "example_question_sqls"), _count(optimized_space, "instructions", "example_question_sqls")),
    ("Join specs", _count(serialized_space, "instructions", "join_specs"), _count(optimized_space, "instructions", "join_specs")),
    ("Text instructions", _count(serialized_space, "instructions", "text_instructions"), _count(optimized_space, "instructions", "text_instructions")),
    ("SQL functions", _count(serialized_space, "instructions", "sql_functions"), _count(optimized_space, "instructions", "sql_functions")),
    ("Filter snippets", _count(serialized_space, "instructions", "sql_snippets", "filters"), _count(optimized_space, "instructions", "sql_snippets", "filters")),
    ("Expression snippets", _count(serialized_space, "instructions", "sql_snippets", "expressions"), _count(optimized_space, "instructions", "sql_snippets", "expressions")),
    ("Measure snippets", _count(serialized_space, "instructions", "sql_snippets", "measures"), _count(optimized_space, "instructions", "sql_snippets", "measures")),
    ("Columns with descriptions", _count_columns_with(serialized_space, "description"), _count_columns_with(optimized_space, "description")),
    ("Columns with synonyms", _count_columns_with(serialized_space, "synonyms"), _count_columns_with(optimized_space, "synonyms")),
]

print(f"\nConfig changes:")
for label, orig, opt in diff_items:
    marker = " *" if orig != opt else ""
    print(f"  {label}: {orig} → {opt}{marker}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Create Optimized Space

# COMMAND ----------

from datetime import datetime

optimized_space_id = None

if not validation["valid"]:
    print("✗ Cannot create optimized space — config has validation errors.")
    print("  Fix validation errors and re-run.")
elif not failed_details:
    print("✓ All benchmarks passed — no optimized space needed.")
else:
    title_prefix = f"[Optimized {datetime.now().strftime('%Y-%m-%d')}] "
    create_result = genie.create_optimized_space(
        original_space_id=SPACE_ID,
        updated_config=optimized_space,
        title_prefix=title_prefix,
        space_info=space_info,
    )
    optimized_space_id = create_result["new_space_id"]
    print(f"✓ Optimized space created:")
    print(f"  Title: {create_result['new_space_title']}")
    print(f"  Space ID: {optimized_space_id}")
    print(f"  URL: {create_result['new_space_url']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11: Re-Evaluate Optimized Space and Compare
# MAGIC
# MAGIC Run the same benchmark eval against the optimized space and compare
# MAGIC before/after accuracy.

# COMMAND ----------

optimized_metrics = None

if not optimized_space_id:
    print("Skipping re-evaluation — no optimized space was created.")
else:
    print(f"Running benchmark eval on optimized space {optimized_space_id}...")

    opt_eval_run = genie.create_eval_run(optimized_space_id)
    opt_eval_run_id = opt_eval_run.get("eval_run_id")
    if not opt_eval_run_id:
        raise ValueError(f"Could not extract eval_run_id from response: {opt_eval_run}")

    print(f"Eval run created: {opt_eval_run_id}")
    print(
        f"Polling (interval={POLL_INTERVAL_SECONDS}s, timeout={POLL_TIMEOUT_SECONDS}s)..."
    )
    genie.poll_eval_run(
        optimized_space_id,
        opt_eval_run_id,
        poll_interval=POLL_INTERVAL_SECONDS,
        timeout=POLL_TIMEOUT_SECONDS,
    )
    print("✓ Optimized eval run completed")

    # Fetch detailed results
    opt_results_list = genie.list_eval_results(optimized_space_id, opt_eval_run_id)

    def _fetch_opt_detail(r):
        rid = r.get("result_id")
        if not rid:
            return None
        return genie.get_eval_result_details(optimized_space_id, opt_eval_run_id, rid)

    with ThreadPoolExecutor(max_workers=10) as executor:
        opt_details = [
            d for d in executor.map(_fetch_opt_detail, opt_results_list) if d is not None
        ]

    print(f"Fetched {len(opt_details)} detailed results")

    # Score and log to MLflow
    opt_results_df, optimized_metrics, opt_run_id = _score_eval_details(
        opt_details, benchmark_question_lookup, run_name="optimized",
    )
    print(f"✓ Optimized MLflow Run ID: {opt_run_id}")

    # Compare before vs after
    print(f"\n{'Scorer':<25} {'Before':>10} {'After':>10} {'Delta':>10}")
    print("-" * 57)
    for name in sorted(baseline_metrics):
        before = baseline_metrics[name]
        after = optimized_metrics[name]
        delta = after - before
        sign = "+" if delta > 0 else ""
        print(f"  {name:<23} {before:>9.1%} {after:>9.1%} {sign}{delta:>8.1%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("Genie Space Optimization Summary")
print("=" * 60)
print(f"Space: {space_info['title']} ({SPACE_ID})")
print(f"Benchmark questions evaluated: {len(all_details)}")
print(f"Correct: {correct_count}/{len(all_details)}")
print(f"Failures: {len(failed_details)}/{len(all_details)}")
print(f"Assessment reasons triggered: {len(scorer_results)}")
print(f"MLflow experiment: {experiment.name}")
print(f"Baseline MLflow run ID: {mlflow_run_id}")
print(f"Config valid: {validation['valid']}")
if optimized_space_id:
    print(f"Optimized space: {optimized_space_id}")
if optimized_metrics:
    print(f"\nAccuracy comparison:")
    for name in sorted(baseline_metrics):
        before = baseline_metrics[name]
        after = optimized_metrics[name]
        delta = after - before
        sign = "+" if delta > 0 else ""
        print(f"  {name}: {before:.1%} → {after:.1%} ({sign}{delta:.1%})")
if qa_validation["summary"]["fail"] > 0 or qa_validation["summary"]["warn"] > 0:
    print(
        f"\n⚠ Benchmark quality concerns: "
        f"{qa_validation['summary']['fail']} FAIL, "
        f"{qa_validation['summary']['warn']} WARN"
    )
    print(
        "  Review Step 3 output — eval results for flagged benchmarks "
        "may not be reliable."
    )
print("=" * 60)

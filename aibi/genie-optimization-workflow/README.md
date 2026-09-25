# Genie Agent Optimization Workflow

A prototype workflow that automatically improves the accuracy of a [Databricks Genie Agent](https://docs.databricks.com/aws/en/genie-agents/) by running a closed-loop pipeline: snapshot the space, validate the benchmark set, measure baseline accuracy, apply optimization "levers," and re-evaluate until a target accuracy is reached (or rounds are exhausted).

The pipeline is a 5-task Databricks job that mixes standard notebook tasks with **Genie Code automations** (agentic tasks driven by natural-language prompts). All run state is passed between tasks through a Delta artifacts table.

## Pipeline

```
intake_and_snapshot   (notebook)      → fetch space config, write run manifest + snapshot
        ↓
benchmark_qc          (Genie Code)    → validate / repair the benchmark question set
        ↓
begin_baseline_run    (notebook)      → start the benchmark eval run, poll to completion, record status + accuracy
        ↓
optimize              (Genie Code)    → analyze failures, apply levers, re-evaluate in a loop
        ↓
publish_and_audit     (notebook)      → compile audit report, write final run summary
```

Each task writes a row to `<catalog>.<schema>.gso_prototype_artifacts` keyed by `run_id`, which is how downstream tasks (including the Genie Code prompts) read the prior task's output.

### Tasks

| Task | Type | What it does |
|------|------|--------------|
| `intake_and_snapshot` | Notebook | Fetches the Genie Space config (`w.genie.get_space` with the full serialized space) and writes `run_manifest` + `space_config_snapshot` artifacts. |
| `benchmark_qc` | Genie Code | Reviews the space's own benchmark set (question clarity, gold SQL validity, question↔SQL alignment) using its Genie benchmarking knowledge, repairs benchmarks in place (up to `benchmark_repair_max_tries` passes, gated by `benchmark_policy`), skips what it cannot fix, and writes a `benchmark_qc` artifact with counts, repair rationale, the approved `benchmark_question_ids`, and whether ≥15 valid benchmarks remain. |
| `begin_baseline_run` | Notebook | Requires a nonempty `approved_benchmark_question_ids` list from QC, starts a Genie benchmark eval run (`genie_create_eval_run`) on those questions, and polls it to completion. Writes a `baseline_run` artifact with the `eval_run_id`, status, accuracy counts, and any error. Failed or unfinished evaluations fail the task after diagnostics are saved. |
| `optimize` | Genie Code | Iterative loop (up to `max_rounds`), each round has four phases: ANALYZE (classify failures by root cause), RECOMMEND (specific changes + expected impact), ACT (apply levers), RE-EVALUATE (rerun the same benchmark questions as the baseline). Stops early when accuracy ≥ `target_accuracy`. Changes stack — never reverted between rounds. Task timeout is 4 hours, since each round runs its own eval. |
| `publish_and_audit` | Notebook | Runs after upstream tasks finish, even if they failed (`run_if: ALL_DONE`). Validates required artifacts, re-reads the optimizer's `final_eval_run_id` from the Genie API to confirm the reported final accuracy, and captures the post-optimization space snapshot (`space_config_post_opt`, full serialized space). Writes a `run_summary` with accuracy and audit completeness; incomplete audits save their errors and fail the task. |

### Optimization levers

The `optimize` prompt can act through four levers (selectable via the `levers` job parameter):

1. **Space instructions** — append general guidance to the Genie Space
2. **Table descriptions** — `COMMENT ON TABLE ...`
3. **Column descriptions** — `ALTER TABLE ... ALTER COLUMN ... COMMENT ...`
4. **Example SQL / certified questions** — curated question→SQL pairs

## Repository layout

```
genie-optimization-workflow/
├── deploy.py                     # Databricks notebook source (creates the automations + 5-task job)
├── notebooks/
│   ├── intake_and_snapshot.py    # Databricks notebook source (task 1)
│   ├── begin_baseline_run.py     # Databricks notebook source (task 3)
│   └── publish_and_audit.py      # Databricks notebook source (task 5)
├── prompts/
│   ├── benchmark_qc.md           # Prompt for the benchmark_qc Genie Code automation
│   └── optimize.md               # Prompt for the optimize Genie Code automation
└── tests/
    └── test_workflow.py          # Local tests for the task notebooks (mocked SDK + Spark)
```

All `.py` files are Databricks notebook sources — upload them to the workspace as notebooks (the `%magic` and `# COMMAND ----------` markers are the Databricks format). The Genie Code tasks (`benchmark_qc`, `optimize`) have no notebook; their logic lives entirely in the prompt `.md` files, which `deploy` registers as Genie Code automations.

## Deployment

### Prerequisites

- The **Genie Code Job Task** beta enabled for your account/workspace in the [Databricks preview portal](https://previews.databricks.com). Without it, the `genie_task` entries in the job definition are not recognized — the job is still created, but the Genie Code tasks appear in the workflow UI as unconfigured tasks you must set up manually.
- Databricks CLI installed and configured (used for uploading notebooks and triggering runs — all SDK code runs inside the workspace, not locally)
- Benchmark questions loaded into the Genie Space (the eval-run API evaluates the space's own benchmark set; up to 500 questions per space)
- A SQL warehouse ID for validating benchmark SQL
- Serverless compute for notebook tasks. The job defines no clusters, so the notebook tasks run on serverless; each notebook declares environment version 6 and `databricks-sdk>=0.102.0` (the version exposing `genie_create_eval_run` / `genie_get_eval_run`) in its header

### Steps

1. Upload the notebooks to a workspace directory (as notebooks, not raw files). Include the `notebooks/` and `prompts/` folders — the prompt `.md` files become plain Workspace files that the deploy notebook reads directly:

   ```bash
   databricks workspace import-dir ./genie-optimization-workflow \
       /Workspace/Users/you@company.com/gso-prototype
   ```

   (or upload the `.py` files individually via the workspace UI as notebooks)

   Any directory works, but keep `deploy`, `notebooks/`, and `prompts/` together: `deploy` finds the task notebooks and prompts relative to its own location.

2. Deploy the automations and job: open the `deploy` notebook in the target workspace, run the first cell (**Widgets**) to create the widgets, fill them in, then click *Run all*. It authenticates with the notebook's own context — no token needed.

   Parameters (widgets at the top of the notebook):

   | Widget | Default | Description |
   |--------|---------|-------------|
   | `space_id` | `""` | Target Genie Space, saved as the job's `space_id` default |
   | `catalog` / `schema` | `""` | Unity Catalog location for the artifacts table, saved as job defaults |
   | `warehouse_id` | `""` | SQL warehouse for validating benchmark SQL, saved as a job default |

   Set `space_id`, `catalog`, `schema`, and `warehouse_id` here so the job runs against your space with a plain **Run now**. If `space_id`, `catalog`, or `schema` is empty, the job's default run is a dry run.

   `deploy` creates two Genie Code automations (via the internal scheduled-insights API) and one job named `gso-prototype-v2`, wiring the automation `configuration_id`s into the job's `genie_task` entries.

> **One-time only**: deploying always creates new automations and a new job — it does not update or reuse existing ones. Re-running the deploy notebook duplicates both; delete the old job and automations first if you need to redeploy.

3. Run the job: click **Run now** on the job page, or `databricks jobs run-now <job_id>`.

   To point the job at a different space later, edit the defaults under **Job parameters** in the job's details panel (no redeploy needed). For a one-off run against another space, use **Run now with different parameters**, or override at run time:

   ```bash
   databricks jobs run-now <job_id> --json '{
     "job_parameters": {
       "space_id": "<genie-space-id>",
       "catalog": "<catalog>",
       "schema": "<schema>",
       "warehouse_id": "<warehouse-id>"
     }
   }'
   ```

## Job parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `run_id` | `{{job.run_id}}` | Run identifier; auto-set to the job run ID each run (overridable at run-now) |
| `space_id` | `deploy` widget | Target Genie Space. Empty → dry run (tasks skip API/Delta work) |
| `catalog` / `schema` | `deploy` widgets | Unity Catalog location for artifacts. Either empty → dry run |
| `levers` | `[1,2,3,4]` | Which optimization levers the optimizer may use |
| `max_rounds` | `3` | Max optimization iterations |
| `target_accuracy` | `0.90` | Stop when accuracy reaches this |
| `benchmark_policy` | `repair_allowed` | Whether QC may repair broken benchmarks |
| `benchmark_repair_max_tries` | `3` | Repair attempts per benchmark |
| `warehouse_id` | `deploy` widget | SQL warehouse for validating benchmark SQL |
| `triggered_by` | `""` | Leave empty: `intake_and_snapshot` looks up who started the run (`creator_user_name` from the Jobs API) and records it with the trigger type in the run manifest. Set it only to attribute a run to someone else, e.g. from an external orchestrator |

## Artifacts table

All tasks read/write `<catalog>.<schema>.gso_prototype_artifacts`:

`space_config_snapshot` (written by `intake_and_snapshot`, before optimization) and `space_config_post_opt` (written by `publish_and_audit`, after optimization) pair up as the before/after audit trail of the Genie Space. Both capture the full serialized space (`get_space` with `include_serialized_space=True`) — instructions, data sources, and benchmarks — so benchmark_qc repairs and optimize's space-level changes are diffable. UC-level changes the optimizer may apply (table/column comments) live in Unity Catalog metadata, not in `get_space` output, and are recorded only in the `optimization_result` artifact.

| Column | Description |
|--------|-------------|
| `run_id` | Groups all rows from one pipeline run |
| `artifact_type` | `run_manifest`, `space_config_snapshot`, `benchmark_qc`, `baseline_run`, `optimization_result`, `space_config_post_opt`, `run_summary` |
| `payload` | JSON string with the artifact contents |
| `created_at` | Timestamp |

JSON payloads and run IDs are bound as SQL parameters. This preserves nested
serialized-space JSON, SQL quotes, backslashes, and newlines without manual escaping.

## Run outcomes

- **Dry run:** if any of `space_id`, `catalog`, or `schema` is empty, each task
  exits before API calls or Delta reads/writes. Fully configured runs require a
  nonempty `run_id`; the job supplies it automatically.
- **QC handoff:** missing, malformed, or empty approvals stop baseline evaluation.
  The workflow never falls back to all benchmarks. A corpus of 1–14 approved
  questions may run, with `is_sufficient = false` recorded in QC.
- **Baseline failure:** API errors, unsuccessful terminal statuses, polling
  exhaustion, and invalid counts produce a `baseline_run` diagnostic with
  `status = 'FAILED'`, an error, and `accuracy = null`; the notebook then raises
  an error. The optimizer does not run; the audit still runs and records an
  incomplete `run_summary`.
- **Incomplete audit:** missing/invalid required artifacts, a final accuracy that
  does not match the `final_eval_run_id` eval run, or a failed final snapshot produce a `run_summary` with `status = 'INCOMPLETE'`,
  `audit_complete = false`, `errors`, and `target_met = null`; the audit task
  then raises an error. Final accuracy is never inferred from the baseline.
- **Completed run:** a valid audit has `status = 'SUCCESS'` and
  `audit_complete = true`. Finishing below target is still a successful workflow
  execution, with `target_met = false`.

## Local checks

From this example directory, run the notebook regression tests with Python's
standard library:

```bash
python3 -m unittest discover -s tests -v
```

These execute the notebook source with mocked Databricks and Spark interfaces.
They cover artifact values, QC approvals, evaluation failures, dry runs, and
audit completeness. They do not replace a workspace smoke test of the native
benchmark API and Genie Code automations.

## Prototype limitations

- **Prompt-defined tasks**: Genie Code chooses its actions from the prompts. Downstream notebooks validate required handoff fields, and re-check the final accuracy against the named eval run, but do not independently verify every claimed optimization change. The eval-run API does not return question IDs, so the audit checks the final run covers the same number of questions as the baseline, not the exact set.
- **Internal API**: the `deploy` notebook uses the `/api/2.0/alerts-internal/scheduled-insights` endpoint for Genie Code automations, which is internal and may change.

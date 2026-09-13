# Genie Space Optimization (GSO) Workflow

A prototype workflow that automatically improves the accuracy of a [Databricks Genie Space](https://docs.databricks.com/aws/en/genie/) by running a closed-loop pipeline: snapshot the space, validate the benchmark set, measure baseline accuracy, apply optimization "levers," and re-evaluate until a target accuracy is reached (or rounds are exhausted).

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
| `begin_baseline_run` | Notebook | Starts a Genie benchmark eval run (`genie_create_eval_run`) on the benchmark_qc-approved `benchmark_question_ids` (all questions if no artifact), polls it to completion, and writes a `baseline_run` artifact with the `eval_run_id`, final status, and accuracy counts. The `optimize` task reads the per-question results via that `eval_run_id`. |
| `optimize` | Genie Code | Iterative loop (up to `max_rounds`), each round has four phases: ANALYZE (classify failures by root cause), RECOMMEND (specific changes + expected impact), ACT (apply levers), RE-EVALUATE (rerun the same benchmark questions as the baseline). Stops early when accuracy ≥ `target_accuracy`. Changes stack — never reverted between rounds. |
| `publish_and_audit` | Notebook | Reads all artifacts for the run, captures the post-optimization space snapshot (`space_config_post_opt`, full serialized space), prints an audit report (QC stats, baseline vs. final accuracy, per-round changes, target met?), and writes the `run_summary` artifact. |

### Optimization levers

The `optimize` prompt can act through four levers (selectable via the `levers` job parameter):

1. **Space instructions** — append general guidance to the Genie Space
2. **Table descriptions** — `COMMENT ON TABLE ...`
3. **Column descriptions** — `ALTER TABLE ... ALTER COLUMN ... COMMENT ...`
4. **Example SQL / certified questions** — curated question→SQL pairs

## Repository layout

```
genie-optimization-workflow/
├── deploy.py                 # Databricks notebook source (creates the automations + 5-task job)
├── intake_and_snapshot.py    # Databricks notebook source (task 1)
├── begin_baseline_run.py     # Databricks notebook source (task 3)
├── publish_and_audit.py      # Databricks notebook source (task 5)
└── prompts/
    ├── benchmark_qc.md       # Prompt for the benchmark_qc Genie Code automation
    └── optimize.md           # Prompt for the optimize Genie Code automation
```

All `.py` files are Databricks notebook sources — upload them to the workspace as notebooks (the `%magic` and `# COMMAND ----------` markers are the Databricks format). The Genie Code tasks (`benchmark_qc`, `optimize`) have no notebook; their logic lives entirely in the prompt `.md` files, which `deploy` registers as Genie Code automations.

## Deployment

### Prerequisites

- Python with `databricks-sdk` installed
- `DATABRICKS_HOST` / `DATABRICKS_TOKEN` (or another SDK auth method) pointing at the target workspace
- Benchmark questions loaded into the Genie Space (the eval-run API evaluates the space's own benchmark set; up to 500 questions per space)
- A SQL warehouse ID for validating benchmark SQL

### Steps

1. Upload the notebooks to a workspace directory (as notebooks, not raw files). Include the `prompts/` folder — the prompt `.md` files become plain Workspace files that the deploy notebook reads directly:

   ```bash
   databricks workspace import-dir ./genie-optimization-workflow \
       /Workspace/Users/you@company.com/gso-prototype
   ```

   (or upload the `.py` files individually via the workspace UI as notebooks)

2. Deploy the automations and job: open the `deploy` notebook in the target workspace and click *Run all*. It authenticates with the notebook's own context — no token needed.

   Optional parameters (widgets at the top of the notebook):

   | Widget | Default | Description |
   |--------|---------|-------------|
   | `notebook_root` | `<your home>/gso-prototype` | Workspace path holding the task notebooks |
   | `prompts_dir` | `<notebook_root>/prompts` | Workspace path holding the prompt `.md` files |

   `deploy` creates two Genie Code automations (via the internal scheduled-insights API) and one job named `gso-prototype-v2`, wiring the automation `configuration_id`s into the job's `genie_task` entries.

3. Run the job:

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
| `run_id` | `""` | Run identifier; empty = ad-hoc (tasks use whatever the widget holds) |
| `space_id` | `""` | Target Genie Space. Empty → dry run (tasks skip API/Delta work) |
| `catalog` / `schema` | `""` | Unity Catalog location for artifacts |
| `levers` | `[1,2,3,4,5,6]` | Which optimization levers the optimizer may use |
| `max_rounds` | `3` | Max optimization iterations |
| `target_accuracy` | `0.90` | Stop when accuracy reaches this |
| `benchmark_policy` | `repair_allowed` | Whether QC may repair broken benchmarks |
| `benchmark_repair_max_tries` | `3` | Repair attempts per benchmark |
| `warehouse_id` | `""` | SQL warehouse for validating benchmark SQL |
| `llm_model` | `databricks-claude-sonnet-4-6` | Model for Genie Code tasks |
| `triggered_by` | `""` | Free-form provenance field |

## Artifacts table

All tasks read/write `<catalog>.<schema>.gso_prototype_artifacts`:

`space_config_snapshot` (written by `intake_and_snapshot`, before optimization) and `space_config_post_opt` (written by `publish_and_audit`, after optimization) pair up as the before/after audit trail of the Genie Space. Both capture the full serialized space (`get_space` with `include_serialized_space=True`) — instructions, data sources, and benchmarks — so benchmark_qc repairs and optimize's space-level changes are diffable. UC-level changes the optimizer may apply (table/column comments) live in Unity Catalog metadata, not in `get_space` output, and are recorded only in the `optimization_result` artifact.

| Column | Description |
|--------|-------------|
| `run_id` | Groups all rows from one pipeline run |
| `artifact_type` | `run_manifest`, `space_config_snapshot`, `benchmark_qc`, `baseline_run`, `optimization_result`, `space_config_post_opt`, `run_summary` |
| `payload` | JSON string with the artifact contents |
| `created_at` | Timestamp |

## Prototype limitations

- **String-interpolated SQL**: notebooks build `INSERT`/`SELECT` statements via f-strings rather than parameterized queries — fine for a prototype with internal parameters, but not safe against arbitrary input.
- **Prompt-defined tasks**: the Genie Code tasks execute whatever the LLM decides from the prompt; there is no hard guarantee on artifact shape beyond what the prompt asks for.
- **Internal API**: the `deploy` notebook uses the `/api/2.0/alerts-internal/scheduled-insights` endpoint for Genie Code automations, which is internal and may change.
- **Dry-run support**: every notebook degrades gracefully when `space_id` / `catalog` / `schema` are empty, so the DAG can run end-to-end without touching a real space.

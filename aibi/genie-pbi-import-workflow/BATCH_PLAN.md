# Batch Import Plan

Status: design only, not implemented.

## Goal

Run one job that imports every `.pbit` file in a Unity Catalog volume (for example, 50 files) and creates one metric view and one Genie Agent per file. Today the workflow handles one `.pbit` per run, so 50 files means 50 manually triggered runs that execute one after another.

## Approach

Keep the existing 6-task job (`genie-pbi-import-workflow`) as the **per-file child job**, and add a **parent job** that discovers the files and runs the child once per file.

A Lakeflow Jobs `for_each_task` repeats exactly one nested task, not a DAG. The per-file pipeline is a 6-task DAG with a condition task and `run_if: ALL_DONE`, so it cannot be nested directly. The nested task is a `run_job_task` that triggers the child job instead.

```
genie-pbi-import-batch  (new parent job)
  discover_files        (notebook)  → list volume, build per-file params, set task value
        ↓
  import_each           (for_each_task, concurrency N)
     └─ run_job_task → genie-pbi-import-workflow (existing 6-task job, DAG unchanged)
        ↓
  batch_report          (notebook, run_if ALL_DONE) → roll up audit_summary across child runs
```

What this gives us without custom code:

- **Isolation:** each file gets its own child run with a unique `{{job.run_id}}`, so the artifacts table keeps runs separate exactly as it does today.
- **Failure containment:** one bad `.pbit` fails its own iteration; the others keep running.
- **Repair:** repairing the parent re-runs only the failed iterations. This matters at 50 files, where headless `/importBI` will fail on some of them.
- **Visibility:** the for_each UI shows status per iteration, and each one links to its child's 6-task run.

Nesting `genie_task` directly inside the for_each is not planned. It would skip preflight, validation, and audit, and it is not confirmed that the Genie Code Job Task beta supports it.

## Changes

### 1. New `notebooks/discover_files.py`

- List `<pbit_volume_path>/*.pbit`.
- Build one object per file: `{pbit_filename, metric_view_name, agent_name, agent_instructions}` (see [Per-file naming](#2-per-file-naming)).
- Fail on name collisions: two files that map to the same `metric_view_name` would overwrite each other, because the import replaces an existing metric view.
- Create the artifacts table (see [change 4](#4-create-the-artifacts-table-once)).
- Skip files that already imported cleanly (see [change 6](#6-re-run-behavior)).
- Optionally move the cheap `.pbit` checks here (zip opens, `DataModelSchema` present), so a `.pbix` renamed to `.pbit` is rejected before it takes a for_each slot.
- Optionally report missing UC source tables across the whole set, so we can fix data gaps before spending iterations on them.
- Publish the list with `dbutils.jobs.taskValues.set("files", [...])`. Fifty entries is far below the task value size limit.

The for_each task reads the list and maps fields into the child job's parameters:

```json
{
  "task_key": "import_each",
  "depends_on": [{"task_key": "discover_files"}],
  "for_each_task": {
    "inputs": "{{tasks.discover_files.values.files}}",
    "concurrency": 3,
    "task": {
      "task_key": "import_one",
      "run_job_task": {
        "job_id": "<child_job_id>",
        "job_parameters": {
          "batch_id": "{{job.run_id}}",
          "pbit_filename": "{{input.pbit_filename}}",
          "metric_view_name": "{{input.metric_view_name}}",
          "agent_name": "{{input.agent_name}}",
          "agent_instructions": "{{input.agent_instructions}}"
        }
      }
    }
  }
}
```

`pbit_volume_path`, `metric_view_catalog`, `metric_view_schema`, `warehouse_id`, and `create_agent` come from the child job's defaults, or the parent passes them through from its own job parameters.

Before implementing, confirm against the Jobs API docs that `{{job.run_id}}` inside a `run_job_task` nested in a for_each resolves to the parent's run ID.

### 2. Per-file naming

Today a person chooses `metric_view_name`, `agent_name`, and `agent_instructions` for each run. Batch mode needs one of these:

- **Derived:** convert the filename to a UC identifier (`Sales Q3 Model.pbit` → `sales_q3_model`) and use the filename stem as the agent name. Leave `agent_instructions` empty, so the agent prompt falls back to the metric view's comments (existing behavior).
- **Manifest with derived fallback:** an optional `manifest.csv` or `manifest.yaml` in the volume sets the name and business context per file, and derived values cover any file it doesn't list.

### 3. Child job concurrency

The child job currently sets `max_concurrent_runs: 1` with queueing on. Left as is, the for_each would start N child runs that still execute one at a time. Raise it to at least the for_each `concurrency`. Queueing can stay on.

### 4. Create the artifacts table once

Concurrent append-only `INSERT`s into Delta don't conflict. Concurrent `CREATE TABLE IF NOT EXISTS` against a new schema can race, though. Create the table in `discover_files` before the for_each starts. The statement can stay in `setup_and_preflight` so standalone single-file runs keep working; after the first creation it does nothing.

### 5. Link child runs to the batch

- Add a `batch_id` job parameter to the child job, defaulting to `""` (standalone runs).
- The parent passes `{{job.run_id}}` as `batch_id`.
- `setup_and_preflight` writes `batch_id` into the `setup_config` payload.
- New `notebooks/batch_report.py` (`run_if: ALL_DONE`) finds every child `run_id` whose `setup_config.batch_id` matches, takes the latest `audit_summary` for each, and writes a `batch_summary` artifact (keyed by the parent `run_id`) with one entry per file: `pbit_filename`, `status`, `metric_view_fqn`, `agent_space_id`, `pbi_measures_unmatched`, `errors`.
- A file whose child run never wrote `setup_config` (for example, preflight failed on parameters) will not appear under the `batch_id`. `batch_report` should compare against the file list from `discover_files` and mark those files `NOT_STARTED` or `PREFLIGHT_FAILED`.

Alternative: add a `batch_id` column to the artifacts table. It is cleaner to query but changes the schema, and every task's `INSERT` has to change with it.

### 6. Re-run behavior

With `create_agent = true`, every run creates a new Genie space. Re-running a whole batch would duplicate up to 50 spaces.

- `discover_files` skips files whose latest `audit_summary` for the same `metric_view_name` has `status = 'SUCCESS'`.
- A `force` parameter (`true`/`false`, default `false`) on the parent job turns the skip off.

A plain re-run of the parent then means "retry whatever didn't finish". Repair still covers retrying failed iterations within a single parent run.

### 7. `deploy.py`

- Create the child job first. Raise its `max_concurrent_runs` and add the `batch_id` parameter.
- Create the parent job `genie-pbi-import-batch`, wiring the child's `job_id` into `run_job_task`.
- Parent job parameters: `pbit_volume_path`, `metric_view_catalog`, `metric_view_schema`, `warehouse_id`, `create_agent`, `concurrency` (if it can be parameterized; otherwise a deploy widget), `force`.
- The existing one-time-only caveat (no update or reuse) applies to the parent job too.

### 8. Tests and README

- Add tests for `discover_files` (name derivation, collision detection, skip logic, manifest parsing) and `batch_report` (rollup, missing-child handling), using the existing mocked-SDK pattern in `tests/test_workflow.py`.
- README: add a "Batch import" section with the parent pipeline diagram, the parent job parameters, and a query on `batch_summary`.

## Concurrency guidance

for_each `concurrency` goes up to 100, but other limits apply first:

- Genie Code automations and the partner-powered model behind `/importBI` likely have per-workspace throughput limits. None are documented that we have found.
- Parallel imports, validation smoke tests (one `MEASURE()` query per measure), and Genie Agents all use the same SQL warehouse.

Start at 3–5, watch the Genie Code tasks for throttling and timeouts, then raise it. `concurrency: 1` is a valid sequential mode that still gives repair and the rollup.

## Risks at batch scale

- **No interactive trial run.** The README advises trying `/importBI` interactively on a file before running it in the job, which nobody will do for 50 files. Expect a higher share of `PARTIAL` and `FAILED` results; `batch_summary` is the triage queue.
- **Unresolved source tables add up.** The per-file missing-table warning is easy to ignore on one run but turns into many failed iterations across a batch. The set-wide report in `discover_files` addresses this.
- **Agent sprawl.** Fifty new Genie spaces need owners, permissions, and naming conventions. Decide on a naming pattern and who gets access before the first large run.

## Possible next step: continuous ingestion

Add a file arrival trigger on the volume to the parent job. With the skip logic from change 6, each trigger processes only new or not-yet-successful files, so bulk import becomes ongoing ingestion with little extra code. One thing to check: a file arrival trigger starts the job but does not pass filenames, which is why discovery stays in `discover_files`.

## Open decisions

- [ ] Naming: derived only, or manifest with a derived fallback?
- [ ] In batch mode, create agents for `PARTIAL` imports, or require `SUCCESS`?
- [ ] Link runs with a `batch_id` field in the `setup_config` payload, or with a new column?
- [ ] Starting `concurrency` value, and whether it is a job parameter or a deploy-time setting.
- [ ] Scope: parent job in `deploy.py` from the start, or a separate `deploy_batch` notebook?

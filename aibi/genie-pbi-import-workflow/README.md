# Genie PBI Import Workflow

A prototype workflow that migrates a Power BI semantic model to Databricks without manual steps: it reads a `.pbit` file from a Unity Catalog volume, uses Genie Code's `/importBI` to translate it into a [Unity Catalog metric view](https://docs.databricks.com/aws/en/metric-views/), validates the result deterministically, and optionally creates a [Genie Agent](https://docs.databricks.com/aws/en/genie-agents/) on top of it.

The pipeline is a 6-task Databricks job that mixes standard notebook tasks with **Genie Code automations** (agentic tasks driven by natural-language prompts). Notebook tasks do the checks that should not depend on an LLM (file parsing, metric view validation, the audit); Genie Code does the translation and the agent setup. All run state is passed between tasks through a Delta artifacts table.

## Pipeline

```
setup_and_preflight    (notebook)       → validate inputs, parse the .pbit, check UC sources
        ↓
import_metric_view     (Genie Code)     → /importBI → metric view
        ↓
validate_metric_view   (notebook)       → type, source tables, smoke-test every measure
        ↓
should_create_agent    (condition)      → create_agent == "true"?
        ↓ true
create_genie_agent     (Genie Code)     → Genie Agent on the metric view
        ↓
audit_and_report       (notebook)       → verify handoffs and the agent, write audit_summary
                                          (run_if: ALL_DONE)
```

Each task writes a row to `<metric_view_catalog>.<metric_view_schema>.genie_pbi_import_workflow_artifacts` keyed by `run_id`, which is how downstream tasks (including the Genie Code prompts) read the prior task's output.

### Tasks

| Task | Type | What it does |
|------|------|--------------|
| `setup_and_preflight` | Notebook | Validates the job parameters, creates the artifacts table, and reads `DataModelSchema` from the `.pbit` (a zip archive; the model is UTF-16LE JSON). Lists the Power BI tables and measures, and checks which source tables exist in Unity Catalog: it reads the catalog/schema/table navigation steps the Databricks Power BI connector writes into each table's M query, and falls back to `<metric_view_catalog>.<metric_view_schema>.<lowercased_table_name>` when the query doesn't name a UC table. Missing tables are a warning only. Writes `setup_config`. |
| `import_metric_view` | Genie Code | Runs `/importBI` on the PBIT path and saves `<metric_view_catalog>.<metric_view_schema>.<metric_view_name>`, with `comment`, `display_name` (the Power BI name), and `format` on each field. Measures it cannot translate are left out and listed in `measures_not_translated`. Writes `import_result` with status `SUCCESS`, `PARTIAL`, or `FAILED`; fails the task on `FAILED`. |
| `validate_metric_view` | Notebook | Reads the object with `w.tables.get` and requires `table_type = METRIC_VIEW`. Parses the YAML definition and checks that the `source` and every (nested) join table exist. Runs `MEASURE()` on each measure separately, so one bad DAX translation is reported by name, plus one grouped query on the first dimension. Writes `validation`, then fails the task if any check failed. |
| `should_create_agent` | Condition | Continues to `create_genie_agent` only when the `create_agent` job parameter is `true`. Validation failures never reach this task: they fail `validate_metric_view`. |
| `create_genie_agent` | Genie Code | Creates a new Genie space named `agent_name` with the metric view as its only data source, general instructions from `agent_instructions` (or from the metric view's comments when empty), and 5–8 sample questions built from the defined measures and dimensions. Writes `agent_result` with the new `space_id`. Does not retry on failure. |
| `audit_and_report` | Notebook | Runs after upstream tasks finish, even if they failed. Checks the required artifacts, re-reads the reported Genie space with `get_space` and confirms it uses the metric view, and lists Power BI measures that are missing from the metric view (matched on name or `display_name`). Writes `audit_summary`; incomplete audits save their errors and fail the task. |

`audit_and_report` depends on both `validate_metric_view` and `create_genie_agent`. When `create_agent` is `false`, `create_genie_agent` is excluded, and Jobs excludes a task whose dependencies are all excluded, even with `run_if: ALL_DONE`; the second dependency keeps the audit running.

## Repository layout

```
genie-pbi-import-workflow/
├── deploy.py                       # Databricks notebook source (creates the automations + 6-task job)
├── notebooks/
│   ├── setup_and_preflight.py      # Databricks notebook source (task 1)
│   ├── validate_metric_view.py     # Databricks notebook source (task 3)
│   └── audit_and_report.py         # Databricks notebook source (task 6)
├── prompts/
│   ├── import_metric_view.md       # Prompt for the import_metric_view Genie Code automation
│   └── create_genie_agent.md       # Prompt for the create_genie_agent Genie Code automation
└── tests/
    └── test_workflow.py            # Local tests for the task notebooks (mocked SDK + Spark)
```

All `.py` files are Databricks notebook sources — upload them to the workspace as notebooks. The Genie Code tasks have no notebook; their logic lives entirely in the prompt `.md` files, which `deploy` registers as Genie Code automations.

## Deployment

### Prerequisites

- The **Genie Code Job Task** beta enabled for your account/workspace in the [Databricks preview portal](https://previews.databricks.com). Without it, the `genie_task` entries in the job definition are not recognized — the job is still created, but the Genie Code tasks appear in the workflow UI as unconfigured tasks you must set up manually.
- **Partner-powered AI** enabled for both the account and the workspace (required by `/importBI`).
- A full Unity Catalog Volume directory path containing the `.pbit` files, such as `/Volumes/main/lending_demo/raw_data/powerbi_files`.
- A destination catalog and schema where the workflow can create metric views and its artifacts table. This location can differ from the PBIT Volume and source-table locations.
- The Power BI model's source tables in Unity Catalog, readable by the job's run-as identity.
- A SQL warehouse ID (required when `create_agent` is `true`; the Genie Agent runs on it).
- Databricks CLI installed and configured (used for uploading files and triggering runs — all SDK code runs inside the workspace, not locally).
- Serverless compute for notebook tasks. The job defines no clusters, so the notebook tasks run on serverless; each notebook declares environment version 6 and `databricks-sdk>=0.102.0` in its header (`validate_metric_view` also installs `pyyaml`).

Save the Power BI report as a template (*File → Export → Power BI template*) to get a `.pbit`. A `.pbix` stores the model in a binary format that the preflight cannot read.

**Try the import interactively first.** In a job, Genie Code runs with auto-approve and nobody can answer its questions, such as which UC table a Power BI table maps to. Run the `import_metric_view` prompt (with the parameters filled in) in an interactive Genie Code session on one `.pbit` first, and promote it to the job once it imports cleanly.

### Steps

1. If you cloned the full `databricks-genai-examples` repo, change into this example's directory first:

   ```bash
   cd databricks-genai-examples/aibi/genie-pbi-import-workflow
   ```

2. Upload the example to a workspace directory. Include `notebooks/` and `prompts/` — the prompt `.md` files become plain Workspace files that the deploy notebook reads directly:

   ```bash
   databricks workspace import-dir . /Workspace/Users/you@company.com/genie-pbi-import-workflow
   ```

   Any directory works, but keep `deploy`, `notebooks/`, and `prompts/` together: `deploy` finds the task notebooks and prompts relative to its own location.

3. Upload a `.pbit` to the volume:

   ```bash
   databricks fs cp ./<model>.pbit dbfs:/Volumes/<volume-catalog>/<volume-schema>/<volume>/
   ```

4. Deploy the automations and job: open the `deploy` notebook in the target workspace, run the first cell (**Widgets**), fill in the widgets, then click *Run all*. It authenticates with the notebook's own context — no token needed.

   | Widget | Default | Description |
   |--------|---------|-------------|
   | `pbit_volume_path` | `""` | Full UC Volume directory containing the `.pbit`, for example `/Volumes/main/lending_demo/raw_data/powerbi_files` |
   | `metric_view_catalog` / `metric_view_schema` | `""` | Destination for the generated metric view and workflow artifacts table |
   | `warehouse_id` | `""` | SQL warehouse for Genie Code and the Genie Agent |
   | `pbit_filename` / `metric_view_name` / `agent_name` | `""` | Optional defaults for a first file; usually set per run |

   All widgets are saved as job parameter defaults. `deploy` creates two Genie Code automations (via the internal scheduled-insights API) and creates or resets the exact-name job `genie-pbi-import-workflow`, wiring the new automation `configuration_id`s into the job's `genie_task` entries.

> **Redeployment behavior**: the first deployment creates the job; later deployments reset the existing exact-name job in place, preserving its job ID and run history. Genie Code automations are still recreated on every deployment. If multiple exact-name jobs already exist from an older version of this example, deployment stops and lists their job IDs so you can delete or rename duplicates first.

5. Run the job, one `.pbit` per run:

   ```bash
   databricks jobs run-now <job_id> --json '{
     "job_parameters": {
       "pbit_volume_path": "/Volumes/<volume-catalog>/<volume-schema>/<volume>",
       "pbit_filename": "<model>.pbit",
       "metric_view_name": "<metric_view>",
       "agent_name": "<Agent name>",
       "agent_instructions": "<one or two sentences of business context>"
     }
   }'
   ```

   Or click **Run now with different parameters** on the job page.

## Job parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `run_id` | `{{job.run_id}}` | Run identifier; auto-set to the job run ID each run |
| `pbit_volume_path` | `deploy` widget | Full UC Volume directory holding the `.pbit` files. Required for a configured run |
| `metric_view_catalog` / `metric_view_schema` | `deploy` widgets | Destination for generated metric views and the artifacts table. Either empty → dry run |
| `pbit_filename` | `deploy` widget | Which `.pbit` to import. Empty → dry run. Must end in `.pbit` |
| `metric_view_name` | `deploy` widget | Name of the metric view to create in `metric_view_catalog.metric_view_schema`. Required |
| `create_agent` | `true` | `true` or `false`. Whether to create a Genie Agent after validation passes |
| `agent_name` | `deploy` widget | Name of the Genie Agent. Required when `create_agent` is `true` |
| `agent_instructions` | `""` | Business context for the agent's general instructions. Empty → derived from the metric view's comments |
| `warehouse_id` | `deploy` widget | SQL warehouse. Required when `create_agent` is `true` |

`setup_and_preflight` rejects invalid values (unknown `create_agent`, a non-`.pbit` file, missing agent settings) before any task acts on them.

### Re-runs and multiple files

- **Same file, same `metric_view_name`:** the import replaces the existing metric view. This is intended, so you can refine a migration by re-running it. Add a version suffix to `metric_view_name` to keep earlier versions.
- **Genie Agent:** every run with `create_agent = true` creates a new Genie space; it never updates an existing one. Set `create_agent = false` when re-running only to refine the metric view.
- **Multiple files:** trigger one run per `.pbit` with its own `pbit_filename` and `metric_view_name`. Each run writes under its own `run_id`, so results stay separate in the artifacts table. (`max_concurrent_runs` is 1 and queueing is on, so runs execute one after another.)

## Artifacts table

All tasks read/write `<metric_view_catalog>.<metric_view_schema>.genie_pbi_import_workflow_artifacts`:

| Column | Description |
|--------|-------------|
| `run_id` | Groups all rows from one pipeline run |
| `task_name` | Task that wrote the row |
| `artifact_type` | `setup_config`, `import_result`, `validation`, `agent_result`, `audit_summary` |
| `payload` | JSON string with the artifact contents |
| `created_at` | Timestamp |

When a task is repaired and writes its artifact again, the audit uses the latest row of each type. JSON payloads and run IDs are bound as SQL parameters, so quotes, backslashes, and newlines in payloads (DAX expressions, error messages) are preserved without manual escaping.

To see the outcome of a run:

```sql
SELECT payload:status, payload:metric_view_fqn, payload:agent_space_id,
       payload:pbi_measures_unmatched, payload:errors
FROM <metric_view_catalog>.<metric_view_schema>.genie_pbi_import_workflow_artifacts
WHERE run_id = '<run_id>' AND artifact_type = 'audit_summary'
ORDER BY created_at DESC
LIMIT 1
```

## Run outcomes

- **Dry run:** if any of `metric_view_catalog`, `metric_view_schema`, or `pbit_filename` is empty, each task exits before API calls or Delta reads/writes.
- **Preflight failure:** invalid parameters, a missing file, or a `.pbit` without `DataModelSchema` fail `setup_and_preflight`. Nothing downstream runs except the audit, which records an `INCOMPLETE` summary.
- **Import failure:** `import_result.status = 'FAILED'` fails the task; validation and agent creation do not run. `PARTIAL` continues: validation decides whether what was saved is usable, and the audit lists the Power BI measures that did not come through.
- **Validation failure:** the `validation` artifact records every failed check (`errors`, per-measure `smoke_tests`), then the task fails. No Genie Agent is created on a broken metric view.
- **Incomplete audit:** missing or invalid required artifacts, an unsuccessful handoff, or a Genie space that cannot be read or does not use the metric view produce `audit_summary` with `status = 'INCOMPLETE'`, `audit_complete = false`, and `errors`; the audit task then fails.
- **Completed run:** `status = 'SUCCESS'` and `audit_complete = true`. Unmatched Power BI measures do not fail the audit; review `pbi_measures_unmatched` alongside `import_result.measures_not_translated`.

## Local checks

From this example directory, run the notebook regression tests with Python's standard library:

```bash
python3 -B -m unittest discover -s tests -v  # -B: no __pycache__ to upload by mistake
```

These execute the notebook source with mocked Databricks SDK, Spark, and a generated `.pbit` fixture. They cover parameter checks, `.pbit` parsing, source resolution, metric view validation failures, dry runs, and audit completeness. They do not replace a workspace smoke test of `/importBI` and the Genie Code automations.

## Prototype limitations

- **Prompt-defined tasks**: Genie Code chooses its actions from the prompts. The validate task independently checks the metric view, and the audit re-reads the Genie space, but the quality of DAX→SQL translations is only checked to the point of "the measure runs". Compare key measures against Power BI before relying on them.
- **Headless `/importBI`**: `/importBI` is designed for interactive sessions where it can ask questions. In the job, the prompt tells it to skip what it cannot resolve and record why. Complex models (many-to-many relationships, time-intelligence DAX, calculation groups) are more likely to come back `PARTIAL`.
- **Source table preflight**: the M-query parsing recognizes the Databricks Power BI connector's navigation steps. Models built on other connectors or native SQL fall back to a naming-convention guess, so their "missing" warnings are hints, not errors.
- **Internal API**: the `deploy` notebook uses the `/api/2.0/alerts-internal/scheduled-insights` endpoint for Genie Code automations, which is internal and may change.

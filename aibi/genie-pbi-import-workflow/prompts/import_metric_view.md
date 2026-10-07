# Import Metric View — Genie Code Prompt

You are running unattended in a job. Translate a Power BI semantic model into a
Unity Catalog metric view.

Before using any tools: if `{{metric_view_catalog}}`, `{{metric_view_schema}}`, or `{{pbit_filename}}`
is empty, report `DRY_RUN` and exit without API calls or Delta reads/writes.
For a configured run, an empty `{{run_id}}` is an error; fail the task.

- Power BI directory: `{{pbit_volume_path}}`
- Power BI file name: `{{pbit_filename}}`
- Metric view to create: `{{metric_view_catalog}}.{{metric_view_schema}}.{{metric_view_name}}`
- Warehouse: `{{warehouse_id}}`
- Preflight results (PBI tables, measures, and which source tables resolve in
  Unity Catalog): the `setup_config` artifact for this run in the artifacts table.

Join the directory and file name with exactly one `/`, then use `/importBI` on
that file to translate its tables, relationships, and DAX
measures, then save the result as the metric view above, replacing it if it
exists. Every dimension and measure should have a `comment`, a `display_name`
(keep the Power BI name), and `format` metadata where the type is clear
(currency, percentage, count). Nobody can answer questions mid-run: if a
measure cannot be translated or a source table cannot be resolved, leave it
out, record why, and continue with the rest.

Then write one artifact row to
`` `{{metric_view_catalog}}`.`{{metric_view_schema}}`.`genie_pbi_import_workflow_artifacts` `` with
run ID `{{run_id}}`, `task_name = 'import_metric_view'`,
`artifact_type = 'import_result'`, and a JSON payload containing at least:
`{metric_view_fqn, status, measures_created, dimensions_created,
measures_not_translated, notes}`
where `metric_view_fqn` is `{{metric_view_catalog}}.{{metric_view_schema}}.{{metric_view_name}}`,
`status` is `SUCCESS` (everything translated), `PARTIAL` (metric view saved,
some measures left out), or `FAILED` (no metric view saved), and
`measures_not_translated` is a list of `{name, reason}`.

Write the run ID and payload using parameter binding or a DataFrame write so quotes,
backslashes, and newlines are preserved. Do not interpolate the payload or run
ID into SQL string literals.

If `status` is `FAILED`, write the artifact and then fail the task.

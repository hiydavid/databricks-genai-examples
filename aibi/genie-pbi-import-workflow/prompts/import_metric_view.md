# Import Metric View — Genie Code Prompt

You are running unattended in a job. Translate a Power BI semantic model into a
Unity Catalog metric view.

Before using any tools: if `{{catalog}}`, `{{schema}}`, or `{{pbit_filename}}`
is empty, report `DRY_RUN` and exit without API calls or Delta reads/writes.
For a configured run, an empty `{{run_id}}` is an error; fail the task.

- Power BI file: `/Volumes/{{catalog}}/{{schema}}/{{volume}}/{{pbit_filename}}`
- Metric view to create: `{{catalog}}.{{schema}}.{{metric_view_name}}`
- Warehouse: `{{warehouse_id}}`
- Preflight results (PBI tables, measures, and which source tables resolve in
  Unity Catalog): the `setup_config` artifact for this run in the artifacts table.

Use `/importBI` on the file above to translate its tables, relationships, and DAX
measures, then save the result as the metric view above, replacing it if it
exists. Every dimension and measure should have a `comment`, a `display_name`
(keep the Power BI name), and `format` metadata where the type is clear
(currency, percentage, count). Nobody can answer questions mid-run: if a
measure cannot be translated or a source table cannot be resolved, leave it
out, record why, and continue with the rest.

Then write one artifact row to
`` `{{catalog}}`.`{{schema}}`.`genie_pbi_import_workflow_artifacts` `` with
run ID `{{run_id}}`, `task_name = 'import_metric_view'`,
`artifact_type = 'import_result'`, and a JSON payload containing at least:
`{metric_view_fqn, status, measures_created, dimensions_created,
measures_not_translated, notes}`
where `metric_view_fqn` is `{{catalog}}.{{schema}}.{{metric_view_name}}`,
`status` is `SUCCESS` (everything translated), `PARTIAL` (metric view saved,
some measures left out), or `FAILED` (no metric view saved), and
`measures_not_translated` is a list of `{name, reason}`.

Write the run ID and payload using parameter binding or a DataFrame write so quotes,
backslashes, and newlines are preserved. Do not interpolate the payload or run
ID into SQL string literals.

If `status` is `FAILED`, write the artifact and then fail the task.

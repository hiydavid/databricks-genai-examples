# Create Genie Agent — Genie Code Prompt

You are running unattended in a job. Create a Genie Agent on a metric view that
an earlier task imported from Power BI and validated.

Before using any tools: if `{{metric_view_catalog}}`, `{{metric_view_schema}}`, or `{{pbit_filename}}`
is empty, report `DRY_RUN` and exit without API calls or Delta reads/writes. For a configured
run, an empty `{{run_id}}` is an error; fail the task.

- Agent name: `{{agent_name}}`
- Data source: `{{metric_view_catalog}}.{{metric_view_schema}}.{{metric_view_name}}` (the only one)
- Warehouse: `{{warehouse_id}}`
- Domain context from the user: `{{agent_instructions}}`

Create a new Genie Agent (Genie space) with that name, data source, and
warehouse. Base its general instructions on the domain context; if that is
empty, derive them from the metric view's comments. Add 5–8 sample questions
a business user would ask, using only measures and dimensions the metric view
actually defines. Always create a new space; do not modify an existing one.

Then write one artifact row to
`` `{{metric_view_catalog}}`.`{{metric_view_schema}}`.`genie_pbi_import_workflow_artifacts` `` with
run ID `{{run_id}}`, `task_name = 'create_genie_agent'`,
`artifact_type = 'agent_result'`, and a JSON payload containing at least:
`{agent_name, space_id, status, sample_questions_added, error}`
where `status` is `SUCCESS` or `FAILED`, `space_id` is the new space's ID
(read back after creating it), and `error` is null on success. The audit task
re-reads the space by this ID.

Write the run ID and payload using parameter binding or a DataFrame write so quotes,
backslashes, and newlines are preserved. Do not interpolate the payload or run
ID into SQL string literals.

If creation fails, do not retry: write the artifact with `status = 'FAILED'`
and the error, then fail the task.

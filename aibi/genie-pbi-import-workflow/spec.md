NFCU PBI → Metric View Lakeflow Pipeline Spec
Overview
An automated Lakeflow Job that ingests Power BI .pbit files from a Unity Catalog Volume, uses Genie Code to translate the semantic model into UC Metric Views, validates the output, and optionally creates a Genie Agent — all without manual intervention.

Prerequisites
Workspace Configuration
Genie Code Task (Beta) enabled via Settings → Previews → search "Genie Code"
Partner-Powered AI enabled for both account and workspace (required for /importBI)
A SQL Warehouse available for Genie Code and notebook tasks
Unity Catalog Setup
Catalog: <catalog> (e.g., nfcu_lending)
Schema: <schema> (e.g., semantic_layer)
Volume: <catalog>.<schema>.pbi_files — upload the .pbit files here
Artifacts table: <catalog>.<schema>.pipeline_artifacts — created by Task 1
Job Parameters (defined at job level)
Parameter
Example Value
Description
catalog
nfcu_lending
Target UC catalog
schema
semantic_layer
Target UC schema
pbit_filename
nfcu_lending_desktop_semantic.pbit
Which .pbit to import
metric_view_name
mv_nfcu_lending
Name for the created metric view
agent_name
NFCU Lending Agent
Name for the Genie Agent (Task 4)

Artifacts Table Schema
All inter-task communication goes through a shared Delta table. Genie Code tasks write here via SQL; notebook tasks read/write via PySpark.
CREATE TABLE IF NOT EXISTS <catalog>.<schema>.pipeline_artifacts (
  run_id        STRING    NOT NULL COMMENT 'Lakeflow job run ID',
  task_name     STRING    NOT NULL COMMENT 'Name of the task that wrote this row',
  artifact_type STRING    NOT NULL COMMENT 'Type key: setup_config, import_result, validation, agent_result, audit_summary',
  payload       STRING    NOT NULL COMMENT 'JSON payload with task output',
  created_at    TIMESTAMP NOT NULL DEFAULT current_timestamp()
)
USING DELTA
COMMENT 'Inter-task message bus for the PBI migration pipeline'

Task DAG
Task 1 (Notebook)     Task 2 (Genie Code)     Task 3 (Notebook)     Task 4 (Genie Code)     Task 5 (Notebook)
   Setup &         →     Import PBI &       →     Validate         →     Create Genie     →     Audit &
   Preflight             Create Metric View        Metric View            Agent (optional)       Report
                                                                                            (run_if: ALL_DONE)

Task 1: Setup & Preflight
Type: Notebook (Python)
Depends on: None
Purpose: Validate inputs, create the artifacts table, parse PBI metadata for reference, and write the initial config artifact.
Logic
# -- Parameters (injected from job parameters) --
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
pbit_filename = dbutils.widgets.get("pbit_filename")
metric_view_name = dbutils.widgets.get("metric_view_name")
run_id = dbutils.widgets.get("run_id")  # or use spark.conf.get("spark.databricks.job.runId")

# -- 1. Create artifacts table if not exists --
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS `{catalog}`.`{schema}`.pipeline_artifacts (
    run_id STRING, task_name STRING, artifact_type STRING,
    payload STRING, created_at TIMESTAMP DEFAULT current_timestamp()
  ) USING DELTA
""")

# -- 2. Verify the .pbit file exists in the volume --
volume_path = f"/Volumes/{catalog}/{schema}/pbi_files/{pbit_filename}"
import os
assert os.path.exists(volume_path), f"PBIT file not found: {volume_path}"

# -- 3. (Optional) Parse DataModelSchema from .pbit for reference --
import zipfile, json
with zipfile.ZipFile(volume_path, 'r') as z:
    with z.open('DataModelSchema') as f:
        raw = f.read()
        # .pbit DataModelSchema is UTF-16LE encoded
        schema_json = json.loads(raw.decode('utf-16-le').lstrip('\ufeff'))

tables = [t['name'] for t in schema_json.get('model', {}).get('tables', [])]
measures = []
for t in schema_json.get('model', {}).get('tables', []):
    for m in t.get('measures', []):
        measures.append({"table": t['name'], "name": m['name'], "expression": m.get('expression', '')})

# -- 4. Validate source tables exist in Unity Catalog --
# The PBI model references source tables via partition/source expressions.
# Extract the actual UC table references and check each one exists.
source_tables = []
for t in schema_json.get('model', {}).get('tables', []):
    # PBI tables have partitions[].source.expression with the UC table reference
    for p in t.get('partitions', []):
        src = p.get('source', {})
        expr = src.get('expression', '')
        if isinstance(expr, list):
            expr = ' '.join(expr)
        # Look for catalog.schema.table patterns in the expression
        # Common patterns: "catalog.schema.table" or let Source = ...
        if expr:
            source_tables.append({"pbi_table": t['name'], "source_expression": expr[:200]})

# Also check for explicit annotations or connection strings
# that reference UC tables (e.g., in M-query or direct references)
table_validation_results = []
for t in schema_json.get('model', {}).get('tables', []):
    # Skip calculated tables (no physical source)
    is_calculated = any(
        p.get('source', {}).get('type', '') == 'calculated'
        for p in t.get('partitions', [])
    )
    if is_calculated:
        table_validation_results.append({
            "pbi_table": t['name'], "type": "calculated", "uc_exists": "N/A"
        })
        continue

    # Try to find a matching UC table by name convention
    # (the actual mapping depends on how the PBI model was built)
    candidate_fqn = f"{catalog}.{schema}.{t['name'].lower().replace(' ', '_')}"
    try:
        spark.sql(f"DESCRIBE TABLE {candidate_fqn}")
        table_validation_results.append({
            "pbi_table": t['name'], "uc_table": candidate_fqn, "uc_exists": True
        })
    except Exception:
        table_validation_results.append({
            "pbi_table": t['name'], "uc_table": candidate_fqn, "uc_exists": False
        })

tables_found = [r for r in table_validation_results if r.get('uc_exists') == True]
tables_missing = [r for r in table_validation_results if r.get('uc_exists') == False]
tables_calculated = [r for r in table_validation_results if r.get('uc_exists') == 'N/A']

if tables_missing:
    print(f"⚠️  WARNING: {len(tables_missing)} source table(s) not found in UC:")
    for t in tables_missing:
        print(f"   - PBI table '{t['pbi_table']}' → expected UC table '{t['uc_table']}' NOT FOUND")
    print("   The /importBI agent may resolve these differently. Continuing...")

# -- 5. Write setup config artifact --
import json as json_lib
config_payload = json_lib.dumps({
    "catalog": catalog,
    "schema": schema,
    "pbit_filename": pbit_filename,
    "volume_path": volume_path,
    "metric_view_name": metric_view_name,
    "pbi_tables": tables,
    "measures_found": len(measures),
    "measure_names": [m['name'] for m in measures],
    "source_table_validation": {
        "tables_found_in_uc": len(tables_found),
        "tables_missing_from_uc": len(tables_missing),
        "tables_calculated": len(tables_calculated),
        "missing_details": tables_missing
    }
})

spark.sql(f"""
  INSERT INTO `{catalog}`.`{schema}`.pipeline_artifacts
  VALUES ('{run_id}', 'setup', 'setup_config', '{config_payload}', current_timestamp())
""")

# -- 6. Set task value so downstream tasks know about missing tables --
dbutils.jobs.taskValues.set(key="tables_missing_count", value=len(tables_missing))

print(f"✅ Preflight complete: {len(tables)} PBI tables, {len(measures)} measures")
print(f"   UC source tables: {len(tables_found)} found, {len(tables_missing)} missing, {len(tables_calculated)} calculated")
Output Artifact
{
  "artifact_type": "setup_config",
  "payload": {
    "catalog": "nfcu_lending",
    "schema": "semantic_layer",
    "pbit_filename": "nfcu_lending_desktop_semantic.pbit",
    "volume_path": "/Volumes/nfcu_lending/semantic_layer/pbi_files/nfcu_lending_desktop_semantic.pbit",
    "metric_view_name": "mv_nfcu_lending",
    "pbi_tables": ["fact_transactions", "dim_customer", "dim_product", "..."],
    "measures_found": 24,
    "measure_names": ["Total Spend", "Delinquency Rate", "..."],
    "source_table_validation": {
      "tables_found_in_uc": 8,
      "tables_missing_from_uc": 2,
      "tables_calculated": 1,
      "missing_details": [
        {"pbi_table": "DateTable", "uc_table": "nfcu_lending.semantic_layer.datetable", "uc_exists": false},
        {"pbi_table": "Measures", "uc_table": "nfcu_lending.semantic_layer.measures", "uc_exists": false}
      ]
    }
  }
}

Task 2: Import PBI & Create Metric View
Type: Genie Code Task
Depends on: Task 1
Purpose: Use Genie Code's /importBI skill to translate the Power BI semantic model into a UC Metric View.
Prompt
You are running as part of an automated pipeline. Your job is to create a Unity Catalog metric view from a Power BI file.

STEPS:
1. Navigate to the metric view editor and create a new metric view named `{{catalog}}.{{schema}}.{{metric_view_name}}`.
2. Use the /importBI command to import the Power BI file located at: /Volumes/{{catalog}}/{{schema}}/pbi_files/{{pbit_filename}}
3. Let the import complete. Review the generated YAML definition.
4. Ensure all measures and dimensions have:
   - A `comment` describing what they represent
   - A `display_name` for human-readable labels
   - Appropriate `format` metadata (currency for dollar amounts, percentage for rates, number for counts)
5. Save the metric view.
6. After saving, run this SQL to confirm it was created:
   ```sql
   DESCRIBE EXTENDED `{{catalog}}`.`{{schema}}`.`{{metric_view_name}}`
Write the result to the artifacts table:INSERT INTO `{{catalog}}`.`{{schema}}`.pipeline_artifacts
VALUES (
  '{{job.run_id}}',
  'import_metric_view',
  'import_result',
  '{"metric_view_fqn": "{{catalog}}.{{schema}}.{{metric_view_name}}", "status": "SUCCESS_OR_FAILURE", "measures_created": N, "dimensions_created": N, "notes": "any issues encountered"}',
  current_timestamp()
)
IMPORTANT:
Do NOT skip the /importBI step. The .pbit file contains DAX measures that must be translated to SQL.
If the import encounters errors (e.g., source tables not found), document them in the artifact payload and continue with what can be created.
Use generous timeouts — complex .pbit files can take several minutes to parse.

### Timeout
Set to **600 seconds** (10 minutes) — `/importBI` on complex models can be slow.

### Output Artifact
```json
{
  "artifact_type": "import_result",
  "payload": {
    "metric_view_fqn": "nfcu_lending.semantic_layer.mv_nfcu_lending",
    "status": "SUCCESS",
    "measures_created": 24,
    "dimensions_created": 12,
    "notes": "All DAX measures translated successfully"
  }
}

Task 3: Validate Metric View
Type: Notebook (Python)
Depends on: Task 2
Purpose: Deterministically verify the metric view was created correctly and measures produce valid results.
Logic
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
metric_view_name = dbutils.widgets.get("metric_view_name")
run_id = dbutils.widgets.get("run_id")

fqn = f"`{catalog}`.`{schema}`.`{metric_view_name}`"

# -- 1. Verify the metric view exists and is the right type --
desc = spark.sql(f"DESCRIBE EXTENDED {fqn}").collect()
table_type_row = [r for r in desc if r['col_name'] == 'Type']
assert len(table_type_row) > 0, f"Metric view {fqn} not found"
assert 'METRIC_VIEW' in str(table_type_row[0]['data_type']), f"{fqn} is not a METRIC_VIEW"

# -- 2. Get column inventory --
cols = spark.sql(f"DESCRIBE {fqn}").collect()
dimensions = [c for c in cols if c['data_type'] != 'MEASURE']
measures = [c for c in cols if c['data_type'] == 'MEASURE']

# -- 3. Validate source tables referenced by the metric view --
# Extract the source table(s) from the metric view definition
source_table_validation = []
try:
    view_text_rows = [r for r in desc if r['col_name'] == 'View Text']
    if view_text_rows:
        view_text = view_text_rows[0]['data_type']
        # Parse the YAML source field to find referenced tables
        import re, yaml
        yaml_match = re.search(r'\$\$(.*?)\$\$', view_text, re.DOTALL)
        if yaml_match:
            mv_yaml = yaml.safe_load(yaml_match.group(1))
            # Check main source
            source_ref = mv_yaml.get('source', '')
            if isinstance(source_ref, str) and not source_ref.strip().upper().startswith('SELECT'):
                try:
                    spark.sql(f"DESCRIBE TABLE {source_ref}")
                    source_table_validation.append({"table": source_ref, "exists": True, "role": "source"})
                except Exception as e:
                    source_table_validation.append({"table": source_ref, "exists": False, "role": "source", "error": str(e)[:200]})
            # Check joined tables
            for j in mv_yaml.get('joins', []):
                join_src = j.get('source', '')
                if isinstance(join_src, str) and not join_src.strip().upper().startswith('SELECT'):
                    try:
                        spark.sql(f"DESCRIBE TABLE {join_src}")
                        source_table_validation.append({"table": join_src, "exists": True, "role": f"join:{j.get('name', '?')}"})
                    except Exception as e:
                        source_table_validation.append({"table": join_src, "exists": False, "role": f"join:{j.get('name', '?')}", "error": str(e)[:200]})
except Exception as e:
    source_table_validation.append({"parse_error": str(e)[:200]})

missing_sources = [s for s in source_table_validation if s.get('exists') == False]
source_validation_status = "FAIL" if missing_sources else "PASS"
if missing_sources:
    print(f"❌ Source table validation FAILED — {len(missing_sources)} table(s) missing:")
    for s in missing_sources:
        print(f"   - {s['table']} (role: {s['role']}): {s.get('error', 'not found')}")

# -- 4. Run a smoke test query --
# Pick the first measure and first dimension for a basic MEASURE() query
if measures and dimensions:
    test_measure = measures[0]['col_name']
    test_dim = dimensions[0]['col_name']
    try:
        result = spark.sql(f"""
            SELECT `{test_dim}`, MEASURE(`{test_measure}`) AS val
            FROM {fqn}
            GROUP BY ALL
            LIMIT 5
        """).collect()
        smoke_test_status = "PASS"
        smoke_test_rows = len(result)
    except Exception as e:
        smoke_test_status = f"FAIL: {str(e)}"
        smoke_test_rows = 0
else:
    smoke_test_status = "SKIP: no measures or dimensions found"
    smoke_test_rows = 0

# -- 5. Write validation artifact --
import json
overall_pass = (smoke_test_status == "PASS") and (source_validation_status == "PASS")
validation_payload = json.dumps({
    "metric_view_fqn": f"{catalog}.{schema}.{metric_view_name}",
    "exists": True,
    "is_metric_view": True,
    "dimension_count": len(dimensions),
    "measure_count": len(measures),
    "dimension_names": [d['col_name'] for d in dimensions],
    "measure_names": [m['col_name'] for m in measures],
    "source_table_validation": {
        "status": source_validation_status,
        "tables_checked": len(source_table_validation),
        "tables_missing": len(missing_sources),
        "details": source_table_validation
    },
    "smoke_test_status": smoke_test_status,
    "smoke_test_rows": smoke_test_rows,
    "overall_status": "PASS" if overall_pass else "FAIL"
})

spark.sql(f"""
  INSERT INTO `{catalog}`.`{schema}`.pipeline_artifacts
  VALUES ('{run_id}', 'validate', 'validation', '{validation_payload}', current_timestamp())
""")

# -- 6. Set task value for downstream branching --
overall = "PASS" if overall_pass else "FAIL"
dbutils.jobs.taskValues.set(key="validation_status", value=overall)

print(f"{'✅' if overall == 'PASS' else '❌'} Validation {overall}: {len(measures)} measures, {len(dimensions)} dimensions")
Output Artifact
{
  "artifact_type": "validation",
  "payload": {
    "metric_view_fqn": "nfcu_lending.semantic_layer.mv_nfcu_lending",
    "exists": true,
    "is_metric_view": true,
    "dimension_count": 12,
    "measure_count": 24,
    "source_table_validation": {
      "status": "PASS",
      "tables_checked": 3,
      "tables_missing": 0,
      "details": [
        {"table": "nfcu_lending.semantic_layer.fact_transactions", "exists": true, "role": "source"},
        {"table": "nfcu_lending.semantic_layer.dim_customer", "exists": true, "role": "join:customers"},
        {"table": "nfcu_lending.semantic_layer.dim_product", "exists": true, "role": "join:products"}
      ]
    },
    "smoke_test_status": "PASS",
    "smoke_test_rows": 5,
    "overall_status": "PASS"
  }
}

Task 4: Create Genie Agent (Optional)
Type: Genie Code Task
Depends on: Task 3 (only runs if validation passed)
Run condition: {{tasks.validate_metric_view.values.validation_status}} == "PASS"
Purpose: Create a Genie Agent backed by the newly created metric view.
Prompt
You are running as part of an automated pipeline. Your job is to create a Genie Agent from a metric view.

STEPS:
1. Create a new Genie Agent named "{{agent_name}}".
2. Add the metric view `{{catalog}}.{{schema}}.{{metric_view_name}}` as the data source.
3. Read the metric view's measures and dimensions to understand the data model.
4. Add 5-8 sample questions that a lending business analyst would ask, based on the actual measures and dimensions available. Examples:
   - "What is the total outstanding balance by product type?"
   - "Show me the delinquency rate trend over the last 12 months"
   - "Which customer segments have the highest credit utilization?"
5. Add general instructions that describe the lending domain context:
   - This is a credit card lending portfolio for a financial institution
   - Measures represent KPIs like balances, delinquency rates, charge-offs, and payment rates
   - Time dimensions should default to the most recent period unless specified
6. Write the result to the artifacts table:
   ```sql
   INSERT INTO `{{catalog}}`.`{{schema}}`.pipeline_artifacts
   VALUES (
     '{{job.run_id}}',
     'create_genie_agent',
     'agent_result',
     '{"agent_name": "{{agent_name}}", "space_id": "<SPACE_ID>", "status": "SUCCESS_OR_FAILURE", "sample_questions_added": N}',
     current_timestamp()
   )
IMPORTANT:
The metric view must already exist (created by the previous task).
If the Genie Agent creation fails, document the error in the artifact and do not retry.

### Timeout
Set to **300 seconds** (5 minutes).
------
## Task 5: Audit & Report

**Type**: Notebook (Python)
**Depends on**: Task 4 (or Task 3 if Task 4 is skipped)
**Run condition**: `ALL_DONE` (runs even if upstream tasks fail)
**Purpose**: Produce a summary of the entire pipeline run for auditability.

### Logic

```python
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
run_id = dbutils.widgets.get("run_id")

# -- 1. Read all artifacts for this run --
artifacts = spark.sql(f"""
  SELECT task_name, artifact_type, payload, created_at
  FROM `{catalog}`.`{schema}`.pipeline_artifacts
  WHERE run_id = '{run_id}'
  ORDER BY created_at
""").collect()

# -- 2. Build summary --
import json

summary = {
    "run_id": run_id,
    "total_tasks_logged": len(artifacts),
    "tasks": {}
}

for a in artifacts:
    try:
        payload = json.loads(a['payload'])
    except:
        payload = {"raw": a['payload']}
    summary["tasks"][a['artifact_type']] = {
        "task_name": a['task_name'],
        "payload": payload,
        "timestamp": str(a['created_at'])
    }

# -- 3. Determine overall pipeline status --
validation = summary["tasks"].get("validation", {}).get("payload", {})
overall_status = validation.get("overall_status", "UNKNOWN")

summary["pipeline_status"] = overall_status
summary["metric_view_created"] = validation.get("exists", False)
summary["measures_created"] = validation.get("measure_count", 0)
summary["dimensions_created"] = validation.get("dimension_count", 0)

# -- 4. Write audit summary artifact --
summary_json = json.dumps(summary)
spark.sql(f"""
  INSERT INTO `{catalog}`.`{schema}`.pipeline_artifacts
  VALUES ('{run_id}', 'audit', 'audit_summary', '{summary_json}', current_timestamp())
""")

# -- 5. Print summary for job logs --
print("=" * 60)
print(f"PIPELINE RUN SUMMARY: {run_id}")
print(f"Status: {overall_status}")
print(f"Metric View Created: {validation.get('exists', False)}")
print(f"Measures: {validation.get('measure_count', 0)}")
print(f"Dimensions: {validation.get('dimension_count', 0)}")
print(f"Smoke Test: {validation.get('smoke_test_status', 'N/A')}")
print("=" * 60)

Notes & Caveats
/importBI in headless Genie Code Task mode
The /importBI command is proven in interactive Genie Code sessions where you attach a file to the chat. In a headless Genie Code Task, the agent references the .pbit via its UC Volume path instead of a file attachment. This should work — Genie Code Tasks inherit the same tools and settings as interactive sessions, and /importBI docs confirm it can reference files stored in a UC Volume. However, the headless auto-approve mode means the agent can't ask you clarifying questions mid-import (e.g., "which tables should I map to?"). Mitigation: before automating, run the exact Task 2 prompt in an interactive Genie Code session to confirm the .pbit imports cleanly. Once validated, promote to the Lakeflow Job.
Idempotency (re-running the same .pbit)
Each .pbit file gets its own metric_view_name job parameter (e.g., mv_nfcu_lending vs. mv_nfcu_lending_1001). Running the pipeline on different .pbit files will create different metric views — no collision. If you re-run the pipeline on the same .pbit with the same metric_view_name, the /importBI agent will overwrite the existing metric view via CREATE OR REPLACE semantics. This is intentional — it allows iterative refinement. If you need to preserve previous versions, append a timestamp or version suffix to the metric_view_name parameter.
Scope: one .pbit per pipeline run
This spec is designed for one workflow run per .pbit file. To process nfcu_lending_desktop_semantic.pbit and nfcu_lending_desktop_semantic_1001.pbit, trigger two separate job runs with different pbit_filename and metric_view_name parameters.
Future extension (batch mode): Extend Task 1 to list all .pbit files in the volume, then use a ForEach task or parameterized job triggers to fan out one pipeline run per file. The artifacts table already supports this — each run writes its own run_id, so batch results stay isolated and queryable.

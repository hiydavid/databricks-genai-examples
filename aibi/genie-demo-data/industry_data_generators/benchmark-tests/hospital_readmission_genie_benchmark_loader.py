# Databricks notebook source

# MAGIC %md
# MAGIC # Hospital Readmission - Genie Benchmark Loader
# MAGIC
# MAGIC Loads 30 ground-truth benchmark Q&A pairs (natural-language question + correct
# MAGIC Databricks SQL) into a target **Genie Space** by mutating ONLY the benchmark
# MAGIC portion of the space's serialized config.
# MAGIC
# MAGIC **What it does**
# MAGIC 1. `GET`s the Genie Space (with `include_serialized_space=true`).
# MAGIC 2. Replaces `serialized["benchmarks"]["questions"]` with the 30 entries.
# MAGIC 3. `PATCH`es the space (echoing existing title/description/warehouse_id).
# MAGIC 4. Round-trip verifies the 30 questions landed and nothing else changed.
# MAGIC
# MAGIC **SAFETY:** This notebook mutates **ONLY** `benchmarks.questions`. It does NOT
# MAGIC touch `config`, `data_sources`, `instructions`, or `version` of the space.
# MAGIC Re-running cleanly REPLACES the 30 benchmark questions (idempotent, no dupes).
# MAGIC
# MAGIC The dataset is fully synthetic (patient IDs only, no PHI). Questions are clinical/operational.

# COMMAND ----------

# MAGIC %pip install databricks-sdk --upgrade -q
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Widgets & Configuration

# COMMAND ----------

# =============================================================================
# CONFIGURATION
# =============================================================================
dbutils.widgets.text("space_id", "", "Target Genie Space ID (required)")
dbutils.widgets.text("catalog", "dhuang_catalog", "Unity Catalog name")
dbutils.widgets.text("schema", "hospital_readmission", "Schema / database name")

space_id = dbutils.widgets.get("space_id").strip()
catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()

if not space_id:
    raise ValueError("Widget 'space_id' is required (the target Genie Space ID).")

print(f"space_id : {space_id}")
print(f"catalog  : {catalog}")
print(f"schema   : {schema}")

# COMMAND ----------

import json
import uuid

from databricks.sdk import WorkspaceClient

# Auto-authenticates inside a Databricks notebook.
w = WorkspaceClient()
print("WorkspaceClient ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Benchmark Q&A (30 questions)
# MAGIC
# MAGIC Each entry: a business question, a difficulty tag (`EASY` / `MEDIUM` / `HARD`),
# MAGIC and the ground-truth Databricks SQL. Tables are referenced with `{catalog}` /
# MAGIC `{schema}` Python format placeholders, rendered from the widgets at load time.
# MAGIC
# MAGIC Mix: 5 EASY / 15 MEDIUM / 10 HARD. Note: each encounter has at most one
# MAGIC readmission row, so `COUNT(DISTINCT readmission_id) / COUNT(DISTINCT encounter_id)`
# MAGIC is the 30-day readmission rate.

# COMMAND ----------

BENCHMARKS = [
    {
        "question": "How many index admission encounters are in the dataset?",
        "difficulty": "EASY",
        "sql": """SELECT COUNT(*) AS total_encounters
FROM `{catalog}`.`{schema}`.`encounters`""",
    },
    {
        "question": "How many hospitals are in our network, and how many of them run a care-transition program?",
        "difficulty": "EASY",
        "sql": """SELECT COUNT(*) AS total_hospitals,
       SUM(CASE WHEN has_transition_program THEN 1 ELSE 0 END) AS hospitals_with_transition_program
FROM `{catalog}`.`{schema}`.`hospitals`""",
    },
    {
        "question": "What is the average length of stay, in days, across all encounters?",
        "difficulty": "EASY",
        "sql": """SELECT ROUND(AVG(length_of_stay_days), 2) AS avg_length_of_stay_days
FROM `{catalog}`.`{schema}`.`encounters`""",
    },
    {
        "question": "How many encounters do we have for each primary diagnosis group?",
        "difficulty": "EASY",
        "sql": """SELECT primary_diagnosis_group,
       COUNT(*) AS encounter_count
FROM `{catalog}`.`{schema}`.`encounters`
GROUP BY primary_diagnosis_group
ORDER BY encounter_count DESC""",
    },
    {
        "question": "How many patients fall into each clinical risk band?",
        "difficulty": "EASY",
        "sql": """SELECT clinical_risk_band,
       COUNT(*) AS patient_count
FROM `{catalog}`.`{schema}`.`patients`
GROUP BY clinical_risk_band
ORDER BY patient_count DESC""",
    },
    {
        "question": "What is our overall 30-day readmission rate?",
        "difficulty": "MEDIUM",
        "sql": """SELECT COUNT(DISTINCT e.encounter_id) AS encounters,
       COUNT(DISTINCT r.readmission_id) AS readmissions,
       ROUND(COUNT(DISTINCT r.readmission_id) * 100.0 / COUNT(DISTINCT e.encounter_id), 2) AS readmission_rate_pct
FROM `{catalog}`.`{schema}`.`encounters` e
LEFT JOIN `{catalog}`.`{schema}`.`readmissions` r
  ON e.encounter_id = r.index_encounter_id""",
    },
    {
        "question": "What is the 30-day readmission rate for each primary diagnosis group, ranked highest to lowest?",
        "difficulty": "MEDIUM",
        "sql": """SELECT e.primary_diagnosis_group,
       COUNT(DISTINCT e.encounter_id) AS encounters,
       COUNT(DISTINCT r.readmission_id) AS readmissions,
       ROUND(COUNT(DISTINCT r.readmission_id) * 100.0 / COUNT(DISTINCT e.encounter_id), 2) AS readmission_rate_pct
FROM `{catalog}`.`{schema}`.`encounters` e
LEFT JOIN `{catalog}`.`{schema}`.`readmissions` r
  ON e.encounter_id = r.index_encounter_id
GROUP BY e.primary_diagnosis_group
ORDER BY readmission_rate_pct DESC""",
    },
    {
        "question": "Which five hospitals have the highest 30-day readmission rates?",
        "difficulty": "MEDIUM",
        "sql": """SELECT h.hospital_name,
       COUNT(DISTINCT e.encounter_id) AS encounters,
       COUNT(DISTINCT r.readmission_id) AS readmissions,
       ROUND(COUNT(DISTINCT r.readmission_id) * 100.0 / COUNT(DISTINCT e.encounter_id), 2) AS readmission_rate_pct
FROM `{catalog}`.`{schema}`.`encounters` e
JOIN `{catalog}`.`{schema}`.`hospitals` h
  ON e.hospital_id = h.hospital_id
LEFT JOIN `{catalog}`.`{schema}`.`readmissions` r
  ON e.encounter_id = r.index_encounter_id
GROUP BY h.hospital_name
ORDER BY readmission_rate_pct DESC
LIMIT 5""",
    },
    {
        "question": "How does the 30-day readmission rate compare between patients who received a 7-day follow-up and those who did not?",
        "difficulty": "MEDIUM",
        "sql": """SELECT ct.follow_up_within_7_days,
       COUNT(DISTINCT e.encounter_id) AS encounters,
       COUNT(DISTINCT r.readmission_id) AS readmissions,
       ROUND(COUNT(DISTINCT r.readmission_id) * 100.0 / COUNT(DISTINCT e.encounter_id), 2) AS readmission_rate_pct
FROM `{catalog}`.`{schema}`.`encounters` e
JOIN `{catalog}`.`{schema}`.`care_transitions` ct
  ON e.encounter_id = ct.encounter_id
LEFT JOIN `{catalog}`.`{schema}`.`readmissions` r
  ON e.encounter_id = r.index_encounter_id
GROUP BY ct.follow_up_within_7_days
ORDER BY readmission_rate_pct DESC""",
    },
    {
        "question": "What is the average allowed claim amount by payer type?",
        "difficulty": "MEDIUM",
        "sql": """SELECT payer_type,
       COUNT(*) AS claim_count,
       ROUND(AVG(allowed_amount_usd), 2) AS avg_allowed_amount_usd
FROM `{catalog}`.`{schema}`.`claims`
GROUP BY payer_type
ORDER BY avg_allowed_amount_usd DESC""",
    },
    {
        "question": "What is the total allowed amount and average patient responsibility for each service line?",
        "difficulty": "MEDIUM",
        "sql": """SELECT service_line,
       COUNT(*) AS claim_count,
       ROUND(SUM(allowed_amount_usd), 2) AS total_allowed_usd,
       ROUND(AVG(patient_responsibility_usd), 2) AS avg_patient_responsibility_usd
FROM `{catalog}`.`{schema}`.`claims`
GROUP BY service_line
ORDER BY total_allowed_usd DESC""",
    },
    {
        "question": "Do weekend discharges have a higher 30-day readmission rate than weekday discharges?",
        "difficulty": "MEDIUM",
        "sql": """SELECT e.weekend_discharge,
       COUNT(DISTINCT e.encounter_id) AS encounters,
       COUNT(DISTINCT r.readmission_id) AS readmissions,
       ROUND(COUNT(DISTINCT r.readmission_id) * 100.0 / COUNT(DISTINCT e.encounter_id), 2) AS readmission_rate_pct
FROM `{catalog}`.`{schema}`.`encounters` e
LEFT JOIN `{catalog}`.`{schema}`.`readmissions` r
  ON e.encounter_id = r.index_encounter_id
GROUP BY e.weekend_discharge
ORDER BY readmission_rate_pct DESC""",
    },
    {
        "question": "Which service lines have the longest average length of stay?",
        "difficulty": "MEDIUM",
        "sql": """SELECT service_line,
       COUNT(*) AS encounters,
       ROUND(AVG(length_of_stay_days), 2) AS avg_length_of_stay_days
FROM `{catalog}`.`{schema}`.`encounters`
GROUP BY service_line
ORDER BY avg_length_of_stay_days DESC""",
    },
    {
        "question": "What is the 7-day follow-up rate and average number of outreach attempts by hospital region?",
        "difficulty": "MEDIUM",
        "sql": """SELECT h.region,
       COUNT(*) AS transitions,
       ROUND(SUM(CASE WHEN ct.follow_up_within_7_days THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS follow_up_rate_pct,
       ROUND(AVG(ct.outreach_attempts), 2) AS avg_outreach_attempts
FROM `{catalog}`.`{schema}`.`care_transitions` ct
JOIN `{catalog}`.`{schema}`.`hospitals` h
  ON ct.hospital_id = h.hospital_id
GROUP BY h.region
ORDER BY follow_up_rate_pct DESC""",
    },
    {
        "question": "For readmitted patients, what is the average days to readmission and average readmission cost by the original diagnosis group?",
        "difficulty": "MEDIUM",
        "sql": """SELECT e.primary_diagnosis_group,
       COUNT(*) AS readmissions,
       ROUND(AVG(r.days_to_readmission), 1) AS avg_days_to_readmission,
       ROUND(AVG(r.readmission_cost_usd), 2) AS avg_readmission_cost_usd
FROM `{catalog}`.`{schema}`.`readmissions` r
JOIN `{catalog}`.`{schema}`.`encounters` e
  ON r.index_encounter_id = e.encounter_id
GROUP BY e.primary_diagnosis_group
ORDER BY avg_days_to_readmission""",
    },
    {
        "question": "What is the 30-day readmission rate for each clinical risk band?",
        "difficulty": "MEDIUM",
        "sql": """SELECT p.clinical_risk_band,
       COUNT(DISTINCT e.encounter_id) AS encounters,
       COUNT(DISTINCT r.readmission_id) AS readmissions,
       ROUND(COUNT(DISTINCT r.readmission_id) * 100.0 / COUNT(DISTINCT e.encounter_id), 2) AS readmission_rate_pct
FROM `{catalog}`.`{schema}`.`encounters` e
JOIN `{catalog}`.`{schema}`.`patients` p
  ON e.patient_id = p.patient_id
LEFT JOIN `{catalog}`.`{schema}`.`readmissions` r
  ON e.encounter_id = r.index_encounter_id
GROUP BY p.clinical_risk_band
ORDER BY readmission_rate_pct DESC""",
    },
    {
        "question": "Which service lines have the highest claim denial rates?",
        "difficulty": "MEDIUM",
        "sql": """SELECT service_line,
       COUNT(*) AS claim_count,
       SUM(CASE WHEN claim_status = 'Denied' THEN 1 ELSE 0 END) AS denied_claims,
       ROUND(SUM(CASE WHEN claim_status = 'Denied' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS denial_rate_pct
FROM `{catalog}`.`{schema}`.`claims`
GROUP BY service_line
ORDER BY denial_rate_pct DESC""",
    },
    {
        "question": "What were the monthly encounter volumes in 2024?",
        "difficulty": "MEDIUM",
        "sql": """SELECT encounter_month,
       COUNT(*) AS encounters
FROM `{catalog}`.`{schema}`.`encounters`
WHERE encounter_year = 2024
GROUP BY encounter_month
ORDER BY encounter_month""",
    },
    {
        "question": "What is the 30-day readmission rate by discharge disposition?",
        "difficulty": "MEDIUM",
        "sql": """SELECT e.discharge_disposition,
       COUNT(DISTINCT e.encounter_id) AS encounters,
       COUNT(DISTINCT r.readmission_id) AS readmissions,
       ROUND(COUNT(DISTINCT r.readmission_id) * 100.0 / COUNT(DISTINCT e.encounter_id), 2) AS readmission_rate_pct
FROM `{catalog}`.`{schema}`.`encounters` e
LEFT JOIN `{catalog}`.`{schema}`.`readmissions` r
  ON e.encounter_id = r.index_encounter_id
GROUP BY e.discharge_disposition
ORDER BY readmission_rate_pct DESC""",
    },
    {
        "question": "What is the total allowed claim amount and the denial rate for each hospital region?",
        "difficulty": "MEDIUM",
        "sql": """SELECT h.region,
       COUNT(*) AS claim_count,
       ROUND(SUM(c.allowed_amount_usd), 2) AS total_allowed_usd,
       ROUND(SUM(CASE WHEN c.claim_status = 'Denied' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS denial_rate_pct
FROM `{catalog}`.`{schema}`.`claims` c
JOIN `{catalog}`.`{schema}`.`hospitals` h
  ON c.hospital_id = h.hospital_id
GROUP BY h.region
ORDER BY total_allowed_usd DESC""",
    },
    {
        "question": "For each diagnosis group, which hospital has the highest 30-day readmission rate among hospitals with at least 50 encounters for that diagnosis?",
        "difficulty": "HARD",
        "sql": """WITH diag_hosp AS (
  SELECT e.primary_diagnosis_group AS diagnosis_group,
         h.hospital_name,
         COUNT(DISTINCT e.encounter_id) AS encounters,
         COUNT(DISTINCT r.readmission_id) AS readmissions,
         COUNT(DISTINCT r.readmission_id) * 100.0 / COUNT(DISTINCT e.encounter_id) AS readmission_rate_pct
  FROM `{catalog}`.`{schema}`.`encounters` e
  JOIN `{catalog}`.`{schema}`.`hospitals` h
    ON e.hospital_id = h.hospital_id
  LEFT JOIN `{catalog}`.`{schema}`.`readmissions` r
    ON e.encounter_id = r.index_encounter_id
  GROUP BY e.primary_diagnosis_group, h.hospital_name
  HAVING COUNT(DISTINCT e.encounter_id) >= 50
),
ranked AS (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY diagnosis_group
           ORDER BY readmission_rate_pct DESC, encounters DESC, hospital_name
         ) AS rn
  FROM diag_hosp
)
SELECT diagnosis_group,
       hospital_name,
       encounters,
       readmissions,
       ROUND(readmission_rate_pct, 2) AS readmission_rate_pct
FROM ranked
WHERE rn = 1
ORDER BY readmission_rate_pct DESC""",
    },
    {
        "question": "How did our 30-day readmission rate change month over month during 2024?",
        "difficulty": "HARD",
        "sql": """WITH monthly AS (
  SELECT DATE_TRUNC('MONTH', e.discharge_date) AS discharge_month,
         COUNT(DISTINCT e.encounter_id) AS encounters,
         COUNT(DISTINCT r.readmission_id) AS readmissions,
         COUNT(DISTINCT r.readmission_id) * 100.0 / COUNT(DISTINCT e.encounter_id) AS readmission_rate_pct
  FROM `{catalog}`.`{schema}`.`encounters` e
  LEFT JOIN `{catalog}`.`{schema}`.`readmissions` r
    ON e.encounter_id = r.index_encounter_id
  WHERE e.discharge_date >= DATE'2024-01-01'
    AND e.discharge_date < DATE'2025-01-01'
  GROUP BY DATE_TRUNC('MONTH', e.discharge_date)
)
SELECT discharge_month,
       encounters,
       readmissions,
       ROUND(readmission_rate_pct, 2) AS readmission_rate_pct,
       ROUND(readmission_rate_pct - LAG(readmission_rate_pct) OVER (ORDER BY discharge_month), 2) AS change_vs_prev_month
FROM monthly
ORDER BY discharge_month""",
    },
    {
        "question": "Did the care-transition program reduce readmissions? Compare the readmission rate at program hospitals before and after launch against hospitals with no program.",
        "difficulty": "HARD",
        "sql": """SELECT CASE
         WHEN h.has_transition_program AND e.discharge_date >= h.transition_program_start_date THEN 'Program - After Launch'
         WHEN h.has_transition_program AND e.discharge_date <  h.transition_program_start_date THEN 'Program - Before Launch'
         ELSE 'No Program'
       END AS cohort,
       COUNT(DISTINCT e.encounter_id) AS encounters,
       COUNT(DISTINCT r.readmission_id) AS readmissions,
       ROUND(COUNT(DISTINCT r.readmission_id) * 100.0 / COUNT(DISTINCT e.encounter_id), 2) AS readmission_rate_pct
FROM `{catalog}`.`{schema}`.`encounters` e
JOIN `{catalog}`.`{schema}`.`hospitals` h
  ON e.hospital_id = h.hospital_id
LEFT JOIN `{catalog}`.`{schema}`.`readmissions` r
  ON e.encounter_id = r.index_encounter_id
GROUP BY 1
ORDER BY readmission_rate_pct DESC""",
    },
    {
        "question": "What are the median and 90th-percentile allowed claim amounts for each service line?",
        "difficulty": "HARD",
        "sql": """SELECT service_line,
       COUNT(*) AS claim_count,
       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY allowed_amount_usd), 2) AS median_allowed_usd,
       ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY allowed_amount_usd), 2) AS p90_allowed_usd
FROM `{catalog}`.`{schema}`.`claims`
GROUP BY service_line
ORDER BY p90_allowed_usd DESC""",
    },
    {
        "question": "Which age-band and clinical-risk-band combinations have the highest 30-day readmission rates?",
        "difficulty": "HARD",
        "sql": """SELECT p.age_band,
       p.clinical_risk_band,
       COUNT(DISTINCT e.encounter_id) AS encounters,
       COUNT(DISTINCT r.readmission_id) AS readmissions,
       ROUND(COUNT(DISTINCT r.readmission_id) * 100.0 / COUNT(DISTINCT e.encounter_id), 2) AS readmission_rate_pct
FROM `{catalog}`.`{schema}`.`encounters` e
JOIN `{catalog}`.`{schema}`.`patients` p
  ON e.patient_id = p.patient_id
LEFT JOIN `{catalog}`.`{schema}`.`readmissions` r
  ON e.encounter_id = r.index_encounter_id
GROUP BY p.age_band, p.clinical_risk_band
ORDER BY readmission_rate_pct DESC
LIMIT 10""",
    },
    {
        "question": "What is the 30-day readmission rate for each discharge quarter, and how do the quarters rank?",
        "difficulty": "HARD",
        "sql": """WITH quarterly AS (
  SELECT DATE_TRUNC('QUARTER', e.discharge_date) AS discharge_quarter,
         COUNT(DISTINCT e.encounter_id) AS encounters,
         COUNT(DISTINCT r.readmission_id) AS readmissions,
         COUNT(DISTINCT r.readmission_id) * 100.0 / COUNT(DISTINCT e.encounter_id) AS readmission_rate_pct
  FROM `{catalog}`.`{schema}`.`encounters` e
  LEFT JOIN `{catalog}`.`{schema}`.`readmissions` r
    ON e.encounter_id = r.index_encounter_id
  GROUP BY DATE_TRUNC('QUARTER', e.discharge_date)
)
SELECT discharge_quarter,
       encounters,
       readmissions,
       ROUND(readmission_rate_pct, 2) AS readmission_rate_pct,
       RANK() OVER (ORDER BY readmission_rate_pct DESC) AS rate_rank
FROM quarterly
ORDER BY discharge_quarter""",
    },
    {
        "question": "Which five hospitals account for the most total allowed claim spend, and what percentage of network spend does each represent?",
        "difficulty": "HARD",
        "sql": """WITH hospital_spend AS (
  SELECT h.hospital_name,
         SUM(c.allowed_amount_usd) AS total_allowed_usd
  FROM `{catalog}`.`{schema}`.`claims` c
  JOIN `{catalog}`.`{schema}`.`hospitals` h
    ON c.hospital_id = h.hospital_id
  GROUP BY h.hospital_name
)
SELECT hospital_name,
       ROUND(total_allowed_usd, 2) AS total_allowed_usd,
       ROUND(total_allowed_usd * 100.0 / SUM(total_allowed_usd) OVER (), 2) AS pct_of_network_spend
FROM hospital_spend
ORDER BY total_allowed_usd DESC
LIMIT 5""",
    },
    {
        "question": "Among high-risk patients, how does the 30-day readmission rate vary by whether they received both 7-day follow-up and medication reconciliation, only one, or neither?",
        "difficulty": "HARD",
        "sql": """SELECT CASE
         WHEN ct.follow_up_within_7_days AND ct.medication_reconciliation_completed THEN 'Both follow-up and med rec'
         WHEN ct.follow_up_within_7_days OR ct.medication_reconciliation_completed THEN 'One of the two'
         ELSE 'Neither'
       END AS transition_support,
       COUNT(DISTINCT e.encounter_id) AS encounters,
       COUNT(DISTINCT r.readmission_id) AS readmissions,
       ROUND(COUNT(DISTINCT r.readmission_id) * 100.0 / COUNT(DISTINCT e.encounter_id), 2) AS readmission_rate_pct
FROM `{catalog}`.`{schema}`.`encounters` e
JOIN `{catalog}`.`{schema}`.`patients` p
  ON e.patient_id = p.patient_id
JOIN `{catalog}`.`{schema}`.`care_transitions` ct
  ON e.encounter_id = ct.encounter_id
LEFT JOIN `{catalog}`.`{schema}`.`readmissions` r
  ON e.encounter_id = r.index_encounter_id
WHERE p.clinical_risk_band IN ('High', 'Very High')
GROUP BY 1
ORDER BY readmission_rate_pct DESC""",
    },
    {
        "question": "By hospital region, how does the 30-day readmission rate compare for weekend versus weekday discharges, and what is the gap in percentage points?",
        "difficulty": "HARD",
        "sql": """WITH region_rates AS (
  SELECT h.region,
         COUNT(DISTINCT CASE WHEN e.weekend_discharge THEN e.encounter_id END) AS weekend_encounters,
         COUNT(DISTINCT CASE WHEN e.weekend_discharge THEN r.readmission_id END) AS weekend_readmissions,
         COUNT(DISTINCT CASE WHEN NOT e.weekend_discharge THEN e.encounter_id END) AS weekday_encounters,
         COUNT(DISTINCT CASE WHEN NOT e.weekend_discharge THEN r.readmission_id END) AS weekday_readmissions
  FROM `{catalog}`.`{schema}`.`encounters` e
  JOIN `{catalog}`.`{schema}`.`hospitals` h
    ON e.hospital_id = h.hospital_id
  LEFT JOIN `{catalog}`.`{schema}`.`readmissions` r
    ON e.encounter_id = r.index_encounter_id
  GROUP BY h.region
)
SELECT region,
       ROUND(weekend_readmissions * 100.0 / NULLIF(weekend_encounters, 0), 2) AS weekend_readmission_rate_pct,
       ROUND(weekday_readmissions * 100.0 / NULLIF(weekday_encounters, 0), 2) AS weekday_readmission_rate_pct,
       ROUND(weekend_readmissions * 100.0 / NULLIF(weekend_encounters, 0)
           - weekday_readmissions * 100.0 / NULLIF(weekday_encounters, 0), 2) AS weekend_minus_weekday_pp
FROM region_rates
ORDER BY weekend_minus_weekday_pp DESC""",
    },
    {
        "question": "Which index-admission diagnosis groups drive the most unplanned 30-day readmissions, and what cumulative share of all unplanned readmissions do they account for?",
        "difficulty": "HARD",
        "sql": """WITH unplanned AS (
  SELECT e.primary_diagnosis_group AS diagnosis_group,
         COUNT(*) AS unplanned_readmissions
  FROM `{catalog}`.`{schema}`.`readmissions` r
  JOIN `{catalog}`.`{schema}`.`encounters` e
    ON r.index_encounter_id = e.encounter_id
  WHERE NOT r.planned_readmission
  GROUP BY e.primary_diagnosis_group
)
SELECT diagnosis_group,
       unplanned_readmissions,
       ROUND(unplanned_readmissions * 100.0 / SUM(unplanned_readmissions) OVER (), 2) AS pct_of_unplanned,
       ROUND(SUM(unplanned_readmissions) OVER (
               ORDER BY unplanned_readmissions DESC
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
             ) * 100.0 / SUM(unplanned_readmissions) OVER (), 2) AS cumulative_pct_of_unplanned
FROM unplanned
ORDER BY unplanned_readmissions DESC""",
    },
]

assert len(BENCHMARKS) == 30, f"Expected 30 benchmarks, found {len(BENCHMARKS)}"
_mix = {"EASY": 0, "MEDIUM": 0, "HARD": 0}
for _b in BENCHMARKS:
    _mix[_b["difficulty"]] += 1
print(f"Loaded {len(BENCHMARKS)} benchmarks. Difficulty mix: {_mix}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Fetch the Genie Space (pre-image snapshot)

# COMMAND ----------

resp = w.api_client.do(
    "GET",
    f"/api/2.0/genie/spaces/{space_id}",
    query={"include_serialized_space": "true"},
)
serialized = json.loads(resp["serialized_space"])  # inner JSON string -> dict

# Pre-image snapshots used for the round-trip verification (step 5).
import copy

pre_data_sources = copy.deepcopy(serialized.get("data_sources"))
pre_instructions = copy.deepcopy(serialized.get("instructions"))
pre_version = serialized.get("version")
pre_benchmark_count = len((serialized.get("benchmarks") or {}).get("questions") or [])

print(f"Fetched space: {resp.get('title')!r}")
print(f"  serialized_space top-level keys : {sorted(serialized.keys())}")
print(f"  existing benchmark questions    : {pre_benchmark_count}")
print(f"  serialized version              : {pre_version}")
ds_tables = (pre_data_sources or {}).get("tables")
if isinstance(ds_tables, list):
    print(f"  data_sources.tables             : {len(ds_tables)} table(s) (left untouched)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Mutate ONLY `benchmarks.questions`, then PATCH

# COMMAND ----------

# Build the 30 benchmark entries in the Genie benchmark schema.
questions = []
for b in BENCHMARKS:
    sql = b["sql"].format(catalog=catalog, schema=schema)
    questions.append(
        {
            "id": uuid.uuid4().hex,  # 32-hex opaque id
            "question": [b["question"]],
            "answer": [
                {
                    "format": "SQL",
                    "content": [ln + "\n" for ln in sql.split("\n")],
                }
            ],
        }
    )

# REPLACE the benchmark questions; do NOT touch config/data_sources/instructions/version.
serialized.setdefault("benchmarks", {})["questions"] = questions
serialized.setdefault("version", 2)

# PATCH, echoing existing metadata so nothing else is clobbered.
body = {"serialized_space": json.dumps(serialized)}
for k in ("title", "description", "warehouse_id"):
    if resp.get(k) is not None:
        body[k] = resp[k]

w.api_client.do("PATCH", f"/api/2.0/genie/spaces/{space_id}", body=body)
print(f"PATCH submitted: replaced benchmarks.questions with {len(questions)} entries.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Round-trip verification

# COMMAND ----------

resp2 = w.api_client.do(
    "GET",
    f"/api/2.0/genie/spaces/{space_id}",
    query={"include_serialized_space": "true"},
)
serialized2 = json.loads(resp2["serialized_space"])

post_questions = (serialized2.get("benchmarks") or {}).get("questions") or []

# (a) exactly 30 benchmark questions landed
assert len(post_questions) == 30, f"Expected 30 benchmark questions, found {len(post_questions)}"

# (b) the 30 question texts match what we loaded (order-independent)
expected_texts = sorted(b["question"] for b in BENCHMARKS)
actual_texts = sorted(
    (q["question"][0] if isinstance(q.get("question"), list) else q.get("question"))
    for q in post_questions
)
assert actual_texts == expected_texts, "Benchmark question texts do not match what was loaded."

# (c) data_sources / instructions unchanged vs the pre-image
assert serialized2.get("data_sources") == pre_data_sources, "data_sources changed -- unexpected!"
assert serialized2.get("instructions") == pre_instructions, "instructions changed -- unexpected!"

# (d) version unchanged if it pre-existed (we only setdefault it when absent)
expected_version = pre_version if pre_version is not None else 2
assert serialized2.get("version") == expected_version, (
    f"version changed unexpectedly: {pre_version} -> {serialized2.get('version')}"
)

print("=" * 70)
print("ROUND-TRIP VERIFICATION PASSED")
print("=" * 70)
print(f"  benchmark questions loaded : {len(post_questions)} (was {pre_benchmark_count})")
print(f"  question texts match        : True")
print(f"  data_sources unchanged      : True")
print(f"  instructions unchanged      : True")
print(f"  version unchanged           : True ({serialized2.get('version')})")
print(f"  space                       : {resp2.get('title')!r} ({space_id})")

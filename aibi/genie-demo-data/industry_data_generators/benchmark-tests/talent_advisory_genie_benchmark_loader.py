# Databricks notebook source

# MAGIC %md
# MAGIC # Talent Advisory - Genie Benchmark Loader
# MAGIC
# MAGIC Loads 30 ground-truth benchmark question/answer pairs into a Databricks AI/BI
# MAGIC **Genie Space** for the Talent Advisory (HR) domain by mutating **only** the
# MAGIC benchmark portion of the space's serialized configuration.
# MAGIC
# MAGIC The 30 questions span workforce planning, the hiring funnel, retention &
# MAGIC attrition, internal mobility, compensation, and succession - each paired with a
# MAGIC correct Databricks SQL answer that has been validated live against
# MAGIC `dhuang_catalog.talent_advisory`.
# MAGIC
# MAGIC ### What this notebook does
# MAGIC 1. (optional) Validates all 30 ground-truth SQL statements live before touching the space.
# MAGIC 2. `GET`s the Genie Space with its serialized config.
# MAGIC 3. Replaces **only** `serialized_space["benchmarks"]["questions"]` with the 30 entries.
# MAGIC 4. `PATCH`es the space back, echoing existing metadata.
# MAGIC 5. Round-trip verifies the 30 benchmarks landed and that `data_sources`,
# MAGIC    `instructions`, and `version` are unchanged.
# MAGIC
# MAGIC > **Safety note:** This notebook mutates **ONLY** `benchmarks.questions`. It does
# MAGIC > **not** touch `config`, `data_sources`, `instructions`, or `version`, and it
# MAGIC > preserves every other key of the serialized space. Re-running cleanly REPLACES
# MAGIC > the 30 benchmark questions (idempotent - no duplicates).

# COMMAND ----------

# MAGIC %pip install databricks-sdk --upgrade -q
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Widgets & configuration

# COMMAND ----------

dbutils.widgets.text("space_id", "", "Target Genie Space ID (required)")
dbutils.widgets.text("catalog", "dhuang_catalog", "Unity Catalog name")
dbutils.widgets.text("schema", "talent_advisory", "Schema / database name")
dbutils.widgets.dropdown("run_sql_validation", "true", ["true", "false"], "Validate SQL live before loading")
dbutils.widgets.dropdown("run_conversation_check", "false", ["true", "false"], "Sample Genie conversation check (optional)")

space_id = dbutils.widgets.get("space_id").strip()
catalog = dbutils.widgets.get("catalog").strip() or "dhuang_catalog"
schema = dbutils.widgets.get("schema").strip() or "talent_advisory"
run_sql_validation = dbutils.widgets.get("run_sql_validation").strip().lower() == "true"
run_conversation_check = dbutils.widgets.get("run_conversation_check").strip().lower() == "true"

if not space_id:
    raise ValueError("Widget 'space_id' is required - set it to the target Genie Space ID.")

import json
import uuid

from databricks.sdk import WorkspaceClient

# WorkspaceClient auto-authenticates inside Databricks.
w = WorkspaceClient()

print(f"Target Genie Space : {space_id}")
print(f"Data location      : {catalog}.{schema}")
print(f"SQL validation     : {run_sql_validation}")
print(f"Conversation check : {run_conversation_check}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The 30 benchmark questions
# MAGIC
# MAGIC Each entry is `{"question", "difficulty", "sql"}`. The SQL uses `{catalog}` /
# MAGIC `{schema}` Python format placeholders rendered at load time. Mix: 8 EASY /
# MAGIC 14 MEDIUM / 8 HARD.

# COMMAND ----------

BENCHMARKS = [
    # ---------------- EASY (8) ----------------
    {"question": "How many active employees do we currently have?", "difficulty": "EASY",
     "sql": "SELECT COUNT(*) AS active_employees FROM `{catalog}`.`{schema}`.`employees` WHERE is_active = true"},
    {"question": "How many work locations do we have in each region?", "difficulty": "EASY",
     "sql": "SELECT region, COUNT(*) AS location_count FROM `{catalog}`.`{schema}`.`locations` GROUP BY region ORDER BY location_count DESC, region"},
    {"question": "How many employees are in each job family?", "difficulty": "EASY",
     "sql": "SELECT job_family, COUNT(*) AS employee_count FROM `{catalog}`.`{schema}`.`employees` GROUP BY job_family ORDER BY employee_count DESC"},
    {"question": "How many hiring requisitions were opened in each year?", "difficulty": "EASY",
     "sql": "SELECT opened_year, COUNT(*) AS requisitions FROM `{catalog}`.`{schema}`.`requisitions` GROUP BY opened_year ORDER BY opened_year"},
    {"question": "What was the average performance rating in the 2025 review cycle?", "difficulty": "EASY",
     "sql": "SELECT ROUND(AVG(performance_rating), 2) AS avg_performance_rating FROM `{catalog}`.`{schema}`.`performance_reviews` WHERE review_year = 2025"},
    {"question": "How many roles are flagged as critical roles?", "difficulty": "EASY",
     "sql": "SELECT COUNT(*) AS critical_roles FROM `{catalog}`.`{schema}`.`job_roles` WHERE is_critical_role = true"},
    {"question": "How many employees have left, broken down by termination type?", "difficulty": "EASY",
     "sql": "SELECT termination_type, COUNT(*) AS exits FROM `{catalog}`.`{schema}`.`employees` WHERE termination_type IS NOT NULL GROUP BY termination_type ORDER BY exits DESC"},
    {"question": "How many learning activities are completed versus in progress or dropped?", "difficulty": "EASY",
     "sql": "SELECT completion_status, COUNT(*) AS activities FROM `{catalog}`.`{schema}`.`learning_activity` GROUP BY completion_status ORDER BY activities DESC"},

    # ---------------- MEDIUM (14) ----------------
    {"question": "For filled requisitions, what is the average time to fill and the total offers and accepted offers by job family?", "difficulty": "MEDIUM",
     "sql": "SELECT job_family, COUNT(*) AS filled_requisitions, ROUND(AVG(time_to_fill_days), 1) AS avg_time_to_fill_days, SUM(offer_count) AS offers, SUM(accepted_offer_count) AS accepted_offers FROM `{catalog}`.`{schema}`.`mart_hiring_funnel` WHERE status = 'Filled' GROUP BY job_family ORDER BY avg_time_to_fill_days DESC"},
    {"question": "Which org units have the most open requisitions right now? Show the top 10 by org unit name.", "difficulty": "MEDIUM",
     "sql": "SELECT o.org_unit_name, COUNT(*) AS open_requisitions FROM `{catalog}`.`{schema}`.`requisitions` r JOIN `{catalog}`.`{schema}`.`org_units` o ON r.org_unit_id = o.org_unit_id WHERE r.status = 'Open' GROUP BY o.org_unit_name ORDER BY open_requisitions DESC, o.org_unit_name LIMIT 10"},
    {"question": "What is the offer acceptance rate by candidate market segment (scarce, competitive, balanced)?", "difficulty": "MEDIUM",
     "sql": "SELECT r.candidate_market, COUNT(*) AS offers, ROUND(SUM(CASE WHEN a.offer_accepted THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS acceptance_rate_pct FROM `{catalog}`.`{schema}`.`applications` a JOIN `{catalog}`.`{schema}`.`requisitions` r ON a.requisition_id = r.requisition_id WHERE a.offer_accepted IS NOT NULL GROUP BY r.candidate_market ORDER BY acceptance_rate_pct DESC"},
    {"question": "What is the average base salary and compa-ratio by job level for 2025?", "difficulty": "MEDIUM",
     "sql": "SELECT job_level, COUNT(*) AS snapshots, ROUND(AVG(base_salary_usd), 0) AS avg_base_salary_usd, ROUND(AVG(compa_ratio), 3) AS avg_compa_ratio FROM `{catalog}`.`{schema}`.`compensation_snapshots` WHERE snapshot_year = 2025 GROUP BY job_level ORDER BY avg_base_salary_usd DESC"},
    {"question": "In the most recent quarter, how many high-risk employees and what average engagement score does each business unit have?", "difficulty": "MEDIUM",
     "sql": "SELECT business_unit, SUM(high_risk_employee_count) AS high_risk_employees, SUM(employee_count) AS employees, ROUND(AVG(avg_engagement_score), 2) AS avg_engagement_score FROM `{catalog}`.`{schema}`.`mart_retention_engagement` WHERE snapshot_quarter_label = (SELECT MAX(snapshot_quarter_label) FROM `{catalog}`.`{schema}`.`mart_retention_engagement`) GROUP BY business_unit ORDER BY high_risk_employees DESC"},
    {"question": "How has the average engagement score trended by quarter?", "difficulty": "MEDIUM",
     "sql": "SELECT pulse_quarter_label, ROUND(AVG(engagement_score), 2) AS avg_engagement_score, COUNT(*) AS pulses FROM `{catalog}`.`{schema}`.`engagement_pulses` GROUP BY pulse_quarter_label ORDER BY pulse_quarter_label"},
    {"question": "How many promotions, transfers, internal fills, and learning completions happened each year across the company?", "difficulty": "MEDIUM",
     "sql": "SELECT snapshot_year, SUM(promotion_count) AS promotions, SUM(transfer_count) AS transfers, SUM(internal_fill_count) AS internal_fills, SUM(learning_completion_count) AS learning_completions FROM `{catalog}`.`{schema}`.`mart_internal_mobility` GROUP BY snapshot_year ORDER BY snapshot_year"},
    {"question": "Which skills have the largest average skill gap? Show the top 10 by skill name.", "difficulty": "MEDIUM",
     "sql": "SELECT s.skill_name, ROUND(AVG(es.skill_gap), 2) AS avg_skill_gap, COUNT(*) AS assessments FROM `{catalog}`.`{schema}`.`employee_skills` es JOIN `{catalog}`.`{schema}`.`skills` s ON es.skill_id = s.skill_id GROUP BY s.skill_name ORDER BY avg_skill_gap DESC, s.skill_name LIMIT 10"},
    {"question": "What share of filled requisitions were filled internally each year?", "difficulty": "MEDIUM",
     "sql": "SELECT opened_year, COUNT(*) AS filled_requisitions, SUM(CASE WHEN is_internal_fill THEN 1 ELSE 0 END) AS internal_fills, ROUND(SUM(CASE WHEN is_internal_fill THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS internal_fill_rate_pct FROM `{catalog}`.`{schema}`.`requisitions` WHERE status = 'Filled' GROUP BY opened_year ORDER BY opened_year"},
    {"question": "In 2025, how many hires and exits did each business unit have, and what was the annual attrition rate?", "difficulty": "MEDIUM",
     "sql": "WITH monthly AS (SELECT business_unit, snapshot_month, SUM(active_headcount) AS month_active_headcount, SUM(hires) AS month_hires, SUM(exits) AS month_exits FROM `{catalog}`.`{schema}`.`mart_workforce_planning` WHERE snapshot_year = 2025 GROUP BY business_unit, snapshot_month) SELECT business_unit, SUM(month_hires) AS hires_2025, SUM(month_exits) AS exits_2025, ROUND(AVG(month_active_headcount), 0) AS avg_monthly_headcount, ROUND(SUM(month_exits) * 100.0 / NULLIF(AVG(month_active_headcount), 0), 2) AS attrition_rate_pct FROM monthly GROUP BY business_unit ORDER BY attrition_rate_pct DESC"},
    {"question": "Who are the 10 employees with the highest total cash compensation (base plus bonus) in 2025? Show their names and roles.", "difficulty": "MEDIUM",
     "sql": "SELECT e.employee_name, jr.role_title, c.job_family, ROUND(c.base_salary_usd + c.bonus_actual_usd, 0) AS total_cash_usd FROM `{catalog}`.`{schema}`.`compensation_snapshots` c JOIN `{catalog}`.`{schema}`.`employees` e ON c.employee_id = e.employee_id JOIN `{catalog}`.`{schema}`.`job_roles` jr ON c.role_id = jr.role_id WHERE c.snapshot_year = 2025 ORDER BY total_cash_usd DESC LIMIT 10"},
    {"question": "What is the distribution of employees across performance buckets in the 2025 review cycle?", "difficulty": "MEDIUM",
     "sql": "SELECT performance_bucket, COUNT(*) AS reviews, ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct_of_reviews FROM `{catalog}`.`{schema}`.`performance_reviews` WHERE review_year = 2025 GROUP BY performance_bucket ORDER BY reviews DESC"},
    {"question": "Which job families have the most critical skill gaps among employees?", "difficulty": "MEDIUM",
     "sql": "SELECT e.job_family, SUM(CASE WHEN es.is_critical_gap THEN 1 ELSE 0 END) AS critical_gaps, COUNT(*) AS total_assessments FROM `{catalog}`.`{schema}`.`employee_skills` es JOIN `{catalog}`.`{schema}`.`employees` e ON es.employee_id = e.employee_id GROUP BY e.job_family ORDER BY critical_gaps DESC"},
    {"question": "What is the readiness-level breakdown of named successors in our succession plans?", "difficulty": "MEDIUM",
     "sql": "SELECT readiness_level, COUNT(*) AS succession_plans, ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct_of_plans FROM `{catalog}`.`{schema}`.`succession_plans` GROUP BY readiness_level ORDER BY succession_plans DESC"},

    # ---------------- HARD (8) ----------------
    {"question": "For each business unit, which job family carries the highest average retention risk in 2025?", "difficulty": "HARD",
     "sql": "WITH risk AS (SELECT o.business_unit, rr.job_family, ROUND(AVG(rr.risk_score), 1) AS avg_risk_score, COUNT(*) AS snapshots FROM `{catalog}`.`{schema}`.`retention_risk_snapshots` rr JOIN `{catalog}`.`{schema}`.`org_units` o ON rr.org_unit_id = o.org_unit_id WHERE rr.snapshot_year = 2025 GROUP BY o.business_unit, rr.job_family), ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY business_unit ORDER BY avg_risk_score DESC, job_family) AS rn FROM risk) SELECT business_unit, job_family, avg_risk_score, snapshots FROM ranked WHERE rn = 1 ORDER BY avg_risk_score DESC"},
    {"question": "How did the average engagement score for Sales East change year over year from 2023 to 2025?", "difficulty": "HARD",
     "sql": "WITH yearly AS (SELECT e.pulse_year, ROUND(AVG(e.engagement_score), 3) AS avg_engagement FROM `{catalog}`.`{schema}`.`engagement_pulses` e JOIN `{catalog}`.`{schema}`.`org_units` o ON e.org_unit_id = o.org_unit_id WHERE o.org_unit_name = 'Sales East' GROUP BY e.pulse_year) SELECT pulse_year, avg_engagement, ROUND(avg_engagement - LAG(avg_engagement) OVER (ORDER BY pulse_year), 3) AS yoy_change FROM yearly ORDER BY pulse_year"},
    {"question": "Compare the average peak annual retention risk of high performers paid below 0.90 compa-ratio against other high performers and everyone else.", "difficulty": "HARD",
     "sql": "WITH annual_risk AS (SELECT employee_id, snapshot_year, MAX(risk_score) AS max_risk_score FROM `{catalog}`.`{schema}`.`retention_risk_snapshots` GROUP BY employee_id, snapshot_year) SELECT CASE WHEN p.is_high_performer AND c.compa_ratio < 0.90 THEN 'High performer below 0.90 compa' WHEN p.is_high_performer THEN 'High performer at/above 0.90 compa' ELSE 'Not a high performer' END AS segment, COUNT(*) AS employee_years, ROUND(AVG(r.max_risk_score), 1) AS avg_max_risk_score FROM `{catalog}`.`{schema}`.`compensation_snapshots` c JOIN `{catalog}`.`{schema}`.`performance_reviews` p ON c.employee_id = p.employee_id AND c.snapshot_year = p.review_year JOIN annual_risk r ON c.employee_id = r.employee_id AND c.snapshot_year = r.snapshot_year GROUP BY segment ORDER BY avg_max_risk_score DESC"},
    {"question": "What is the median and 90th-percentile time to fill for filled requisitions by job family?", "difficulty": "HARD",
     "sql": "SELECT job_family, COUNT(*) AS filled_requisitions, ROUND(PERCENTILE(time_to_fill_days, 0.5), 1) AS p50_time_to_fill_days, ROUND(PERCENTILE(time_to_fill_days, 0.9), 1) AS p90_time_to_fill_days FROM `{catalog}`.`{schema}`.`requisitions` WHERE status = 'Filled' AND time_to_fill_days IS NOT NULL GROUP BY job_family ORDER BY p90_time_to_fill_days DESC"},
    {"question": "For employees hired in 2023, what percentage were still active at the end of 2024 and at the end of 2025?", "difficulty": "HARD",
     "sql": "WITH cohort AS (SELECT employee_id, termination_date FROM `{catalog}`.`{schema}`.`employees` WHERE hire_date BETWEEN DATE'2023-01-01' AND DATE'2023-12-31') SELECT COUNT(*) AS hired_2023, SUM(CASE WHEN termination_date IS NULL OR termination_date > DATE'2024-12-31' THEN 1 ELSE 0 END) AS active_end_2024, ROUND(SUM(CASE WHEN termination_date IS NULL OR termination_date > DATE'2024-12-31' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS retained_end_2024_pct, SUM(CASE WHEN termination_date IS NULL OR termination_date > DATE'2025-12-31' THEN 1 ELSE 0 END) AS active_end_2025, ROUND(SUM(CASE WHEN termination_date IS NULL OR termination_date > DATE'2025-12-31' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS retained_end_2025_pct FROM cohort"},
    {"question": "Rank job families by 2025 regretted-loss exits and show each family's share of all regretted losses.", "difficulty": "HARD",
     "sql": "WITH regretted AS (SELECT job_family, COUNT(*) AS regretted_exits FROM `{catalog}`.`{schema}`.`talent_events` WHERE event_type = 'Exit' AND is_regretted_loss = true AND event_year = 2025 GROUP BY job_family) SELECT job_family, regretted_exits, ROUND(regretted_exits * 100.0 / SUM(regretted_exits) OVER (), 1) AS pct_of_total, RANK() OVER (ORDER BY regretted_exits DESC) AS rank FROM regretted ORDER BY regretted_exits DESC, job_family"},
    {"question": "By job family in 2025, what is the gap between the average compa-ratio of high performers and everyone else?", "difficulty": "HARD",
     "sql": "WITH comp AS (SELECT c.job_family, p.is_high_performer, c.compa_ratio FROM `{catalog}`.`{schema}`.`compensation_snapshots` c JOIN `{catalog}`.`{schema}`.`performance_reviews` p ON c.employee_id = p.employee_id AND c.snapshot_year = p.review_year WHERE c.snapshot_year = 2025) SELECT job_family, ROUND(AVG(CASE WHEN is_high_performer THEN compa_ratio END), 3) AS high_performer_avg_compa, ROUND(AVG(CASE WHEN NOT is_high_performer THEN compa_ratio END), 3) AS other_avg_compa, ROUND(AVG(CASE WHEN is_high_performer THEN compa_ratio END) - AVG(CASE WHEN NOT is_high_performer THEN compa_ratio END), 3) AS compa_gap FROM comp GROUP BY job_family HAVING COUNT(CASE WHEN is_high_performer THEN 1 END) > 0 ORDER BY compa_gap"},
    {"question": "Which org units had the biggest increase in high-risk employees from Q1 2024 to Q4 2024? Show the top 10.", "difficulty": "HARD",
     "sql": "WITH q AS (SELECT o.org_unit_name, rr.snapshot_quarter_label, SUM(CASE WHEN rr.risk_band = 'High' THEN 1 ELSE 0 END) AS high_risk FROM `{catalog}`.`{schema}`.`retention_risk_snapshots` rr JOIN `{catalog}`.`{schema}`.`org_units` o ON rr.org_unit_id = o.org_unit_id WHERE rr.snapshot_quarter_label IN ('2024-Q1', '2024-Q4') GROUP BY o.org_unit_name, rr.snapshot_quarter_label) SELECT org_unit_name, MAX(CASE WHEN snapshot_quarter_label = '2024-Q1' THEN high_risk END) AS high_risk_q1, MAX(CASE WHEN snapshot_quarter_label = '2024-Q4' THEN high_risk END) AS high_risk_q4, MAX(CASE WHEN snapshot_quarter_label = '2024-Q4' THEN high_risk END) - MAX(CASE WHEN snapshot_quarter_label = '2024-Q1' THEN high_risk END) AS change_in_high_risk FROM q GROUP BY org_unit_name ORDER BY change_in_high_risk DESC LIMIT 10"},
]

assert len(BENCHMARKS) == 30, f"expected 30 benchmarks, found {len(BENCHMARKS)}"
_mix = {}
for _b in BENCHMARKS:
    _mix[_b["difficulty"]] = _mix.get(_b["difficulty"], 0) + 1
print(f"Loaded {len(BENCHMARKS)} benchmark questions. Difficulty mix: {_mix}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validate every SQL live (gated by `run_sql_validation`)
# MAGIC
# MAGIC Runs all 30 ground-truth queries against `{catalog}.{schema}` before mutating
# MAGIC the space. If ANY query fails, the notebook raises and stops - we never push
# MAGIC benchmarks whose answers don't execute.

# COMMAND ----------

if run_sql_validation:
    results = []
    failures = []
    for i, b in enumerate(BENCHMARKS, 1):
        rendered = b["sql"].format(catalog=catalog, schema=schema)
        try:
            row_count = spark.sql(rendered).count()
            results.append((i, b["difficulty"], "PASS", row_count, ""))
        except Exception as e:  # noqa: BLE001 - surface any analysis/runtime error
            msg = str(e).splitlines()[0][:160] if str(e) else type(e).__name__
            results.append((i, b["difficulty"], "FAIL", None, msg))
            failures.append((i, b["question"], msg))

    pass_df = spark.createDataFrame(
        [(r[0], r[1], r[2], r[3], r[4]) for r in results],
        "idx int, difficulty string, status string, row_count int, error string",
    )
    display(pass_df.orderBy("idx"))

    n_pass = sum(1 for r in results if r[2] == "PASS")
    print(f"\nSQL validation: {n_pass}/{len(BENCHMARKS)} passed.")
    if failures:
        for idx, q, msg in failures:
            print(f"  [{idx:02d}] FAIL: {q}\n        {msg}")
        raise RuntimeError(
            f"{len(failures)} benchmark SQL statement(s) failed validation - aborting "
            f"before mutating the Genie Space."
        )
    print("All 30 ground-truth SQL statements executed successfully.")
else:
    print("run_sql_validation=false - skipping live SQL validation.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Fetch the Genie Space (GET) and snapshot the pre-image

# COMMAND ----------

resp = w.api_client.do(
    "GET",
    f"/api/2.0/genie/spaces/{space_id}",
    query={"include_serialized_space": "true"},
)

if "serialized_space" not in resp:
    raise RuntimeError(
        "GET response did not include 'serialized_space'. Confirm the space_id is "
        "correct and that this principal can read the space."
    )

# The serialized space is a JSON-encoded string -> parse to a dict.
serialized = json.loads(resp["serialized_space"])

# Pre-image snapshots for the post-PATCH verification in step 6. We deep-copy via a
# JSON round-trip so later mutation of `serialized` cannot alias these snapshots.
pre_data_sources = json.loads(json.dumps(serialized.get("data_sources")))
pre_instructions = json.loads(json.dumps(serialized.get("instructions")))
pre_version = serialized.get("version")
pre_benchmark_count = len((serialized.get("benchmarks") or {}).get("questions") or [])

print(f"Space title        : {resp.get('title')}")
print(f"Serialized version : {pre_version}")
print(f"data_sources tables: {len((pre_data_sources or {}).get('tables') or [])}")
print(f"instructions keys  : {list((pre_instructions or {}).keys()) if isinstance(pre_instructions, dict) else type(pre_instructions).__name__}")
print(f"existing benchmarks: {pre_benchmark_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Mutate ONLY `benchmarks.questions`, then PATCH the space
# MAGIC
# MAGIC We replace the benchmark question list and leave `config`, `data_sources`,
# MAGIC `instructions`, and `version` untouched.

# COMMAND ----------

questions = []
for b in BENCHMARKS:
    sql = b["sql"].format(catalog=catalog, schema=schema)
    questions.append({
        "id": uuid.uuid4().hex,  # 32-hex opaque id
        "question": [b["question"]],
        "answer": [{
            "format": "SQL",
            "content": [ln + "\n" for ln in sql.split("\n")],
        }],
    })

# Mutate ONLY the benchmark portion. Do NOT touch config / data_sources /
# instructions / version, and preserve every other key already present.
serialized.setdefault("benchmarks", {})["questions"] = questions  # REPLACE
serialized.setdefault("version", 2)

# PATCH the space, echoing existing metadata so nothing else is clobbered.
body = {"serialized_space": json.dumps(serialized)}
for k in ("title", "description", "warehouse_id"):
    if resp.get(k) is not None:
        body[k] = resp[k]

w.api_client.do("PATCH", f"/api/2.0/genie/spaces/{space_id}", body=body)
print(f"PATCH submitted: replaced benchmarks.questions with {len(questions)} entries.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Round-trip verify
# MAGIC
# MAGIC Re-fetch the space and assert: exactly 30 benchmarks landed, their questions
# MAGIC match what we sent, and `data_sources` / `instructions` / `version` are
# MAGIC unchanged versus the pre-image.

# COMMAND ----------

resp2 = w.api_client.do(
    "GET",
    f"/api/2.0/genie/spaces/{space_id}",
    query={"include_serialized_space": "true"},
)
serialized2 = json.loads(resp2["serialized_space"])

post_questions = (serialized2.get("benchmarks") or {}).get("questions") or []
assert len(post_questions) == 30, f"expected 30 benchmark questions, found {len(post_questions)}"

# The 30 questions must match what we authored, in order.
expected_questions = [b["question"] for b in BENCHMARKS]
actual_questions = [(q.get("question") or [None])[0] for q in post_questions]
assert actual_questions == expected_questions, "benchmark question text does not match what was sent"

# data_sources / instructions / version must be UNCHANGED vs the pre-image.
post_data_sources = serialized2.get("data_sources")
post_instructions = serialized2.get("instructions")
post_version = serialized2.get("version")
assert post_data_sources == pre_data_sources, "data_sources changed - it must remain untouched"
assert post_instructions == pre_instructions, "instructions changed - they must remain untouched"
assert post_version == pre_version, "version changed - it must remain untouched"

print("=" * 64)
print("VERIFICATION PASSED")
print("=" * 64)
print(f"  Space ID          : {space_id}")
print(f"  Benchmarks loaded : {len(post_questions)} (was {pre_benchmark_count})")
print(f"  data_sources      : UNCHANGED")
print(f"  instructions      : UNCHANGED")
print(f"  version           : UNCHANGED ({post_version})")
print(f"  Difficulty mix    : {_mix}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. (Optional) Sample Genie conversation check
# MAGIC
# MAGIC Gated by `run_conversation_check` (default **false**). For a small sample of
# MAGIC questions, exercises the Genie Conversation API and prints Genie's answer for
# MAGIC manual spot-checking. This is best-effort and never fails the notebook.

# COMMAND ----------

if run_conversation_check:
    import time

    SAMPLE_SIZE = 3
    sample = BENCHMARKS[:SAMPLE_SIZE]
    for b in sample:
        q = b["question"]
        print("=" * 64)
        print(f"Q: {q}")
        try:
            start = w.api_client.do(
                "POST",
                f"/api/2.0/genie/spaces/{space_id}/start-conversation",
                body={"content": q},
            )
            conversation_id = (start.get("conversation") or {}).get("id") or start.get("conversation_id")
            message_id = (start.get("message") or {}).get("id") or start.get("message_id")
            status = None
            message = {}
            for _ in range(30):  # poll up to ~150s
                message = w.api_client.do(
                    "GET",
                    f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}",
                )
                status = message.get("status")
                if status in ("COMPLETED", "FAILED", "CANCELLED", "QUERY_RESULT_EXPIRED"):
                    break
                time.sleep(5)
            print(f"   status: {status}")
            for attachment in (message.get("attachments") or []):
                if attachment.get("text"):
                    print(f"   text: {attachment['text'].get('content')}")
                if attachment.get("query"):
                    print(f"   query: {attachment['query'].get('query')}")
        except Exception as e:  # noqa: BLE001 - optional check, never fail the run
            print(f"   conversation check error (non-fatal): {e}")
else:
    print("run_conversation_check=false - skipping optional Genie conversation check.")

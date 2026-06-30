# Databricks notebook source

# MAGIC %md
# MAGIC # Horizon Bank — Genie Benchmark Loader
# MAGIC
# MAGIC Loads **30 benchmark Q&A pairs** (natural-language question + ground-truth
# MAGIC Databricks SQL) into a target **Genie Space** by mutating **only** the
# MAGIC benchmark portion of the space's serialized config.
# MAGIC
# MAGIC **What it does**
# MAGIC 1. `GET`s the Genie Space (`include_serialized_space=true`).
# MAGIC 2. Replaces `serialized_space.benchmarks.questions` with the 30 entries.
# MAGIC 3. `PATCH`es the space back.
# MAGIC 4. Round-trips a `GET` to verify the 30 questions landed and that
# MAGIC    `data_sources` / `instructions` / `version` are unchanged.
# MAGIC
# MAGIC **⚠️ Safety note:** This notebook mutates **ONLY** `benchmarks.questions`.
# MAGIC It does **not** touch `config`, `data_sources`, `instructions`, or `version`
# MAGIC (other than `setdefault`-ing `version` if it is somehow absent). Re-running is
# MAGIC idempotent: the 30 benchmark questions are fully replaced (no duplicates).
# MAGIC
# MAGIC **Data:** `dhuang_catalog.horizon_bank` (6 tables: products, branches,
# MAGIC customers, accounts, transactions, service_requests).

# COMMAND ----------

# MAGIC %pip install databricks-sdk --upgrade -q
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration & widgets

# COMMAND ----------

dbutils.widgets.text("space_id", "", "Genie Space ID (required)")
dbutils.widgets.text("catalog", "dhuang_catalog", "Unity Catalog name")
dbutils.widgets.text("schema", "horizon_bank", "Schema / database name")

space_id = dbutils.widgets.get("space_id").strip()
catalog = dbutils.widgets.get("catalog").strip() or "dhuang_catalog"
schema = dbutils.widgets.get("schema").strip() or "horizon_bank"

if not space_id:
    raise ValueError("space_id widget is required — set it to the target Genie Space ID.")

import json
import uuid

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()  # auto-authenticates inside Databricks

print(f"Target Genie Space : {space_id}")
print(f"Data location      : {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Benchmark questions (30) — grounded in `dhuang_catalog.horizon_bank`
# MAGIC
# MAGIC Difficulty mix: **5 EASY / 15 MEDIUM / 10 HARD**. Every SQL references tables as
# MAGIC `` `{catalog}`.`{schema}`.`<table>` `` and is rendered with
# MAGIC `.format(catalog=catalog, schema=schema)` at use time.

# COMMAND ----------

BENCHMARKS = [
    # ===================== EASY (5) =====================
    {
        "question": "How many active customers do we currently have?",
        "difficulty": "EASY",
        "sql": """SELECT COUNT(*) AS active_customers
FROM `{catalog}`.`{schema}`.`customers`
WHERE is_active = TRUE""",
    },
    {
        "question": "What was the total transaction amount in 2024?",
        "difficulty": "EASY",
        "sql": """SELECT ROUND(SUM(amount_usd), 2) AS total_transaction_amount_2024
FROM `{catalog}`.`{schema}`.`transactions`
WHERE transaction_year = 2024""",
    },
    {
        "question": "How many accounts do we have of each account type?",
        "difficulty": "EASY",
        "sql": """SELECT account_type, COUNT(*) AS account_count
FROM `{catalog}`.`{schema}`.`accounts`
GROUP BY account_type
ORDER BY account_count DESC""",
    },
    {
        "question": "Which products carry an annual fee of more than $50?",
        "difficulty": "EASY",
        "sql": """SELECT product_name, product_category, annual_fee_usd
FROM `{catalog}`.`{schema}`.`products`
WHERE annual_fee_usd > 50
ORDER BY annual_fee_usd DESC""",
    },
    {
        "question": "How many transactions were flagged by fraud or AML monitoring?",
        "difficulty": "EASY",
        "sql": """SELECT COUNT(*) AS flagged_transactions
FROM `{catalog}`.`{schema}`.`transactions`
WHERE is_flagged = TRUE""",
    },
    # ===================== MEDIUM (15) =====================
    {
        "question": "What was the total deposit volume by region in 2024?",
        "difficulty": "MEDIUM",
        "sql": """SELECT c.region, ROUND(SUM(t.amount_usd), 2) AS deposit_volume
FROM `{catalog}`.`{schema}`.`transactions` t
JOIN `{catalog}`.`{schema}`.`customers` c ON t.customer_id = c.customer_id
WHERE t.transaction_type = 'Deposit' AND t.transaction_year = 2024
GROUP BY c.region
ORDER BY deposit_volume DESC""",
    },
    {
        "question": "Which five branches processed the highest total transaction amount?",
        "difficulty": "MEDIUM",
        "sql": """SELECT b.branch_name, b.region,
       ROUND(SUM(t.amount_usd), 2) AS total_transaction_amount
FROM `{catalog}`.`{schema}`.`transactions` t
JOIN `{catalog}`.`{schema}`.`branches` b ON t.branch_id = b.branch_id
GROUP BY b.branch_name, b.region
ORDER BY total_transaction_amount DESC
LIMIT 5""",
    },
    {
        "question": "What is the mobile channel's share of transactions in each year?",
        "difficulty": "MEDIUM",
        "sql": """SELECT transaction_year,
       ROUND(COUNT_IF(channel = 'Mobile') * 100.0 / COUNT(*), 1) AS mobile_share_pct
FROM `{catalog}`.`{schema}`.`transactions`
GROUP BY transaction_year
ORDER BY transaction_year""",
    },
    {
        "question": "What is the average account balance by relationship tier?",
        "difficulty": "MEDIUM",
        "sql": """SELECT c.relationship_tier, ROUND(AVG(a.current_balance_usd), 2) AS avg_balance
FROM `{catalog}`.`{schema}`.`accounts` a
JOIN `{catalog}`.`{schema}`.`customers` c ON a.customer_id = c.customer_id
GROUP BY c.relationship_tier
ORDER BY avg_balance DESC""",
    },
    {
        "question": "Which account types generate the most fee revenue?",
        "difficulty": "MEDIUM",
        "sql": """SELECT a.account_type, ROUND(SUM(t.fee_usd), 2) AS total_fee_revenue
FROM `{catalog}`.`{schema}`.`transactions` t
JOIN `{catalog}`.`{schema}`.`accounts` a ON t.account_id = a.account_id
GROUP BY a.account_type
ORDER BY total_fee_revenue DESC""",
    },
    {
        "question": "What is the complaint rate by service request channel?",
        "difficulty": "MEDIUM",
        "sql": """SELECT channel,
       COUNT(*) AS total_requests,
       ROUND(COUNT_IF(category = 'Complaint') * 100.0 / COUNT(*), 1) AS complaint_rate_pct
FROM `{catalog}`.`{schema}`.`service_requests`
GROUP BY channel
ORDER BY complaint_rate_pct DESC""",
    },
    {
        "question": "Who are our top 10 customers by total transaction amount, and what tier and segment are they in?",
        "difficulty": "MEDIUM",
        "sql": """SELECT c.customer_name, c.relationship_tier, c.customer_segment,
       ROUND(SUM(t.amount_usd), 2) AS total_transaction_amount
FROM `{catalog}`.`{schema}`.`transactions` t
JOIN `{catalog}`.`{schema}`.`customers` c ON t.customer_id = c.customer_id
GROUP BY c.customer_id, c.customer_name, c.relationship_tier, c.customer_segment
ORDER BY total_transaction_amount DESC
LIMIT 10""",
    },
    {
        "question": "What is the average resolution time in days for each service request category?",
        "difficulty": "MEDIUM",
        "sql": """SELECT category, ROUND(AVG(resolution_time_days), 1) AS avg_resolution_days
FROM `{catalog}`.`{schema}`.`service_requests`
WHERE status = 'Resolved'
GROUP BY category
ORDER BY avg_resolution_days DESC""",
    },
    {
        "question": "How many delinquent accounts do we have in each region?",
        "difficulty": "MEDIUM",
        "sql": """SELECT c.region, COUNT(*) AS delinquent_accounts
FROM `{catalog}`.`{schema}`.`accounts` a
JOIN `{catalog}`.`{schema}`.`customers` c ON a.customer_id = c.customer_id
WHERE a.status = 'Delinquent'
GROUP BY c.region
ORDER BY delinquent_accounts DESC""",
    },
    {
        "question": "How does our total balance break down across deposit, loan, and credit accounts?",
        "difficulty": "MEDIUM",
        "sql": """SELECT
  CASE
    WHEN account_type IN ('Checking', 'Savings') THEN 'Deposit'
    WHEN account_type IN ('Mortgage', 'Auto Loan', 'Home Equity') THEN 'Loan'
    ELSE 'Credit'
  END AS balance_group,
  ROUND(SUM(current_balance_usd), 2) AS total_balance
FROM `{catalog}`.`{schema}`.`accounts`
GROUP BY 1
ORDER BY total_balance DESC""",
    },
    {
        "question": "Which five products are held by the most customers?",
        "difficulty": "MEDIUM",
        "sql": """SELECT p.product_name, p.product_category,
       COUNT(DISTINCT a.customer_id) AS customer_count
FROM `{catalog}`.`{schema}`.`accounts` a
JOIN `{catalog}`.`{schema}`.`products` p ON a.product_id = p.product_id
GROUP BY p.product_name, p.product_category
ORDER BY customer_count DESC
LIMIT 5""",
    },
    {
        "question": "What was the monthly transaction count trend across 2025?",
        "difficulty": "MEDIUM",
        "sql": """SELECT transaction_month, COUNT(*) AS transaction_count
FROM `{catalog}`.`{schema}`.`transactions`
WHERE transaction_year = 2025
GROUP BY transaction_month
ORDER BY transaction_month""",
    },
    {
        "question": "What share of transactions came through digital channels (Mobile or Online) in each region?",
        "difficulty": "MEDIUM",
        "sql": """SELECT c.region,
       ROUND(COUNT_IF(t.channel IN ('Mobile', 'Online')) * 100.0 / COUNT(*), 1) AS digital_share_pct
FROM `{catalog}`.`{schema}`.`transactions` t
JOIN `{catalog}`.`{schema}`.`customers` c ON t.customer_id = c.customer_id
GROUP BY c.region
ORDER BY digital_share_pct DESC""",
    },
    {
        "question": "How does the average transaction amount compare between digital and in-person channels for each customer segment?",
        "difficulty": "MEDIUM",
        "sql": """SELECT c.customer_segment,
       CASE WHEN t.channel IN ('Mobile', 'Online') THEN 'Digital' ELSE 'In-Person' END AS channel_type,
       ROUND(AVG(t.amount_usd), 2) AS avg_transaction_amount
FROM `{catalog}`.`{schema}`.`transactions` t
JOIN `{catalog}`.`{schema}`.`customers` c ON t.customer_id = c.customer_id
GROUP BY c.customer_segment, 2
ORDER BY c.customer_segment, channel_type""",
    },
    {
        "question": "How much fee revenue does each region generate, and how many branches contribute to it?",
        "difficulty": "MEDIUM",
        "sql": """SELECT b.region,
       COUNT(DISTINCT b.branch_id) AS contributing_branches,
       ROUND(SUM(t.fee_usd), 2) AS total_fee_revenue
FROM `{catalog}`.`{schema}`.`transactions` t
JOIN `{catalog}`.`{schema}`.`branches` b ON t.branch_id = b.branch_id
GROUP BY b.region
ORDER BY total_fee_revenue DESC""",
    },
    # ===================== HARD (10) =====================
    {
        "question": "For each region, which branch has the highest total transaction amount?",
        "difficulty": "HARD",
        "sql": """WITH branch_totals AS (
  SELECT b.region, b.branch_name, SUM(t.amount_usd) AS total_amount
  FROM `{catalog}`.`{schema}`.`transactions` t
  JOIN `{catalog}`.`{schema}`.`branches` b ON t.branch_id = b.branch_id
  GROUP BY b.region, b.branch_name
),
ranked AS (
  SELECT region, branch_name, total_amount,
         ROW_NUMBER() OVER (PARTITION BY region ORDER BY total_amount DESC) AS rn
  FROM branch_totals
)
SELECT region, branch_name, ROUND(total_amount, 2) AS total_amount
FROM ranked
WHERE rn = 1
ORDER BY total_amount DESC""",
    },
    {
        "question": "What was the month-over-month deposit volume growth throughout 2024?",
        "difficulty": "HARD",
        "sql": """WITH monthly AS (
  SELECT transaction_month AS mo, SUM(amount_usd) AS deposit_volume
  FROM `{catalog}`.`{schema}`.`transactions`
  WHERE transaction_type = 'Deposit' AND transaction_year = 2024
  GROUP BY transaction_month
)
SELECT mo,
       ROUND(deposit_volume, 2) AS deposit_volume,
       ROUND(LAG(deposit_volume) OVER (ORDER BY mo), 2) AS prev_month_volume,
       ROUND((deposit_volume - LAG(deposit_volume) OVER (ORDER BY mo))
             * 100.0 / LAG(deposit_volume) OVER (ORDER BY mo), 1) AS mom_growth_pct
FROM monthly
ORDER BY mo""",
    },
    {
        "question": "What percentage of total deposit volume comes from the top 10% of customers by deposit volume?",
        "difficulty": "HARD",
        "sql": """WITH cust_deposits AS (
  SELECT customer_id, SUM(amount_usd) AS total_deposits
  FROM `{catalog}`.`{schema}`.`transactions`
  WHERE transaction_type = 'Deposit'
  GROUP BY customer_id
),
ranked AS (
  SELECT customer_id, total_deposits,
         NTILE(10) OVER (ORDER BY total_deposits DESC) AS decile
  FROM cust_deposits
)
SELECT
  CASE WHEN decile = 1 THEN 'Top 10%' ELSE 'Other 90%' END AS segment,
  ROUND(SUM(total_deposits), 2) AS deposit_volume,
  ROUND(SUM(total_deposits) * 100.0 / SUM(SUM(total_deposits)) OVER (), 1) AS pct_of_total
FROM ranked
GROUP BY 1
ORDER BY deposit_volume DESC""",
    },
    {
        "question": "What are the median and 90th-percentile transaction amounts for each account type?",
        "difficulty": "HARD",
        "sql": """SELECT a.account_type,
       ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.amount_usd), 2) AS median_amount,
       ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY t.amount_usd), 2) AS p90_amount
FROM `{catalog}`.`{schema}`.`transactions` t
JOIN `{catalog}`.`{schema}`.`accounts` a ON t.account_id = a.account_id
GROUP BY a.account_type
ORDER BY p90_amount DESC""",
    },
    {
        "question": "How did the average deposit amount change quarter-over-quarter in 2024?",
        "difficulty": "HARD",
        "sql": """WITH quarterly AS (
  SELECT transaction_quarter AS qtr, AVG(amount_usd) AS avg_deposit
  FROM `{catalog}`.`{schema}`.`transactions`
  WHERE transaction_type = 'Deposit' AND transaction_year = 2024
  GROUP BY transaction_quarter
)
SELECT qtr,
       ROUND(avg_deposit, 2) AS avg_deposit_amount,
       ROUND((avg_deposit - LAG(avg_deposit) OVER (ORDER BY qtr))
             * 100.0 / LAG(avg_deposit) OVER (ORDER BY qtr), 1) AS qoq_change_pct
FROM quarterly
ORDER BY qtr""",
    },
    {
        "question": "Grouping customers by the year they joined, how many are in each cohort and what is their average deposit balance?",
        "difficulty": "HARD",
        "sql": """WITH cohorts AS (
  SELECT customer_id, YEAR(customer_since_date) AS cohort_year
  FROM `{catalog}`.`{schema}`.`customers`
),
deposit_bal AS (
  SELECT customer_id, SUM(current_balance_usd) AS deposit_balance
  FROM `{catalog}`.`{schema}`.`accounts`
  WHERE account_type IN ('Checking', 'Savings')
  GROUP BY customer_id
)
SELECT co.cohort_year,
       COUNT(DISTINCT co.customer_id) AS customers,
       ROUND(AVG(db.deposit_balance), 2) AS avg_deposit_balance
FROM cohorts co
LEFT JOIN deposit_bal db ON co.customer_id = db.customer_id
GROUP BY co.cohort_year
ORDER BY co.cohort_year""",
    },
    {
        "question": "What was the year-over-year growth in total fee revenue?",
        "difficulty": "HARD",
        "sql": """WITH yearly AS (
  SELECT transaction_year AS yr, SUM(fee_usd) AS fee_revenue
  FROM `{catalog}`.`{schema}`.`transactions`
  GROUP BY transaction_year
)
SELECT yr,
       ROUND(fee_revenue, 2) AS fee_revenue,
       ROUND((fee_revenue - LAG(fee_revenue) OVER (ORDER BY yr))
             * 100.0 / LAG(fee_revenue) OVER (ORDER BY yr), 1) AS yoy_growth_pct
FROM yearly
ORDER BY yr""",
    },
    {
        "question": "For each relationship tier, what is the average number of accounts per customer and the average total balance per customer?",
        "difficulty": "HARD",
        "sql": """WITH per_customer AS (
  SELECT c.customer_id, c.relationship_tier,
         COUNT(a.account_id) AS num_accounts,
         SUM(a.current_balance_usd) AS total_balance
  FROM `{catalog}`.`{schema}`.`customers` c
  JOIN `{catalog}`.`{schema}`.`accounts` a ON c.customer_id = a.customer_id
  GROUP BY c.customer_id, c.relationship_tier
)
SELECT relationship_tier,
       COUNT(*) AS customers,
       ROUND(AVG(num_accounts), 2) AS avg_accounts_per_customer,
       ROUND(AVG(total_balance), 2) AS avg_total_balance_per_customer
FROM per_customer
GROUP BY relationship_tier
ORDER BY avg_total_balance_per_customer DESC""",
    },
    {
        "question": "How does each branch region's service-request complaint rate compare to the bank-wide complaint rate?",
        "difficulty": "HARD",
        "sql": """WITH region_stats AS (
  SELECT b.region,
         COUNT(*) AS total_requests,
         COUNT_IF(sr.category = 'Complaint') AS complaints
  FROM `{catalog}`.`{schema}`.`service_requests` sr
  JOIN `{catalog}`.`{schema}`.`branches` b ON sr.branch_id = b.branch_id
  GROUP BY b.region
)
SELECT region,
       total_requests,
       ROUND(complaints * 100.0 / total_requests, 1) AS region_complaint_rate_pct,
       ROUND(SUM(complaints) OVER () * 100.0 / SUM(total_requests) OVER (), 1) AS bank_complaint_rate_pct
FROM region_stats
ORDER BY region_complaint_rate_pct DESC""",
    },
    {
        "question": "Within each product category, which products have the highest average account balance?",
        "difficulty": "HARD",
        "sql": """WITH product_balances AS (
  SELECT p.product_category, p.product_name,
         COUNT(a.account_id) AS account_count,
         AVG(a.current_balance_usd) AS avg_balance
  FROM `{catalog}`.`{schema}`.`accounts` a
  JOIN `{catalog}`.`{schema}`.`products` p ON a.product_id = p.product_id
  GROUP BY p.product_category, p.product_name
)
SELECT product_category, product_name, account_count,
       ROUND(avg_balance, 2) AS avg_balance,
       RANK() OVER (PARTITION BY product_category ORDER BY avg_balance DESC) AS rank_in_category
FROM product_balances
ORDER BY product_category, rank_in_category""",
    },
]

assert len(BENCHMARKS) == 30, f"Expected 30 benchmarks, found {len(BENCHMARKS)}"
_diff = {}
for _b in BENCHMARKS:
    _diff[_b["difficulty"]] = _diff.get(_b["difficulty"], 0) + 1
print(f"Loaded {len(BENCHMARKS)} benchmark questions. Difficulty mix: {_diff}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Fetch the Genie Space + snapshot the pre-image

# COMMAND ----------

resp = w.api_client.do(
    "GET",
    f"/api/2.0/genie/spaces/{space_id}",
    query={"include_serialized_space": "true"},
)
serialized = json.loads(resp["serialized_space"])  # inner JSON string -> dict

# Pre-image snapshots (deep copies via JSON round-trip) for step-6 verification.
pre_data_sources = json.loads(json.dumps(serialized.get("data_sources")))
pre_instructions = json.loads(json.dumps(serialized.get("instructions")))
pre_version = serialized.get("version")
# We will setdefault version to 2 below, so the expected post-value treats a
# missing pre-version as 2 (no real change to an existing version).
expected_version = pre_version if pre_version is not None else 2

n_pre_benchmarks = len((serialized.get("benchmarks") or {}).get("questions") or [])
print(f"Fetched space '{resp.get('title')}' (id={space_id}).")
print(f"  serialized_space keys : {sorted(serialized.keys())}")
print(f"  existing version      : {pre_version}")
print(f"  existing benchmarks   : {n_pre_benchmarks} question(s)")
print(f"  data_sources tables   : {len((serialized.get('data_sources') or {}).get('tables') or [])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Build benchmark entries and mutate ONLY `benchmarks.questions`

# COMMAND ----------

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

# REPLACE the benchmark questions; leave everything else untouched.
serialized.setdefault("benchmarks", {})["questions"] = questions
serialized.setdefault("version", 2)

print(f"Prepared {len(questions)} benchmark entries to load.")
print("Mutated keys: benchmarks.questions (+ version setdefault). Nothing else changed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. PATCH the space (echo existing metadata so nothing else is clobbered)

# COMMAND ----------

body = {"serialized_space": json.dumps(serialized)}
for k in ("title", "description", "warehouse_id"):
    if resp.get(k) is not None:
        body[k] = resp[k]

w.api_client.do("PATCH", f"/api/2.0/genie/spaces/{space_id}", body=body)
print("PATCH submitted — benchmark questions written to the space.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Round-trip verification

# COMMAND ----------

resp2 = w.api_client.do(
    "GET",
    f"/api/2.0/genie/spaces/{space_id}",
    query={"include_serialized_space": "true"},
)
serialized2 = json.loads(resp2["serialized_space"])

post_questions = (serialized2.get("benchmarks") or {}).get("questions") or []
post_texts = [q.get("question", [None])[0] for q in post_questions]
expected_texts = [b["question"] for b in BENCHMARKS]

# (a) Exactly 30 benchmark questions landed.
assert len(post_questions) == 30, f"Expected 30 questions, found {len(post_questions)}"
# (b) The 30 questions match what we loaded, in order (idempotent replace).
assert post_texts == expected_texts, "Loaded benchmark questions do not match BENCHMARKS"
# (c) Untouched sections are byte-for-byte unchanged vs the pre-image.
assert serialized2.get("data_sources") == pre_data_sources, "data_sources changed!"
assert serialized2.get("instructions") == pre_instructions, "instructions changed!"
assert serialized2.get("version") == expected_version, (
    f"version changed! pre={pre_version} expected={expected_version} "
    f"post={serialized2.get('version')}"
)

print("=" * 70)
print("✓ ROUND-TRIP VERIFICATION PASSED")
print("=" * 70)
print(f"  Benchmark questions loaded : {len(post_questions)} / 30")
print(f"  Questions match BENCHMARKS  : True")
print(f"  data_sources unchanged      : True")
print(f"  instructions unchanged      : True")
print(f"  version unchanged           : True (={serialized2.get('version')})")
print(f"  Space                       : {resp2.get('title')} ({space_id})")

# Databricks notebook source

# MAGIC %md
# MAGIC # SaaS Customer Churn Genie Benchmark Loader
# MAGIC
# MAGIC Loads 30 benchmark questions for the SaaS customer churn Genie Space.
# MAGIC
# MAGIC Safety note: this notebook fetches the existing Genie Space, mutates ONLY `benchmarks.questions` in the serialized space, patches the space, and then round-trip verifies that data sources, instructions, and version are unchanged.

# COMMAND ----------

# MAGIC %pip install databricks-sdk --upgrade -q
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Widgets and Configuration

# COMMAND ----------

from databricks.sdk import WorkspaceClient

DEFAULT_CATALOG = "dhuang_catalog"
DEFAULT_SCHEMA = "saas_churn"


def create_text_widget(name, default, label):
    try:
        dbutils.widgets.text(name, default, label)
    except Exception:
        pass


create_text_widget("space_id", "", "Target Genie Space ID")
create_text_widget("catalog", DEFAULT_CATALOG, "Unity Catalog name")
create_text_widget("schema", DEFAULT_SCHEMA, "Schema name")


def widget_value(name, default=""):
    try:
        value = dbutils.widgets.get(name).strip()
        return value if value else default
    except Exception:
        return default


space_id = widget_value("space_id")
catalog = widget_value("catalog", DEFAULT_CATALOG)
schema = widget_value("schema", DEFAULT_SCHEMA)

if not space_id:
    raise ValueError("The space_id widget is required.")

w = WorkspaceClient()

print(f"Configured benchmark loader for `{catalog}`.`{schema}`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Benchmark Questions

# COMMAND ----------

BENCHMARKS = [
    {
        "question": "What are total ARR and MRR by subscription status?",
        "difficulty": "EASY",
        "sql": """
SELECT
  `Subscription Status` AS `subscription_status`,
  MEASURE(`Subscription Count`) AS `subscription_count`,
  ROUND(MEASURE(`MRR`), 2) AS `total_mrr_usd`,
  ROUND(MEASURE(`ARR`), 2) AS `total_arr_usd`
FROM `{catalog}`.`{schema}`.`mv_subscription_revenue`
GROUP BY `Subscription Status`
ORDER BY `total_arr_usd` DESC
""",
    },
    {
        "question": "Which plan tiers have the most purchased seats?",
        "difficulty": "EASY",
        "sql": """
SELECT
  `plan_tier`,
  COUNT(*) AS `subscription_count`,
  SUM(`seats_purchased`) AS `seats_purchased`,
  ROUND(AVG(`discount_pct`), 2) AS `avg_discount_pct`
FROM `{catalog}`.`{schema}`.`subscriptions`
GROUP BY `plan_tier`
ORDER BY `seats_purchased` DESC, `plan_tier`
""",
    },
    {
        "question": "What churned ARR is associated with each churn reason?",
        "difficulty": "EASY",
        "sql": """
SELECT
  `Churn Reason` AS `churn_reason`,
  MEASURE(`Churn Event Count`) AS `churn_event_count`,
  ROUND(MEASURE(`Churned ARR`), 2) AS `churned_arr_usd`
FROM `{catalog}`.`{schema}`.`mv_churn_risk`
GROUP BY `Churn Reason`
ORDER BY `churned_arr_usd` DESC, `churn_reason`
""",
    },
    {
        "question": "How many support tickets were created by severity and current status?",
        "difficulty": "EASY",
        "sql": """
SELECT
  `severity`,
  `status`,
  COUNT(*) AS `ticket_count`
FROM `{catalog}`.`{schema}`.`support_tickets`
GROUP BY `severity`, `status`
ORDER BY
  CASE `severity`
    WHEN 'Critical' THEN 1
    WHEN 'High' THEN 2
    WHEN 'Medium' THEN 3
    WHEN 'Low' THEN 4
    ELSE 5
  END,
  `status`
""",
    },
    {
        "question": "What was average seat utilization and health score by plan tier in 2025?",
        "difficulty": "EASY",
        "sql": """
SELECT
  `Plan Tier` AS `plan_tier`,
  ROUND(MEASURE(`Average Seat Utilization Pct`), 2) AS `avg_seat_utilization_pct`,
  ROUND(MEASURE(`Average Health Score`), 2) AS `avg_health_score`,
  MEASURE(`Queries Run`) AS `queries_run`
FROM `{catalog}`.`{schema}`.`mv_product_usage`
WHERE `Usage Year` = 2025
GROUP BY `Plan Tier`
ORDER BY `avg_seat_utilization_pct` DESC, `plan_tier`
""",
    },
    {
        "question": "What are ARR, MRR, and logo churn rate by customer segment?",
        "difficulty": "MEDIUM",
        "sql": """
SELECT
  a.`segment`,
  COUNT(*) AS `subscription_count`,
  SUM(CASE WHEN s.`subscription_status` = 'Active' THEN 1 ELSE 0 END) AS `active_subscriptions`,
  SUM(CASE WHEN s.`subscription_status` = 'Churned' THEN 1 ELSE 0 END) AS `churned_subscriptions`,
  ROUND(SUM(s.`monthly_recurring_revenue_usd`), 2) AS `mrr_usd`,
  ROUND(SUM(s.`annual_recurring_revenue_usd`), 2) AS `arr_usd`,
  ROUND(SUM(CASE WHEN s.`subscription_status` = 'Churned' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS `logo_churn_rate_pct`
FROM `{catalog}`.`{schema}`.`subscriptions` s
JOIN `{catalog}`.`{schema}`.`accounts` a
  ON s.`account_id` = a.`account_id`
GROUP BY a.`segment`
ORDER BY `arr_usd` DESC
""",
    },
    {
        "question": "Which acquisition channels have the highest ARR churn rate?",
        "difficulty": "MEDIUM",
        "sql": """
SELECT
  a.`acquisition_channel`,
  COUNT(*) AS `subscription_count`,
  ROUND(SUM(s.`annual_recurring_revenue_usd`), 2) AS `total_arr_usd`,
  ROUND(SUM(CASE WHEN s.`subscription_status` = 'Churned' THEN s.`annual_recurring_revenue_usd` ELSE 0 END), 2) AS `churned_arr_usd`,
  ROUND(SUM(CASE WHEN s.`subscription_status` = 'Churned' THEN s.`annual_recurring_revenue_usd` ELSE 0 END) * 100.0 / SUM(s.`annual_recurring_revenue_usd`), 2) AS `arr_churn_rate_pct`
FROM `{catalog}`.`{schema}`.`subscriptions` s
JOIN `{catalog}`.`{schema}`.`accounts` a
  ON s.`account_id` = a.`account_id`
GROUP BY a.`acquisition_channel`
ORDER BY `arr_churn_rate_pct` DESC, `total_arr_usd` DESC
""",
    },
    {
        "question": "For each billing term and plan tier, what is active ARR and average discount?",
        "difficulty": "MEDIUM",
        "sql": """
SELECT
  `billing_term`,
  `plan_tier`,
  COUNT(*) AS `active_subscription_count`,
  ROUND(SUM(`annual_recurring_revenue_usd`), 2) AS `active_arr_usd`,
  ROUND(AVG(`monthly_recurring_revenue_usd`), 2) AS `avg_mrr_usd`,
  ROUND(AVG(`discount_pct`), 2) AS `avg_discount_pct`
FROM `{catalog}`.`{schema}`.`subscriptions`
WHERE `subscription_status` = 'Active'
GROUP BY `billing_term`, `plan_tier`
ORDER BY `billing_term`, `active_arr_usd` DESC
""",
    },
    {
        "question": "Which ten active accounts have the highest ARR and what is their latest health score?",
        "difficulty": "MEDIUM",
        "sql": """
WITH latest_month AS (
  SELECT
    `subscription_id`,
    MAX(`usage_month`) AS `latest_usage_month`
  FROM `{catalog}`.`{schema}`.`product_usage`
  GROUP BY `subscription_id`
)
SELECT
  a.`account_name`,
  a.`segment`,
  s.`plan_tier`,
  ROUND(s.`annual_recurring_revenue_usd`, 2) AS `arr_usd`,
  u.`usage_month` AS `latest_usage_month`,
  u.`seat_utilization_pct`,
  u.`health_score`
FROM `{catalog}`.`{schema}`.`subscriptions` s
JOIN `{catalog}`.`{schema}`.`accounts` a
  ON s.`account_id` = a.`account_id`
JOIN latest_month lm
  ON s.`subscription_id` = lm.`subscription_id`
JOIN `{catalog}`.`{schema}`.`product_usage` u
  ON lm.`subscription_id` = u.`subscription_id`
 AND lm.`latest_usage_month` = u.`usage_month`
WHERE s.`subscription_status` = 'Active'
ORDER BY s.`annual_recurring_revenue_usd` DESC, a.`account_id`
LIMIT 10
""",
    },
    {
        "question": "Do AI feature adopters churn less than non-adopters, and how does recent utilization compare?",
        "difficulty": "MEDIUM",
        "sql": """
WITH recent_usage AS (
  SELECT
    `account_id`,
    ROUND(AVG(`seat_utilization_pct`), 2) AS `avg_2025_utilization_pct`,
    ROUND(AVG(`health_score`), 2) AS `avg_2025_health_score`
  FROM `{catalog}`.`{schema}`.`product_usage`
  WHERE `usage_year` = 2025
  GROUP BY `account_id`
)
SELECT
  a.`ai_feature_adopter`,
  COUNT(*) AS `subscription_count`,
  SUM(CASE WHEN s.`subscription_status` = 'Churned' THEN 1 ELSE 0 END) AS `churned_subscriptions`,
  ROUND(SUM(CASE WHEN s.`subscription_status` = 'Churned' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS `logo_churn_rate_pct`,
  ROUND(AVG(ru.`avg_2025_utilization_pct`), 2) AS `avg_2025_utilization_pct`,
  ROUND(AVG(ru.`avg_2025_health_score`), 2) AS `avg_2025_health_score`,
  ROUND(SUM(s.`annual_recurring_revenue_usd`), 2) AS `arr_usd`
FROM `{catalog}`.`{schema}`.`accounts` a
JOIN `{catalog}`.`{schema}`.`subscriptions` s
  ON a.`account_id` = s.`account_id`
LEFT JOIN recent_usage ru
  ON a.`account_id` = ru.`account_id`
GROUP BY a.`ai_feature_adopter`
ORDER BY a.`ai_feature_adopter` DESC
""",
    },
    {
        "question": "Which segments had the most churned ARR by month in 2025?",
        "difficulty": "MEDIUM",
        "sql": """
SELECT
  DATE_TRUNC('MONTH', `churn_date`) AS `churn_month`,
  `segment`,
  COUNT(*) AS `churn_event_count`,
  ROUND(SUM(`churned_arr_usd`), 2) AS `churned_arr_usd`
FROM `{catalog}`.`{schema}`.`churn_events`
WHERE `churn_year` = 2025
GROUP BY DATE_TRUNC('MONTH', `churn_date`), `segment`
ORDER BY `churn_month`, `churned_arr_usd` DESC, `segment`
""",
    },
    {
        "question": "Which segments have the highest late invoice rate and open balance?",
        "difficulty": "MEDIUM",
        "sql": """
SELECT
  a.`segment`,
  COUNT(*) AS `invoice_count`,
  SUM(CASE WHEN i.`days_late` > 0 THEN 1 ELSE 0 END) AS `late_invoice_count`,
  ROUND(SUM(CASE WHEN i.`days_late` > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS `late_invoice_rate_pct`,
  ROUND(SUM(CASE WHEN i.`payment_status` = 'Open' THEN i.`amount_due_usd` - i.`amount_paid_usd` ELSE 0 END), 2) AS `open_balance_usd`
FROM `{catalog}`.`{schema}`.`invoices` i
JOIN `{catalog}`.`{schema}`.`accounts` a
  ON i.`account_id` = a.`account_id`
GROUP BY a.`segment`
ORDER BY `late_invoice_rate_pct` DESC, `open_balance_usd` DESC
""",
    },
    {
        "question": "Are support tickets resolved within target hours by severity?",
        "difficulty": "MEDIUM",
        "sql": """
SELECT
  `severity`,
  COUNT(*) AS `resolved_ticket_count`,
  ROUND(AVG(`resolution_time_hours`), 2) AS `avg_resolution_hours`,
  ROUND(PERCENTILE_APPROX(`resolution_time_hours`, 0.5), 2) AS `median_resolution_hours`,
  ROUND(SUM(CASE
    WHEN `severity` = 'Low' AND `resolution_time_hours` <= 24 THEN 1
    WHEN `severity` = 'Medium' AND `resolution_time_hours` <= 72 THEN 1
    WHEN `severity` IN ('High', 'Critical') AND `resolution_time_hours` <= 168 THEN 1
    ELSE 0
  END) * 100.0 / COUNT(*), 2) AS `within_target_pct`,
  ROUND(AVG(`csat_score`), 2) AS `avg_csat_score`
FROM `{catalog}`.`{schema}`.`support_tickets`
WHERE `status` = 'Resolved'
GROUP BY `severity`
ORDER BY
  CASE `severity`
    WHEN 'Critical' THEN 1
    WHEN 'High' THEN 2
    WHEN 'Medium' THEN 3
    WHEN 'Low' THEN 4
    ELSE 5
  END
""",
    },
    {
        "question": "How many support tickets do we get per $100K of ARR by segment?",
        "difficulty": "MEDIUM",
        "sql": """
WITH tickets_by_subscription AS (
  SELECT
    `subscription_id`,
    COUNT(*) AS `ticket_count`
  FROM `{catalog}`.`{schema}`.`support_tickets`
  GROUP BY `subscription_id`
)
SELECT
  a.`segment`,
  SUM(COALESCE(t.`ticket_count`, 0)) AS `ticket_count`,
  ROUND(SUM(s.`annual_recurring_revenue_usd`), 2) AS `arr_usd`,
  ROUND(SUM(COALESCE(t.`ticket_count`, 0)) / (SUM(s.`annual_recurring_revenue_usd`) / 100000.0), 2) AS `tickets_per_100k_arr`
FROM `{catalog}`.`{schema}`.`subscriptions` s
JOIN `{catalog}`.`{schema}`.`accounts` a
  ON s.`account_id` = a.`account_id`
LEFT JOIN tickets_by_subscription t
  ON s.`subscription_id` = t.`subscription_id`
GROUP BY a.`segment`
ORDER BY `tickets_per_100k_arr` DESC
""",
    },
    {
        "question": "How does recent usage differ between active and churned subscriptions?",
        "difficulty": "MEDIUM",
        "sql": """
WITH recent_usage AS (
  SELECT
    `subscription_id`,
    ROUND(AVG(`seat_utilization_pct`), 2) AS `avg_recent_utilization_pct`,
    ROUND(AVG(`health_score`), 2) AS `avg_recent_health_score`,
    ROUND(AVG(`active_users`), 2) AS `avg_recent_active_users`,
    SUM(`queries_run`) AS `recent_queries_run`
  FROM `{catalog}`.`{schema}`.`product_usage`
  WHERE `usage_month` >= DATE '2025-07-01'
  GROUP BY `subscription_id`
)
SELECT
  s.`subscription_status`,
  COUNT(*) AS `subscription_count`,
  ROUND(AVG(ru.`avg_recent_utilization_pct`), 2) AS `avg_recent_utilization_pct`,
  ROUND(AVG(ru.`avg_recent_health_score`), 2) AS `avg_recent_health_score`,
  ROUND(AVG(ru.`avg_recent_active_users`), 2) AS `avg_recent_active_users`,
  SUM(ru.`recent_queries_run`) AS `recent_queries_run`
FROM `{catalog}`.`{schema}`.`subscriptions` s
LEFT JOIN recent_usage ru
  ON s.`subscription_id` = ru.`subscription_id`
GROUP BY s.`subscription_status`
ORDER BY s.`subscription_status`
""",
    },
    {
        "question": "Which churn reasons are most common within each segment?",
        "difficulty": "MEDIUM",
        "sql": """
SELECT
  `segment`,
  `churn_reason`,
  COUNT(*) AS `churn_event_count`,
  ROUND(SUM(`churned_arr_usd`), 2) AS `churned_arr_usd`,
  ROUND(AVG(`prior_6mo_avg_utilization_pct`), 2) AS `avg_prior_6mo_utilization_pct`,
  ROUND(AVG(`prior_6mo_avg_health_score`), 2) AS `avg_prior_6mo_health_score`
FROM `{catalog}`.`{schema}`.`churn_events`
GROUP BY `segment`, `churn_reason`
ORDER BY `segment`, `churn_event_count` DESC, `churned_arr_usd` DESC
""",
    },
    {
        "question": "Which accounts have the largest unpaid or late invoice balances?",
        "difficulty": "MEDIUM",
        "sql": """
WITH invoice_health AS (
  SELECT
    `account_id`,
    COUNT(*) AS `invoice_count`,
    SUM(CASE WHEN `payment_status` = 'Open' THEN 1 ELSE 0 END) AS `open_invoice_count`,
    SUM(CASE WHEN `days_late` > 0 THEN 1 ELSE 0 END) AS `late_invoice_count`,
    ROUND(SUM(CASE WHEN `payment_status` = 'Open' THEN `amount_due_usd` - `amount_paid_usd` ELSE 0 END), 2) AS `open_balance_usd`,
    ROUND(SUM(CASE WHEN `days_late` > 0 THEN `amount_due_usd` ELSE 0 END), 2) AS `late_invoice_due_usd`
  FROM `{catalog}`.`{schema}`.`invoices`
  GROUP BY `account_id`
)
SELECT
  a.`account_name`,
  a.`segment`,
  s.`subscription_status`,
  ROUND(s.`annual_recurring_revenue_usd`, 2) AS `arr_usd`,
  ih.`open_invoice_count`,
  ih.`late_invoice_count`,
  ih.`open_balance_usd`,
  ih.`late_invoice_due_usd`
FROM invoice_health ih
JOIN `{catalog}`.`{schema}`.`accounts` a
  ON ih.`account_id` = a.`account_id`
JOIN `{catalog}`.`{schema}`.`subscriptions` s
  ON ih.`account_id` = s.`account_id`
WHERE ih.`open_balance_usd` > 0 OR ih.`late_invoice_count` > 0
ORDER BY ih.`open_balance_usd` DESC, ih.`late_invoice_due_usd` DESC, s.`annual_recurring_revenue_usd` DESC
LIMIT 10
""",
    },
    {
        "question": "How do payment statuses vary by billing term and subscription status?",
        "difficulty": "MEDIUM",
        "sql": """
SELECT
  s.`billing_term`,
  s.`subscription_status`,
  i.`payment_status`,
  COUNT(*) AS `invoice_count`,
  ROUND(SUM(i.`amount_due_usd`), 2) AS `amount_due_usd`,
  ROUND(AVG(i.`days_late`), 2) AS `avg_days_late`
FROM `{catalog}`.`{schema}`.`invoices` i
JOIN `{catalog}`.`{schema}`.`subscriptions` s
  ON i.`subscription_id` = s.`subscription_id`
GROUP BY s.`billing_term`, s.`subscription_status`, i.`payment_status`
ORDER BY s.`billing_term`, s.`subscription_status`, `invoice_count` DESC
""",
    },
    {
        "question": "What is monthly query volume and AI active user share by segment during 2025?",
        "difficulty": "MEDIUM",
        "sql": """
SELECT
  `usage_month`,
  `segment`,
  SUM(`queries_run`) AS `queries_run`,
  SUM(`active_users`) AS `active_users`,
  SUM(`ai_feature_active_users`) AS `ai_feature_active_users`,
  ROUND(SUM(`ai_feature_active_users`) * 100.0 / SUM(`active_users`), 2) AS `ai_active_user_share_pct`
FROM `{catalog}`.`{schema}`.`product_usage`
WHERE `usage_year` = 2025
GROUP BY `usage_month`, `segment`
ORDER BY `usage_month`, `segment`
""",
    },
    {
        "question": "Which regions generate the most active ARR, and what share comes from AI feature adopters?",
        "difficulty": "MEDIUM",
        "sql": """
SELECT
  a.`region`,
  COUNT(*) AS `active_subscription_count`,
  ROUND(SUM(s.`annual_recurring_revenue_usd`), 2) AS `active_arr_usd`,
  ROUND(SUM(CASE WHEN a.`ai_feature_adopter` THEN s.`annual_recurring_revenue_usd` ELSE 0 END), 2) AS `ai_adopter_arr_usd`,
  ROUND(SUM(CASE WHEN a.`ai_feature_adopter` THEN s.`annual_recurring_revenue_usd` ELSE 0 END) * 100.0 / SUM(s.`annual_recurring_revenue_usd`), 2) AS `ai_adopter_arr_share_pct`
FROM `{catalog}`.`{schema}`.`subscriptions` s
JOIN `{catalog}`.`{schema}`.`accounts` a
  ON s.`account_id` = a.`account_id`
WHERE s.`subscription_status` = 'Active'
GROUP BY a.`region`
ORDER BY `active_arr_usd` DESC
""",
    },
    {
        "question": "Which subscription start quarters have the strongest 12-month retention?",
        "difficulty": "HARD",
        "sql": """
WITH cohorts AS (
  SELECT
    DATE_TRUNC('QUARTER', `start_date`) AS `start_quarter`,
    `billing_term`,
    `subscription_id`,
    `annual_recurring_revenue_usd`,
    CASE
      WHEN `end_date` IS NULL OR `end_date` >= ADD_MONTHS(`start_date`, 12) THEN 1
      ELSE 0
    END AS `retained_12mo`
  FROM `{catalog}`.`{schema}`.`subscriptions`
)
SELECT
  `start_quarter`,
  `billing_term`,
  COUNT(*) AS `cohort_size`,
  SUM(`retained_12mo`) AS `retained_after_12mo`,
  ROUND(SUM(`retained_12mo`) * 100.0 / COUNT(*), 2) AS `logo_retention_12mo_pct`,
  ROUND(SUM(`annual_recurring_revenue_usd`), 2) AS `cohort_arr_usd`,
  ROUND(SUM(CASE WHEN `retained_12mo` = 1 THEN `annual_recurring_revenue_usd` ELSE 0 END) * 100.0 / SUM(`annual_recurring_revenue_usd`), 2) AS `arr_retention_12mo_pct`
FROM cohorts
GROUP BY `start_quarter`, `billing_term`
ORDER BY `logo_retention_12mo_pct` DESC, `cohort_arr_usd` DESC
""",
    },
    {
        "question": "Which segments had the steepest deterioration in rolling three-month utilization during 2025?",
        "difficulty": "HARD",
        "sql": """
WITH monthly AS (
  SELECT
    `segment`,
    `usage_month`,
    ROUND(AVG(`seat_utilization_pct`), 2) AS `avg_utilization_pct`
  FROM `{catalog}`.`{schema}`.`product_usage`
  WHERE `usage_year` = 2025
  GROUP BY `segment`, `usage_month`
),
rolling AS (
  SELECT
    `segment`,
    `usage_month`,
    `avg_utilization_pct`,
    ROUND(AVG(`avg_utilization_pct`) OVER (
      PARTITION BY `segment`
      ORDER BY `usage_month`
      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 2) AS `rolling_3mo_utilization_pct`
  FROM monthly
),
changes AS (
  SELECT
    `segment`,
    `usage_month`,
    `avg_utilization_pct`,
    `rolling_3mo_utilization_pct`,
    LAG(`rolling_3mo_utilization_pct`) OVER (
      PARTITION BY `segment`
      ORDER BY `usage_month`
    ) AS `prior_rolling_3mo_utilization_pct`
  FROM rolling
)
SELECT
  `segment`,
  `usage_month`,
  `avg_utilization_pct`,
  `rolling_3mo_utilization_pct`,
  `prior_rolling_3mo_utilization_pct`,
  ROUND(`rolling_3mo_utilization_pct` - `prior_rolling_3mo_utilization_pct`, 2) AS `rolling_3mo_change_pct_points`
FROM changes
WHERE `prior_rolling_3mo_utilization_pct` IS NOT NULL
ORDER BY `rolling_3mo_change_pct_points` ASC, `segment`, `usage_month`
LIMIT 10
""",
    },
    {
        "question": "How much ARR sits in each churn-risk decile based on latest health score?",
        "difficulty": "HARD",
        "sql": """
WITH latest_month AS (
  SELECT
    `subscription_id`,
    MAX(`usage_month`) AS `latest_usage_month`
  FROM `{catalog}`.`{schema}`.`product_usage`
  GROUP BY `subscription_id`
),
latest_health AS (
  SELECT
    u.`subscription_id`,
    u.`seat_utilization_pct`,
    u.`health_score`
  FROM `{catalog}`.`{schema}`.`product_usage` u
  JOIN latest_month lm
    ON u.`subscription_id` = lm.`subscription_id`
   AND u.`usage_month` = lm.`latest_usage_month`
),
scored AS (
  SELECT
    s.`subscription_id`,
    s.`subscription_status`,
    s.`annual_recurring_revenue_usd`,
    lh.`seat_utilization_pct`,
    lh.`health_score`,
    NTILE(10) OVER (ORDER BY lh.`health_score` ASC, lh.`seat_utilization_pct` ASC, s.`subscription_id`) AS `risk_decile`
  FROM `{catalog}`.`{schema}`.`subscriptions` s
  JOIN latest_health lh
    ON s.`subscription_id` = lh.`subscription_id`
)
SELECT
  `risk_decile`,
  COUNT(*) AS `subscription_count`,
  ROUND(AVG(`health_score`), 2) AS `avg_latest_health_score`,
  ROUND(AVG(`seat_utilization_pct`), 2) AS `avg_latest_utilization_pct`,
  ROUND(SUM(`annual_recurring_revenue_usd`), 2) AS `arr_usd`,
  ROUND(SUM(CASE WHEN `subscription_status` = 'Churned' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS `logo_churn_rate_pct`
FROM scored
GROUP BY `risk_decile`
ORDER BY `risk_decile`
""",
    },
    {
        "question": "For each segment, what churn reason generated the most ARR loss?",
        "difficulty": "HARD",
        "sql": """
WITH reason_arr AS (
  SELECT
    `segment`,
    `churn_reason`,
    COUNT(*) AS `churn_event_count`,
    ROUND(SUM(`churned_arr_usd`), 2) AS `churned_arr_usd`
  FROM `{catalog}`.`{schema}`.`churn_events`
  GROUP BY `segment`, `churn_reason`
),
ranked AS (
  SELECT
    *,
    RANK() OVER (
      PARTITION BY `segment`
      ORDER BY `churned_arr_usd` DESC, `churn_reason`
    ) AS `reason_rank`
  FROM reason_arr
)
SELECT
  `segment`,
  `churn_reason`,
  `churn_event_count`,
  `churned_arr_usd`
FROM ranked
WHERE `reason_rank` = 1
ORDER BY `segment`
""",
    },
    {
        "question": "Which active accounts combine low utilization, unpaid invoices, and open or escalated support?",
        "difficulty": "HARD",
        "sql": """
WITH recent_usage AS (
  SELECT
    `account_id`,
    ROUND(AVG(`seat_utilization_pct`), 2) AS `avg_recent_utilization_pct`,
    ROUND(AVG(`health_score`), 2) AS `avg_recent_health_score`
  FROM `{catalog}`.`{schema}`.`product_usage`
  WHERE `usage_month` >= DATE '2025-10-01'
  GROUP BY `account_id`
),
invoice_risk AS (
  SELECT
    `account_id`,
    SUM(CASE WHEN `payment_status` = 'Open' THEN 1 ELSE 0 END) AS `open_invoice_count`,
    SUM(CASE WHEN `days_late` >= 30 THEN 1 ELSE 0 END) AS `seriously_late_invoice_count`,
    ROUND(SUM(CASE WHEN `payment_status` = 'Open' THEN `amount_due_usd` - `amount_paid_usd` ELSE 0 END), 2) AS `open_balance_usd`
  FROM `{catalog}`.`{schema}`.`invoices`
  WHERE `invoice_year` = 2025
  GROUP BY `account_id`
),
ticket_risk AS (
  SELECT
    `account_id`,
    SUM(CASE WHEN `status` IN ('Open', 'Escalated') THEN 1 ELSE 0 END) AS `open_or_escalated_ticket_count`,
    SUM(CASE WHEN `severity` IN ('High', 'Critical') THEN 1 ELSE 0 END) AS `high_or_critical_ticket_count`
  FROM `{catalog}`.`{schema}`.`support_tickets`
  WHERE `ticket_year` = 2025
  GROUP BY `account_id`
),
risk AS (
  SELECT
    a.`account_name`,
    a.`segment`,
    s.`subscription_id`,
    s.`plan_tier`,
    ROUND(s.`annual_recurring_revenue_usd`, 2) AS `arr_usd`,
    ru.`avg_recent_utilization_pct`,
    ru.`avg_recent_health_score`,
    COALESCE(ir.`open_invoice_count`, 0) AS `open_invoice_count`,
    COALESCE(ir.`seriously_late_invoice_count`, 0) AS `seriously_late_invoice_count`,
    COALESCE(ir.`open_balance_usd`, 0) AS `open_balance_usd`,
    COALESCE(tr.`open_or_escalated_ticket_count`, 0) AS `open_or_escalated_ticket_count`,
    COALESCE(tr.`high_or_critical_ticket_count`, 0) AS `high_or_critical_ticket_count`
  FROM `{catalog}`.`{schema}`.`subscriptions` s
  JOIN `{catalog}`.`{schema}`.`accounts` a
    ON s.`account_id` = a.`account_id`
  LEFT JOIN recent_usage ru
    ON s.`account_id` = ru.`account_id`
  LEFT JOIN invoice_risk ir
    ON s.`account_id` = ir.`account_id`
  LEFT JOIN ticket_risk tr
    ON s.`account_id` = tr.`account_id`
  WHERE s.`subscription_status` = 'Active'
)
SELECT
  *,
  (CASE WHEN `avg_recent_utilization_pct` < 45 THEN 1 ELSE 0 END
   + CASE WHEN `open_invoice_count` > 0 OR `seriously_late_invoice_count` > 0 THEN 1 ELSE 0 END
   + CASE WHEN `open_or_escalated_ticket_count` > 0 THEN 1 ELSE 0 END
   + CASE WHEN `high_or_critical_ticket_count` > 0 THEN 1 ELSE 0 END) AS `risk_signal_count`
FROM risk
WHERE `avg_recent_utilization_pct` < 55
   OR `open_invoice_count` > 0
   OR `seriously_late_invoice_count` > 0
   OR `open_or_escalated_ticket_count` > 0
ORDER BY `risk_signal_count` DESC, `arr_usd` DESC
LIMIT 15
""",
    },
    {
        "question": "Which plan tiers show worsening health and rising escalation rates from 2024 to 2025?",
        "difficulty": "HARD",
        "sql": """
WITH usage_by_year AS (
  SELECT
    `plan_tier`,
    `usage_year`,
    ROUND(AVG(`health_score`), 2) AS `avg_health_score`,
    ROUND(AVG(`seat_utilization_pct`), 2) AS `avg_utilization_pct`
  FROM `{catalog}`.`{schema}`.`product_usage`
  WHERE `usage_year` IN (2024, 2025)
  GROUP BY `plan_tier`, `usage_year`
),
ticket_by_year AS (
  SELECT
    s.`plan_tier`,
    t.`ticket_year`,
    COUNT(*) AS `ticket_count`,
    SUM(CASE WHEN t.`is_escalated` THEN 1 ELSE 0 END) AS `escalated_ticket_count`,
    ROUND(SUM(CASE WHEN t.`is_escalated` THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS `escalation_rate_pct`
  FROM `{catalog}`.`{schema}`.`support_tickets` t
  JOIN `{catalog}`.`{schema}`.`subscriptions` s
    ON t.`subscription_id` = s.`subscription_id`
  WHERE t.`ticket_year` IN (2024, 2025)
  GROUP BY s.`plan_tier`, t.`ticket_year`
)
SELECT
  u25.`plan_tier`,
  u24.`avg_health_score` AS `avg_health_score_2024`,
  u25.`avg_health_score` AS `avg_health_score_2025`,
  ROUND(u25.`avg_health_score` - u24.`avg_health_score`, 2) AS `health_score_change`,
  u24.`avg_utilization_pct` AS `avg_utilization_pct_2024`,
  u25.`avg_utilization_pct` AS `avg_utilization_pct_2025`,
  ROUND(u25.`avg_utilization_pct` - u24.`avg_utilization_pct`, 2) AS `utilization_change_pct_points`,
  t24.`escalation_rate_pct` AS `escalation_rate_pct_2024`,
  t25.`escalation_rate_pct` AS `escalation_rate_pct_2025`,
  ROUND(t25.`escalation_rate_pct` - t24.`escalation_rate_pct`, 2) AS `escalation_rate_change_pct_points`
FROM usage_by_year u25
JOIN usage_by_year u24
  ON u25.`plan_tier` = u24.`plan_tier`
 AND u25.`usage_year` = 2025
 AND u24.`usage_year` = 2024
JOIN ticket_by_year t25
  ON u25.`plan_tier` = t25.`plan_tier`
 AND t25.`ticket_year` = 2025
JOIN ticket_by_year t24
  ON u25.`plan_tier` = t24.`plan_tier`
 AND t24.`ticket_year` = 2024
ORDER BY `health_score_change` ASC, `escalation_rate_change_pct_points` DESC
""",
    },
    {
        "question": "What is the monthly churned ARR run-rate and three-month moving average in 2025?",
        "difficulty": "HARD",
        "sql": """
WITH months AS (
  SELECT DISTINCT
    `usage_month` AS `month_start`
  FROM `{catalog}`.`{schema}`.`product_usage`
  WHERE `usage_year` = 2025
),
monthly_churn AS (
  SELECT
    DATE_TRUNC('MONTH', `churn_date`) AS `month_start`,
    COUNT(*) AS `churn_event_count`,
    ROUND(SUM(`churned_arr_usd`), 2) AS `churned_arr_usd`
  FROM `{catalog}`.`{schema}`.`churn_events`
  WHERE `churn_year` = 2025
  GROUP BY DATE_TRUNC('MONTH', `churn_date`)
),
monthly AS (
  SELECT
    m.`month_start`,
    COALESCE(mc.`churn_event_count`, 0) AS `churn_event_count`,
    COALESCE(mc.`churned_arr_usd`, 0) AS `churned_arr_usd`
  FROM months m
  LEFT JOIN monthly_churn mc
    ON m.`month_start` = mc.`month_start`
)
SELECT
  `month_start`,
  `churn_event_count`,
  `churned_arr_usd`,
  ROUND(AVG(`churned_arr_usd`) OVER (
    ORDER BY `month_start`
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  ), 2) AS `churned_arr_3mo_moving_avg_usd`
FROM monthly
ORDER BY `month_start`
""",
    },
    {
        "question": "Which industries have the largest churn-rate gap between AI adopters and non-adopters?",
        "difficulty": "HARD",
        "sql": """
WITH rates AS (
  SELECT
    a.`industry`,
    a.`ai_feature_adopter`,
    COUNT(*) AS `subscription_count`,
    SUM(CASE WHEN s.`subscription_status` = 'Churned' THEN 1 ELSE 0 END) AS `churned_subscription_count`,
    ROUND(SUM(CASE WHEN s.`subscription_status` = 'Churned' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS `logo_churn_rate_pct`
  FROM `{catalog}`.`{schema}`.`accounts` a
  JOIN `{catalog}`.`{schema}`.`subscriptions` s
    ON a.`account_id` = s.`account_id`
  GROUP BY a.`industry`, a.`ai_feature_adopter`
),
pivoted AS (
  SELECT
    `industry`,
    MAX(CASE WHEN `ai_feature_adopter` THEN `logo_churn_rate_pct` END) AS `ai_adopter_churn_rate_pct`,
    MAX(CASE WHEN NOT `ai_feature_adopter` THEN `logo_churn_rate_pct` END) AS `non_adopter_churn_rate_pct`,
    MAX(CASE WHEN `ai_feature_adopter` THEN `subscription_count` END) AS `ai_adopter_subscription_count`,
    MAX(CASE WHEN NOT `ai_feature_adopter` THEN `subscription_count` END) AS `non_adopter_subscription_count`
  FROM rates
  GROUP BY `industry`
)
SELECT
  `industry`,
  `ai_adopter_subscription_count`,
  `ai_adopter_churn_rate_pct`,
  `non_adopter_subscription_count`,
  `non_adopter_churn_rate_pct`,
  ROUND(`non_adopter_churn_rate_pct` - `ai_adopter_churn_rate_pct`, 2) AS `non_adopter_minus_adopter_churn_gap_pct_points`
FROM pivoted
WHERE `ai_adopter_churn_rate_pct` IS NOT NULL
  AND `non_adopter_churn_rate_pct` IS NOT NULL
ORDER BY `non_adopter_minus_adopter_churn_gap_pct_points` DESC, `industry`
""",
    },
    {
        "question": "What is the 2025 annualized logo churn rate by segment, measured against the average monthly active subscription base?",
        "difficulty": "HARD",
        "sql": """
WITH months AS (
  SELECT DISTINCT
    `usage_month` AS `month_start`
  FROM `{catalog}`.`{schema}`.`product_usage`
  WHERE `usage_year` = 2025
),
monthly_active AS (
  SELECT
    m.`month_start`,
    a.`segment`,
    COUNT(*) AS `active_subscription_count`
  FROM months m
  JOIN `{catalog}`.`{schema}`.`subscriptions` s
    ON s.`start_date` <= LAST_DAY(m.`month_start`)
   AND (s.`end_date` IS NULL OR s.`end_date` >= m.`month_start`)
  JOIN `{catalog}`.`{schema}`.`accounts` a
    ON s.`account_id` = a.`account_id`
  GROUP BY m.`month_start`, a.`segment`
),
avg_base AS (
  -- Average the per-month active counts; do NOT sum point-in-time levels across months.
  SELECT
    `segment`,
    AVG(`active_subscription_count`) AS `avg_monthly_active_base`
  FROM monthly_active
  GROUP BY `segment`
),
churns AS (
  SELECT
    `segment`,
    COUNT(*) AS `churned_subscription_count`
  FROM `{catalog}`.`{schema}`.`churn_events`
  WHERE `churn_year` = 2025
  GROUP BY `segment`
)
SELECT
  b.`segment`,
  ROUND(b.`avg_monthly_active_base`, 1) AS `avg_monthly_active_base`,
  COALESCE(c.`churned_subscription_count`, 0) AS `churned_subscriptions_2025`,
  ROUND(COALESCE(c.`churned_subscription_count`, 0) * 100.0 / b.`avg_monthly_active_base`, 2) AS `annual_logo_churn_rate_pct`
FROM avg_base b
LEFT JOIN churns c
  ON b.`segment` = c.`segment`
ORDER BY `annual_logo_churn_rate_pct` DESC, b.`segment`
""",
    },
    {
        "question": "Within each segment, which active accounts have a latest health score below their segment's 25th-percentile health score?",
        "difficulty": "HARD",
        "sql": """
WITH latest_month AS (
  SELECT
    `subscription_id`,
    MAX(`usage_month`) AS `latest_usage_month`
  FROM `{catalog}`.`{schema}`.`product_usage`
  GROUP BY `subscription_id`
),
latest_health AS (
  SELECT
    a.`account_name`,
    a.`segment`,
    s.`subscription_id`,
    s.`annual_recurring_revenue_usd`,
    u.`health_score` AS `latest_health_score`,
    u.`seat_utilization_pct` AS `latest_utilization_pct`
  FROM `{catalog}`.`{schema}`.`product_usage` u
  JOIN latest_month lm
    ON u.`subscription_id` = lm.`subscription_id`
   AND u.`usage_month` = lm.`latest_usage_month`
  JOIN `{catalog}`.`{schema}`.`subscriptions` s
    ON u.`subscription_id` = s.`subscription_id`
  JOIN `{catalog}`.`{schema}`.`accounts` a
    ON s.`account_id` = a.`account_id`
  WHERE s.`subscription_status` = 'Active'
),
segment_threshold AS (
  SELECT
    `segment`,
    PERCENTILE_APPROX(`latest_health_score`, 0.25) AS `segment_p25_health_score`
  FROM latest_health
  GROUP BY `segment`
)
SELECT
  lh.`segment`,
  lh.`account_name`,
  ROUND(lh.`latest_health_score`, 2) AS `latest_health_score`,
  ROUND(st.`segment_p25_health_score`, 2) AS `segment_p25_health_score`,
  ROUND(lh.`latest_utilization_pct`, 2) AS `latest_utilization_pct`,
  ROUND(lh.`annual_recurring_revenue_usd`, 2) AS `arr_usd`
FROM latest_health lh
JOIN segment_threshold st
  ON lh.`segment` = st.`segment`
WHERE lh.`latest_health_score` < st.`segment_p25_health_score`
ORDER BY lh.`segment`, lh.`latest_health_score`, `arr_usd` DESC
""",
    },
]

assert len(BENCHMARKS) == 30

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Fetch Genie Space

# COMMAND ----------

import copy
import json

resp = w.api_client.do(
    "GET",
    f"/api/2.0/genie/spaces/{space_id}",
    query={"include_serialized_space": "true"},
)
serialized = json.loads(resp["serialized_space"])

pre_data_sources = copy.deepcopy(serialized.get("data_sources"))
pre_instructions = copy.deepcopy(serialized.get("instructions"))
pre_version = copy.deepcopy(serialized.get("version"))

print(f"Fetched Genie Space `{space_id}`.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Replace Benchmark Questions

# COMMAND ----------

import uuid


def render_sql(benchmark):
    return benchmark["sql"].strip().format(catalog=catalog, schema=schema)


questions = []
for benchmark in BENCHMARKS:
    sql = render_sql(benchmark)
    questions.append(
        {
            "id": uuid.uuid4().hex,
            "question": [benchmark["question"]],
            "answer": [
                {
                    "format": "SQL",
                    "content": [line + "\n" for line in sql.split("\n")],
                }
            ],
        }
    )

if pre_version is None:
    raise ValueError("Serialized space is missing version; refusing to patch because only benchmarks.questions may be mutated.")

serialized.setdefault("benchmarks", {})["questions"] = questions
serialized.setdefault("version", 2)

print(f"Prepared {len(questions)} benchmark questions.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Patch Genie Space

# COMMAND ----------

body = {"serialized_space": json.dumps(serialized)}
for key in ("title", "description", "warehouse_id"):
    if resp.get(key) is not None:
        body[key] = resp[key]

w.api_client.do("PATCH", f"/api/2.0/genie/spaces/{space_id}", body=body)

print(f"Patched Genie Space `{space_id}`.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Round-trip Verification

# COMMAND ----------

verify_resp = w.api_client.do(
    "GET",
    f"/api/2.0/genie/spaces/{space_id}",
    query={"include_serialized_space": "true"},
)
verify_serialized = json.loads(verify_resp["serialized_space"])
verify_questions = verify_serialized.get("benchmarks", {}).get("questions", [])

expected_question_text = [benchmark["question"] for benchmark in BENCHMARKS]
actual_question_text = [
    question.get("question", [None])[0]
    for question in verify_questions
]

assert len(verify_questions) == 30, f"Expected 30 benchmark questions, found {len(verify_questions)}."
assert actual_question_text == expected_question_text, "Round-trip benchmark questions do not match expected order/text."
assert verify_serialized.get("data_sources") == pre_data_sources, "data_sources changed during patch."
assert verify_serialized.get("instructions") == pre_instructions, "instructions changed during patch."
assert verify_serialized.get("version") == pre_version, "version changed during patch."

print("Round-trip verification succeeded.")
print("Benchmarks replaced: 30")
print("Verified unchanged keys: data_sources, instructions, version")

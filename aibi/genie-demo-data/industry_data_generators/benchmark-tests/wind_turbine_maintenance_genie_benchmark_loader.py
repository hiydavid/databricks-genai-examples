# Databricks notebook source

# MAGIC %md
# MAGIC # Wind Turbine Predictive Maintenance Genie Benchmark Loader
# MAGIC
# MAGIC Loads 30 benchmark questions into a target Genie Space for the wind turbine
# MAGIC maintenance demo dataset. This notebook:
# MAGIC
# MAGIC - Defines 30 benchmark questions (5 EASY / 15 MEDIUM / 10 HARD) as SQL.
# MAGIC - Fetches the Genie Space and replaces ONLY `benchmarks.questions` in the
# MAGIC   serialized configuration, then patches it back.
# MAGIC - Round-trip verifies that the questions loaded and that `data_sources`,
# MAGIC   `instructions`, and `version` are left unchanged.
# MAGIC
# MAGIC Safety note: this notebook mutates ONLY `benchmarks.questions` in the serialized Genie Space configuration.

# COMMAND ----------

# MAGIC %pip install databricks-sdk --upgrade -q
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

# ============================================================
# CONFIGURATION — edit these three values, then Run All
# ============================================================
space_id = ""                # REQUIRED: target Genie Space ID (e.g. "01ef...")
catalog  = "dhuang_catalog"  # Unity Catalog name
schema   = "wind_turbine_maintenance"  # Schema / database name
# ============================================================

import json
from copy import deepcopy

from databricks.sdk import WorkspaceClient

if not space_id:
    raise ValueError("space_id is required — set it at the top of this cell.")

w = WorkspaceClient()

print(f"Catalog: {catalog}")
print(f"Schema : {schema}")
print(f"Space  : {space_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Benchmarks

# COMMAND ----------

BENCHMARKS = [
    {
        "question": "How many active wind farms do we operate in each region?",
        "difficulty": "EASY",
        "sql": """SELECT
  `region`,
  COUNT(*) AS `active_farm_count`
FROM `{catalog}`.`{schema}`.`wind_farms`
WHERE `is_active` = TRUE
GROUP BY `region`
ORDER BY `active_farm_count` DESC, `region`""",
    },
    {
        "question": "How many turbines do we have by model and operating status, and what rated capacity does each group represent?",
        "difficulty": "EASY",
        "sql": """SELECT
  `turbine_model`,
  `status`,
  COUNT(*) AS `turbine_count`,
  ROUND(SUM(`rated_capacity_mw`), 2) AS `total_rated_capacity_mw`
FROM `{catalog}`.`{schema}`.`turbines`
GROUP BY `turbine_model`, `status`
ORDER BY `turbine_model`, `status`""",
    },
    {
        "question": "What is the component inventory by component type and current status?",
        "difficulty": "EASY",
        "sql": """SELECT
  `component_type`,
  `status`,
  COUNT(*) AS `component_count`
FROM `{catalog}`.`{schema}`.`components`
GROUP BY `component_type`, `status`
ORDER BY `component_type`, `status`""",
    },
    {
        "question": "How many maintenance events do we have by maintenance type, and what did they cost?",
        "difficulty": "EASY",
        "sql": """SELECT
  `maintenance_type`,
  COUNT(*) AS `event_count`,
  ROUND(SUM(`cost_usd`), 2) AS `total_cost_usd`,
  ROUND(AVG(`duration_hours`), 2) AS `avg_duration_hours`
FROM `{catalog}`.`{schema}`.`maintenance_events`
GROUP BY `maintenance_type`
ORDER BY `event_count` DESC, `maintenance_type`""",
    },
    {
        "question": "Which regions have the highest average capacity factor in the turbine performance metric view?",
        "difficulty": "EASY",
        "sql": """SELECT
  `Region` AS `region`,
  MEASURE(`Reading Count`) AS `reading_count`,
  ROUND(MEASURE(`Average Capacity Factor Pct`), 2) AS `avg_capacity_factor_pct`,
  MEASURE(`High Anomaly Count`) AS `high_anomaly_count`
FROM `{catalog}`.`{schema}`.`mv_turbine_performance`
GROUP BY `Region`
ORDER BY `avg_capacity_factor_pct` DESC, `region`""",
    },
    {
        "question": "Which wind farms have the most installed rated capacity, and how many turbines are operating, derated, or offline?",
        "difficulty": "MEDIUM",
        "sql": """SELECT
  wf.`farm_name`,
  wf.`region`,
  COUNT(*) AS `turbine_count`,
  ROUND(SUM(t.`rated_capacity_mw`), 2) AS `total_rated_capacity_mw`,
  COUNT_IF(t.`status` = 'Operating') AS `operating_turbines`,
  COUNT_IF(t.`status` = 'Derated') AS `derated_turbines`,
  COUNT_IF(t.`status` = 'Offline') AS `offline_turbines`
FROM `{catalog}`.`{schema}`.`turbines` t
JOIN `{catalog}`.`{schema}`.`wind_farms` wf
  ON t.`farm_id` = wf.`farm_id`
GROUP BY wf.`farm_name`, wf.`region`
ORDER BY `total_rated_capacity_mw` DESC, wf.`farm_name`""",
    },
    {
        "question": "By turbine model, what was the 2025 average anomaly score and share of high-anomaly readings?",
        "difficulty": "MEDIUM",
        "sql": """SELECT
  t.`turbine_model`,
  COUNT(*) AS `reading_count`,
  ROUND(AVG(sr.`anomaly_score`), 2) AS `avg_anomaly_score`,
  COUNT_IF(sr.`anomaly_score` >= 70) AS `high_anomaly_readings`,
  ROUND(100.0 * COUNT_IF(sr.`anomaly_score` >= 70) / COUNT(*), 2) AS `high_anomaly_pct`
FROM `{catalog}`.`{schema}`.`sensor_readings` sr
JOIN `{catalog}`.`{schema}`.`turbines` t
  ON sr.`turbine_id` = t.`turbine_id`
WHERE sr.`reading_year` = 2025
GROUP BY t.`turbine_model`
ORDER BY `high_anomaly_pct` DESC, t.`turbine_model`""",
    },
    {
        "question": "During winter months, how does icing affect capacity factor by terrain type?",
        "difficulty": "MEDIUM",
        "sql": """SELECT
  wf.`terrain_type`,
  sr.`icing_flag`,
  COUNT(*) AS `reading_count`,
  ROUND(AVG(sr.`capacity_factor_pct`), 2) AS `avg_capacity_factor_pct`,
  ROUND(AVG(sr.`power_output_kw`), 2) AS `avg_power_output_kw`
FROM `{catalog}`.`{schema}`.`sensor_readings` sr
JOIN `{catalog}`.`{schema}`.`wind_farms` wf
  ON sr.`farm_id` = wf.`farm_id`
WHERE sr.`reading_month` IN (1, 2, 12)
GROUP BY wf.`terrain_type`, sr.`icing_flag`
ORDER BY wf.`terrain_type`, sr.`icing_flag` DESC""",
    },
    {
        "question": "Which component types fail most often for each turbine model?",
        "difficulty": "MEDIUM",
        "sql": """SELECT
  t.`turbine_model`,
  c.`component_type`,
  COUNT(f.`failure_id`) AS `failure_count`,
  ROUND(SUM(f.`downtime_hours`), 2) AS `total_downtime_hours`,
  ROUND(AVG(f.`repair_cost_usd`), 2) AS `avg_repair_cost_usd`
FROM `{catalog}`.`{schema}`.`failure_events` f
JOIN `{catalog}`.`{schema}`.`components` c
  ON f.`component_id` = c.`component_id`
JOIN `{catalog}`.`{schema}`.`turbines` t
  ON f.`turbine_id` = t.`turbine_id`
GROUP BY t.`turbine_model`, c.`component_type`
ORDER BY `failure_count` DESC, t.`turbine_model`, c.`component_type`""",
    },
    {
        "question": "What were the top 20 farm and maintenance type combinations by maintenance cost in 2025?",
        "difficulty": "MEDIUM",
        "sql": """SELECT
  wf.`farm_name`,
  wf.`region`,
  m.`maintenance_type`,
  COUNT(*) AS `event_count`,
  ROUND(SUM(m.`cost_usd`), 2) AS `total_cost_usd`,
  ROUND(AVG(m.`duration_hours`), 2) AS `avg_duration_hours`
FROM `{catalog}`.`{schema}`.`maintenance_events` m
JOIN `{catalog}`.`{schema}`.`wind_farms` wf
  ON m.`farm_id` = wf.`farm_id`
WHERE m.`maintenance_year` = 2025
GROUP BY wf.`farm_name`, wf.`region`, m.`maintenance_type`
ORDER BY `total_cost_usd` DESC, wf.`farm_name`, m.`maintenance_type`
LIMIT 20""",
    },
    {
        "question": "For failures with and without recent preventive maintenance, how do downtime and repair costs compare by severity?",
        "difficulty": "MEDIUM",
        "sql": """SELECT
  `had_recent_preventive`,
  `severity`,
  COUNT(*) AS `failure_count`,
  ROUND(AVG(`downtime_hours`), 2) AS `avg_downtime_hours`,
  ROUND(PERCENTILE_APPROX(`downtime_hours`, 0.5), 2) AS `median_downtime_hours`,
  ROUND(AVG(`repair_cost_usd`), 2) AS `avg_repair_cost_usd`
FROM `{catalog}`.`{schema}`.`failure_events`
GROUP BY `had_recent_preventive`, `severity`
ORDER BY `had_recent_preventive` DESC, `severity`""",
    },
    {
        "question": "Which turbines had the most readings within 60 days of a failure, and how elevated were their anomaly and vibration readings?",
        "difficulty": "MEDIUM",
        "sql": """SELECT
  wf.`farm_name`,
  sr.`turbine_id`,
  t.`turbine_model`,
  COUNT(*) AS `near_failure_readings`,
  ROUND(AVG(sr.`anomaly_score`), 2) AS `avg_anomaly_score`,
  ROUND(AVG(sr.`vibration_mm_s`), 2) AS `avg_vibration_mm_s`,
  ROUND(MAX(sr.`gearbox_temp_c`), 2) AS `max_gearbox_temp_c`
FROM `{catalog}`.`{schema}`.`sensor_readings` sr
JOIN `{catalog}`.`{schema}`.`turbines` t
  ON sr.`turbine_id` = t.`turbine_id`
JOIN `{catalog}`.`{schema}`.`wind_farms` wf
  ON sr.`farm_id` = wf.`farm_id`
WHERE sr.`days_to_next_failure` BETWEEN 0 AND 60
GROUP BY wf.`farm_name`, sr.`turbine_id`, t.`turbine_model`
ORDER BY `near_failure_readings` DESC, `avg_anomaly_score` DESC, sr.`turbine_id`
LIMIT 10""",
    },
    {
        "question": "Which farms have the most components on watchlist or already replaced?",
        "difficulty": "MEDIUM",
        "sql": """SELECT
  wf.`farm_name`,
  wf.`region`,
  COUNT(*) AS `at_risk_component_count`,
  COUNT_IF(c.`status` = 'Watchlist') AS `watchlist_components`,
  COUNT_IF(c.`status` = 'Replaced') AS `replaced_components`
FROM `{catalog}`.`{schema}`.`components` c
JOIN `{catalog}`.`{schema}`.`turbines` t
  ON c.`turbine_id` = t.`turbine_id`
JOIN `{catalog}`.`{schema}`.`wind_farms` wf
  ON t.`farm_id` = wf.`farm_id`
WHERE c.`status` IN ('Watchlist', 'Replaced')
GROUP BY wf.`farm_name`, wf.`region`
ORDER BY `at_risk_component_count` DESC, wf.`farm_name`""",
    },
    {
        "question": "Which component types have the highest corrective maintenance rate in the maintenance reliability metric view?",
        "difficulty": "MEDIUM",
        "sql": """SELECT
  `Component Type` AS `component_type`,
  MEASURE(`Maintenance Count`) AS `maintenance_count`,
  MEASURE(`Corrective Count`) AS `corrective_count`,
  ROUND(100.0 * MEASURE(`Corrective Count`) / MEASURE(`Maintenance Count`), 2) AS `corrective_pct`,
  ROUND(MEASURE(`Total Maintenance Cost`), 2) AS `total_maintenance_cost_usd`
FROM `{catalog}`.`{schema}`.`mv_maintenance_reliability`
GROUP BY `Component Type`
ORDER BY `corrective_pct` DESC, `component_type`""",
    },
    {
        "question": "Which farm and turbine model combinations have the strongest capacity factor in the turbine performance metric view?",
        "difficulty": "MEDIUM",
        "sql": """SELECT
  `Farm Name` AS `farm_name`,
  `Turbine Model` AS `turbine_model`,
  MEASURE(`Reading Count`) AS `reading_count`,
  ROUND(MEASURE(`Average Capacity Factor Pct`), 2) AS `avg_capacity_factor_pct`,
  ROUND(MEASURE(`Average Anomaly Score`), 2) AS `avg_anomaly_score`
FROM `{catalog}`.`{schema}`.`mv_turbine_performance`
GROUP BY `Farm Name`, `Turbine Model`
ORDER BY `avg_capacity_factor_pct` DESC, `farm_name`, `turbine_model`
LIMIT 10""",
    },
    {
        "question": "Which regions have accumulated the most failure downtime and repair cost?",
        "difficulty": "MEDIUM",
        "sql": """SELECT
  wf.`region`,
  COUNT(*) AS `failure_count`,
  ROUND(SUM(f.`downtime_hours`), 2) AS `total_downtime_hours`,
  ROUND(SUM(f.`repair_cost_usd`), 2) AS `total_repair_cost_usd`,
  ROUND(AVG(f.`downtime_hours`), 2) AS `avg_downtime_hours`
FROM `{catalog}`.`{schema}`.`failure_events` f
JOIN `{catalog}`.`{schema}`.`wind_farms` wf
  ON f.`farm_id` = wf.`farm_id`
GROUP BY wf.`region`
ORDER BY `total_downtime_hours` DESC, wf.`region`""",
    },
    {
        "question": "By component type and work order priority, how long and costly are maintenance work orders?",
        "difficulty": "MEDIUM",
        "sql": """SELECT
  c.`component_type`,
  m.`work_order_priority`,
  COUNT(*) AS `work_order_count`,
  ROUND(AVG(m.`duration_hours`), 2) AS `avg_duration_hours`,
  ROUND(AVG(m.`cost_usd`), 2) AS `avg_cost_usd`,
  ROUND(SUM(m.`cost_usd`), 2) AS `total_cost_usd`
FROM `{catalog}`.`{schema}`.`maintenance_events` m
JOIN `{catalog}`.`{schema}`.`components` c
  ON m.`component_id` = c.`component_id`
GROUP BY c.`component_type`, m.`work_order_priority`
ORDER BY c.`component_type`,
  CASE m.`work_order_priority`
    WHEN 'High' THEN 1
    WHEN 'Medium' THEN 2
    ELSE 3
  END""",
    },
    {
        "question": "Across winter months, where are icing rates highest and how much lower is capacity factor when icing is present?",
        "difficulty": "MEDIUM",
        "sql": """WITH winter AS (
  SELECT
    `reading_month`,
    COUNT(*) AS `reading_count`,
    COUNT_IF(`icing_flag`) AS `icing_readings`,
    AVG(CASE WHEN `icing_flag` THEN `capacity_factor_pct` END) AS `avg_capacity_factor_when_icing`,
    AVG(CASE WHEN NOT `icing_flag` THEN `capacity_factor_pct` END) AS `avg_capacity_factor_without_icing`
  FROM `{catalog}`.`{schema}`.`sensor_readings`
  WHERE `reading_month` IN (1, 2, 12)
  GROUP BY `reading_month`
)
SELECT
  `reading_month`,
  `reading_count`,
  `icing_readings`,
  ROUND(100.0 * `icing_readings` / `reading_count`, 2) AS `icing_rate_pct`,
  ROUND(`avg_capacity_factor_when_icing`, 2) AS `avg_capacity_factor_when_icing`,
  ROUND(`avg_capacity_factor_without_icing`, 2) AS `avg_capacity_factor_without_icing`,
  ROUND(`avg_capacity_factor_without_icing` - `avg_capacity_factor_when_icing`, 2) AS `capacity_factor_gap_pct_points`
FROM winter
ORDER BY `icing_rate_pct` DESC, `reading_month`""",
    },
    {
        "question": "Which technician teams and regions handled the most high-priority work orders?",
        "difficulty": "MEDIUM",
        "sql": """SELECT
  m.`technician_team`,
  wf.`region`,
  COUNT(*) AS `high_priority_events`,
  COUNT_IF(m.`maintenance_type` = 'Corrective') AS `corrective_events`,
  ROUND(AVG(m.`duration_hours`), 2) AS `avg_duration_hours`,
  ROUND(SUM(m.`cost_usd`), 2) AS `total_cost_usd`
FROM `{catalog}`.`{schema}`.`maintenance_events` m
JOIN `{catalog}`.`{schema}`.`wind_farms` wf
  ON m.`farm_id` = wf.`farm_id`
WHERE m.`work_order_priority` = 'High'
GROUP BY m.`technician_team`, wf.`region`
ORDER BY `high_priority_events` DESC, `total_cost_usd` DESC, m.`technician_team`, wf.`region`
LIMIT 15""",
    },
    {
        "question": "On average, how old are components when they fail, by component type and manufacturer, and how does that compare to their expected service life?",
        "difficulty": "MEDIUM",
        "sql": """SELECT
  c.`component_type`,
  c.`manufacturer`,
  COUNT(*) AS `failure_count`,
  ROUND(AVG(DATEDIFF(f.`failure_date`, c.`install_date`) / 365.25), 2) AS `avg_age_years_at_failure`,
  ROUND(AVG(c.`expected_life_years`), 1) AS `avg_expected_life_years`,
  ROUND(AVG(f.`repair_cost_usd`), 2) AS `avg_repair_cost_usd`
FROM `{catalog}`.`{schema}`.`failure_events` f
JOIN `{catalog}`.`{schema}`.`components` c
  ON f.`component_id` = c.`component_id`
GROUP BY c.`component_type`, c.`manufacturer`
HAVING COUNT(*) >= 5
ORDER BY `avg_age_years_at_failure`, c.`component_type`, c.`manufacturer`""",
    },
    {
        "question": "How have monthly failure counts and downtime changed month over month?",
        "difficulty": "HARD",
        "sql": """WITH monthly_failures AS (
  SELECT
    DATE_TRUNC('MONTH', `failure_date`) AS `failure_month`,
    COUNT(*) AS `failure_count`,
    SUM(`downtime_hours`) AS `total_downtime_hours`
  FROM `{catalog}`.`{schema}`.`failure_events`
  GROUP BY DATE_TRUNC('MONTH', `failure_date`)
)
SELECT
  `failure_month`,
  `failure_count`,
  ROUND(`total_downtime_hours`, 2) AS `total_downtime_hours`,
  `failure_count` - LAG(`failure_count`) OVER (ORDER BY `failure_month`) AS `failure_count_mom_change`,
  ROUND(`total_downtime_hours` - LAG(`total_downtime_hours`) OVER (ORDER BY `failure_month`), 2) AS `downtime_mom_change_hours`
FROM monthly_failures
ORDER BY `failure_month`""",
    },
    {
        "question": "For each wind farm, which turbine had the highest average anomaly score in 2025?",
        "difficulty": "HARD",
        "sql": """WITH turbine_scores AS (
  SELECT
    wf.`farm_id`,
    wf.`farm_name`,
    t.`turbine_id`,
    t.`turbine_model`,
    COUNT(*) AS `reading_count`,
    ROUND(AVG(sr.`anomaly_score`), 2) AS `avg_anomaly_score`,
    COUNT_IF(sr.`anomaly_score` >= 70) AS `high_anomaly_readings`,
    ROUND(AVG(sr.`capacity_factor_pct`), 2) AS `avg_capacity_factor_pct`
  FROM `{catalog}`.`{schema}`.`sensor_readings` sr
  JOIN `{catalog}`.`{schema}`.`turbines` t
    ON sr.`turbine_id` = t.`turbine_id`
  JOIN `{catalog}`.`{schema}`.`wind_farms` wf
    ON t.`farm_id` = wf.`farm_id`
  WHERE sr.`reading_year` = 2025
  GROUP BY wf.`farm_id`, wf.`farm_name`, t.`turbine_id`, t.`turbine_model`
), ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY `farm_id` ORDER BY `avg_anomaly_score` DESC, `turbine_id`) AS `anomaly_rank`
  FROM turbine_scores
)
SELECT
  `farm_name`,
  `turbine_id`,
  `turbine_model`,
  `reading_count`,
  `avg_anomaly_score`,
  `high_anomaly_readings`,
  `avg_capacity_factor_pct`
FROM ranked
WHERE `anomaly_rank` = 1
ORDER BY `avg_anomaly_score` DESC, `farm_name`""",
    },
    {
        "question": "How do anomaly score, vibration, gearbox temperature, and capacity factor change as a turbine gets closer to a known failure?",
        "difficulty": "HARD",
        "sql": """WITH failure_windows AS (
  SELECT
    CASE
      WHEN `days_to_next_failure` BETWEEN 0 AND 60 THEN '0-60 days to failure'
      WHEN `days_to_next_failure` BETWEEN 61 AND 120 THEN '61-120 days to failure'
      WHEN `days_to_next_failure` > 120 THEN 'More than 120 days to failure'
      ELSE 'No known future failure'
    END AS `failure_window`,
    CASE
      WHEN `days_to_next_failure` BETWEEN 0 AND 60 THEN 1
      WHEN `days_to_next_failure` BETWEEN 61 AND 120 THEN 2
      WHEN `days_to_next_failure` > 120 THEN 3
      ELSE 4
    END AS `sort_order`,
    `anomaly_score`,
    `vibration_mm_s`,
    `gearbox_temp_c`,
    `capacity_factor_pct`
  FROM `{catalog}`.`{schema}`.`sensor_readings`
)
SELECT
  `failure_window`,
  COUNT(*) AS `reading_count`,
  ROUND(AVG(`anomaly_score`), 2) AS `avg_anomaly_score`,
  ROUND(AVG(`vibration_mm_s`), 2) AS `avg_vibration_mm_s`,
  ROUND(AVG(`gearbox_temp_c`), 2) AS `avg_gearbox_temp_c`,
  ROUND(AVG(`capacity_factor_pct`), 2) AS `avg_capacity_factor_pct`
FROM failure_windows
GROUP BY `failure_window`, `sort_order`
ORDER BY `sort_order`""",
    },
    {
        "question": "Which component types have the highest failure rate per installed component?",
        "difficulty": "HARD",
        "sql": """WITH component_counts AS (
  SELECT
    `component_type`,
    COUNT(*) AS `installed_components`
  FROM `{catalog}`.`{schema}`.`components`
  GROUP BY `component_type`
), failure_counts AS (
  SELECT
    `component_type`,
    COUNT(*) AS `failure_count`,
    SUM(`repair_cost_usd`) AS `total_repair_cost_usd`,
    SUM(`downtime_hours`) AS `total_downtime_hours`
  FROM `{catalog}`.`{schema}`.`failure_events`
  GROUP BY `component_type`
), rates AS (
  SELECT
    c.`component_type`,
    c.`installed_components`,
    COALESCE(f.`failure_count`, 0) AS `failure_count`,
    COALESCE(f.`total_repair_cost_usd`, 0.0) AS `total_repair_cost_usd`,
    COALESCE(f.`total_downtime_hours`, 0.0) AS `total_downtime_hours`,
    100.0 * COALESCE(f.`failure_count`, 0) / c.`installed_components` AS `failure_rate_pct`
  FROM component_counts c
  LEFT JOIN failure_counts f
    ON c.`component_type` = f.`component_type`
)
SELECT
  `component_type`,
  `installed_components`,
  `failure_count`,
  ROUND(`failure_rate_pct`, 2) AS `failure_rate_pct`,
  ROUND(`total_downtime_hours`, 2) AS `total_downtime_hours`,
  ROUND(`total_repair_cost_usd`, 2) AS `total_repair_cost_usd`,
  RANK() OVER (ORDER BY `failure_rate_pct` DESC, `component_type`) AS `failure_rate_rank`
FROM rates
ORDER BY `failure_rate_rank`, `component_type`""",
    },
    {
        "question": "Which wind farms are most exposed when combining high anomalies, failures, downtime, and watchlist components?",
        "difficulty": "HARD",
        "sql": """WITH sensor_risk AS (
  SELECT
    `farm_id`,
    COUNT_IF(`anomaly_score` >= 70) AS `high_anomaly_readings`,
    ROUND(AVG(`anomaly_score`), 2) AS `avg_anomaly_score`
  FROM `{catalog}`.`{schema}`.`sensor_readings`
  GROUP BY `farm_id`
), failure_risk AS (
  SELECT
    `farm_id`,
    COUNT(*) AS `failure_count`,
    SUM(`downtime_hours`) AS `total_downtime_hours`
  FROM `{catalog}`.`{schema}`.`failure_events`
  GROUP BY `farm_id`
), component_risk AS (
  SELECT
    t.`farm_id`,
    COUNT_IF(c.`status` = 'Watchlist') AS `watchlist_components`,
    COUNT_IF(c.`status` = 'Replaced') AS `replaced_components`
  FROM `{catalog}`.`{schema}`.`components` c
  JOIN `{catalog}`.`{schema}`.`turbines` t
    ON c.`turbine_id` = t.`turbine_id`
  GROUP BY t.`farm_id`
)
SELECT
  wf.`farm_name`,
  wf.`region`,
  COALESCE(sr.`high_anomaly_readings`, 0) AS `high_anomaly_readings`,
  COALESCE(fr.`failure_count`, 0) AS `failure_count`,
  ROUND(COALESCE(fr.`total_downtime_hours`, 0.0), 2) AS `total_downtime_hours`,
  COALESCE(cr.`watchlist_components`, 0) AS `watchlist_components`,
  COALESCE(cr.`replaced_components`, 0) AS `replaced_components`,
  ROUND(
    COALESCE(sr.`high_anomaly_readings`, 0) * 0.5
    + COALESCE(fr.`failure_count`, 0) * 5.0
    + COALESCE(fr.`total_downtime_hours`, 0.0) / 24.0
    + COALESCE(cr.`watchlist_components`, 0) * 2.0
    + COALESCE(cr.`replaced_components`, 0) * 3.0,
    2
  ) AS `risk_score`
FROM `{catalog}`.`{schema}`.`wind_farms` wf
LEFT JOIN sensor_risk sr
  ON wf.`farm_id` = sr.`farm_id`
LEFT JOIN failure_risk fr
  ON wf.`farm_id` = fr.`farm_id`
LEFT JOIN component_risk cr
  ON wf.`farm_id` = cr.`farm_id`
ORDER BY `risk_score` DESC, wf.`farm_name`""",
    },
    {
        "question": "Which regions improved or worsened in average capacity factor year over year?",
        "difficulty": "HARD",
        "sql": """WITH yearly_region AS (
  SELECT
    wf.`region`,
    sr.`reading_year`,
    AVG(sr.`capacity_factor_pct`) AS `avg_capacity_factor_pct`,
    AVG(sr.`power_output_kw`) AS `avg_power_output_kw`
  FROM `{catalog}`.`{schema}`.`sensor_readings` sr
  JOIN `{catalog}`.`{schema}`.`wind_farms` wf
    ON sr.`farm_id` = wf.`farm_id`
  GROUP BY wf.`region`, sr.`reading_year`
)
SELECT
  `region`,
  `reading_year`,
  ROUND(`avg_capacity_factor_pct`, 2) AS `avg_capacity_factor_pct`,
  ROUND(`avg_power_output_kw`, 2) AS `avg_power_output_kw`,
  ROUND(`avg_capacity_factor_pct` - LAG(`avg_capacity_factor_pct`) OVER (PARTITION BY `region` ORDER BY `reading_year`), 2) AS `capacity_factor_yoy_change`,
  ROUND(`avg_power_output_kw` - LAG(`avg_power_output_kw`) OVER (PARTITION BY `region` ORDER BY `reading_year`), 2) AS `power_output_yoy_change_kw`
FROM yearly_region
ORDER BY `region`, `reading_year`""",
    },
    {
        "question": "For each wind farm, which turbine drove the most corrective maintenance cost?",
        "difficulty": "HARD",
        "sql": """WITH turbine_cost AS (
  SELECT
    wf.`farm_id`,
    wf.`farm_name`,
    t.`turbine_id`,
    t.`turbine_model`,
    COUNT(*) AS `corrective_events`,
    SUM(m.`cost_usd`) AS `total_corrective_cost_usd`,
    AVG(m.`duration_hours`) AS `avg_corrective_duration_hours`
  FROM `{catalog}`.`{schema}`.`maintenance_events` m
  JOIN `{catalog}`.`{schema}`.`turbines` t
    ON m.`turbine_id` = t.`turbine_id`
  JOIN `{catalog}`.`{schema}`.`wind_farms` wf
    ON t.`farm_id` = wf.`farm_id`
  WHERE m.`maintenance_type` = 'Corrective'
  GROUP BY wf.`farm_id`, wf.`farm_name`, t.`turbine_id`, t.`turbine_model`
), ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY `farm_id` ORDER BY `total_corrective_cost_usd` DESC, `turbine_id`) AS `cost_rank`
  FROM turbine_cost
)
SELECT
  `farm_name`,
  `turbine_id`,
  `turbine_model`,
  `corrective_events`,
  ROUND(`total_corrective_cost_usd`, 2) AS `total_corrective_cost_usd`,
  ROUND(`avg_corrective_duration_hours`, 2) AS `avg_corrective_duration_hours`
FROM ranked
WHERE `cost_rank` = 1
ORDER BY `total_corrective_cost_usd` DESC, `farm_name`""",
    },
    {
        "question": "Which turbines show the strongest 2025 anomaly signals, and how many recorded failures have they had?",
        "difficulty": "HARD",
        "sql": """WITH sensor_summary AS (
  SELECT
    `turbine_id`,
    COUNT(*) AS `reading_count`,
    ROUND(AVG(`anomaly_score`), 2) AS `avg_anomaly_score`,
    COUNT_IF(`anomaly_score` >= 70) AS `high_anomaly_readings`,
    COUNT_IF(`anomaly_score` >= 50) AS `elevated_anomaly_readings`,
    ROUND(MAX(`vibration_mm_s`), 2) AS `max_vibration_mm_s`,
    ROUND(MAX(`gearbox_temp_c`), 2) AS `max_gearbox_temp_c`
  FROM `{catalog}`.`{schema}`.`sensor_readings`
  WHERE `reading_year` = 2025
  GROUP BY `turbine_id`
), failure_summary AS (
  SELECT
    `turbine_id`,
    COUNT(*) AS `failure_count`,
    ROUND(SUM(`downtime_hours`), 2) AS `total_downtime_hours`
  FROM `{catalog}`.`{schema}`.`failure_events`
  GROUP BY `turbine_id`
), component_summary AS (
  SELECT
    `turbine_id`,
    COUNT_IF(`status` = 'Watchlist') AS `watchlist_components`,
    COUNT_IF(`status` = 'Replaced') AS `replaced_components`
  FROM `{catalog}`.`{schema}`.`components`
  GROUP BY `turbine_id`
)
SELECT
  wf.`farm_name`,
  t.`turbine_id`,
  t.`turbine_model`,
  ss.`reading_count`,
  ss.`avg_anomaly_score`,
  ss.`high_anomaly_readings`,
  ss.`elevated_anomaly_readings`,
  ss.`max_vibration_mm_s`,
  ss.`max_gearbox_temp_c`,
  COALESCE(fs.`failure_count`, 0) AS `failure_count`,
  COALESCE(fs.`total_downtime_hours`, 0.0) AS `total_downtime_hours`,
  COALESCE(cs.`watchlist_components`, 0) AS `watchlist_components`,
  COALESCE(cs.`replaced_components`, 0) AS `replaced_components`
FROM sensor_summary ss
JOIN `{catalog}`.`{schema}`.`turbines` t
  ON ss.`turbine_id` = t.`turbine_id`
JOIN `{catalog}`.`{schema}`.`wind_farms` wf
  ON t.`farm_id` = wf.`farm_id`
LEFT JOIN failure_summary fs
  ON ss.`turbine_id` = fs.`turbine_id`
LEFT JOIN component_summary cs
  ON ss.`turbine_id` = cs.`turbine_id`
ORDER BY ss.`high_anomaly_readings` DESC, ss.`elevated_anomaly_readings` DESC, ss.`avg_anomaly_score` DESC, t.`turbine_id`
LIMIT 10""",
    },
    {
        "question": "For turbines that have failed more than once, what is the mean time between failures in days, and which turbines have the shortest intervals?",
        "difficulty": "HARD",
        "sql": """WITH ordered_failures AS (
  SELECT
    `turbine_id`,
    `failure_date`,
    LAG(`failure_date`) OVER (PARTITION BY `turbine_id` ORDER BY `failure_date`) AS `prev_failure_date`
  FROM `{catalog}`.`{schema}`.`failure_events`
), failure_intervals AS (
  SELECT
    `turbine_id`,
    DATEDIFF(`failure_date`, `prev_failure_date`) AS `days_between_failures`
  FROM ordered_failures
  WHERE `prev_failure_date` IS NOT NULL
)
SELECT
  wf.`farm_name`,
  t.`turbine_id`,
  t.`turbine_model`,
  COUNT(*) AS `failure_interval_count`,
  ROUND(AVG(fi.`days_between_failures`), 1) AS `mean_days_between_failures`,
  MIN(fi.`days_between_failures`) AS `min_days_between_failures`,
  MAX(fi.`days_between_failures`) AS `max_days_between_failures`
FROM failure_intervals fi
JOIN `{catalog}`.`{schema}`.`turbines` t
  ON fi.`turbine_id` = t.`turbine_id`
JOIN `{catalog}`.`{schema}`.`wind_farms` wf
  ON t.`farm_id` = wf.`farm_id`
GROUP BY wf.`farm_name`, t.`turbine_id`, t.`turbine_model`
ORDER BY `mean_days_between_failures` ASC, `failure_interval_count` DESC, t.`turbine_id`
LIMIT 15""",
    },
    {
        "question": "When turbines are grouped into quartiles by their 2025 average capacity factor, how do anomaly scores, failures, and downtime compare across quartiles?",
        "difficulty": "HARD",
        "sql": """WITH turbine_perf AS (
  SELECT
    sr.`turbine_id`,
    AVG(sr.`capacity_factor_pct`) AS `avg_capacity_factor_pct`,
    AVG(sr.`anomaly_score`) AS `avg_anomaly_score`
  FROM `{catalog}`.`{schema}`.`sensor_readings` sr
  WHERE sr.`reading_year` = 2025
  GROUP BY sr.`turbine_id`
), bucketed AS (
  SELECT
    `turbine_id`,
    `avg_capacity_factor_pct`,
    `avg_anomaly_score`,
    NTILE(4) OVER (ORDER BY `avg_capacity_factor_pct` DESC) AS `capacity_factor_quartile`
  FROM turbine_perf
), turbine_failures AS (
  SELECT
    `turbine_id`,
    COUNT(*) AS `failure_count`,
    SUM(`downtime_hours`) AS `total_downtime_hours`
  FROM `{catalog}`.`{schema}`.`failure_events`
  GROUP BY `turbine_id`
)
SELECT
  b.`capacity_factor_quartile`,
  COUNT(*) AS `turbine_count`,
  ROUND(AVG(b.`avg_capacity_factor_pct`), 2) AS `avg_capacity_factor_pct`,
  ROUND(AVG(b.`avg_anomaly_score`), 2) AS `avg_anomaly_score`,
  SUM(COALESCE(tf.`failure_count`, 0)) AS `total_failures`,
  ROUND(SUM(COALESCE(tf.`total_downtime_hours`, 0.0)), 2) AS `total_downtime_hours`,
  ROUND(1.0 * SUM(COALESCE(tf.`failure_count`, 0)) / COUNT(*), 2) AS `avg_failures_per_turbine`
FROM bucketed b
LEFT JOIN turbine_failures tf
  ON b.`turbine_id` = tf.`turbine_id`
GROUP BY b.`capacity_factor_quartile`
ORDER BY b.`capacity_factor_quartile`""",
    },
]

assert len(BENCHMARKS) == 30, f"Expected 30 benchmarks, found {len(BENCHMARKS)}"


def render_sql(benchmark):
    return benchmark["sql"].strip().format(catalog=catalog, schema=schema)


difficulty_counts = {}
for benchmark in BENCHMARKS:
    difficulty_counts[benchmark["difficulty"]] = difficulty_counts.get(benchmark["difficulty"], 0) + 1
print(f"Loaded {len(BENCHMARKS)} benchmarks: {difficulty_counts}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Fetch Genie Space

# COMMAND ----------

resp = w.api_client.do(
    "GET",
    f"/api/2.0/genie/spaces/{space_id}",
    query={"include_serialized_space": "true"},
)

serialized = json.loads(resp["serialized_space"])
pre_data_sources = deepcopy(serialized.get("data_sources"))
pre_instructions = deepcopy(serialized.get("instructions"))
pre_version = deepcopy(serialized.get("version"))

print(f"Fetched Genie Space: {resp.get('title', space_id)}")
print(f"Existing benchmark question count: {len(serialized.get('benchmarks', {}).get('questions', []))}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Replace Benchmark Questions

# COMMAND ----------

import uuid


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

serialized.setdefault("benchmarks", {})["questions"] = questions
serialized.setdefault("version", 2)

print(f"Prepared {len(questions)} benchmark questions for patch.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Patch Genie Space

# COMMAND ----------

body = {"serialized_space": json.dumps(serialized)}
for key in ("title", "description", "warehouse_id"):
    if resp.get(key) is not None:
        body[key] = resp[key]

w.api_client.do("PATCH", f"/api/2.0/genie/spaces/{space_id}", body=body)
print("Patched Genie Space benchmark questions.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Round-Trip Verification

# COMMAND ----------

post_resp = w.api_client.do(
    "GET",
    f"/api/2.0/genie/spaces/{space_id}",
    query={"include_serialized_space": "true"},
)
post_serialized = json.loads(post_resp["serialized_space"])

post_questions = post_serialized.get("benchmarks", {}).get("questions", [])
expected_questions = [benchmark["question"] for benchmark in BENCHMARKS]
actual_questions = [question["question"][0] for question in post_questions]

assert len(post_questions) == 30, f"Expected 30 benchmark questions, found {len(post_questions)}"
assert actual_questions == expected_questions, "Round-trip benchmark questions do not match expected questions."
assert post_serialized.get("data_sources") == pre_data_sources, "data_sources changed unexpectedly."
assert post_serialized.get("instructions") == pre_instructions, "instructions changed unexpectedly."
assert post_serialized.get("version") == pre_version, "version changed unexpectedly."

print("Round-trip verification succeeded.")
print(f"Benchmark questions loaded: {len(post_questions)}")
print("Verified data_sources, instructions, and version are unchanged.")

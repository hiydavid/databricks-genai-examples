# Databricks notebook source

# MAGIC %md
# MAGIC # Retail Apparel Genie Benchmark Loader
# MAGIC
# MAGIC Loads 30 benchmark questions for the retail apparel Genie Space.
# MAGIC
# MAGIC Safety note: this notebook mutates ONLY `benchmarks.questions` in the serialized Genie Space config. It fetches the current space, replaces the benchmark question list, patches the space, and verifies that `data_sources`, `instructions`, and `version` are unchanged.

# COMMAND ----------

# MAGIC %pip install databricks-sdk --upgrade -q
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import copy
import json
import time
import uuid

from databricks.sdk import WorkspaceClient

dbutils.widgets.text("space_id", "", "Target Genie Space ID")
dbutils.widgets.text("catalog", "dhuang_catalog", "Unity Catalog")
dbutils.widgets.text("schema", "retail_apparel", "Schema")
dbutils.widgets.dropdown("run_sql_validation", "true", ["true", "false"], "Run SQL validation")
dbutils.widgets.dropdown("run_conversation_check", "false", ["true", "false"], "Run conversation spot check")

space_id = dbutils.widgets.get("space_id").strip()
catalog = dbutils.widgets.get("catalog").strip() or "dhuang_catalog"
schema = dbutils.widgets.get("schema").strip() or "retail_apparel"
run_sql_validation = dbutils.widgets.get("run_sql_validation").strip().lower() == "true"
run_conversation_check = dbutils.widgets.get("run_conversation_check").strip().lower() == "true"

if not space_id:
    raise ValueError("Widget 'space_id' is required.")

w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Benchmarks

# COMMAND ----------

BENCHMARKS = [
    {
        "question": "Which product categories have the most active products, and what is their average list price?",
        "difficulty": "EASY",
        "sql": """
SELECT
  `category`,
  COUNT(*) AS `active_product_count`,
  ROUND(AVG(`list_price_usd`), 2) AS `avg_list_price_usd`
FROM `{catalog}`.`{schema}`.`products`
WHERE `is_active` = true
GROUP BY `category`
ORDER BY `active_product_count` DESC, `category`
""".strip(),
    },
    {
        "question": "How many stores do we have by region and store type?",
        "difficulty": "EASY",
        "sql": """
SELECT
  `region`,
  `store_type`,
  COUNT(*) AS `store_count`,
  SUM(`selling_sqft`) AS `total_selling_sqft`
FROM `{catalog}`.`{schema}`.`stores`
GROUP BY `region`, `store_type`
ORDER BY `region`, `store_type`
""".strip(),
    },
    {
        "question": "How many active customers are in each loyalty tier?",
        "difficulty": "EASY",
        "sql": """
SELECT
  `loyalty_tier`,
  COUNT(*) AS `active_customer_count`
FROM `{catalog}`.`{schema}`.`customers`
WHERE `is_active` = true
GROUP BY `loyalty_tier`
ORDER BY `active_customer_count` DESC, `loyalty_tier`
""".strip(),
    },
    {
        "question": "What were monthly units sold and net sales in 2025?",
        "difficulty": "EASY",
        "sql": """
SELECT
  CAST(`Sale Month` AS DATE) AS `sale_month`,
  MEASURE(`Units Sold`) AS `units_sold`,
  ROUND(MEASURE(`Net Sales`), 2) AS `net_sales_usd`
FROM `{catalog}`.`{schema}`.`mv_retail_sales`
WHERE `Sale Year` = 2025
GROUP BY `Sale Month`
ORDER BY `sale_month`
""".strip(),
    },
    {
        "question": "Which sales channel generated the most net sales overall?",
        "difficulty": "EASY",
        "sql": """
SELECT
  `channel`,
  COUNT(*) AS `order_count`,
  SUM(`quantity`) AS `units_sold`,
  ROUND(SUM(`net_sales_usd`), 2) AS `net_sales_usd`
FROM `{catalog}`.`{schema}`.`sales`
GROUP BY `channel`
ORDER BY `net_sales_usd` DESC, `channel`
""".strip(),
    },
    {
        "question": "What are total returns and return dollars by return reason?",
        "difficulty": "EASY",
        "sql": """
SELECT
  `Return Reason` AS `return_reason`,
  MEASURE(`Return Count`) AS `return_count`,
  MEASURE(`Returned Units`) AS `returned_units`,
  ROUND(MEASURE(`Return Amount`), 2) AS `return_amount_usd`
FROM `{catalog}`.`{schema}`.`mv_retail_returns`
GROUP BY `Return Reason`
ORDER BY `return_count` DESC, `return_reason`
""".strip(),
    },
    {
        "question": "Which product seasons have the highest average list price and target margin?",
        "difficulty": "EASY",
        "sql": """
SELECT
  `season`,
  COUNT(*) AS `product_count`,
  ROUND(AVG(`list_price_usd`), 2) AS `avg_list_price_usd`,
  ROUND(AVG(`target_margin_pct`), 2) AS `avg_target_margin_pct`
FROM `{catalog}`.`{schema}`.`products`
GROUP BY `season`
ORDER BY `avg_list_price_usd` DESC, `season`
""".strip(),
    },
    {
        "question": "Which inventory months had the most stockout days?",
        "difficulty": "EASY",
        "sql": """
SELECT
  CAST(`Snapshot Month` AS DATE) AS `snapshot_month`,
  MEASURE(`Stockout Days`) AS `stockout_days`,
  ROUND(MEASURE(`Lost Sales Estimate Units`), 2) AS `lost_sales_estimate_units`
FROM `{catalog}`.`{schema}`.`mv_inventory_health`
GROUP BY `Snapshot Month`
ORDER BY `stockout_days` DESC, `snapshot_month`
LIMIT 10
""".strip(),
    },
    {
        "question": "Which product categories generated the most net sales and gross margin?",
        "difficulty": "MEDIUM",
        "sql": """
SELECT
  p.`category`,
  SUM(s.`quantity`) AS `units_sold`,
  ROUND(SUM(s.`net_sales_usd`), 2) AS `net_sales_usd`,
  ROUND(SUM(s.`gross_margin_usd`), 2) AS `gross_margin_usd`,
  ROUND(SUM(s.`gross_margin_usd`) * 100.0 / SUM(s.`net_sales_usd`), 2) AS `gross_margin_pct`
FROM `{catalog}`.`{schema}`.`sales` s
JOIN `{catalog}`.`{schema}`.`products` p
  ON s.`product_id` = p.`product_id`
GROUP BY p.`category`
ORDER BY `net_sales_usd` DESC, p.`category`
""".strip(),
    },
    {
        "question": "What were the top 10 products by net sales in 2025?",
        "difficulty": "MEDIUM",
        "sql": """
SELECT
  p.`product_id`,
  p.`sku`,
  p.`product_name`,
  p.`category`,
  p.`brand_line`,
  SUM(s.`quantity`) AS `units_sold`,
  ROUND(SUM(s.`net_sales_usd`), 2) AS `net_sales_usd`
FROM `{catalog}`.`{schema}`.`sales` s
JOIN `{catalog}`.`{schema}`.`products` p
  ON s.`product_id` = p.`product_id`
WHERE s.`sale_year` = 2025
GROUP BY p.`product_id`, p.`sku`, p.`product_name`, p.`category`, p.`brand_line`
ORDER BY `net_sales_usd` DESC, p.`product_id`
LIMIT 10
""".strip(),
    },
    {
        "question": "How did online sales share and online order share change by year?",
        "difficulty": "MEDIUM",
        "sql": """
SELECT
  `sale_year`,
  ROUND(SUM(CASE WHEN `channel` = 'Online' THEN `net_sales_usd` ELSE 0 END), 2) AS `online_net_sales_usd`,
  ROUND(SUM(CASE WHEN `channel` = 'Store' THEN `net_sales_usd` ELSE 0 END), 2) AS `store_net_sales_usd`,
  ROUND(SUM(CASE WHEN `channel` = 'Online' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS `online_order_share_pct`,
  ROUND(SUM(CASE WHEN `channel` = 'Online' THEN `net_sales_usd` ELSE 0 END) * 100.0 / SUM(`net_sales_usd`), 2) AS `online_sales_share_pct`
FROM `{catalog}`.`{schema}`.`sales`
GROUP BY `sale_year`
ORDER BY `sale_year`
""".strip(),
    },
    {
        "question": "What is the return order rate by sales channel?",
        "difficulty": "MEDIUM",
        "sql": """
SELECT
  s.`channel`,
  COUNT(DISTINCT s.`sale_id`) AS `order_count`,
  COUNT(DISTINCT r.`return_id`) AS `returned_order_count`,
  ROUND(COUNT(DISTINCT r.`return_id`) * 100.0 / COUNT(DISTINCT s.`sale_id`), 2) AS `return_order_rate_pct`,
  ROUND(SUM(COALESCE(r.`return_amount_usd`, 0)), 2) AS `return_amount_usd`
FROM `{catalog}`.`{schema}`.`sales` s
LEFT JOIN `{catalog}`.`{schema}`.`returns` r
  ON s.`sale_id` = r.`sale_id`
GROUP BY s.`channel`
ORDER BY `return_order_rate_pct` DESC, s.`channel`
""".strip(),
    },
    {
        "question": "Which physical store regions generated the most net sales per selling square foot?",
        "difficulty": "MEDIUM",
        "sql": """
WITH store_capacity AS (
  SELECT
    `region`,
    SUM(`selling_sqft`) AS `total_selling_sqft`
  FROM `{catalog}`.`{schema}`.`stores`
  WHERE `store_type` <> 'Ecommerce'
  GROUP BY `region`
),
sales_by_region AS (
  SELECT
    st.`region`,
    ROUND(SUM(s.`net_sales_usd`), 2) AS `net_sales_usd`
  FROM `{catalog}`.`{schema}`.`sales` s
  JOIN `{catalog}`.`{schema}`.`stores` st
    ON s.`store_id` = st.`store_id`
  WHERE st.`store_type` <> 'Ecommerce'
  GROUP BY st.`region`
)
SELECT
  sr.`region`,
  sr.`net_sales_usd`,
  sc.`total_selling_sqft`,
  ROUND(sr.`net_sales_usd` / sc.`total_selling_sqft`, 2) AS `net_sales_per_selling_sqft`
FROM sales_by_region sr
JOIN store_capacity sc
  ON sr.`region` = sc.`region`
ORDER BY `net_sales_per_selling_sqft` DESC, sr.`region`
""".strip(),
    },
    {
        "question": "Which loyalty tiers contribute the largest revenue share and average order value?",
        "difficulty": "MEDIUM",
        "sql": """
WITH tier_sales AS (
  SELECT
    `loyalty_tier`,
    COUNT(*) AS `order_count`,
    COUNT(DISTINCT `customer_id`) AS `buying_customer_count`,
    ROUND(SUM(`net_sales_usd`), 2) AS `net_sales_usd`,
    ROUND(AVG(`net_sales_usd`), 2) AS `avg_order_value_usd`
  FROM `{catalog}`.`{schema}`.`sales`
  GROUP BY `loyalty_tier`
),
total_sales AS (
  SELECT SUM(`net_sales_usd`) AS `total_net_sales_usd`
  FROM tier_sales
)
SELECT
  ts.`loyalty_tier`,
  ts.`order_count`,
  ts.`buying_customer_count`,
  ts.`net_sales_usd`,
  ts.`avg_order_value_usd`,
  ROUND(ts.`net_sales_usd` * 100.0 / total_sales.`total_net_sales_usd`, 2) AS `revenue_share_pct`
FROM tier_sales ts
CROSS JOIN total_sales
ORDER BY `revenue_share_pct` DESC, ts.`loyalty_tier`
""".strip(),
    },
    {
        "question": "How much deeper are discounts during clearance periods by year?",
        "difficulty": "MEDIUM",
        "sql": """
SELECT
  `sale_year`,
  CASE WHEN `is_clearance` THEN 'Clearance' ELSE 'Non-clearance' END AS `clearance_period`,
  COUNT(*) AS `order_count`,
  ROUND(AVG(`discount_pct`), 2) AS `avg_discount_pct`,
  ROUND(SUM(`discount_amount_usd`), 2) AS `discount_amount_usd`,
  ROUND(SUM(`net_sales_usd`), 2) AS `net_sales_usd`
FROM `{catalog}`.`{schema}`.`sales`
GROUP BY `sale_year`, CASE WHEN `is_clearance` THEN 'Clearance' ELSE 'Non-clearance' END
ORDER BY `sale_year`, `clearance_period`
""".strip(),
    },
    {
        "question": "Which product categories have the highest return dollars as a percent of net sales?",
        "difficulty": "MEDIUM",
        "sql": """
WITH sales_by_category AS (
  SELECT
    p.`category`,
    COUNT(*) AS `order_count`,
    ROUND(SUM(s.`net_sales_usd`), 2) AS `net_sales_usd`
  FROM `{catalog}`.`{schema}`.`sales` s
  JOIN `{catalog}`.`{schema}`.`products` p
    ON s.`product_id` = p.`product_id`
  GROUP BY p.`category`
),
returns_by_category AS (
  SELECT
    p.`category`,
    COUNT(DISTINCT r.`return_id`) AS `return_count`,
    SUM(r.`return_quantity`) AS `returned_units`,
    ROUND(SUM(r.`return_amount_usd`), 2) AS `return_amount_usd`
  FROM `{catalog}`.`{schema}`.`returns` r
  JOIN `{catalog}`.`{schema}`.`products` p
    ON r.`product_id` = p.`product_id`
  GROUP BY p.`category`
)
SELECT
  s.`category`,
  s.`order_count`,
  COALESCE(r.`return_count`, 0) AS `return_count`,
  COALESCE(r.`returned_units`, 0) AS `returned_units`,
  s.`net_sales_usd`,
  COALESCE(r.`return_amount_usd`, 0) AS `return_amount_usd`,
  ROUND(COALESCE(r.`return_amount_usd`, 0) * 100.0 / s.`net_sales_usd`, 2) AS `return_amount_pct_of_sales`
FROM sales_by_category s
LEFT JOIN returns_by_category r
  ON s.`category` = r.`category`
ORDER BY `return_amount_pct_of_sales` DESC, s.`category`
""".strip(),
    },
    {
        "question": "Which categories had the worst Q4 2025 stockout health?",
        "difficulty": "MEDIUM",
        "sql": """
SELECT
  p.`category`,
  COUNT(*) AS `snapshot_count`,
  SUM(CASE WHEN i.`stockout_days` >= 7 THEN 1 ELSE 0 END) AS `severe_stockout_snapshot_count`,
  ROUND(SUM(CASE WHEN i.`stockout_days` >= 7 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS `severe_stockout_snapshot_rate_pct`,
  SUM(i.`stockout_days`) AS `stockout_days`,
  ROUND(SUM(i.`lost_sales_estimate_units`), 2) AS `lost_sales_estimate_units`,
  ROUND(SUM(i.`inventory_value_usd`), 2) AS `inventory_value_usd`
FROM `{catalog}`.`{schema}`.`inventory_snapshots` i
JOIN `{catalog}`.`{schema}`.`products` p
  ON i.`product_id` = p.`product_id`
WHERE i.`snapshot_year` = 2025
  AND i.`snapshot_month_num` IN (10, 11, 12)
GROUP BY p.`category`
ORDER BY `severe_stockout_snapshot_rate_pct` DESC, `stockout_days` DESC, p.`category`
""".strip(),
    },
    {
        "question": "Which store types carried the most inventory value in 2025?",
        "difficulty": "MEDIUM",
        "sql": """
SELECT
  st.`store_type`,
  COUNT(*) AS `snapshot_count`,
  ROUND(AVG(i.`inventory_value_usd`), 2) AS `avg_inventory_value_per_snapshot_usd`,
  ROUND(SUM(i.`inventory_value_usd`), 2) AS `total_inventory_value_usd`,
  SUM(i.`on_hand_units`) AS `on_hand_units`
FROM `{catalog}`.`{schema}`.`inventory_snapshots` i
JOIN `{catalog}`.`{schema}`.`stores` st
  ON i.`store_id` = st.`store_id`
WHERE i.`snapshot_year` = 2025
GROUP BY st.`store_type`
ORDER BY `total_inventory_value_usd` DESC, st.`store_type`
""".strip(),
    },
    {
        "question": "Which acquisition channels generated the most revenue from customers acquired before 2022?",
        "difficulty": "MEDIUM",
        "sql": """
SELECT
  c.`acquisition_channel`,
  COUNT(*) AS `order_count`,
  COUNT(DISTINCT s.`customer_id`) AS `buying_customer_count`,
  ROUND(SUM(s.`net_sales_usd`), 2) AS `net_sales_usd`,
  ROUND(AVG(s.`net_sales_usd`), 2) AS `avg_order_value_usd`
FROM `{catalog}`.`{schema}`.`sales` s
JOIN `{catalog}`.`{schema}`.`customers` c
  ON s.`customer_id` = c.`customer_id`
WHERE c.`customer_since_date` < DATE '2022-01-01'
GROUP BY c.`acquisition_channel`
ORDER BY `net_sales_usd` DESC, c.`acquisition_channel`
""".strip(),
    },
    {
        "question": "For online returns, which reasons are most common within each product category?",
        "difficulty": "MEDIUM",
        "sql": """
SELECT
  p.`category`,
  r.`return_reason`,
  COUNT(*) AS `return_count`,
  SUM(r.`return_quantity`) AS `returned_units`,
  ROUND(SUM(r.`return_amount_usd`), 2) AS `return_amount_usd`,
  ROUND(AVG(r.`days_to_return`), 1) AS `avg_days_to_return`
FROM `{catalog}`.`{schema}`.`returns` r
JOIN `{catalog}`.`{schema}`.`products` p
  ON r.`product_id` = p.`product_id`
WHERE r.`channel` = 'Online'
GROUP BY p.`category`, r.`return_reason`
ORDER BY p.`category`, `return_count` DESC, r.`return_reason`
""".strip(),
    },
    {
        "question": "What was the monthly return rate for sales made in 2025?",
        "difficulty": "MEDIUM",
        "sql": """
SELECT
  CAST(DATE_TRUNC('MONTH', s.`sale_date`) AS DATE) AS `sale_month`,
  COUNT(DISTINCT s.`sale_id`) AS `order_count`,
  COUNT(DISTINCT r.`return_id`) AS `returned_order_count`,
  ROUND(COUNT(DISTINCT r.`return_id`) * 100.0 / COUNT(DISTINCT s.`sale_id`), 2) AS `return_order_rate_pct`,
  ROUND(SUM(s.`net_sales_usd`), 2) AS `net_sales_usd`,
  ROUND(SUM(COALESCE(r.`return_amount_usd`, 0)), 2) AS `return_amount_usd`
FROM `{catalog}`.`{schema}`.`sales` s
LEFT JOIN `{catalog}`.`{schema}`.`returns` r
  ON s.`sale_id` = r.`sale_id`
WHERE s.`sale_year` = 2025
GROUP BY CAST(DATE_TRUNC('MONTH', s.`sale_date`) AS DATE)
ORDER BY `sale_month`
""".strip(),
    },
    {
        "question": "Which brand lines delivered the highest gross margin percent in 2025?",
        "difficulty": "MEDIUM",
        "sql": """
SELECT
  p.`brand_line`,
  COUNT(*) AS `order_count`,
  SUM(s.`quantity`) AS `units_sold`,
  ROUND(SUM(s.`net_sales_usd`), 2) AS `net_sales_usd`,
  ROUND(SUM(s.`gross_margin_usd`), 2) AS `gross_margin_usd`,
  ROUND(SUM(s.`gross_margin_usd`) * 100.0 / SUM(s.`net_sales_usd`), 2) AS `gross_margin_pct`
FROM `{catalog}`.`{schema}`.`sales` s
JOIN `{catalog}`.`{schema}`.`products` p
  ON s.`product_id` = p.`product_id`
WHERE s.`sale_year` = 2025
GROUP BY p.`brand_line`
HAVING SUM(s.`net_sales_usd`) > 0
ORDER BY `gross_margin_pct` DESC, p.`brand_line`
""".strip(),
    },
    {
        "question": "Which product categories had the strongest net sales growth from 2024 to 2025?",
        "difficulty": "HARD",
        "sql": """
WITH category_year_sales AS (
  SELECT
    p.`category`,
    s.`sale_year`,
    SUM(s.`net_sales_usd`) AS `net_sales_usd`
  FROM `{catalog}`.`{schema}`.`sales` s
  JOIN `{catalog}`.`{schema}`.`products` p
    ON s.`product_id` = p.`product_id`
  WHERE s.`sale_year` IN (2024, 2025)
  GROUP BY p.`category`, s.`sale_year`
),
pivoted AS (
  SELECT
    `category`,
    SUM(CASE WHEN `sale_year` = 2024 THEN `net_sales_usd` ELSE 0 END) AS `net_sales_2024_usd`,
    SUM(CASE WHEN `sale_year` = 2025 THEN `net_sales_usd` ELSE 0 END) AS `net_sales_2025_usd`
  FROM category_year_sales
  GROUP BY `category`
)
SELECT
  `category`,
  ROUND(`net_sales_2024_usd`, 2) AS `net_sales_2024_usd`,
  ROUND(`net_sales_2025_usd`, 2) AS `net_sales_2025_usd`,
  ROUND(`net_sales_2025_usd` - `net_sales_2024_usd`, 2) AS `net_sales_growth_usd`,
  ROUND((`net_sales_2025_usd` - `net_sales_2024_usd`) * 100.0 / `net_sales_2024_usd`, 2) AS `yoy_growth_pct`
FROM pivoted
WHERE `net_sales_2024_usd` > 0
ORDER BY `yoy_growth_pct` DESC, `category`
""".strip(),
    },
    {
        "question": "For each region, which store had the highest 2025 net sales?",
        "difficulty": "HARD",
        "sql": """
WITH store_sales AS (
  SELECT
    st.`region`,
    st.`store_id`,
    st.`store_name`,
    st.`store_type`,
    SUM(s.`quantity`) AS `units_sold`,
    ROUND(SUM(s.`net_sales_usd`), 2) AS `net_sales_usd`,
    DENSE_RANK() OVER (PARTITION BY st.`region` ORDER BY SUM(s.`net_sales_usd`) DESC, st.`store_id`) AS `revenue_rank`
  FROM `{catalog}`.`{schema}`.`sales` s
  JOIN `{catalog}`.`{schema}`.`stores` st
    ON s.`store_id` = st.`store_id`
  WHERE s.`sale_year` = 2025
  GROUP BY st.`region`, st.`store_id`, st.`store_name`, st.`store_type`
)
SELECT
  `region`,
  `store_id`,
  `store_name`,
  `store_type`,
  `units_sold`,
  `net_sales_usd`
FROM store_sales
WHERE `revenue_rank` = 1
ORDER BY `region`, `store_id`
""".strip(),
    },
    {
        "question": "Who are the top 10 customers by lifetime net sales, and what percent of their spend was returned?",
        "difficulty": "HARD",
        "sql": """
WITH sales_by_customer AS (
  SELECT
    `customer_id`,
    COUNT(*) AS `order_count`,
    ROUND(SUM(`net_sales_usd`), 2) AS `net_sales_usd`
  FROM `{catalog}`.`{schema}`.`sales`
  GROUP BY `customer_id`
),
returns_by_customer AS (
  SELECT
    `customer_id`,
    COUNT(*) AS `return_count`,
    ROUND(SUM(`return_amount_usd`), 2) AS `return_amount_usd`
  FROM `{catalog}`.`{schema}`.`returns`
  GROUP BY `customer_id`
),
ranked_customers AS (
  SELECT
    c.`customer_id`,
    c.`customer_name`,
    c.`loyalty_tier`,
    s.`order_count`,
    s.`net_sales_usd`,
    COALESCE(r.`return_count`, 0) AS `return_count`,
    COALESCE(r.`return_amount_usd`, 0) AS `return_amount_usd`,
    ROUND(COALESCE(r.`return_amount_usd`, 0) * 100.0 / s.`net_sales_usd`, 2) AS `returned_spend_pct`,
    DENSE_RANK() OVER (ORDER BY s.`net_sales_usd` DESC, c.`customer_id`) AS `customer_rank`
  FROM sales_by_customer s
  JOIN `{catalog}`.`{schema}`.`customers` c
    ON s.`customer_id` = c.`customer_id`
  LEFT JOIN returns_by_customer r
    ON s.`customer_id` = r.`customer_id`
)
SELECT
  `customer_rank`,
  `customer_id`,
  `customer_name`,
  `loyalty_tier`,
  `order_count`,
  `net_sales_usd`,
  `return_count`,
  `return_amount_usd`,
  `returned_spend_pct`
FROM ranked_customers
WHERE `customer_rank` <= 10
ORDER BY `customer_rank`, `customer_id`
""".strip(),
    },
    {
        "question": "Which product-store pairs had repeated severe stockouts in 2025 and the most sales anyway?",
        "difficulty": "HARD",
        "sql": """
WITH severe_stockouts AS (
  SELECT
    `product_id`,
    `store_id`,
    COUNT(*) AS `severe_stockout_months`,
    SUM(`stockout_days`) AS `stockout_days`,
    ROUND(SUM(`lost_sales_estimate_units`), 2) AS `lost_sales_estimate_units`
  FROM `{catalog}`.`{schema}`.`inventory_snapshots`
  WHERE `snapshot_year` = 2025
    AND `stockout_days` >= 7
  GROUP BY `product_id`, `store_id`
  HAVING COUNT(*) >= 3
),
sales_2025 AS (
  SELECT
    `product_id`,
    `store_id`,
    SUM(`quantity`) AS `units_sold`,
    ROUND(SUM(`net_sales_usd`), 2) AS `net_sales_usd`
  FROM `{catalog}`.`{schema}`.`sales`
  WHERE `sale_year` = 2025
  GROUP BY `product_id`, `store_id`
)
SELECT
  p.`product_id`,
  p.`product_name`,
  p.`category`,
  st.`store_id`,
  st.`store_name`,
  st.`store_type`,
  ss.`severe_stockout_months`,
  ss.`stockout_days`,
  ss.`lost_sales_estimate_units`,
  COALESCE(s.`units_sold`, 0) AS `units_sold`,
  COALESCE(s.`net_sales_usd`, 0) AS `net_sales_usd`
FROM severe_stockouts ss
JOIN `{catalog}`.`{schema}`.`products` p
  ON ss.`product_id` = p.`product_id`
JOIN `{catalog}`.`{schema}`.`stores` st
  ON ss.`store_id` = st.`store_id`
LEFT JOIN sales_2025 s
  ON ss.`product_id` = s.`product_id`
 AND ss.`store_id` = s.`store_id`
ORDER BY ss.`severe_stockout_months` DESC, `net_sales_usd` DESC, p.`product_id`, st.`store_id`
LIMIT 15
""".strip(),
    },
    {
        "question": "For each loyalty tier, what share of 2024 buyers purchased again in 2025?",
        "difficulty": "HARD",
        "sql": """
WITH buyers_2024 AS (
  SELECT DISTINCT
    `customer_id`,
    `loyalty_tier`
  FROM `{catalog}`.`{schema}`.`sales`
  WHERE `sale_year` = 2024
),
buyers_2025 AS (
  SELECT DISTINCT
    `customer_id`
  FROM `{catalog}`.`{schema}`.`sales`
  WHERE `sale_year` = 2025
)
SELECT
  b24.`loyalty_tier`,
  COUNT(*) AS `buyers_2024`,
  SUM(CASE WHEN b25.`customer_id` IS NOT NULL THEN 1 ELSE 0 END) AS `retained_buyers_2025`,
  ROUND(SUM(CASE WHEN b25.`customer_id` IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS `retention_rate_pct`
FROM buyers_2024 b24
LEFT JOIN buyers_2025 b25
  ON b24.`customer_id` = b25.`customer_id`
GROUP BY b24.`loyalty_tier`
ORDER BY `retention_rate_pct` DESC, b24.`loyalty_tier`
""".strip(),
    },
    {
        "question": "Which categories are above average on both return rate and discount rate?",
        "difficulty": "HARD",
        "sql": """
WITH category_metrics AS (
  SELECT
    p.`category`,
    COUNT(DISTINCT s.`sale_id`) AS `order_count`,
    COUNT(DISTINCT r.`return_id`) AS `returned_order_count`,
    ROUND(COUNT(DISTINCT r.`return_id`) * 100.0 / COUNT(DISTINCT s.`sale_id`), 2) AS `return_order_rate_pct`,
    ROUND(AVG(s.`discount_pct`), 2) AS `avg_discount_pct`
  FROM `{catalog}`.`{schema}`.`sales` s
  JOIN `{catalog}`.`{schema}`.`products` p
    ON s.`product_id` = p.`product_id`
  LEFT JOIN `{catalog}`.`{schema}`.`returns` r
    ON s.`sale_id` = r.`sale_id`
  GROUP BY p.`category`
),
overall_metrics AS (
  SELECT
    ROUND(COUNT(DISTINCT s.`sale_id`) * 1.0, 2) AS `overall_order_count`,
    ROUND(COUNT(DISTINCT r.`return_id`) * 100.0 / COUNT(DISTINCT s.`sale_id`), 2) AS `overall_return_order_rate_pct`,
    ROUND(AVG(s.`discount_pct`), 2) AS `overall_avg_discount_pct`
  FROM `{catalog}`.`{schema}`.`sales` s
  LEFT JOIN `{catalog}`.`{schema}`.`returns` r
    ON s.`sale_id` = r.`sale_id`
)
SELECT
  cm.`category`,
  cm.`order_count`,
  cm.`returned_order_count`,
  cm.`return_order_rate_pct`,
  om.`overall_return_order_rate_pct`,
  cm.`avg_discount_pct`,
  om.`overall_avg_discount_pct`,
  CASE
    WHEN cm.`return_order_rate_pct` > om.`overall_return_order_rate_pct`
     AND cm.`avg_discount_pct` > om.`overall_avg_discount_pct`
    THEN true ELSE false
  END AS `above_average_return_and_discount`
FROM category_metrics cm
CROSS JOIN overall_metrics om
ORDER BY `above_average_return_and_discount` DESC, cm.`return_order_rate_pct` DESC, cm.`category`
""".strip(),
    },
    {
        "question": "For each product season, which 2025 month was the sales peak and how much of the season's sales did it represent?",
        "difficulty": "HARD",
        "sql": """
WITH season_month_sales AS (
  SELECT
    p.`season`,
    s.`sale_month`,
    ROUND(SUM(s.`net_sales_usd`), 2) AS `net_sales_usd`
  FROM `{catalog}`.`{schema}`.`sales` s
  JOIN `{catalog}`.`{schema}`.`products` p
    ON s.`product_id` = p.`product_id`
  WHERE s.`sale_year` = 2025
  GROUP BY p.`season`, s.`sale_month`
),
ranked_months AS (
  SELECT
    `season`,
    `sale_month`,
    `net_sales_usd`,
    SUM(`net_sales_usd`) OVER (PARTITION BY `season`) AS `season_net_sales_usd`,
    DENSE_RANK() OVER (PARTITION BY `season` ORDER BY `net_sales_usd` DESC, `sale_month`) AS `sales_rank`
  FROM season_month_sales
)
SELECT
  `season`,
  `sale_month` AS `peak_sale_month`,
  `net_sales_usd` AS `peak_month_net_sales_usd`,
  ROUND(`season_net_sales_usd`, 2) AS `season_net_sales_usd`,
  ROUND(`net_sales_usd` * 100.0 / `season_net_sales_usd`, 2) AS `peak_month_share_pct`
FROM ranked_months
WHERE `sales_rank` = 1
ORDER BY `season`
""".strip(),
    },
    {
        "question": "Which physical stores produced the most 2025 gross margin per annual rent dollar, and what was their stockout burden?",
        "difficulty": "HARD",
        "sql": """
WITH store_sales AS (
  SELECT
    st.`store_id`,
    st.`store_name`,
    st.`region`,
    st.`store_type`,
    st.`monthly_rent_usd`,
    ROUND(SUM(s.`net_sales_usd`), 2) AS `net_sales_usd`,
    ROUND(SUM(s.`gross_margin_usd`), 2) AS `gross_margin_usd`
  FROM `{catalog}`.`{schema}`.`sales` s
  JOIN `{catalog}`.`{schema}`.`stores` st
    ON s.`store_id` = st.`store_id`
  WHERE s.`sale_year` = 2025
    AND st.`store_type` <> 'Ecommerce'
    AND st.`monthly_rent_usd` > 0
  GROUP BY st.`store_id`, st.`store_name`, st.`region`, st.`store_type`, st.`monthly_rent_usd`
),
inventory_burden AS (
  SELECT
    `store_id`,
    SUM(`stockout_days`) AS `stockout_days`,
    ROUND(SUM(`lost_sales_estimate_units`), 2) AS `lost_sales_estimate_units`
  FROM `{catalog}`.`{schema}`.`inventory_snapshots`
  WHERE `snapshot_year` = 2025
  GROUP BY `store_id`
)
SELECT
  ss.`store_id`,
  ss.`store_name`,
  ss.`region`,
  ss.`store_type`,
  ss.`net_sales_usd`,
  ss.`gross_margin_usd`,
  ss.`monthly_rent_usd`,
  ROUND(ss.`gross_margin_usd` / (ss.`monthly_rent_usd` * 12), 2) AS `gross_margin_per_annual_rent_dollar`,
  ib.`stockout_days`,
  ib.`lost_sales_estimate_units`
FROM store_sales ss
LEFT JOIN inventory_burden ib
  ON ss.`store_id` = ib.`store_id`
ORDER BY `gross_margin_per_annual_rent_dollar` DESC, ss.`store_id`
""".strip(),
    },
]

assert len(BENCHMARKS) == 30, f"Expected 30 benchmarks, found {len(BENCHMARKS)}"
assert {b["difficulty"] for b in BENCHMARKS} <= {"EASY", "MEDIUM", "HARD"}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate SQL

# COMMAND ----------

if run_sql_validation:
    validation_results = []
    failures = []

    for idx, benchmark in enumerate(BENCHMARKS, start=1):
        rendered_sql = benchmark["sql"].format(catalog=catalog, schema=schema)
        try:
            rows = spark.sql(rendered_sql).collect()
            validation_results.append(
                {
                    "question_number": idx,
                    "difficulty": benchmark["difficulty"],
                    "status": "PASS",
                    "row_count": len(rows),
                    "question": benchmark["question"],
                }
            )
        except Exception as exc:
            failures.append((idx, benchmark["question"], rendered_sql, str(exc)))
            validation_results.append(
                {
                    "question_number": idx,
                    "difficulty": benchmark["difficulty"],
                    "status": "FAIL",
                    "row_count": None,
                    "question": benchmark["question"],
                }
            )

    spark.createDataFrame(validation_results).orderBy("question_number").show(30, truncate=False)

    if failures:
        failure_text = "\n\n".join(
            f"Question {idx}: {question}\nError: {error}\nSQL:\n{sql}"
            for idx, question, sql, error in failures
        )
        raise RuntimeError(f"SQL validation failed for {len(failures)} benchmark(s):\n{failure_text}")

    print(f"Validated {len(validation_results)} benchmark SQL statements against `{catalog}`.`{schema}`.")
else:
    print("Skipping SQL validation because run_sql_validation=false.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch Genie Space

# COMMAND ----------

resp = w.api_client.do(
    "GET",
    f"/api/2.0/genie/spaces/{space_id}",
    query={"include_serialized_space": "true"},
)
serialized = json.loads(resp["serialized_space"])

pre_data_sources = copy.deepcopy(serialized.get("data_sources"))
pre_instructions = copy.deepcopy(serialized.get("instructions"))
pre_version = copy.deepcopy(serialized.get("version"))

print(f"Fetched Genie Space {space_id}: {resp.get('title', '<untitled>')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Replace Benchmark Questions

# COMMAND ----------

questions = []
for benchmark in BENCHMARKS:
    sql = benchmark["sql"].format(catalog=catalog, schema=schema)
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

body = {"serialized_space": json.dumps(serialized)}
for key in ("title", "description", "warehouse_id"):
    if resp.get(key) is not None:
        body[key] = resp[key]

w.api_client.do("PATCH", f"/api/2.0/genie/spaces/{space_id}", body=body)

print(f"Patched Genie Space {space_id} with {len(questions)} benchmark questions.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Round-Trip Verify

# COMMAND ----------

verify_resp = w.api_client.do(
    "GET",
    f"/api/2.0/genie/spaces/{space_id}",
    query={"include_serialized_space": "true"},
)
verify_serialized = json.loads(verify_resp["serialized_space"])

actual_questions = verify_serialized.get("benchmarks", {}).get("questions", [])
actual_question_text = [q["question"][0] for q in actual_questions]
expected_question_text = [b["question"] for b in BENCHMARKS]

assert len(actual_questions) == 30, f"Expected 30 benchmark questions, found {len(actual_questions)}"
assert actual_question_text == expected_question_text, "Round-trip question text does not match BENCHMARKS."
assert verify_serialized.get("data_sources") == pre_data_sources, "data_sources changed unexpectedly."
assert verify_serialized.get("instructions") == pre_instructions, "instructions changed unexpectedly."
assert verify_serialized.get("version") == pre_version, "version changed unexpectedly."

print("Round-trip verification succeeded.")
print(f"  Genie Space ID: {space_id}")
print(f"  Catalog/schema: `{catalog}`.`{schema}`")
print(f"  Benchmark questions: {len(actual_questions)}")
print("  Verified unchanged: data_sources, instructions, version")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional Conversation Spot Check

# COMMAND ----------

def _message_text(message):
    for key in ("content", "text", "answer"):
        value = message.get(key)
        if isinstance(value, str):
            return value
        if isinstance(value, list):
            return " ".join(str(item) for item in value)
    attachments = message.get("attachments") or []
    return json.dumps(attachments)[:2000]


if run_conversation_check:
    sample = BENCHMARKS[:3]
    for benchmark in sample:
        start = w.api_client.do(
            "POST",
            f"/api/2.0/genie/spaces/{space_id}/start-conversation",
            body={"content": benchmark["question"]},
        )
        conversation_id = start.get("conversation_id") or start.get("conversation", {}).get("id")
        message_id = start.get("message_id") or start.get("message", {}).get("id")

        if not conversation_id or not message_id:
            print(f"Started conversation for: {benchmark['question']}")
            print(json.dumps(start, indent=2)[:2000])
            continue

        message = {}
        for _ in range(30):
            message = w.api_client.do(
                "GET",
                f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}",
            )
            status = str(message.get("status", "")).upper()
            if status in {"COMPLETED", "FAILED", "CANCELLED"}:
                break
            time.sleep(2)

        print("=" * 80)
        print(benchmark["question"])
        print(_message_text(message))
else:
    print("Skipping conversation spot check because run_conversation_check=false.")

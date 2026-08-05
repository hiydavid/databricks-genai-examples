# Databricks notebook source

# MAGIC %md
# MAGIC # Bigly Bank — Finance and Treasury
# MAGIC
# MAGIC Generates reconciled monthly profitability, funds-transfer pricing,
# MAGIC provisions, liquidity, and general-ledger summaries in FINANCE.

# COMMAND ----------

from datetime import date
import json

from pyspark.sql import Window
from pyspark.sql import functions as F

try:
    dbutils.widgets.text("catalog", "", "Unity Catalog (required)")
except Exception:
    pass
try:
    dbutils.widgets.text("schema_prefix", "", "Schema prefix (required)")
except Exception:
    pass
try:
    dbutils.widgets.text("seed", "42", "Deterministic seed")
except Exception:
    pass
try:
    dbutils.widgets.text("as_of_date", "2025-12-31", "Inclusive as-of date")
except Exception:
    pass

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA_PREFIX = dbutils.widgets.get("schema_prefix").strip()
SEED = int(dbutils.widgets.get("seed"))
AS_OF_DATE = date.fromisoformat(dbutils.widgets.get("as_of_date").strip())

if not CATALOG:
    raise ValueError("catalog is required")
if not SCHEMA_PREFIX:
    raise ValueError("schema_prefix is required")
if "`" in CATALOG or "`" in SCHEMA_PREFIX:
    raise ValueError("catalog and schema_prefix cannot contain backticks")

CORE_SCHEMA = f"{SCHEMA_PREFIX}_core"
RETAIL_SCHEMA = f"{SCHEMA_PREFIX}_retail"
COMMERCIAL_SCHEMA = f"{SCHEMA_PREFIX}_commercial"
WEALTH_SCHEMA = f"{SCHEMA_PREFIX}_wealth"
OPERATIONS_SCHEMA = f"{SCHEMA_PREFIX}_operations"
RISK_SCHEMA = f"{SCHEMA_PREFIX}_risk"
FINANCE_SCHEMA = f"{SCHEMA_PREFIX}_finance"
CORE = f"`{CATALOG}`.`{CORE_SCHEMA}`"
RETAIL = f"`{CATALOG}`.`{RETAIL_SCHEMA}`"
COMMERCIAL = f"`{CATALOG}`.`{COMMERCIAL_SCHEMA}`"
WEALTH = f"`{CATALOG}`.`{WEALTH_SCHEMA}`"
OPERATIONS = f"`{CATALOG}`.`{OPERATIONS_SCHEMA}`"
RISK = f"`{CATALOG}`.`{RISK_SCHEMA}`"
FINANCE = f"`{CATALOG}`.`{FINANCE_SCHEMA}`"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FINANCE}")


def write_table(df, table_name, comment):
    full_name = f"{FINANCE}.`{table_name}`"
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_name)
    )
    spark.sql(f"COMMENT ON TABLE {full_name} IS '{comment.replace(chr(39), chr(39) * 2)}'")
    print(f"Wrote {full_name}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Funds-transfer pricing

# COMMAND ----------

products = spark.table(f"{CORE}.`products`")
months = spark.table(f"{CORE}.`bank_calendar`").select("month_start").distinct()

funds_transfer_pricing = (
    products.crossJoin(months)
    .withColumn(
        "benchmark_rate_pct",
        F.round(
            F.when(F.year("month_start") == 2023, 4.25)
            .when(F.year("month_start") == 2024, 5.10)
            .otherwise(4.35)
            + (F.month("month_start") - 1) * 0.015,
            3,
        ),
    )
    .withColumn(
        "liquidity_spread_pct",
        F.when(F.col("product_category") == "Deposit", -0.65)
        .when(F.col("product_category").isin("Consumer Lending", "Commercial Lending"), 0.85)
        .otherwise(0.20),
    )
    .withColumn("transfer_rate_pct", F.round(F.col("benchmark_rate_pct") + F.col("liquidity_spread_pct"), 3))
    .withColumn("effective_date", F.col("month_start"))
    .withColumn("ftp_record_id", F.concat(F.lit("FTP-"), F.regexp_replace("product_id", "PRD-", ""), F.lit("-"), F.date_format("month_start", "yyyyMM")))
    .select("ftp_record_id", "effective_date", "month_start", "product_id", "business_line", "product_category", "benchmark_rate_pct", "liquidity_spread_pct", "transfer_rate_pct")
)

write_table(funds_transfer_pricing, "funds_transfer_pricing", "Monthly product-level benchmark and funds-transfer pricing rates.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Credit-loss provisions

# COMMAND ----------

consumer_provisions = (
    spark.table(f"{RETAIL}.`delinquency_snapshots`")
    .withColumn(
        "provision_rate_pct",
        F.when(F.col("days_past_due") >= 90, 45.0)
        .when(F.col("days_past_due") >= 60, 20.0)
        .when(F.col("days_past_due") >= 30, 8.0)
        .otherwise(0.8),
    )
    .withColumn("provision_amount_usd", F.round(F.col("outstanding_principal_usd") * F.col("provision_rate_pct") / 100.0, 2))
    .select("month_start", "product_id", F.lit("Consumer Lending").alias("portfolio"), "outstanding_principal_usd", "provision_rate_pct", "provision_amount_usd", "region")
)

commercial_provisions = (
    spark.table(f"{COMMERCIAL}.`covenant_snapshots`")
    .join(spark.table(f"{COMMERCIAL}.`credit_facilities`").select("facility_id", "product_id", "outstanding_amount_usd"), "facility_id")
    .withColumn("provision_rate_pct", F.when(F.col("is_in_breach"), 12.0).otherwise(1.5))
    .withColumn("provision_amount_usd", F.round(F.col("outstanding_amount_usd") * F.col("provision_rate_pct") / 100.0, 2))
    .select("month_start", "product_id", F.lit("Commercial Banking").alias("portfolio"), F.col("outstanding_amount_usd").alias("outstanding_principal_usd"), "provision_rate_pct", "provision_amount_usd", "region")
)

credit_loss_provisions = (
    consumer_provisions.unionByName(commercial_provisions)
    .groupBy("month_start", "product_id", "portfolio", "region")
    .agg(
        F.sum("outstanding_principal_usd").alias("exposure_usd"),
        F.sum("provision_amount_usd").alias("provision_amount_usd"),
    )
    .withColumn("effective_provision_rate_pct", F.round(F.col("provision_amount_usd") * 100.0 / F.col("exposure_usd"), 3))
    .withColumn("provision_id", F.concat(F.lit("PRV-"), F.regexp_replace("product_id", "PRD-", ""), F.lit("-"), F.regexp_replace("region", " ", ""), F.lit("-"), F.date_format("month_start", "yyyyMM")))
    .select("provision_id", "month_start", "product_id", "portfolio", "region", "exposure_usd", "provision_amount_usd", "effective_provision_rate_pct")
)

write_table(credit_loss_provisions, "credit_loss_provisions", "Monthly expected-credit-loss provisions from consumer delinquency and commercial covenant risk.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Product profitability

# COMMAND ----------

deposit_profitability = (
    spark.table(f"{RETAIL}.`deposit_balance_snapshots`")
    .groupBy("month_start", "product_id")
    .agg(
        F.sum("average_balance_usd").alias("average_balance_usd"),
        F.sum("fee_revenue_usd").alias("fee_revenue_usd"),
        F.countDistinct("account_id").alias("account_count"),
    )
    .withColumn("interest_revenue_usd", F.col("average_balance_usd") * 0.0025)
    .withColumn("direct_cost_usd", F.col("account_count") * 3.5)
    .withColumn("credit_loss_usd", F.lit(0.0))
)

card_profitability = (
    spark.table(f"{RETAIL}.`card_statements`")
    .groupBy("month_start", "product_id")
    .agg(
        F.sum("statement_balance_usd").alias("average_balance_usd"),
        F.sum("fees_usd").alias("fee_revenue_usd"),
        F.countDistinct("account_id").alias("account_count"),
    )
    .withColumn("interest_revenue_usd", F.col("average_balance_usd") * 0.015)
    .withColumn("direct_cost_usd", F.col("account_count") * 5.0)
    .withColumn("credit_loss_usd", F.col("average_balance_usd") * 0.006)
)

loan_profitability = (
    spark.table(f"{RETAIL}.`delinquency_snapshots`")
    .groupBy("month_start", "product_id")
    .agg(
        F.sum("outstanding_principal_usd").alias("average_balance_usd"),
        F.countDistinct("loan_id").alias("account_count"),
    )
    .withColumn("fee_revenue_usd", F.lit(0.0))
    .withColumn("interest_revenue_usd", F.col("average_balance_usd") * 0.0065)
    .withColumn("direct_cost_usd", F.col("account_count") * 8.0)
    .withColumn("credit_loss_usd", F.col("average_balance_usd") * 0.0015)
)

wealth_profitability = (
    spark.table(f"{WEALTH}.`advisory_fees`")
    .join(spark.table(f"{WEALTH}.`wealth_accounts`").select("wealth_account_id", "product_id"), "wealth_account_id")
    .groupBy("month_start", "product_id")
    .agg(
        F.sum("assets_under_management_usd").alias("average_balance_usd"),
        F.sum("fee_amount_usd").alias("fee_revenue_usd"),
        F.countDistinct("wealth_account_id").alias("account_count"),
    )
    .withColumn("interest_revenue_usd", F.lit(0.0))
    .withColumn("direct_cost_usd", F.col("account_count") * 18.0)
    .withColumn("credit_loss_usd", F.lit(0.0))
)

commercial_loan_profitability = (
    spark.table(f"{COMMERCIAL}.`commercial_loans`")
    .crossJoin(months)
    .filter(F.col("month_start") >= F.trunc("origination_date", "month"))
    .filter(F.col("month_start") <= F.trunc(F.least(F.lit(AS_OF_DATE), "maturity_date"), "month"))
    .groupBy("month_start", "product_id")
    .agg(
        F.sum("outstanding_principal_usd").alias("average_balance_usd"),
        F.countDistinct("loan_id").alias("account_count"),
    )
    .withColumn("fee_revenue_usd", F.lit(0.0))
    .withColumn("interest_revenue_usd", F.col("average_balance_usd") * 0.0068)
    .withColumn("direct_cost_usd", F.col("account_count") * 30.0)
    .withColumn("credit_loss_usd", F.col("average_balance_usd") * 0.0020)
)

profitability_inputs = (
    deposit_profitability.unionByName(card_profitability)
    .unionByName(loan_profitability)
    .unionByName(wealth_profitability)
    .unionByName(commercial_loan_profitability)
)

product_profitability_monthly = (
    profitability_inputs.groupBy("month_start", "product_id")
    .agg(
        F.sum("average_balance_usd").alias("average_balance_usd"),
        F.sum("fee_revenue_usd").alias("fee_revenue_usd"),
        F.sum("interest_revenue_usd").alias("interest_revenue_usd"),
        F.sum("direct_cost_usd").alias("direct_cost_usd"),
        F.sum("credit_loss_usd").alias("credit_loss_usd"),
        F.sum("account_count").alias("account_count"),
    )
    .join(products.select("product_id", "product_name", "business_line", "product_category"), "product_id")
    .withColumn("total_revenue_usd", F.round(F.col("fee_revenue_usd") + F.col("interest_revenue_usd"), 2))
    .withColumn("net_income_usd", F.round(F.col("total_revenue_usd") - F.col("direct_cost_usd") - F.col("credit_loss_usd"), 2))
    .withColumn("profitability_id", F.concat(F.lit("PFT-"), F.regexp_replace("product_id", "PRD-", ""), F.lit("-"), F.date_format("month_start", "yyyyMM")))
    .select("profitability_id", "month_start", "product_id", "product_name", "business_line", "product_category", "account_count", "average_balance_usd", "fee_revenue_usd", "interest_revenue_usd", "total_revenue_usd", "direct_cost_usd", "credit_loss_usd", "net_income_usd")
)

write_table(product_profitability_monthly, "product_profitability_monthly", "Monthly product profitability with revenue, direct cost, credit loss, and net income.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## General ledger and liquidity

# COMMAND ----------

product_ledger_entries = (
    spark.table(f"{FINANCE}.`product_profitability_monthly`")
    .groupBy("month_start", "business_line")
    .agg(
        F.sum("interest_revenue_usd").alias("interest_income_usd"),
        F.sum("fee_revenue_usd").alias("fee_income_usd"),
        F.sum("direct_cost_usd").alias("operating_expense_usd"),
        F.sum("credit_loss_usd").alias("credit_loss_expense_usd"),
    )
    .select(
        "month_start",
        "business_line",
        F.explode(
            F.array(
                F.struct(F.lit("Interest Income").alias("account_name"), F.col("interest_income_usd").alias("amount_usd")),
                F.struct(F.lit("Fee Income").alias("account_name"), F.col("fee_income_usd").alias("amount_usd")),
                F.struct(F.lit("Operating Expense").alias("account_name"), (-F.col("operating_expense_usd")).alias("amount_usd")),
                F.struct(F.lit("Credit Loss Expense").alias("account_name"), (-F.col("credit_loss_expense_usd")).alias("amount_usd")),
            )
        ).alias("entry"),
    )
    .select("month_start", "business_line", F.col("entry.account_name").alias("account_name"), F.col("entry.amount_usd").alias("amount_usd"))
)

branch_cost_entries = (
    spark.table(f"{OPERATIONS}.`branch_monthly_performance`")
    .groupBy("month_start")
    .agg((-F.sum("total_operating_cost_usd")).alias("amount_usd"))
    .withColumn("business_line", F.lit("Enterprise Operations"))
    .withColumn("account_name", F.lit("Branch Operating Expense"))
    .select("month_start", "business_line", "account_name", "amount_usd")
)

fraud_loss_entries = (
    spark.table(f"{RISK}.`fraud_loss_events`")
    .withColumn("month_start", F.trunc("loss_date", "month"))
    .groupBy("month_start")
    .agg((-F.sum("net_loss_usd")).alias("amount_usd"))
    .withColumn("business_line", F.lit("Retail"))
    .withColumn("account_name", F.lit("Fraud Loss Expense"))
    .select("month_start", "business_line", "account_name", "amount_usd")
)

general_ledger_monthly = (
    product_ledger_entries.unionByName(branch_cost_entries)
    .unionByName(fraud_loss_entries)
    .withColumn("ledger_entry_id", F.concat(F.lit("GL-"), F.regexp_replace("business_line", " ", ""), F.lit("-"), F.regexp_replace("account_name", " ", ""), F.lit("-"), F.date_format("month_start", "yyyyMM")))
    .select("ledger_entry_id", "month_start", "business_line", "account_name", "amount_usd")
)

write_table(general_ledger_monthly, "general_ledger_monthly", "Monthly synthetic general-ledger summaries by business line and account.")

retail_liquidity = spark.table(f"{RETAIL}.`deposit_balance_snapshots`").groupBy("month_start").agg(F.sum("ending_balance_usd").alias("retail_deposits_usd"))

commercial_monthly_flow = (
    spark.table(f"{COMMERCIAL}.`commercial_transactions`")
    .withColumn("month_start", F.trunc("transaction_date", "month"))
    .groupBy("month_start", "account_id")
    .agg(F.sum("signed_amount_usd").alias("net_flow_usd"))
)
commercial_balance_window = (
    Window.partitionBy("account_id")
    .orderBy("month_start")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)
commercial_liquidity = (
    spark.table(f"{COMMERCIAL}.`commercial_deposit_accounts`")
    .crossJoin(months)
    .filter(F.col("month_start") >= F.trunc("open_date", "month"))
    .join(commercial_monthly_flow, ["account_id", "month_start"], "left")
    .fillna({"net_flow_usd": 0.0})
    .withColumn(
        "ending_balance_usd",
        F.col("opening_balance_usd") + F.sum("net_flow_usd").over(commercial_balance_window),
    )
    .groupBy("month_start")
    .agg(F.sum("ending_balance_usd").alias("commercial_deposits_usd"))
)

liquidity_snapshots = (
    months.join(retail_liquidity, "month_start", "left")
    .join(commercial_liquidity, "month_start", "left")
    .fillna({"retail_deposits_usd": 0.0, "commercial_deposits_usd": 0.0})
    .withColumn("total_deposits_usd", F.col("retail_deposits_usd") + F.col("commercial_deposits_usd"))
    .withColumn("cash_and_equivalents_usd", F.round(F.col("total_deposits_usd") * 0.18, 2))
    .withColumn("high_quality_liquid_assets_usd", F.round(F.col("total_deposits_usd") * 0.27, 2))
    .withColumn("thirty_day_net_outflow_usd", F.round(F.col("total_deposits_usd") * 0.32, 2))
    .withColumn("liquidity_coverage_ratio_pct", F.round((F.col("cash_and_equivalents_usd") + F.col("high_quality_liquid_assets_usd")) * 100.0 / F.col("thirty_day_net_outflow_usd"), 1))
    .withColumn("snapshot_date", F.last_day("month_start"))
    .withColumn("liquidity_snapshot_id", F.concat(F.lit("LIQ-"), F.date_format("month_start", "yyyyMM")))
    .select("liquidity_snapshot_id", "snapshot_date", "month_start", "retail_deposits_usd", "commercial_deposits_usd", "total_deposits_usd", "cash_and_equivalents_usd", "high_quality_liquid_assets_usd", "thirty_day_net_outflow_usd", "liquidity_coverage_ratio_pct")
)

write_table(liquidity_snapshots, "liquidity_snapshots", "Monthly deposit concentration and liquidity coverage measures.")

print(f"Finance generation complete: {CATALOG}.{FINANCE_SCHEMA}")
dbutils.notebook.exit(json.dumps({"schema": f"{CATALOG}.{FINANCE_SCHEMA}", "status": "complete"}))

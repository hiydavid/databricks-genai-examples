# Databricks notebook source

# MAGIC %md
# MAGIC # Bigly Bank — Wealth Management
# MAGIC
# MAGIC Generates wealth accounts, portfolios, securities, monthly holdings,
# MAGIC trades, advisory fees, and client goals in the WEALTH schema.

# COMMAND ----------

from datetime import date
import json

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
WEALTH_SCHEMA = f"{SCHEMA_PREFIX}_wealth"
CORE = f"`{CATALOG}`.`{CORE_SCHEMA}`"
WEALTH = f"`{CATALOG}`.`{WEALTH_SCHEMA}`"
START_DATE = date(AS_OF_DATE.year - 2, 1, 1)

WEALTH_ACCOUNT_COUNT = 3_000
PORTFOLIO_COUNT = 4_000
SECURITY_COUNT = 200
MONTH_COUNT = (AS_OF_DATE.year - START_DATE.year) * 12 + AS_OF_DATE.month - START_DATE.month + 1
HOLDING_SNAPSHOT_COUNT = PORTFOLIO_COUNT * MONTH_COUNT * 2
TRADE_COUNT = 50_000
EMPLOYEE_COUNT = 800

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {WEALTH}")


def stable_fraction(*columns):
    return F.pmod(F.xxhash64(*columns, F.lit(SEED)), F.lit(1_000_000)) / F.lit(
        1_000_000.0
    )


def choose(values, selector):
    return F.element_at(F.array(*[F.lit(v) for v in values]), selector + F.lit(1))


def write_table(df, table_name, comment):
    full_name = f"{WEALTH}.`{table_name}`"
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
# MAGIC ## Accounts, portfolios, and securities

# COMMAND ----------

party_lookup = spark.table(f"{CORE}.`parties`").select(
    "party_id", "party_type", "relationship_tier", "region", "risk_rating"
)

wealth_accounts = (
    spark.range(WEALTH_ACCOUNT_COUNT, numPartitions=8)
    .withColumn("account_number", F.col("id") + 1)
    .withColumn(
        "party_number",
        F.when(F.col("id") < 2_000, F.lit(23_001) + F.col("id"))
        .otherwise(F.lit(1) + F.pmod(F.xxhash64("id", F.lit("wealth-person")), F.lit(20_000))),
    )
    .withColumn("party_id", F.concat(F.lit("PTY-"), F.lpad(F.col("party_number").cast("string"), 6, "0")))
    .join(party_lookup, "party_id")
    .withColumn("wealth_account_id", F.concat(F.lit("WAC-"), F.lpad(F.col("account_number").cast("string"), 6, "0")))
    .withColumn(
        "product_id",
        F.when(stable_fraction("id", F.lit("wealth-product")) < 0.42, "PRD-WM-MGD-01")
        .when(stable_fraction("id", F.lit("wealth-product")) < 0.70, "PRD-WM-BROK-01")
        .when(stable_fraction("id", F.lit("wealth-product")) < 0.86, "PRD-WM-IRA-01")
        .otherwise("PRD-WM-ROTH-01"),
    )
    .withColumn("account_type", F.when(F.col("product_id") == "PRD-WM-MGD-01", "Managed").when(F.col("product_id") == "PRD-WM-BROK-01", "Brokerage").otherwise("Retirement"))
    .withColumn("open_date", F.date_add(F.lit(START_DATE), (stable_fraction("id", F.lit("wealth-open")) * 500).cast("int")))
    .withColumn("u_status", stable_fraction("id", F.lit("wealth-status")))
    .withColumn(
        "close_date",
        F.when(
            F.col("u_status") >= 0.97,
            F.least(
                F.lit(AS_OF_DATE),
                F.date_add(
                    "open_date",
                    (
                        F.lit(180)
                        + F.floor(stable_fraction("id", F.lit("wealth-close")) * 365)
                    ).cast("int"),
                ),
            ),
        ).cast("date"),
    )
    .withColumn("status", F.when(F.col("close_date").isNull(), "Active").otherwise("Closed"))
    .withColumn(
        "advisor_id",
        F.concat(
            F.lit("EMP-"),
            F.lpad((F.pmod(F.xxhash64("id", F.lit("advisor")), F.lit(EMPLOYEE_COUNT)) + 1).cast("string"), 5, "0"),
        ),
    )
    .select("wealth_account_id", "party_id", "product_id", "account_type", "open_date", "close_date", "status", "advisor_id", "party_type", "relationship_tier", "region", "risk_rating")
)

write_table(wealth_accounts, "wealth_accounts", "Wealth-management account master for people and households.")

portfolios = (
    spark.range(PORTFOLIO_COUNT, numPartitions=8)
    .withColumn("portfolio_number", F.col("id") + 1)
    .withColumn(
        "wealth_account_id",
        F.concat(F.lit("WAC-"), F.lpad((F.pmod(F.col("id"), F.lit(WEALTH_ACCOUNT_COUNT)) + 1).cast("string"), 6, "0")),
    )
    .join(spark.table(f"{WEALTH}.`wealth_accounts`"), "wealth_account_id")
    .withColumn("portfolio_id", F.concat(F.lit("PRT-"), F.lpad(F.col("portfolio_number").cast("string"), 6, "0")))
    .withColumn(
        "strategy",
        F.when(stable_fraction("id", F.lit("strategy")) < 0.20, "Income")
        .when(stable_fraction("id", F.lit("strategy")) < 0.62, "Balanced")
        .when(stable_fraction("id", F.lit("strategy")) < 0.88, "Growth")
        .otherwise("Capital Preservation"),
    )
    .withColumn("risk_tolerance", F.when(F.col("strategy") == "Growth", "High").when(F.col("strategy") == "Balanced", "Moderate").otherwise("Low"))
    .withColumn(
        "inception_date",
        F.least(
            F.coalesce("close_date", F.lit(AS_OF_DATE)),
            F.date_add(
                "open_date",
                F.pmod(F.xxhash64("id", F.lit("portfolio-inception")), F.lit(45)).cast("int"),
            ),
        ),
    )
    .withColumn("target_return_pct", F.when(F.col("strategy") == "Growth", 8.5).when(F.col("strategy") == "Balanced", 6.0).when(F.col("strategy") == "Income", 4.5).otherwise(3.0))
    .select("portfolio_id", "wealth_account_id", "party_id", "advisor_id", "strategy", "risk_tolerance", "inception_date", F.col("target_return_pct").cast("double"), "region", "relationship_tier", "close_date")
)

write_table(portfolios, "portfolios", "Investment portfolios with advisor, strategy, and target-return attributes.")

asset_classes = ["US Equity", "International Equity", "Fixed Income", "Cash", "Real Estate", "Alternatives"]
securities = (
    spark.range(SECURITY_COUNT, numPartitions=4)
    .withColumn("security_number", F.col("id") + 1)
    .withColumn("security_id", F.concat(F.lit("SEC-"), F.lpad(F.col("security_number").cast("string"), 4, "0")))
    .withColumn("symbol", F.concat(F.lit("BB"), F.lpad(F.col("security_number").cast("string"), 4, "0")))
    .withColumn("security_name", F.concat(F.lit("Synthetic Investment "), F.col("security_number")))
    .withColumn("asset_class", choose(asset_classes, F.pmod(F.xxhash64("id", F.lit("asset-class")), F.lit(len(asset_classes)))))
    .withColumn("expense_ratio_pct", F.round(stable_fraction("id", F.lit("expense")) * 1.1, 3))
    .withColumn("is_active", F.lit(True))
    .select("security_id", "symbol", "security_name", "asset_class", "expense_ratio_pct", "is_active")
)

write_table(securities, "securities", "Synthetic security master; symbols do not represent real instruments.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Holdings and trades

# COMMAND ----------

portfolio_lookup = spark.table(f"{WEALTH}.`portfolios`")
security_lookup = spark.table(f"{WEALTH}.`securities`")

holding_snapshots = (
    spark.range(HOLDING_SNAPSHOT_COUNT, numPartitions=32)
    .withColumn("holding_number", F.col("id") + 1)
    .withColumn("portfolio_number", F.pmod(F.col("id"), F.lit(PORTFOLIO_COUNT)) + 1)
    .withColumn("month_offset", F.pmod(F.floor(F.col("id") / PORTFOLIO_COUNT), F.lit(MONTH_COUNT)).cast("int"))
    .withColumn("security_slot", F.floor(F.col("id") / (PORTFOLIO_COUNT * MONTH_COUNT)).cast("int"))
    .withColumn(
        "security_number",
        F.pmod(F.xxhash64("portfolio_number", F.lit("security-base")) + F.col("security_slot") * 17, F.lit(SECURITY_COUNT)) + 1,
    )
    .withColumn("portfolio_id", F.concat(F.lit("PRT-"), F.lpad(F.col("portfolio_number").cast("string"), 6, "0")))
    .withColumn("security_id", F.concat(F.lit("SEC-"), F.lpad(F.col("security_number").cast("string"), 4, "0")))
    .join(portfolio_lookup, "portfolio_id")
    .join(security_lookup, "security_id")
    .withColumn("month_start", F.add_months(F.lit(START_DATE), F.col("month_offset")))
    .filter(F.col("month_start") >= F.trunc("inception_date", "month"))
    .filter(F.col("month_start") <= F.trunc(F.coalesce("close_date", F.lit(AS_OF_DATE)), "month"))
    .withColumn("snapshot_date", F.last_day("month_start"))
    .withColumn(
        "market_value_usd",
        F.round(
            F.when(F.col("relationship_tier") == "Private Client", 4.0).when(F.col("relationship_tier") == "Preferred", 2.0).otherwise(1.0)
            * F.pow(F.lit(10.0), F.lit(3.2) + stable_fraction("holding_number", F.lit("market-value")) * 2.8),
            2,
        ),
    )
    .withColumn("cost_basis_usd", F.round(F.col("market_value_usd") * (0.72 + stable_fraction("holding_number", F.lit("cost-basis")) * 0.42), 2))
    .withColumn("unrealized_gain_loss_usd", F.round(F.col("market_value_usd") - F.col("cost_basis_usd"), 2))
    .withColumn("quantity", F.round(F.col("market_value_usd") / (20.0 + stable_fraction("holding_number", F.lit("price")) * 380.0), 4))
    .withColumn("holding_snapshot_id", F.concat(F.lit("HLD-"), F.lpad(F.col("holding_number").cast("string"), 9, "0")))
    .select("holding_snapshot_id", "snapshot_date", "month_start", "portfolio_id", "wealth_account_id", "party_id", "advisor_id", "security_id", "asset_class", "quantity", "market_value_usd", "cost_basis_usd", "unrealized_gain_loss_usd", "strategy", "region")
)

write_table(holding_snapshots, "holding_snapshots", "Monthly portfolio-security holdings with market value and cost basis.")

trades = (
    spark.range(TRADE_COUNT, numPartitions=16)
    .withColumn("trade_number", F.col("id") + 1)
    .withColumn("portfolio_id", F.concat(F.lit("PRT-"), F.lpad((F.pmod(F.xxhash64("id", F.lit("trade-portfolio")), F.lit(PORTFOLIO_COUNT)) + 1).cast("string"), 6, "0")))
    .withColumn("security_id", F.concat(F.lit("SEC-"), F.lpad((F.pmod(F.xxhash64("id", F.lit("trade-security")), F.lit(SECURITY_COUNT)) + 1).cast("string"), 4, "0")))
    .join(portfolio_lookup, "portfolio_id")
    .join(security_lookup, "security_id")
    .withColumn("lifecycle_end", F.coalesce("close_date", F.lit(AS_OF_DATE)))
    .withColumn("trade_date", F.date_add("inception_date", F.floor(stable_fraction("trade_number", F.lit("trade-date")) * (F.datediff("lifecycle_end", "inception_date") + 1)).cast("int")))
    .withColumn("side", F.when(stable_fraction("trade_number", F.lit("trade-side")) < 0.58, "Buy").otherwise("Sell"))
    .withColumn("quantity", F.round(F.lit(1.0) + stable_fraction("trade_number", F.lit("trade-quantity")) * 500.0, 4))
    .withColumn("price_usd", F.round(F.lit(20.0) + stable_fraction("trade_number", F.lit("trade-price")) * 380.0, 2))
    .withColumn("trade_amount_usd", F.round(F.col("quantity") * F.col("price_usd"), 2))
    .withColumn("commission_usd", F.round(F.lit(1.0) + F.col("trade_amount_usd") * 0.0005, 2))
    .withColumn("trade_id", F.concat(F.lit("TRD-"), F.lpad(F.col("trade_number").cast("string"), 8, "0")))
    .select("trade_id", "trade_date", "portfolio_id", "wealth_account_id", "party_id", "advisor_id", "security_id", "asset_class", "side", "quantity", "price_usd", "trade_amount_usd", "commission_usd", "strategy", "region")
)

write_table(trades, "trades", "Portfolio trades linked to synthetic securities and advisors.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advisory fees and goals

# COMMAND ----------

portfolio_month_values = (
    spark.table(f"{WEALTH}.`holding_snapshots`")
    .groupBy("portfolio_id", "wealth_account_id", "party_id", "advisor_id", "month_start", "strategy", "region")
    .agg(F.sum("market_value_usd").alias("assets_under_management_usd"))
)

advisory_fees = (
    portfolio_month_values.withColumn("fee_rate_bps", F.when(F.col("assets_under_management_usd") >= 1_000_000, 65.0).when(F.col("assets_under_management_usd") >= 250_000, 85.0).otherwise(110.0))
    .withColumn("fee_amount_usd", F.round(F.col("assets_under_management_usd") * F.col("fee_rate_bps") / 10_000.0 / 12.0, 2))
    .withColumn("fee_date", F.last_day("month_start"))
    .withColumn("advisory_fee_id", F.concat(F.lit("WFE-"), F.substring("portfolio_id", 5, 6), F.lit("-"), F.date_format("month_start", "yyyyMM")))
    .select("advisory_fee_id", "fee_date", "month_start", "portfolio_id", "wealth_account_id", "party_id", "advisor_id", "assets_under_management_usd", F.col("fee_rate_bps").cast("double"), "fee_amount_usd", "strategy", "region")
)

write_table(advisory_fees, "advisory_fees", "Monthly advisory fees calculated from portfolio assets under management.")

goal_types = ["Retirement", "Education", "Home Purchase", "Legacy", "Income"]
client_goals = (
    spark.range(WEALTH_ACCOUNT_COUNT, numPartitions=8)
    .withColumn("goal_number", F.col("id") + 1)
    .withColumn("wealth_account_id", F.concat(F.lit("WAC-"), F.lpad(F.col("goal_number").cast("string"), 6, "0")))
    .join(spark.table(f"{WEALTH}.`wealth_accounts`").select("wealth_account_id", "party_id", "advisor_id"), "wealth_account_id")
    .withColumn("goal_id", F.concat(F.lit("GOL-"), F.lpad(F.col("goal_number").cast("string"), 6, "0")))
    .withColumn("goal_type", choose(goal_types, F.pmod(F.xxhash64("id", F.lit("goal-type")), F.lit(len(goal_types)))))
    .withColumn("target_date", F.add_months(F.lit(AS_OF_DATE), (F.lit(24) + F.pmod(F.xxhash64("id", F.lit("goal-date")), F.lit(240))).cast("int")))
    .withColumn("target_amount_usd", F.round(F.pow(F.lit(10.0), F.lit(4.5) + stable_fraction("id", F.lit("goal-target")) * 2.2), 2))
    .withColumn("funded_pct", F.round(F.lit(10.0) + stable_fraction("id", F.lit("goal-funded")) * 95.0, 1))
    .withColumn("status", F.when(F.col("funded_pct") >= 100.0, "Funded").when(F.col("funded_pct") >= 70.0, "On Track").otherwise("Needs Attention"))
    .select("goal_id", "wealth_account_id", "party_id", "advisor_id", "goal_type", "target_date", "target_amount_usd", "funded_pct", "status")
)

write_table(client_goals, "client_goals", "Client financial goals and funded progress for advisory conversations.")

print(f"Wealth generation complete: {CATALOG}.{WEALTH_SCHEMA}")
dbutils.notebook.exit(
    json.dumps(
        {
            "schema": f"{CATALOG}.{WEALTH_SCHEMA}",
            "wealth_accounts": WEALTH_ACCOUNT_COUNT,
            "portfolios": PORTFOLIO_COUNT,
            "holding_snapshots_target": HOLDING_SNAPSHOT_COUNT,
            "trades": TRADE_COUNT,
        }
    )
)

# Databricks notebook source

# MAGIC %md
# MAGIC # Bigly Bank — Retail Deposits and Payments
# MAGIC
# MAGIC Generates deposit accounts, ledger transactions, monthly balances,
# MAGIC payment events, and fee events in the RETAIL schema.

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
CORE = f"`{CATALOG}`.`{CORE_SCHEMA}`"
RETAIL = f"`{CATALOG}`.`{RETAIL_SCHEMA}`"
START_DATE = date(AS_OF_DATE.year - 2, 1, 1)

ACCOUNT_COUNT = 30_000
TRANSACTION_COUNT = 750_000
PERSON_COUNT = 20_000
BRANCH_COUNT = 40

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {RETAIL}")


def stable_fraction(*columns):
    return F.pmod(F.xxhash64(*columns, F.lit(SEED)), F.lit(1_000_000)) / F.lit(
        1_000_000.0
    )


def write_table(df, table_name, comment):
    full_name = f"{RETAIL}.`{table_name}`"
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
# MAGIC ## Deposit accounts

# COMMAND ----------

parties = spark.table(f"{CORE}.`parties`").select(
    "party_id", "relationship_tier", "region", "state"
)

account_base = (
    spark.range(ACCOUNT_COUNT, numPartitions=16)
    .withColumn("account_number", F.col("id") + 1)
    .withColumn("u_open", stable_fraction("id", F.lit("deposit-open")))
    .withColumn("u_product", stable_fraction("id", F.lit("deposit-product")))
    .withColumn("u_status", stable_fraction("id", F.lit("deposit-status")))
    .withColumn(
        "party_id",
        F.concat(
            F.lit("PTY-"),
            F.lpad((F.pmod(F.xxhash64("id", F.lit("owner")), F.lit(PERSON_COUNT)) + 1).cast("string"), 6, "0"),
        ),
    )
    .join(parties, "party_id", "inner")
    .withColumn(
        "account_id",
        F.concat(F.lit("DDA-"), F.lpad(F.col("account_number").cast("string"), 7, "0")),
    )
    .withColumn(
        "product_id",
        F.when(F.col("u_product") < 0.42, "PRD-DEP-CHK-01")
        .when(F.col("u_product") < 0.62, "PRD-DEP-CHK-02")
        .when(F.col("u_product") < 0.80, "PRD-DEP-SAV-01")
        .when(F.col("u_product") < 0.96, "PRD-DEP-SAV-02")
        .otherwise("PRD-DEP-CD-01"),
    )
    .withColumn(
        "account_type",
        F.when(F.col("product_id").contains("CHK"), "Checking")
        .when(F.col("product_id").contains("CD"), "Certificate of Deposit")
        .otherwise("Savings"),
    )
    .withColumn(
        "open_date",
        F.date_add(
            F.lit(START_DATE),
            F.floor(F.col("u_open") * F.greatest(F.datediff(F.lit(AS_OF_DATE), F.lit(START_DATE)) - 45, F.lit(1))).cast("int"),
        ),
    )
    .withColumn(
        "close_date",
        F.when(
            F.col("u_status") < 0.05,
            F.least(
                F.lit(AS_OF_DATE),
                F.date_add(F.col("open_date"), (F.lit(180) + F.floor(F.col("u_open") * 720)).cast("int")),
            ),
        ).cast("date"),
    )
    .withColumn(
        "status",
        F.when(F.col("close_date").isNotNull(), "Closed")
        .when(F.col("u_status") < 0.11, "Dormant")
        .otherwise("Active"),
    )
    .withColumn(
        "branch_id",
        F.concat(
            F.lit("BRN-"),
            F.lpad((F.pmod(F.xxhash64("id", F.lit("branch")), F.lit(BRANCH_COUNT)) + 1).cast("string"), 4, "0"),
        ),
    )
    .withColumn(
        "opening_balance_usd",
        F.round(
            F.when(F.col("relationship_tier") == "Private Client", F.lit(4.0))
            .when(F.col("relationship_tier") == "Preferred", F.lit(1.8))
            .otherwise(F.lit(1.0))
            * (F.lit(750.0) + F.pow(F.lit(10.0), F.lit(2.0) + F.col("u_product") * F.lit(3.0))),
            2,
        ),
    )
    .withColumn("interest_rate_pct", F.when(F.col("account_type") == "Checking", 0.10).when(F.col("product_id") == "PRD-DEP-SAV-02", 4.25).when(F.col("account_type") == "Savings", 1.25).otherwise(4.85))
    .select(
        "account_id",
        "party_id",
        "product_id",
        "branch_id",
        "account_type",
        "open_date",
        "close_date",
        "status",
        "opening_balance_usd",
        F.col("interest_rate_pct").cast("double"),
    )
)

write_table(
    account_base,
    "deposit_accounts",
    "Retail checking, savings, and certificate-of-deposit accounts with coherent lifecycles.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ledger transactions

# COMMAND ----------

account_lookup = spark.table(f"{RETAIL}.`deposit_accounts`")

transaction_base = (
    spark.range(TRANSACTION_COUNT, numPartitions=32)
    .withColumn("transaction_number", F.col("id") + 1)
    .withColumn(
        "account_id",
        F.concat(
            F.lit("DDA-"),
            F.lpad((F.pmod(F.xxhash64("id", F.lit("deposit-account")), F.lit(ACCOUNT_COUNT)) + 1).cast("string"), 7, "0"),
        ),
    )
    .join(account_lookup, "account_id", "inner")
    .withColumn("u_date", stable_fraction("transaction_number", F.lit("deposit-date")))
    .withColumn("u_type", stable_fraction("transaction_number", F.lit("deposit-type")))
    .withColumn("u_amount", stable_fraction("transaction_number", F.lit("deposit-amount")))
    .withColumn("u_channel", stable_fraction("transaction_number", F.lit("deposit-channel")))
    .withColumn("lifecycle_end", F.coalesce("close_date", F.lit(AS_OF_DATE)))
    .withColumn(
        "transaction_date",
        F.date_add(
            F.col("open_date"),
            F.floor(F.col("u_date") * (F.datediff("lifecycle_end", "open_date") + 1)).cast("int"),
        ),
    )
    .withColumn(
        "transaction_timestamp",
        F.to_timestamp(
            F.concat_ws(
                " ",
                F.col("transaction_date").cast("string"),
                F.format_string(
                    "%02d:%02d:%02d",
                    F.pmod(F.xxhash64("transaction_number", F.lit("hour")), F.lit(24)),
                    F.pmod(F.xxhash64("transaction_number", F.lit("minute")), F.lit(60)),
                    F.pmod(F.xxhash64("transaction_number", F.lit("second")), F.lit(60)),
                ),
            )
        ),
    )
    .withColumn(
        "transaction_type",
        F.when(F.col("u_type") < 0.28, "Deposit")
        .when(F.col("u_type") < 0.46, "ACH Debit")
        .when(F.col("u_type") < 0.61, "Debit Card Purchase")
        .when(F.col("u_type") < 0.71, "Withdrawal")
        .when(F.col("u_type") < 0.81, "Transfer")
        .when(F.col("u_type") < 0.90, "Bill Payment")
        .when(F.col("u_type") < 0.96, "Fee")
        .otherwise("Interest"),
    )
    .withColumn(
        "channel",
        F.when(F.col("u_channel") < 0.42, "Mobile")
        .when(F.col("u_channel") < 0.68, "Online")
        .when(F.col("u_channel") < 0.82, "Branch")
        .when(F.col("u_channel") < 0.94, "ATM")
        .otherwise("Wire"),
    )
    .withColumn(
        "absolute_amount_usd",
        F.round(
            F.when(F.col("transaction_type") == "Fee", F.lit(35.0))
            .when(F.col("transaction_type") == "Interest", F.lit(1.0) + F.col("u_amount") * 85.0)
            .when(F.col("transaction_type") == "Deposit", F.pow(F.lit(10.0), F.lit(2.0) + F.col("u_amount") * 3.2))
            .when(F.col("transaction_type") == "Transfer", F.pow(F.lit(10.0), F.lit(1.8) + F.col("u_amount") * 2.8))
            .otherwise(F.pow(F.lit(10.0), F.lit(1.0) + F.col("u_amount") * 2.7)),
            2,
        ),
    )
    .withColumn(
        "signed_amount_usd",
        F.when(F.col("transaction_type").isin("Deposit", "Interest"), F.col("absolute_amount_usd"))
        .otherwise(-F.col("absolute_amount_usd")),
    )
    .withColumn(
        "incident_id",
        F.when(
            (F.col("transaction_date").between(F.lit(date(2024, 1, 15)), F.lit(date(2024, 1, 16))))
            & F.col("channel").isin("Mobile", "Online"),
            "INC-MOBILE-20240115",
        ).cast("string"),
    )
    .withColumn(
        "status",
        F.when(F.col("incident_id").isNotNull(), F.when(stable_fraction("transaction_number", F.lit("outage")) < 0.55, "Reversed").otherwise("Posted"))
        .when(stable_fraction("transaction_number", F.lit("status")) < 0.975, "Posted")
        .when(stable_fraction("transaction_number", F.lit("status")) < 0.990, "Pending")
        .otherwise("Reversed"),
    )
    .withColumn("fee_usd", F.when(F.col("transaction_type") == "Fee", F.col("absolute_amount_usd")).otherwise(F.lit(0.0)))
    .withColumn(
        "transaction_id",
        F.concat(F.lit("DTR-"), F.lpad(F.col("transaction_number").cast("string"), 9, "0")),
    )
)

ledger_window = (
    Window.partitionBy("account_id")
    .orderBy("transaction_timestamp", "transaction_id")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

deposit_transactions = (
    transaction_base.withColumn(
        "balance_after_usd",
        F.round(F.col("opening_balance_usd") + F.sum("signed_amount_usd").over(ledger_window), 2),
    )
    .select(
        "transaction_id",
        "transaction_timestamp",
        "transaction_date",
        "account_id",
        "party_id",
        "branch_id",
        "transaction_type",
        "channel",
        "signed_amount_usd",
        "absolute_amount_usd",
        "fee_usd",
        "balance_after_usd",
        "status",
        "incident_id",
    )
)

write_table(
    deposit_transactions,
    "deposit_transactions",
    "Chronological signed deposit-account ledger events; the January 2024 mobile outage creates linked reversals.",
)

payment_events = (
    spark.table(f"{RETAIL}.`deposit_transactions`")
    .filter(F.col("transaction_type").isin("ACH Debit", "Transfer", "Bill Payment"))
    .select(
        F.concat(F.lit("PAY-"), F.substring("transaction_id", 5, 9)).alias("payment_event_id"),
        "transaction_id",
        "account_id",
        "party_id",
        F.col("transaction_timestamp").alias("initiated_at"),
        F.col("transaction_type").alias("payment_type"),
        "channel",
        F.col("absolute_amount_usd").alias("payment_amount_usd"),
        F.col("status").alias("payment_status"),
        "incident_id",
    )
)

write_table(payment_events, "payment_events", "Payment workflow events derived from deposit-account ledger activity.")

fee_events = (
    spark.table(f"{RETAIL}.`deposit_transactions`")
    .filter(F.col("fee_usd") > 0)
    .select(
        F.concat(F.lit("FEE-"), F.substring("transaction_id", 5, 9)).alias("fee_event_id"),
        "transaction_id",
        "account_id",
        "party_id",
        "transaction_date",
        F.lit("Monthly Service or Overdraft Fee").alias("fee_type"),
        "fee_usd",
        (F.col("status") == "Reversed").alias("is_reversed"),
        "incident_id",
    )
)

write_table(fee_events, "fee_events", "Deposit fee charges and reversals linked to their originating ledger events.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monthly balance snapshots

# COMMAND ----------

months = spark.table(f"{CORE}.`bank_calendar`").select("month_start").distinct()
active_account_months = (
    account_lookup.crossJoin(months)
    .filter(F.col("month_start") >= F.trunc("open_date", "month"))
    .filter(F.col("month_start") <= F.trunc(F.coalesce("close_date", F.lit(AS_OF_DATE)), "month"))
)

monthly_activity = (
    spark.table(f"{RETAIL}.`deposit_transactions`")
    .withColumn("month_start", F.trunc("transaction_date", "month"))
    .groupBy("account_id", "month_start")
    .agg(
        F.sum("signed_amount_usd").alias("net_flow_usd"),
        F.sum(F.when(F.col("signed_amount_usd") > 0, F.col("signed_amount_usd")).otherwise(0.0)).alias("inflow_usd"),
        F.sum(F.when(F.col("signed_amount_usd") < 0, -F.col("signed_amount_usd")).otherwise(0.0)).alias("outflow_usd"),
        F.sum("fee_usd").alias("fee_revenue_usd"),
        F.count("*").alias("transaction_count"),
    )
)

month_window = (
    Window.partitionBy("account_id")
    .orderBy("month_start")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

deposit_balance_snapshots = (
    active_account_months.join(monthly_activity, ["account_id", "month_start"], "left")
    .fillna(
        {
            "net_flow_usd": 0.0,
            "inflow_usd": 0.0,
            "outflow_usd": 0.0,
            "fee_revenue_usd": 0.0,
            "transaction_count": 0,
        }
    )
    .withColumn(
        "ending_balance_usd",
        F.round(F.col("opening_balance_usd") + F.sum("net_flow_usd").over(month_window), 2),
    )
    .withColumn(
        "average_balance_usd",
        F.round(F.greatest(F.lit(-500.0), F.col("ending_balance_usd") - F.col("net_flow_usd") / 2), 2),
    )
    .withColumn("snapshot_date", F.last_day("month_start"))
    .select(
        "snapshot_date",
        "month_start",
        "account_id",
        "party_id",
        "product_id",
        "branch_id",
        "account_type",
        "average_balance_usd",
        "ending_balance_usd",
        "inflow_usd",
        "outflow_usd",
        "net_flow_usd",
        "fee_revenue_usd",
        "transaction_count",
    )
)

write_table(
    deposit_balance_snapshots,
    "deposit_balance_snapshots",
    "One row per active deposit account and month with reconciled flows and ending balance.",
)

print(f"Deposit generation complete: {CATALOG}.{RETAIL_SCHEMA}")
dbutils.notebook.exit(
    json.dumps(
        {
            "schema": f"{CATALOG}.{RETAIL_SCHEMA}",
            "deposit_accounts": ACCOUNT_COUNT,
            "deposit_transactions": TRANSACTION_COUNT,
        }
    )
)

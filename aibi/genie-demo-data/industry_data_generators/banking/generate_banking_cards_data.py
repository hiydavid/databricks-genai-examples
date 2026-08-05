# Databricks notebook source

# MAGIC %md
# MAGIC # Bigly Bank — Credit Cards
# MAGIC
# MAGIC Generates card accounts, issued cards, transactions, statements,
# MAGIC payments, disputes, and reward events in the RETAIL schema.

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

CARD_ACCOUNT_COUNT = 12_000
CARD_COUNT = 14_000
TRANSACTION_COUNT = 400_000
PERSON_COUNT = 20_000

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {RETAIL}")


def stable_fraction(*columns):
    return F.pmod(F.xxhash64(*columns, F.lit(SEED)), F.lit(1_000_000)) / F.lit(
        1_000_000.0
    )


def choose(values, selector):
    return F.element_at(F.array(*[F.lit(v) for v in values]), selector + F.lit(1))


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
# MAGIC ## Accounts and issued cards

# COMMAND ----------

parties = spark.table(f"{CORE}.`parties`").select(
    "party_id", "relationship_tier", "risk_rating", "region"
)

card_accounts = (
    spark.range(CARD_ACCOUNT_COUNT, numPartitions=8)
    .withColumn("account_number", F.col("id") + 1)
    .withColumn("u1", stable_fraction("id", F.lit("card-account-u1")))
    .withColumn("u2", stable_fraction("id", F.lit("card-account-u2")))
    .withColumn(
        "account_id",
        F.concat(F.lit("CCA-"), F.lpad(F.col("account_number").cast("string"), 7, "0")),
    )
    .withColumn(
        "party_id",
        F.concat(
            F.lit("PTY-"),
            F.lpad((F.pmod(F.xxhash64("id", F.lit("card-owner")), F.lit(PERSON_COUNT)) + 1).cast("string"), 6, "0"),
        ),
    )
    .join(parties, "party_id", "inner")
    .withColumn(
        "product_id",
        F.when(F.col("u1") < 0.62, "PRD-CARD-01")
        .when(F.col("u1") < 0.88, "PRD-CARD-02")
        .otherwise("PRD-CARD-03"),
    )
    .withColumn(
        "open_date",
        F.date_add(
            F.lit(START_DATE),
            F.floor(F.col("u1") * F.greatest(F.datediff(F.lit(AS_OF_DATE), F.lit(START_DATE)) - 60, F.lit(1))).cast("int"),
        ),
    )
    .withColumn(
        "close_date",
        F.when(
            F.col("u2") < 0.04,
            F.least(F.lit(AS_OF_DATE), F.date_add("open_date", (F.lit(240) + F.floor(F.col("u1") * 540)).cast("int"))),
        ).cast("date"),
    )
    .withColumn("status", F.when(F.col("close_date").isNotNull(), "Closed").when(F.col("u2") < 0.08, "Restricted").otherwise("Active"))
    .withColumn(
        "credit_limit_usd",
        F.round(
            F.when(F.col("relationship_tier") == "Private Client", F.lit(30_000.0) + F.col("u2") * 70_000.0)
            .when(F.col("relationship_tier") == "Preferred", F.lit(10_000.0) + F.col("u2") * 25_000.0)
            .otherwise(F.lit(1_500.0) + F.col("u2") * 13_500.0),
            2,
        ),
    )
    .withColumn("apr_pct", F.when(F.col("product_id") == "PRD-CARD-02", 19.49).when(F.col("product_id") == "PRD-CARD-03", 24.99).otherwise(20.99))
    .select(
        "account_id",
        "party_id",
        "product_id",
        "open_date",
        "close_date",
        "status",
        "credit_limit_usd",
        F.col("apr_pct").cast("double"),
        "risk_rating",
        "region",
    )
)

write_table(card_accounts, "card_accounts", "Credit-card account master with limits, pricing, and lifecycle dates.")

cards = (
    spark.range(CARD_COUNT, numPartitions=8)
    .withColumn("card_number", F.col("id") + 1)
    .withColumn(
        "card_id",
        F.concat(F.lit("CRD-"), F.lpad(F.col("card_number").cast("string"), 8, "0")),
    )
    .withColumn(
        "account_id",
        F.concat(F.lit("CCA-"), F.lpad((F.pmod(F.col("id"), F.lit(CARD_ACCOUNT_COUNT)) + 1).cast("string"), 7, "0")),
    )
    .join(spark.table(f"{RETAIL}.`card_accounts`").select("account_id", "party_id", "open_date", "close_date"), "account_id")
    .withColumn(
        "issued_date",
        F.least(
            F.lit(AS_OF_DATE),
            F.date_add(
                "open_date",
                F.pmod(F.xxhash64("card_number", F.lit("issued")), F.lit(45)).cast("int"),
            ),
        ),
    )
    .withColumn("expiration_date", F.add_months("issued_date", 48))
    .withColumn("card_type", F.when(F.pmod(F.col("id"), F.lit(12)) == 0, "Virtual").otherwise("Physical"))
    .withColumn("network", F.when(F.pmod(F.col("id"), F.lit(3)) == 0, "Mastercard").otherwise("Visa"))
    .withColumn("status", F.when(F.col("close_date").isNotNull(), "Closed").otherwise("Active"))
    .select("card_id", "account_id", "party_id", "issued_date", "expiration_date", "card_type", "network", "status")
)

write_table(cards, "cards", "Synthetic issued-card records without real payment-card numbers.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Card transactions and disputes

# COMMAND ----------

merchant_categories = [
    "Groceries",
    "Dining",
    "Travel",
    "Fuel",
    "Retail",
    "Healthcare",
    "Entertainment",
    "Electronics",
]

card_lookup = spark.table(f"{RETAIL}.`cards`").join(
    spark.table(f"{RETAIL}.`card_accounts`").select(
        "account_id", "product_id", "open_date", "close_date", "credit_limit_usd", "region"
    ),
    "account_id",
)

card_transaction_base = (
    spark.range(TRANSACTION_COUNT, numPartitions=32)
    .withColumn("transaction_number", F.col("id") + 1)
    .withColumn(
        "card_id",
        F.concat(
            F.lit("CRD-"),
            F.lpad((F.pmod(F.xxhash64("id", F.lit("card-selector")), F.lit(CARD_COUNT)) + 1).cast("string"), 8, "0"),
        ),
    )
    .join(card_lookup, "card_id", "inner")
    .withColumn("u_date", stable_fraction("transaction_number", F.lit("card-date")))
    .withColumn("u_amount", stable_fraction("transaction_number", F.lit("card-amount")))
    .withColumn("u_type", stable_fraction("transaction_number", F.lit("card-type")))
    .withColumn("u_risk", stable_fraction("transaction_number", F.lit("card-risk")))
    .withColumn("lifecycle_start", F.col("issued_date"))
    .withColumn(
        "lifecycle_end",
        F.least(F.coalesce("close_date", F.lit(AS_OF_DATE)), "expiration_date", F.lit(AS_OF_DATE)),
    )
    .withColumn(
        "ordinary_date",
        F.date_add(
            "lifecycle_start",
            F.floor(F.col("u_date") * (F.datediff("lifecycle_end", "lifecycle_start") + 1)).cast("int"),
        ),
    )
    .withColumn(
        "holiday_date",
        F.date_add(F.lit(date(2025, 11, 1)), F.pmod(F.xxhash64("transaction_number", F.lit("holiday")), F.lit(61)).cast("int")),
    )
    .withColumn(
        "transaction_date",
        F.when(
            (F.col("u_date") < 0.22) & (F.lit(AS_OF_DATE) >= F.lit(date(2025, 11, 1))),
            F.least("lifecycle_end", F.greatest("lifecycle_start", "holiday_date")),
        ).otherwise("ordinary_date"),
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
        F.when(F.col("u_type") < 0.90, "Purchase")
        .when(F.col("u_type") < 0.94, "Cash Advance")
        .when(F.col("u_type") < 0.98, "Refund")
        .otherwise("Fee"),
    )
    .withColumn(
        "merchant_category",
        choose(
            merchant_categories,
            F.pmod(F.xxhash64("transaction_number", F.lit("merchant")), F.lit(len(merchant_categories))),
        ),
    )
    .withColumn(
        "amount_usd",
        F.round(
            F.when(F.col("transaction_type") == "Fee", F.lit(29.0))
            .when(F.col("transaction_type") == "Cash Advance", F.lit(100.0) + F.col("u_amount") * 1_400.0)
            .otherwise(F.pow(F.lit(10.0), F.lit(0.8) + F.col("u_amount") * 2.4)),
            2,
        ),
    )
    .withColumn("balance_impact_usd", F.when(F.col("transaction_type") == "Refund", -F.col("amount_usd")).otherwise(F.col("amount_usd")))
    .withColumn("channel", F.when(stable_fraction("transaction_number", F.lit("card-channel")) < 0.55, "Card Present").when(stable_fraction("transaction_number", F.lit("card-channel")) < 0.91, "E-commerce").otherwise("Mobile Wallet"))
    .withColumn("is_international", (F.col("merchant_category") == "Travel") & (F.col("u_risk") < 0.22))
    .withColumn(
        "risk_score",
        F.round(
            F.least(
                F.lit(100.0),
                F.col("u_risk") * 55.0
                + F.when(F.col("is_international"), 20.0).otherwise(0.0)
                + F.when((F.month("transaction_date") == 12) & F.col("merchant_category").isin("Electronics", "Travel"), 25.0).otherwise(0.0),
            ),
            1,
        ),
    )
    .withColumn("is_suspected_fraud", F.col("risk_score") >= 78.0)
    .withColumn("authorization_status", F.when(F.col("risk_score") >= 92.0, "Declined").when(F.col("u_risk") < 0.015, "Reversed").otherwise("Approved"))
    .withColumn("posted_date", F.when(F.col("authorization_status") == "Approved", F.least(F.lit(AS_OF_DATE), F.date_add("transaction_date", 1))).cast("date"))
    .withColumn("transaction_id", F.concat(F.lit("CTR-"), F.lpad(F.col("transaction_number").cast("string"), 9, "0")))
    .select(
        "transaction_id",
        "transaction_timestamp",
        "transaction_date",
        "posted_date",
        "card_id",
        "account_id",
        "party_id",
        "product_id",
        "transaction_type",
        "merchant_category",
        "channel",
        "amount_usd",
        "balance_impact_usd",
        "is_international",
        "risk_score",
        "is_suspected_fraud",
        "authorization_status",
        "region",
    )
)

write_table(
    card_transaction_base,
    "card_transactions",
    "Card authorizations and postings with a holiday-spend spike and traceable fraud-risk signals.",
)

card_disputes = (
    spark.table(f"{RETAIL}.`card_transactions`")
    .filter(
        F.col("is_suspected_fraud")
        | (stable_fraction("transaction_id", F.lit("dispute-sample")) < 0.008)
    )
    .withColumn("dispute_id", F.concat(F.lit("DSP-"), F.substring("transaction_id", 5, 9)))
    .withColumn("opened_date", F.least(F.lit(AS_OF_DATE), F.date_add("transaction_date", 5)))
    .withColumn("reason", F.when(F.col("is_suspected_fraud"), "Fraudulent Transaction").otherwise("Product or Service Issue"))
    .withColumn("status", F.when(stable_fraction("transaction_id", F.lit("dispute-status")) < 0.72, "Resolved").otherwise("Open"))
    .withColumn("resolution", F.when(F.col("status") == "Resolved", F.when(stable_fraction("transaction_id", F.lit("dispute-resolution")) < 0.68, "Customer Credit").otherwise("Merchant Favored")).cast("string"))
    .withColumn("disputed_amount_usd", F.col("amount_usd"))
    .withColumn("chargeback_loss_usd", F.when(F.col("resolution") == "Customer Credit", F.round(F.col("amount_usd") * 0.82, 2)).otherwise(0.0))
    .select(
        "dispute_id",
        "transaction_id",
        "account_id",
        "party_id",
        "opened_date",
        "reason",
        "status",
        "resolution",
        "disputed_amount_usd",
        "chargeback_loss_usd",
    )
)

write_table(card_disputes, "card_disputes", "Customer card disputes linked to originating transactions and chargeback losses.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Statements, payments, and rewards

# COMMAND ----------

months = spark.table(f"{CORE}.`bank_calendar`").select("month_start").distinct()
account_months = (
    spark.table(f"{RETAIL}.`card_accounts`")
    .crossJoin(months)
    .filter(F.col("month_start") >= F.trunc("open_date", "month"))
    .filter(F.col("month_start") <= F.trunc(F.coalesce("close_date", F.lit(AS_OF_DATE)), "month"))
)

monthly_card_activity = (
    spark.table(f"{RETAIL}.`card_transactions`")
    .filter(F.col("authorization_status") == "Approved")
    .withColumn("month_start", F.trunc("transaction_date", "month"))
    .groupBy("account_id", "month_start")
    .agg(
        F.sum("balance_impact_usd").alias("net_charges_usd"),
        F.sum(F.when(F.col("transaction_type") == "Fee", F.col("amount_usd")).otherwise(0.0)).alias("fees_usd"),
        F.count("*").alias("transaction_count"),
    )
)

statement_seed = (
    account_months.join(monthly_card_activity, ["account_id", "month_start"], "left")
    .fillna({"net_charges_usd": 0.0, "fees_usd": 0.0, "transaction_count": 0})
    .withColumn("payment_fraction", F.lit(0.22) + stable_fraction("account_id", "month_start", F.lit("payment-fraction")) * 0.83)
    .withColumn("payment_amount_usd", F.round(F.greatest(F.lit(0.0), F.col("net_charges_usd") * F.least(F.lit(1.0), F.col("payment_fraction"))), 2))
    .withColumn("monthly_net_usd", F.col("net_charges_usd") - F.col("payment_amount_usd"))
)

statement_window = Window.partitionBy("account_id").orderBy("month_start").rowsBetween(Window.unboundedPreceding, Window.currentRow)
card_statements = (
    statement_seed.withColumn("statement_balance_usd", F.round(F.greatest(F.lit(0.0), F.sum("monthly_net_usd").over(statement_window)), 2))
    .withColumn("statement_date", F.last_day("month_start"))
    .withColumn("payment_due_date", F.date_add("statement_date", 21))
    .withColumn("minimum_payment_usd", F.round(F.greatest(F.lit(35.0), F.col("statement_balance_usd") * 0.03), 2))
    .withColumn("statement_id", F.concat(F.lit("STM-"), F.substring("account_id", 5, 7), F.lit("-"), F.date_format("month_start", "yyyyMM")))
    .select(
        "statement_id",
        "statement_date",
        "month_start",
        "payment_due_date",
        "account_id",
        "party_id",
        "product_id",
        "net_charges_usd",
        "fees_usd",
        "payment_amount_usd",
        "minimum_payment_usd",
        "statement_balance_usd",
        "credit_limit_usd",
        "transaction_count",
    )
)

write_table(card_statements, "card_statements", "Monthly card-account statements with charges, payments, balances, and utilization inputs.")

card_payments = (
    spark.table(f"{RETAIL}.`card_statements`")
    .filter(F.col("payment_amount_usd") > 0)
    .withColumn("payment_id", F.concat(F.lit("CPY-"), F.substring("statement_id", 5, 14)))
    .withColumn("payment_date", F.least(F.lit(AS_OF_DATE), F.date_add("statement_date", 14)))
    .withColumn("payment_channel", F.when(stable_fraction("statement_id", F.lit("card-payment-channel")) < 0.68, "AutoPay").when(stable_fraction("statement_id", F.lit("card-payment-channel")) < 0.92, "Online").otherwise("Branch"))
    .withColumn("payment_status", F.when(F.col("payment_amount_usd") >= F.col("minimum_payment_usd"), "On Time").otherwise("Partial"))
    .select("payment_id", "statement_id", "account_id", "party_id", "payment_date", "payment_channel", "payment_amount_usd", "payment_status")
)

write_table(card_payments, "card_payments", "Payments linked to monthly card statements.")

card_reward_events = (
    spark.table(f"{RETAIL}.`card_transactions`")
    .filter((F.col("authorization_status") == "Approved") & (F.col("transaction_type") == "Purchase"))
    .withColumn("month_start", F.trunc("transaction_date", "month"))
    .groupBy("account_id", "party_id", "product_id", "month_start")
    .agg(
        F.sum("amount_usd").alias("eligible_spend_usd"),
        F.round(F.sum("amount_usd") * F.when(F.col("product_id") == "PRD-CARD-02", 2.0).otherwise(1.0), 0).alias("points_earned"),
    )
    .withColumn("reward_event_id", F.concat(F.lit("RWD-"), F.substring("account_id", 5, 7), F.lit("-"), F.date_format("month_start", "yyyyMM")))
    .select("reward_event_id", "month_start", "account_id", "party_id", "product_id", "eligible_spend_usd", "points_earned")
)

write_table(card_reward_events, "card_reward_events", "Monthly card reward accruals derived from eligible posted spend.")

print(f"Card generation complete: {CATALOG}.{RETAIL_SCHEMA}")
dbutils.notebook.exit(
    json.dumps(
        {
            "schema": f"{CATALOG}.{RETAIL_SCHEMA}",
            "card_accounts": CARD_ACCOUNT_COUNT,
            "cards": CARD_COUNT,
            "card_transactions": TRANSACTION_COUNT,
        }
    )
)

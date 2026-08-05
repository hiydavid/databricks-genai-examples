# Databricks notebook source

# MAGIC %md
# MAGIC # Bigly Bank — Consumer Lending
# MAGIC
# MAGIC Generates applications, decisions, booked loans, collateral, schedules,
# MAGIC payments, delinquency snapshots, and collection actions in RETAIL.

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
RETAIL_SCHEMA = f"{SCHEMA_PREFIX}_retail"
CORE = f"`{CATALOG}`.`{CORE_SCHEMA}`"
RETAIL = f"`{CATALOG}`.`{RETAIL_SCHEMA}`"
START_DATE = date(AS_OF_DATE.year - 2, 1, 1)

APPLICATION_COUNT = 15_000
PERSON_COUNT = 20_000
EMPLOYEE_COUNT = 800

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
# MAGIC ## Applications and credit decisions

# COMMAND ----------

parties = spark.table(f"{CORE}.`parties`").select(
    "party_id", "relationship_tier", "risk_rating", "income_band", "region", "state"
)

loan_applications = (
    spark.range(APPLICATION_COUNT, numPartitions=16)
    .withColumn("application_number", F.col("id") + 1)
    .withColumn("u1", stable_fraction("id", F.lit("loan-app-u1")))
    .withColumn("u2", stable_fraction("id", F.lit("loan-app-u2")))
    .withColumn("u3", stable_fraction("id", F.lit("loan-app-u3")))
    .withColumn(
        "application_id",
        F.concat(F.lit("LAP-"), F.lpad(F.col("application_number").cast("string"), 7, "0")),
    )
    .withColumn(
        "party_id",
        F.concat(
            F.lit("PTY-"),
            F.lpad((F.pmod(F.xxhash64("id", F.lit("loan-owner")), F.lit(PERSON_COUNT)) + 1).cast("string"), 6, "0"),
        ),
    )
    .join(parties, "party_id", "inner")
    .withColumn(
        "application_date",
        F.date_add(
            F.lit(START_DATE),
            F.floor(F.col("u1") * F.greatest(F.datediff(F.lit(AS_OF_DATE), F.lit(START_DATE)) - 45, F.lit(1))).cast("int"),
        ),
    )
    .withColumn(
        "loan_type",
        F.when(F.col("u2") < 0.38, "Mortgage")
        .when(F.col("u2") < 0.68, "Auto Loan")
        .when(F.col("u2") < 0.82, "HELOC")
        .otherwise("Personal Loan"),
    )
    .withColumn(
        "product_id",
        F.when((F.col("loan_type") == "Mortgage") & (F.col("u3") < 0.30), "PRD-LOAN-MTG-02")
        .when(F.col("loan_type") == "Mortgage", "PRD-LOAN-MTG-01")
        .when(F.col("loan_type") == "Auto Loan", "PRD-LOAN-AUTO-01")
        .when(F.col("loan_type") == "HELOC", "PRD-LOAN-HELOC-01")
        .otherwise("PRD-LOAN-PERS-01"),
    )
    .withColumn(
        "requested_amount_usd",
        F.round(
            F.when(F.col("loan_type") == "Mortgage", F.lit(120_000.0) + F.pow(F.col("u3"), 1.8) * 1_080_000.0)
            .when(F.col("loan_type") == "Auto Loan", F.lit(8_000.0) + F.col("u3") * 82_000.0)
            .when(F.col("loan_type") == "HELOC", F.lit(20_000.0) + F.col("u3") * 380_000.0)
            .otherwise(F.lit(2_500.0) + F.col("u3") * 47_500.0),
            2,
        ),
    )
    .withColumn(
        "credit_score",
        F.floor(
            F.lit(520)
            + F.col("u1") * 280
            + F.when(F.col("relationship_tier") == "Private Client", 35)
            .when(F.col("relationship_tier") == "Preferred", 18)
            .otherwise(0)
        ).cast("int"),
    )
    .withColumn("debt_to_income_pct", F.round(F.lit(12.0) + F.col("u2") * 46.0, 1))
    .withColumn(
        "channel",
        F.when(F.col("u3") < 0.44, "Online")
        .when(F.col("u3") < 0.72, "Branch")
        .when(F.col("u3") < 0.90, "Mobile")
        .otherwise("Broker"),
    )
    .select(
        "application_id",
        "application_date",
        "party_id",
        "product_id",
        "loan_type",
        "requested_amount_usd",
        "credit_score",
        "debt_to_income_pct",
        "channel",
        "income_band",
        "risk_rating",
        "region",
        "state",
    )
)

write_table(loan_applications, "loan_applications", "Consumer-loan applications with risk and affordability inputs.")

credit_decisions = (
    spark.table(f"{RETAIL}.`loan_applications`")
    .withColumn("decision_score", F.col("credit_score") - F.col("debt_to_income_pct") * 2.2 + stable_fraction("application_id", F.lit("decision")) * 55.0)
    .withColumn("decision", F.when(F.col("decision_score") >= 610, "Approved").when(F.col("decision_score") >= 570, "Manual Review").otherwise("Declined"))
    .withColumn("decision_date", F.date_add("application_date", (stable_fraction("application_id", F.lit("decision-days")) * 8).cast("int")))
    .withColumn(
        "approved_amount_usd",
        F.when(
            F.col("decision") == "Approved",
            F.round(F.col("requested_amount_usd") * (0.80 + stable_fraction("application_id", F.lit("approved-amount")) * 0.20), 2),
        ).cast("double"),
    )
    .withColumn(
        "decline_reason",
        F.when(F.col("decision") == "Declined", F.when(F.col("credit_score") < 620, "Credit Score").otherwise("Debt-to-Income Ratio")).cast("string"),
    )
    .withColumn(
        "underwriter_id",
        F.concat(
            F.lit("EMP-"),
            F.lpad((F.pmod(F.xxhash64("application_id", F.lit("underwriter")), F.lit(EMPLOYEE_COUNT)) + 1).cast("string"), 5, "0"),
        ),
    )
    .withColumn("decision_id", F.concat(F.lit("LCD-"), F.substring("application_id", 5, 7)))
    .select(
        "decision_id",
        "application_id",
        "decision_date",
        "decision",
        "decision_score",
        "approved_amount_usd",
        "decline_reason",
        "underwriter_id",
    )
)

write_table(credit_decisions, "credit_decisions", "Underwriting decisions linked one-to-one to loan applications.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Booked loans and collateral

# COMMAND ----------

approved = spark.table(f"{RETAIL}.`loan_applications`").join(
    spark.table(f"{RETAIL}.`credit_decisions`").filter(F.col("decision") == "Approved"),
    "application_id",
)

consumer_loans = (
    approved.withColumn("loan_id", F.concat(F.lit("CLN-"), F.substring("application_id", 5, 7)))
    .withColumn("origination_date", F.date_add("decision_date", (stable_fraction("application_id", F.lit("funding-days")) * 18 + 2).cast("int")))
    .withColumn(
        "term_months",
        F.when(F.col("loan_type") == "Mortgage", F.when(F.col("product_id") == "PRD-LOAN-MTG-02", 180).otherwise(360))
        .when(F.col("loan_type") == "Auto Loan", 60)
        .when(F.col("loan_type") == "HELOC", 120)
        .otherwise(48),
    )
    .withColumn(
        "interest_rate_pct",
        F.round(
            F.when(F.col("loan_type") == "Mortgage", 5.8)
            .when(F.col("loan_type") == "Auto Loan", 5.5)
            .when(F.col("loan_type") == "HELOC", 7.4)
            .otherwise(9.5)
            + (760 - F.least(F.col("credit_score"), F.lit(760))) / 100.0,
            3,
        ),
    )
    .withColumn("original_principal_usd", F.col("approved_amount_usd"))
    .withColumn("maturity_date", F.add_months("origination_date", F.col("term_months")))
    .withColumn("status", F.when(F.col("maturity_date") <= F.lit(AS_OF_DATE), "Paid Off").otherwise("Active"))
    .select(
        "loan_id",
        "application_id",
        "party_id",
        "product_id",
        "loan_type",
        "origination_date",
        "maturity_date",
        "term_months",
        "original_principal_usd",
        "interest_rate_pct",
        "status",
        "credit_score",
        "risk_rating",
        "region",
        "state",
    )
)

write_table(consumer_loans, "consumer_loans", "Booked consumer loans with product-specific terms and pricing.")

loan_collateral = (
    spark.table(f"{RETAIL}.`consumer_loans`")
    .filter(F.col("loan_type").isin("Mortgage", "Auto Loan", "HELOC"))
    .withColumn("collateral_id", F.concat(F.lit("COL-"), F.substring("loan_id", 5, 7)))
    .withColumn("collateral_type", F.when(F.col("loan_type") == "Auto Loan", "Vehicle").otherwise("Residential Property"))
    .withColumn("appraised_value_usd", F.round(F.col("original_principal_usd") * (1.10 + stable_fraction("loan_id", F.lit("appraisal")) * 0.55), 2))
    .withColumn("loan_to_value_pct", F.round(F.col("original_principal_usd") * 100.0 / F.col("appraised_value_usd"), 1))
    .select("collateral_id", "loan_id", "collateral_type", "appraised_value_usd", "loan_to_value_pct", "state")
)

write_table(loan_collateral, "loan_collateral", "Vehicle and residential collateral linked to secured consumer loans.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Payment schedules and actual payments

# COMMAND ----------

loan_payment_schedule = (
    spark.table(f"{RETAIL}.`consumer_loans`")
    .withColumn("installment_number", F.explode(F.sequence(F.lit(1), F.col("term_months"))))
    .withColumn("due_date", F.add_months("origination_date", F.col("installment_number")))
    .filter(F.col("due_date") <= F.lit(AS_OF_DATE))
    .withColumn("scheduled_principal_usd", F.round(F.col("original_principal_usd") / F.col("term_months"), 2))
    .withColumn("scheduled_interest_usd", F.round(F.col("original_principal_usd") * F.col("interest_rate_pct") / 1200.0 * (1.0 - (F.col("installment_number") - 1.0) / F.col("term_months")), 2))
    .withColumn("scheduled_payment_usd", F.round(F.col("scheduled_principal_usd") + F.col("scheduled_interest_usd"), 2))
    .withColumn("schedule_id", F.concat(F.lit("SCH-"), F.substring("loan_id", 5, 7), F.lit("-"), F.lpad("installment_number", 3, "0")))
    .select(
        "schedule_id",
        "loan_id",
        "party_id",
        "installment_number",
        "due_date",
        "scheduled_principal_usd",
        "scheduled_interest_usd",
        "scheduled_payment_usd",
    )
)

write_table(loan_payment_schedule, "loan_payment_schedule", "Contractual consumer-loan payment schedule through the as-of date.")

schedule_with_loan = spark.table(f"{RETAIL}.`loan_payment_schedule`").join(
    spark.table(f"{RETAIL}.`consumer_loans`").select("loan_id", "risk_rating", "region"),
    "loan_id",
)

loan_payments = (
    schedule_with_loan.withColumn("u_pay", stable_fraction("schedule_id", F.lit("loan-payment")))
    .withColumn(
        "stress_multiplier",
        F.when((F.col("region") == "Southeast") & (F.year("due_date") == 2025), 1.7).otherwise(1.0),
    )
    .withColumn(
        "days_late",
        F.when(
            F.col("u_pay") * F.col("stress_multiplier") > 0.96,
            F.lit(60) + F.pmod(F.xxhash64("schedule_id", F.lit("late")), F.lit(61)),
        )
        .when(F.col("u_pay") * F.col("stress_multiplier") > 0.88, F.lit(30))
        .when(F.col("u_pay") > 0.72, F.pmod(F.xxhash64("schedule_id", F.lit("minor-late")), F.lit(15)))
        .otherwise(0)
        .cast("int"),
    )
    .withColumn("payment_date", F.least(F.lit(AS_OF_DATE), F.date_add("due_date", F.col("days_late"))))
    .withColumn("payment_status", F.when(F.col("days_late") >= 60, "Missed").when(F.col("days_late") >= 30, "Late").otherwise("Paid"))
    .withColumn("payment_amount_usd", F.when(F.col("payment_status") == "Missed", 0.0).when(F.col("payment_status") == "Late", F.round(F.col("scheduled_payment_usd") * 0.75, 2)).otherwise(F.col("scheduled_payment_usd")))
    .withColumn("payment_id", F.concat(F.lit("LPY-"), F.substring("schedule_id", 5, 11)))
    .select(
        "payment_id",
        "schedule_id",
        "loan_id",
        "party_id",
        "due_date",
        "payment_date",
        "payment_status",
        "days_late",
        "payment_amount_usd",
        "scheduled_payment_usd",
    )
)

write_table(loan_payments, "loan_payments", "Actual loan payments with a 2025 Southeast delinquency stress pattern.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delinquency and collections

# COMMAND ----------

months = spark.table(f"{CORE}.`bank_calendar`").select("month_start").distinct()
loan_months = (
    spark.table(f"{RETAIL}.`consumer_loans`")
    .crossJoin(months)
    .filter(F.col("month_start") >= F.trunc("origination_date", "month"))
    .filter(F.col("month_start") <= F.trunc(F.least(F.lit(AS_OF_DATE), "maturity_date"), "month"))
)

monthly_payment_health = (
    spark.table(f"{RETAIL}.`loan_payments`")
    .withColumn("month_start", F.trunc("due_date", "month"))
    .groupBy("loan_id", "month_start")
    .agg(
        F.max("days_late").alias("days_past_due"),
        F.sum("scheduled_payment_usd").alias("scheduled_payment_usd"),
        F.sum("payment_amount_usd").alias("actual_payment_usd"),
    )
)

delinquency_snapshots = (
    loan_months.join(monthly_payment_health, ["loan_id", "month_start"], "left")
    .fillna({"days_past_due": 0, "scheduled_payment_usd": 0.0, "actual_payment_usd": 0.0})
    .withColumn("months_on_book", F.greatest(F.lit(1), F.months_between(F.col("month_start"), F.trunc("origination_date", "month")).cast("int") + 1))
    .withColumn("scheduled_principal_to_date", F.least(F.col("original_principal_usd"), F.col("original_principal_usd") * F.col("months_on_book") / F.col("term_months")))
    .withColumn("outstanding_principal_usd", F.round(F.greatest(F.lit(0.0), F.col("original_principal_usd") - F.col("scheduled_principal_to_date")), 2))
    .withColumn(
        "delinquency_bucket",
        F.when(F.col("days_past_due") >= 90, "90+")
        .when(F.col("days_past_due") >= 60, "60-89")
        .when(F.col("days_past_due") >= 30, "30-59")
        .otherwise("Current"),
    )
    .withColumn("snapshot_date", F.last_day("month_start"))
    .select(
        "snapshot_date",
        "month_start",
        "loan_id",
        "party_id",
        "product_id",
        "loan_type",
        "region",
        "risk_rating",
        "outstanding_principal_usd",
        "scheduled_payment_usd",
        "actual_payment_usd",
        "days_past_due",
        "delinquency_bucket",
    )
)

write_table(delinquency_snapshots, "delinquency_snapshots", "One row per active loan-month with outstanding principal and delinquency status.")

collection_actions = (
    spark.table(f"{RETAIL}.`delinquency_snapshots`")
    .filter(F.col("days_past_due") >= 30)
    .withColumn("collection_action_id", F.concat(F.lit("ACT-"), F.substring("loan_id", 5, 7), F.lit("-"), F.date_format("month_start", "yyyyMM")))
    .withColumn("action_date", F.least(F.lit(AS_OF_DATE), F.date_add("snapshot_date", 2)))
    .withColumn("action_type", F.when(F.col("days_past_due") >= 90, "Loss Mitigation Review").when(F.col("days_past_due") >= 60, "Collector Call").otherwise("Payment Reminder"))
    .withColumn("collector_id", F.concat(F.lit("EMP-"), F.lpad((F.pmod(F.xxhash64("loan_id", "month_start"), F.lit(EMPLOYEE_COUNT)) + 1).cast("string"), 5, "0")))
    .withColumn("promise_to_pay_amount_usd", F.round(F.col("scheduled_payment_usd") * (0.50 + stable_fraction("loan_id", "month_start", F.lit("promise")) * 0.50), 2))
    .select("collection_action_id", "loan_id", "party_id", "action_date", "action_type", "collector_id", "days_past_due", "promise_to_pay_amount_usd")
)

write_table(collection_actions, "collection_actions", "Collections workflow actions linked to delinquent loan-month snapshots.")

print(f"Consumer lending generation complete: {CATALOG}.{RETAIL_SCHEMA}")
dbutils.notebook.exit(
    json.dumps(
        {
            "schema": f"{CATALOG}.{RETAIL_SCHEMA}",
            "loan_applications": APPLICATION_COUNT,
            "booked_loans": "derived from approved applications",
        }
    )
)

# Databricks notebook source

# MAGIC %md
# MAGIC # Bigly Bank — Fraud, AML, and KYC
# MAGIC
# MAGIC Generates alerts and investigations from actual RETAIL and COMMERCIAL
# MAGIC transactions, plus KYC reviews, case actions, losses, and recoveries.

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
COMMERCIAL_SCHEMA = f"{SCHEMA_PREFIX}_commercial"
RISK_SCHEMA = f"{SCHEMA_PREFIX}_risk"
CORE = f"`{CATALOG}`.`{CORE_SCHEMA}`"
RETAIL = f"`{CATALOG}`.`{RETAIL_SCHEMA}`"
COMMERCIAL = f"`{CATALOG}`.`{COMMERCIAL_SCHEMA}`"
RISK = f"`{CATALOG}`.`{RISK_SCHEMA}`"

EMPLOYEE_COUNT = 800
KYC_REVIEW_COUNT = 10_000
PARTY_COUNT = 25_000

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {RISK}")


def stable_fraction(*columns):
    return F.pmod(F.xxhash64(*columns, F.lit(SEED)), F.lit(1_000_000)) / F.lit(
        1_000_000.0
    )


def write_table(df, table_name, comment):
    full_name = f"{RISK}.`{table_name}`"
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
# MAGIC ## Transaction alerts

# COMMAND ----------

deposit_candidates = spark.table(f"{RETAIL}.`deposit_transactions`").select(
    "transaction_id",
    "transaction_date",
    "party_id",
    "account_id",
    F.col("absolute_amount_usd").alias("transaction_amount_usd"),
    "channel",
    F.lit(False).alias("is_international"),
    F.lit(False).alias("source_fraud_signal"),
    F.lit(0.0).alias("source_risk_score"),
    F.lit("Deposit").alias("source_domain"),
)

card_candidates = spark.table(f"{RETAIL}.`card_transactions`").select(
    "transaction_id",
    "transaction_date",
    "party_id",
    "account_id",
    F.col("amount_usd").alias("transaction_amount_usd"),
    "channel",
    "is_international",
    F.col("is_suspected_fraud").alias("source_fraud_signal"),
    F.col("risk_score").alias("source_risk_score"),
    F.lit("Card").alias("source_domain"),
)

commercial_candidates = spark.table(f"{COMMERCIAL}.`commercial_transactions`").select(
    "transaction_id",
    "transaction_date",
    "party_id",
    "account_id",
    F.col("absolute_amount_usd").alias("transaction_amount_usd"),
    "channel",
    "is_international",
    F.lit(False).alias("source_fraud_signal"),
    F.lit(0.0).alias("source_risk_score"),
    F.lit("Commercial").alias("source_domain"),
)

transaction_candidates = deposit_candidates.unionByName(card_candidates).unionByName(commercial_candidates)

transaction_alerts = (
    transaction_candidates.filter(
        F.col("source_fraud_signal")
        | (F.col("transaction_amount_usd") >= 10_000)
        | (F.col("is_international") & (F.col("transaction_amount_usd") >= 2_500))
        | (stable_fraction("transaction_id", F.lit("alert-sample")) < 0.006)
    )
    .withColumn("alert_id", F.concat(F.lit("ALT-"), F.upper(F.substring(F.sha2(F.concat_ws("|", "source_domain", "transaction_id"), 256), 1, 16))))
    .withColumn(
        "alert_typology",
        F.when(F.col("source_fraud_signal"), "Suspected Card Fraud")
        .when(F.col("is_international") & (F.col("transaction_amount_usd") >= 2_500), "Cross-Border Funds Movement")
        .when(F.col("transaction_amount_usd") >= 10_000, "Large Value Transaction")
        .when(F.col("channel") == "Wire", "Wire Monitoring")
        .otherwise("Behavioral Anomaly"),
    )
    .withColumn(
        "alert_score",
        F.round(
            F.least(
                F.lit(100.0),
                F.greatest(F.col("source_risk_score"), F.lit(35.0))
                + F.when(F.col("transaction_amount_usd") >= 10_000, 25.0).otherwise(0.0)
                + F.when(F.col("is_international"), 20.0).otherwise(0.0)
                + stable_fraction("transaction_id", F.lit("alert-score")) * 20.0,
            ),
            1,
        ),
    )
    .withColumn("alert_date", F.col("transaction_date"))
    .withColumn("status", F.when(F.col("alert_score") >= 82.0, "Escalated").when(F.col("alert_score") >= 65.0, "Investigating").otherwise("Closed - No Issue"))
    .withColumn("assigned_investigator_id", F.concat(F.lit("EMP-"), F.lpad((F.pmod(F.xxhash64("alert_id", F.lit("investigator")), F.lit(EMPLOYEE_COUNT)) + 1).cast("string"), 5, "0")))
    .select("alert_id", "alert_date", "transaction_id", "source_domain", "party_id", "account_id", "transaction_amount_usd", "channel", "is_international", "alert_typology", "alert_score", "status", "assigned_investigator_id")
)

write_table(transaction_alerts, "transaction_alerts", "Fraud and AML alerts derived from actual retail and commercial transaction signals.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Investigation cases and actions

# COMMAND ----------

fraud_cases = (
    spark.table(f"{RISK}.`transaction_alerts`")
    .filter((F.col("alert_typology") == "Suspected Card Fraud") | ((F.col("source_domain") == "Card") & (F.col("alert_score") >= 75)))
    .withColumn("fraud_case_id", F.concat(F.lit("FRC-"), F.substring("alert_id", 5, 16)))
    .withColumn("opened_date", F.col("alert_date"))
    .withColumn("case_status", F.when(F.col("alert_score") >= 88, "Confirmed Fraud").when(F.col("alert_score") >= 78, "Open Investigation").otherwise("False Positive"))
    .withColumn("closed_date", F.when(F.col("case_status") != "Open Investigation", F.least(F.lit(AS_OF_DATE), F.date_add("opened_date", 12))).cast("date"))
    .withColumn("fraud_type", F.when(F.col("is_international"), "Cross-Border Card Fraud").otherwise("Card Not Present Fraud"))
    .select("fraud_case_id", "alert_id", "transaction_id", "party_id", "account_id", "opened_date", "closed_date", "fraud_type", "case_status", "transaction_amount_usd", "assigned_investigator_id")
)

write_table(fraud_cases, "fraud_cases", "Fraud investigations linked to transaction alerts and originating card activity.")

aml_cases = (
    spark.table(f"{RISK}.`transaction_alerts`")
    .filter(F.col("alert_typology").isin("Cross-Border Funds Movement", "Large Value Transaction", "Wire Monitoring") & (F.col("alert_score") >= 68))
    .withColumn("aml_case_id", F.concat(F.lit("AML-"), F.substring("alert_id", 5, 16)))
    .withColumn("opened_date", F.col("alert_date"))
    .withColumn("case_status", F.when(F.col("alert_score") >= 90, "SAR Filed").when(F.col("alert_score") >= 78, "Enhanced Review").otherwise("Closed - No Issue"))
    .withColumn("closed_date", F.when(F.col("case_status") != "Enhanced Review", F.least(F.lit(AS_OF_DATE), F.date_add("opened_date", 25))).cast("date"))
    .withColumn("typology", F.col("alert_typology"))
    .select("aml_case_id", "alert_id", "transaction_id", "party_id", "account_id", "opened_date", "closed_date", "typology", "case_status", "transaction_amount_usd", "assigned_investigator_id")
)

write_table(aml_cases, "aml_cases", "AML investigations linked to large-value, wire, and cross-border alerts.")

fraud_actions = (
    spark.table(f"{RISK}.`fraud_cases`")
    .select(
        F.col("fraud_case_id").alias("case_id"),
        F.lit("Fraud").alias("case_type"),
        "party_id",
        "opened_date",
        "assigned_investigator_id",
        "case_status",
    )
    .withColumn("action_type", F.when(F.col("case_status") == "Confirmed Fraud", "Block Card and Issue Credit").when(F.col("case_status") == "False Positive", "Close Alert").otherwise("Request Customer Verification"))
)
aml_actions = (
    spark.table(f"{RISK}.`aml_cases`")
    .select(
        F.col("aml_case_id").alias("case_id"),
        F.lit("AML").alias("case_type"),
        "party_id",
        "opened_date",
        "assigned_investigator_id",
        "case_status",
    )
    .withColumn("action_type", F.when(F.col("case_status") == "SAR Filed", "File Suspicious Activity Report").when(F.col("case_status") == "Enhanced Review", "Request Source of Funds").otherwise("Close Alert"))
)

case_actions = (
    fraud_actions.unionByName(aml_actions)
    .withColumn("case_action_id", F.concat(F.lit("CAX-"), F.upper(F.substring(F.sha2(F.concat_ws("|", "case_type", "case_id"), 256), 1, 16))))
    .withColumn("action_date", F.least(F.lit(AS_OF_DATE), F.date_add("opened_date", (stable_fraction("case_id", F.lit("action-days")) * 8 + 1).cast("int"))))
    .withColumn("action_outcome", F.when(F.col("case_status").isin("Confirmed Fraud", "SAR Filed"), "Escalated and Documented").when(F.col("case_status").isin("False Positive", "Closed - No Issue"), "Closed").otherwise("Pending"))
    .select("case_action_id", "case_id", "case_type", "party_id", "action_date", "action_type", "action_outcome", "assigned_investigator_id")
)

write_table(case_actions, "case_actions", "Investigator actions and outcomes across fraud and AML cases.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## KYC reviews and fraud losses

# COMMAND ----------

party_lookup = spark.table(f"{CORE}.`parties`").select("party_id", "party_type", "risk_rating", "region")
kyc_reviews = (
    spark.range(KYC_REVIEW_COUNT, numPartitions=8)
    .withColumn("review_number", F.col("id") + 1)
    .withColumn("party_id", F.concat(F.lit("PTY-"), F.lpad((F.pmod(F.xxhash64("id", F.lit("kyc-party")), F.lit(PARTY_COUNT)) + 1).cast("string"), 6, "0")))
    .join(party_lookup, "party_id")
    .withColumn("review_id", F.concat(F.lit("KYC-"), F.lpad(F.col("review_number").cast("string"), 7, "0")))
    .withColumn("review_date", F.date_add(F.lit(date(2023, 1, 1)), F.pmod(F.xxhash64("id", F.lit("kyc-date")), F.lit((AS_OF_DATE - date(2023, 1, 1)).days + 1)).cast("int")))
    .withColumn("review_type", F.when(F.col("risk_rating") == "High", "Enhanced Due Diligence").when(F.col("party_type") == "Business", "Beneficial Ownership Review").otherwise("Periodic Review"))
    .withColumn("risk_score", F.round(F.when(F.col("risk_rating") == "High", 75.0).when(F.col("risk_rating") == "Medium", 45.0).otherwise(15.0) + stable_fraction("id", F.lit("kyc-score")) * 20.0, 1))
    .withColumn("outcome", F.when(F.col("risk_score") >= 82, "Escalated").when(F.col("risk_score") >= 55, "Restrictions Applied").otherwise("Approved"))
    .withColumn("next_review_date", F.add_months("review_date", F.when(F.col("risk_rating") == "High", 12).when(F.col("risk_rating") == "Medium", 24).otherwise(36)))
    .withColumn("is_overdue", (F.col("next_review_date") < F.lit(AS_OF_DATE)) & (F.col("outcome") != "Escalated"))
    .select("review_id", "party_id", "review_date", "review_type", "risk_score", "outcome", "next_review_date", "is_overdue", "party_type", "risk_rating", "region")
)

write_table(kyc_reviews, "kyc_reviews", "Periodic and enhanced KYC reviews across person, business, and household parties.")

fraud_loss_events = (
    spark.table(f"{RISK}.`fraud_cases`")
    .filter(F.col("case_status") == "Confirmed Fraud")
    .withColumn("loss_event_id", F.concat(F.lit("FLS-"), F.substring("fraud_case_id", 5, 16)))
    .withColumn("loss_date", F.coalesce("closed_date", "opened_date"))
    .withColumn("gross_loss_usd", F.col("transaction_amount_usd"))
    .withColumn("recovery_usd", F.round(F.col("gross_loss_usd") * stable_fraction("fraud_case_id", F.lit("recovery")) * 0.65, 2))
    .withColumn("net_loss_usd", F.round(F.col("gross_loss_usd") - F.col("recovery_usd"), 2))
    .select("loss_event_id", "fraud_case_id", "transaction_id", "party_id", "loss_date", "gross_loss_usd", "recovery_usd", "net_loss_usd", "fraud_type")
)

write_table(fraud_loss_events, "fraud_loss_events", "Confirmed fraud losses and recoveries tied to cases and originating transactions.")

print(f"Financial crime generation complete: {CATALOG}.{RISK_SCHEMA}")
dbutils.notebook.exit(
    json.dumps(
        {
            "schema": f"{CATALOG}.{RISK_SCHEMA}",
            "transaction_alerts": "derived from domain transactions",
            "kyc_reviews": KYC_REVIEW_COUNT,
        }
    )
)

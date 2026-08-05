# Databricks notebook source

# MAGIC %md
# MAGIC # Bigly Bank — Small Business and Commercial Banking
# MAGIC
# MAGIC Generates business profiles, commercial deposits, transactions, credit
# MAGIC facilities, loans, covenant snapshots, and merchant settlements.

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
COMMERCIAL_SCHEMA = f"{SCHEMA_PREFIX}_commercial"
CORE = f"`{CATALOG}`.`{CORE_SCHEMA}`"
COMMERCIAL = f"`{CATALOG}`.`{COMMERCIAL_SCHEMA}`"
START_DATE = date(AS_OF_DATE.year - 2, 1, 1)

BUSINESS_COUNT = 3_000
DEPOSIT_ACCOUNT_COUNT = 5_000
TRANSACTION_COUNT = 150_000
FACILITY_COUNT = 2_200
COMMERCIAL_LOAN_COUNT = 1_800
MERCHANT_SETTLEMENT_COUNT = 100_000
EMPLOYEE_COUNT = 800

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {COMMERCIAL}")


def stable_fraction(*columns):
    return F.pmod(F.xxhash64(*columns, F.lit(SEED)), F.lit(1_000_000)) / F.lit(
        1_000_000.0
    )


def write_table(df, table_name, comment):
    full_name = f"{COMMERCIAL}.`{table_name}`"
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
# MAGIC ## Business profiles and deposits

# COMMAND ----------

business_profiles = (
    spark.table(f"{CORE}.`parties`")
    .filter(F.col("party_type") == "Business")
    .withColumn("business_profile_id", F.concat(F.lit("BIZ-"), F.substring("party_id", 5, 6)))
    .withColumn(
        "legal_structure",
        F.when(stable_fraction("party_id", F.lit("legal")) < 0.52, "LLC")
        .when(stable_fraction("party_id", F.lit("legal")) < 0.76, "S Corporation")
        .when(stable_fraction("party_id", F.lit("legal")) < 0.92, "C Corporation")
        .otherwise("Partnership"),
    )
    .withColumn(
        "annual_revenue_usd",
        F.round(F.pow(F.lit(10.0), F.lit(5.2) + stable_fraction("party_id", F.lit("revenue")) * 3.3), 2),
    )
    .withColumn("employee_count", F.greatest(F.lit(2), F.floor(F.col("annual_revenue_usd") / (80_000 + stable_fraction("party_id", F.lit("employees")) * 140_000)).cast("int")))
    .withColumn("years_in_business", F.lit(1) + F.pmod(F.xxhash64("party_id", F.lit("years")), F.lit(40)))
    .withColumn(
        "relationship_manager_id",
        F.concat(
            F.lit("EMP-"),
            F.lpad((F.pmod(F.xxhash64("party_id", F.lit("rm")), F.lit(EMPLOYEE_COUNT)) + 1).cast("string"), 5, "0"),
        ),
    )
    .select(
        "business_profile_id",
        "party_id",
        F.col("party_name").alias("business_name"),
        "customer_segment",
        "industry",
        "legal_structure",
        "annual_revenue_usd",
        "employee_count",
        "years_in_business",
        "risk_rating",
        "relationship_manager_id",
        "state",
        "region",
        "created_date",
        "is_active",
    )
)

write_table(business_profiles, "business_profiles", "Commercial customer profiles derived from CORE business parties.")

commercial_deposit_accounts = (
    spark.range(DEPOSIT_ACCOUNT_COUNT, numPartitions=8)
    .withColumn("account_number", F.col("id") + 1)
    .withColumn(
        "party_id",
        F.concat(
            F.lit("PTY-"),
            F.lpad((F.lit(20_001) + F.pmod(F.xxhash64("id", F.lit("business-owner")), F.lit(BUSINESS_COUNT))).cast("string"), 6, "0"),
        ),
    )
    .join(spark.table(f"{COMMERCIAL}.`business_profiles`").select("party_id", "annual_revenue_usd", "industry", "risk_rating", "region"), "party_id")
    .withColumn("u1", stable_fraction("id", F.lit("commercial-deposit-u1")))
    .withColumn("account_id", F.concat(F.lit("CDA-"), F.lpad(F.col("account_number").cast("string"), 7, "0")))
    .withColumn("product_id", F.when(F.col("u1") < 0.72, "PRD-COMM-DEP-01").otherwise("PRD-COMM-DEP-02"))
    .withColumn("account_type", F.when(F.col("product_id") == "PRD-COMM-DEP-01", "Business Checking").otherwise("Business Savings"))
    .withColumn("open_date", F.date_add(F.lit(START_DATE), F.floor(F.col("u1") * F.greatest(F.datediff(F.lit(AS_OF_DATE), F.lit(START_DATE)) - 45, F.lit(1))).cast("int")))
    .withColumn("status", F.when(stable_fraction("id", F.lit("commercial-status")) < 0.95, "Active").otherwise("Dormant"))
    .withColumn("opening_balance_usd", F.round(F.greatest(F.lit(2_500.0), F.col("annual_revenue_usd") * (0.01 + stable_fraction("id", F.lit("commercial-balance")) * 0.08)), 2))
    .select("account_id", "party_id", "product_id", "account_type", "open_date", "status", "opening_balance_usd", "industry", "risk_rating", "region")
)

write_table(commercial_deposit_accounts, "commercial_deposit_accounts", "Business checking and savings accounts linked to commercial parties.")

account_lookup = spark.table(f"{COMMERCIAL}.`commercial_deposit_accounts`")
commercial_transaction_base = (
    spark.range(TRANSACTION_COUNT, numPartitions=16)
    .withColumn("transaction_number", F.col("id") + 1)
    .withColumn(
        "account_id",
        F.concat(
            F.lit("CDA-"),
            F.lpad((F.pmod(F.xxhash64("id", F.lit("commercial-account")), F.lit(DEPOSIT_ACCOUNT_COUNT)) + 1).cast("string"), 7, "0"),
        ),
    )
    .join(account_lookup, "account_id")
    .withColumn("u_date", stable_fraction("transaction_number", F.lit("commercial-date")))
    .withColumn("u_type", stable_fraction("transaction_number", F.lit("commercial-type")))
    .withColumn("u_amount", stable_fraction("transaction_number", F.lit("commercial-amount")))
    .withColumn("transaction_date", F.date_add("open_date", F.floor(F.col("u_date") * (F.datediff(F.lit(AS_OF_DATE), "open_date") + 1)).cast("int")))
    .withColumn(
        "transaction_type",
        F.when(F.col("u_type") < 0.35, "Customer Receipt")
        .when(F.col("u_type") < 0.58, "Supplier Payment")
        .when(F.col("u_type") < 0.75, "Payroll")
        .when(F.col("u_type") < 0.88, "Wire Transfer")
        .when(F.col("u_type") < 0.96, "Tax Payment")
        .otherwise("Fee"),
    )
    .withColumn("channel", F.when(F.col("u_type") < 0.52, "ACH").when(F.col("u_type") < 0.82, "Online").otherwise("Wire"))
    .withColumn("absolute_amount_usd", F.round(F.when(F.col("transaction_type") == "Fee", 45.0).otherwise(F.pow(F.lit(10.0), F.lit(2.4) + F.col("u_amount") * 4.1)), 2))
    .withColumn("signed_amount_usd", F.when(F.col("transaction_type") == "Customer Receipt", F.col("absolute_amount_usd")).otherwise(-F.col("absolute_amount_usd")))
    .withColumn("is_international", (F.col("transaction_type") == "Wire Transfer") & (stable_fraction("transaction_number", F.lit("international")) < 0.18))
    .withColumn("status", F.when(stable_fraction("transaction_number", F.lit("commercial-txn-status")) < 0.98, "Posted").otherwise("Reversed"))
    .withColumn("transaction_id", F.concat(F.lit("BTR-"), F.lpad(F.col("transaction_number").cast("string"), 9, "0")))
)

ledger_window = Window.partitionBy("account_id").orderBy("transaction_date", "transaction_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
commercial_transactions = (
    commercial_transaction_base.withColumn("balance_after_usd", F.round(F.col("opening_balance_usd") + F.sum("signed_amount_usd").over(ledger_window), 2))
    .select(
        "transaction_id",
        "transaction_date",
        "account_id",
        "party_id",
        "transaction_type",
        "channel",
        "signed_amount_usd",
        "absolute_amount_usd",
        "is_international",
        "status",
        "balance_after_usd",
        "industry",
        "region",
    )
)

write_table(commercial_transactions, "commercial_transactions", "Signed commercial deposit transactions with cash-flow and international-wire behavior.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Credit facilities, loans, and covenants

# COMMAND ----------

business_lookup = spark.table(f"{COMMERCIAL}.`business_profiles`").select(
    "party_id", "annual_revenue_usd", "industry", "risk_rating", "region"
)

credit_facilities = (
    spark.range(FACILITY_COUNT, numPartitions=8)
    .withColumn("facility_number", F.col("id") + 1)
    .withColumn(
        "party_id",
        F.concat(F.lit("PTY-"), F.lpad((F.lit(20_001) + F.pmod(F.xxhash64("id", F.lit("facility-owner")), F.lit(BUSINESS_COUNT))).cast("string"), 6, "0")),
    )
    .join(business_lookup, "party_id")
    .withColumn("facility_id", F.concat(F.lit("FAC-"), F.lpad(F.col("facility_number").cast("string"), 6, "0")))
    .withColumn("product_id", F.when(stable_fraction("id", F.lit("facility-product")) < 0.70, "PRD-COMM-LOC-01").otherwise("PRD-COMM-TERM-01"))
    .withColumn("commitment_amount_usd", F.round(F.greatest(F.lit(50_000.0), F.col("annual_revenue_usd") * (0.08 + stable_fraction("id", F.lit("commitment")) * 0.28)), 2))
    .withColumn("start_date", F.date_add(F.lit(START_DATE), (stable_fraction("id", F.lit("facility-start")) * 500).cast("int")))
    .withColumn("maturity_date", F.add_months("start_date", F.when(F.col("product_id") == "PRD-COMM-LOC-01", 24).otherwise(60)))
    .withColumn("utilization_pct", F.round(F.least(F.lit(98.0), F.lit(15.0) + stable_fraction("id", F.lit("utilization")) * 78.0 + F.when((F.col("region") == "Southeast") & (F.lit(AS_OF_DATE.year) >= 2025), 8.0).otherwise(0.0)), 1))
    .withColumn("outstanding_amount_usd", F.round(F.col("commitment_amount_usd") * F.col("utilization_pct") / 100.0, 2))
    .withColumn("interest_rate_pct", F.round(F.lit(7.0) + stable_fraction("id", F.lit("facility-rate")) * 5.0, 2))
    .withColumn("status", F.when(F.col("maturity_date") <= F.lit(AS_OF_DATE), "Matured").otherwise("Active"))
    .select("facility_id", "party_id", "product_id", "start_date", "maturity_date", "commitment_amount_usd", "outstanding_amount_usd", "utilization_pct", "interest_rate_pct", "status", "industry", "risk_rating", "region")
)

write_table(credit_facilities, "credit_facilities", "Committed commercial credit facilities with utilization and exposure.")

commercial_loans = (
    spark.range(COMMERCIAL_LOAN_COUNT, numPartitions=8)
    .withColumn("loan_number", F.col("id") + 1)
    .withColumn(
        "party_id",
        F.concat(F.lit("PTY-"), F.lpad((F.lit(20_001) + F.pmod(F.xxhash64("id", F.lit("commercial-loan-owner")), F.lit(BUSINESS_COUNT))).cast("string"), 6, "0")),
    )
    .join(business_lookup, "party_id")
    .withColumn("loan_id", F.concat(F.lit("BLN-"), F.lpad(F.col("loan_number").cast("string"), 6, "0")))
    .withColumn("product_id", F.when(stable_fraction("id", F.lit("commercial-loan-product")) < 0.72, "PRD-COMM-TERM-01").otherwise("PRD-COMM-CRE-01"))
    .withColumn("origination_date", F.date_add(F.lit(START_DATE), (stable_fraction("id", F.lit("commercial-origination")) * 650).cast("int")))
    .withColumn("original_principal_usd", F.round(F.greatest(F.lit(75_000.0), F.col("annual_revenue_usd") * (0.10 + stable_fraction("id", F.lit("commercial-principal")) * 0.45)), 2))
    .withColumn("term_months", F.when(F.col("product_id") == "PRD-COMM-CRE-01", 120).otherwise(60))
    .withColumn("maturity_date", F.add_months("origination_date", F.col("term_months")))
    .withColumn("interest_rate_pct", F.round(F.lit(6.5) + stable_fraction("id", F.lit("commercial-rate")) * 4.0, 2))
    .withColumn("outstanding_principal_usd", F.round(F.col("original_principal_usd") * (0.55 + stable_fraction("id", F.lit("commercial-outstanding")) * 0.42), 2))
    .withColumn("status", F.when(F.col("maturity_date") <= F.lit(AS_OF_DATE), "Matured").otherwise("Active"))
    .select("loan_id", "party_id", "product_id", "origination_date", "maturity_date", "term_months", "original_principal_usd", "outstanding_principal_usd", "interest_rate_pct", "status", "industry", "risk_rating", "region")
)

write_table(commercial_loans, "commercial_loans", "Booked commercial term and real-estate loans.")

quarter_months = spark.table(f"{CORE}.`bank_calendar`").select("month_start").distinct().filter(F.month("month_start").isin(3, 6, 9, 12))
covenant_snapshots = (
    spark.table(f"{COMMERCIAL}.`credit_facilities`")
    .crossJoin(quarter_months)
    .filter(F.col("month_start") >= F.trunc("start_date", "month"))
    .filter(F.col("month_start") <= F.trunc(F.least(F.lit(AS_OF_DATE), "maturity_date"), "month"))
    .withColumn("debt_service_coverage_ratio", F.round(F.lit(0.85) + stable_fraction("facility_id", "month_start", F.lit("dscr")) * 1.65 - F.when((F.col("region") == "Southeast") & (F.year("month_start") == 2025), 0.25).otherwise(0.0), 2))
    .withColumn("leverage_ratio", F.round(F.lit(1.5) + stable_fraction("facility_id", "month_start", F.lit("leverage")) * 4.0 + F.when((F.col("region") == "Southeast") & (F.year("month_start") == 2025), 0.6).otherwise(0.0), 2))
    .withColumn("is_in_breach", (F.col("debt_service_coverage_ratio") < 1.10) | (F.col("leverage_ratio") > 4.75))
    .withColumn("snapshot_date", F.last_day("month_start"))
    .withColumn("covenant_snapshot_id", F.concat(F.lit("CVN-"), F.substring("facility_id", 5, 6), F.lit("-"), F.date_format("month_start", "yyyyMM")))
    .select("covenant_snapshot_id", "snapshot_date", "month_start", "facility_id", "party_id", "debt_service_coverage_ratio", "leverage_ratio", "is_in_breach", "industry", "risk_rating", "region")
)

write_table(covenant_snapshots, "covenant_snapshots", "Quarterly commercial covenant tests with a 2025 Southeast stress pattern.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merchant settlements

# COMMAND ----------

merchant_settlements = (
    spark.range(MERCHANT_SETTLEMENT_COUNT, numPartitions=16)
    .withColumn("settlement_number", F.col("id") + 1)
    .withColumn(
        "party_id",
        F.concat(F.lit("PTY-"), F.lpad((F.lit(20_001) + F.pmod(F.xxhash64("id", F.lit("merchant-owner")), F.lit(BUSINESS_COUNT))).cast("string"), 6, "0")),
    )
    .join(business_lookup, "party_id")
    .withColumn("settlement_date", F.date_add(F.lit(START_DATE), F.pmod(F.xxhash64("settlement_number", F.lit("settlement-date")), F.lit((AS_OF_DATE - START_DATE).days + 1)).cast("int")))
    .withColumn("gross_sales_usd", F.round(F.pow(F.lit(10.0), F.lit(2.0) + stable_fraction("settlement_number", F.lit("gross-sales")) * 3.5), 2))
    .withColumn("processing_fee_usd", F.round(F.col("gross_sales_usd") * (0.018 + stable_fraction("settlement_number", F.lit("processing-fee")) * 0.012), 2))
    .withColumn("chargeback_amount_usd", F.when(stable_fraction("settlement_number", F.lit("merchant-chargeback")) < 0.025, F.round(F.col("gross_sales_usd") * 0.08, 2)).otherwise(0.0))
    .withColumn("net_settlement_usd", F.round(F.col("gross_sales_usd") - F.col("processing_fee_usd") - F.col("chargeback_amount_usd"), 2))
    .withColumn("settlement_id", F.concat(F.lit("MST-"), F.lpad(F.col("settlement_number").cast("string"), 9, "0")))
    .select("settlement_id", "settlement_date", "party_id", "gross_sales_usd", "processing_fee_usd", "chargeback_amount_usd", "net_settlement_usd", "industry", "region")
)

write_table(merchant_settlements, "merchant_settlements", "Merchant-acquiring settlement activity with processing fees and chargebacks.")

print(f"Commercial generation complete: {CATALOG}.{COMMERCIAL_SCHEMA}")
dbutils.notebook.exit(
    json.dumps(
        {
            "schema": f"{CATALOG}.{COMMERCIAL_SCHEMA}",
            "businesses": BUSINESS_COUNT,
            "commercial_transactions": TRANSACTION_COUNT,
            "credit_facilities": FACILITY_COUNT,
            "commercial_loans": COMMERCIAL_LOAN_COUNT,
            "merchant_settlements": MERCHANT_SETTLEMENT_COUNT,
        }
    )
)

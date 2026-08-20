# Databricks notebook source

# MAGIC %md
# MAGIC # Bigly Bank — Shared Core Data
# MAGIC
# MAGIC Generates the conformed dimensions used by every banking domain. Run this
# MAGIC notebook before any domain generator. Installs one pinned dependency
# MAGIC (`faker`); `pandas` already ships with Databricks runtimes.

# COMMAND ----------

# MAGIC %pip install faker==40.36.0 --quiet

# COMMAND ----------

# The install above is normally importable right away. On classic compute the
# driver may need a Python restart to see a new package; on serverless a
# restart would reinitialize the whole notebook environment, so restart only
# when the import actually fails. restartPython() ends this cell and execution
# continues with the next cell.
try:
    import faker  # noqa: F401
except ImportError:
    dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from datetime import date
import json

import pandas as pd
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
CORE = f"`{CATALOG}`.`{CORE_SCHEMA}`"
START_DATE = date(AS_OF_DATE.year - 2, 1, 1)

PARTY_COUNT = 25_000
PERSON_COUNT = 20_000
BUSINESS_COUNT = 3_000
HOUSEHOLD_COUNT = 2_000
RELATIONSHIP_COUNT = 15_000
BRANCH_COUNT = 40
EMPLOYEE_COUNT = 800

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CORE}")


def write_table(df, table_name, comment):
    full_name = f"{CORE}.`{table_name}`"
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_name)
    )
    escaped_comment = comment.replace("'", "''")
    spark.sql(f"COMMENT ON TABLE {full_name} IS '{escaped_comment}'")
    print(f"Wrote {full_name}")


def stable_fraction(*columns):
    return F.pmod(F.xxhash64(*columns, F.lit(SEED)), F.lit(1_000_000)) / F.lit(
        1_000_000.0
    )


def choose(values, selector):
    return F.element_at(F.array(*[F.lit(v) for v in values]), selector + F.lit(1))


@F.pandas_udf("string")
def synthetic_party_name(ids: pd.Series, party_types: pd.Series) -> pd.Series:
    from faker import Faker

    fake = Faker("en_US")
    names = []
    for party_id, party_type in zip(ids, party_types):
        fake.seed_instance(SEED + int(party_id))
        if party_type == "Business":
            names.append(fake.company())
        elif party_type == "Household":
            names.append(f"{fake.last_name()} Household")
        else:
            names.append(fake.name())
    return pd.Series(names)


@F.pandas_udf("string")
def synthetic_employee_name(ids: pd.Series) -> pd.Series:
    from faker import Faker

    fake = Faker("en_US")
    names = []
    for employee_id in ids:
        fake.seed_instance(SEED + 100_000 + int(employee_id))
        names.append(fake.name())
    return pd.Series(names)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Parties and relationships

# COMMAND ----------

states = ["NY", "CA", "TX", "FL", "IL", "NJ", "MA", "GA", "NC", "WA"]
regions = [
    "Northeast",
    "West",
    "Southwest",
    "Southeast",
    "Midwest",
    "Northeast",
    "Northeast",
    "Southeast",
    "Southeast",
    "West",
]
industries = [
    "Professional Services",
    "Retail Trade",
    "Healthcare",
    "Manufacturing",
    "Technology",
    "Hospitality",
    "Construction",
    "Transportation",
]

party_base = (
    spark.range(PARTY_COUNT, numPartitions=8)
    .withColumn("party_number", F.col("id") + F.lit(1))
    .withColumn(
        "party_type",
        F.when(F.col("id") < PERSON_COUNT, F.lit("Person"))
        .when(F.col("id") < PERSON_COUNT + BUSINESS_COUNT, F.lit("Business"))
        .otherwise(F.lit("Household")),
    )
    .withColumn("u1", stable_fraction(F.col("id"), F.lit("party-u1")))
    .withColumn("u2", stable_fraction(F.col("id"), F.lit("party-u2")))
)

parties = (
    party_base.withColumn(
        "party_id",
        F.concat(F.lit("PTY-"), F.lpad(F.col("party_number").cast("string"), 6, "0")),
    )
    .withColumn("party_name", synthetic_party_name("party_number", "party_type"))
    .withColumn("state_index", F.pmod(F.xxhash64("id", F.lit(SEED)), F.lit(len(states))))
    .withColumn("state", choose(states, F.col("state_index")))
    .withColumn("region", choose(regions, F.col("state_index")))
    .withColumn(
        "customer_segment",
        F.when(F.col("party_type") == "Business", F.when(F.col("u1") < 0.75, "Small Business").otherwise("Commercial"))
        .when(F.col("party_type") == "Household", F.when(F.col("u1") < 0.60, "Wealth Management").otherwise("Retail"))
        .when(F.col("u1") < 0.08, "Wealth Management")
        .otherwise("Retail"),
    )
    .withColumn(
        "relationship_tier",
        F.when(F.col("u2") < 0.65, "Standard")
        .when(F.col("u2") < 0.92, "Preferred")
        .otherwise("Private Client"),
    )
    .withColumn(
        "income_band",
        F.when(F.col("party_type") != "Person", F.lit(None).cast("string"))
        .when(F.col("u1") < 0.25, "Under $50K")
        .when(F.col("u1") < 0.62, "$50K-$100K")
        .when(F.col("u1") < 0.90, "$100K-$200K")
        .otherwise("$200K+"),
    )
    .withColumn(
        "industry",
        F.when(
            F.col("party_type") == "Business",
            choose(industries, F.pmod(F.xxhash64("id", F.lit("industry")), F.lit(len(industries)))),
        ).otherwise(F.lit(None).cast("string")),
    )
    .withColumn(
        "risk_rating",
        F.when(F.col("u2") < 0.04, "High")
        .when(F.col("u2") < 0.24, "Medium")
        .otherwise("Low"),
    )
    .withColumn(
        "created_date",
        F.date_add(
            F.lit(START_DATE),
            F.floor(F.col("u1") * F.datediff(F.lit(AS_OF_DATE), F.lit(START_DATE))).cast("int"),
        ),
    )
    .withColumn("is_active", F.col("u2") < 0.96)
    .select(
        "party_id",
        "party_type",
        "party_name",
        "customer_segment",
        "relationship_tier",
        "income_band",
        "industry",
        "risk_rating",
        "state",
        "region",
        "created_date",
        "is_active",
    )
)

write_table(
    parties,
    "parties",
    "Conformed synthetic people, businesses, and households used by every Bigly Bank domain.",
)

relationships = (
    spark.range(RELATIONSHIP_COUNT, numPartitions=8)
    .withColumn(
        "relationship_id",
        F.concat(F.lit("REL-"), F.lpad((F.col("id") + 1).cast("string"), 7, "0")),
    )
    .withColumn(
        "relationship_type",
        F.when(F.col("id") < 6_000, "Household Member")
        .when(F.col("id") < 11_000, "Beneficial Owner")
        .otherwise("Authorized Signer"),
    )
    .withColumn(
        "from_number",
        F.pmod(F.xxhash64("id", F.lit(SEED), F.lit("from")), F.lit(PERSON_COUNT)) + 1,
    )
    .withColumn(
        "to_number",
        F.when(
            F.col("relationship_type") == "Household Member",
            F.lit(PERSON_COUNT + BUSINESS_COUNT + 1)
            + F.pmod(F.xxhash64("id", F.lit("household")), F.lit(HOUSEHOLD_COUNT)),
        ).otherwise(
            F.lit(PERSON_COUNT + 1)
            + F.pmod(F.xxhash64("id", F.lit("business")), F.lit(BUSINESS_COUNT))
        ),
    )
    .withColumn(
        "from_party_id",
        F.concat(F.lit("PTY-"), F.lpad(F.col("from_number").cast("string"), 6, "0")),
    )
    .withColumn(
        "to_party_id",
        F.concat(F.lit("PTY-"), F.lpad(F.col("to_number").cast("string"), 6, "0")),
    )
    .withColumn("effective_date", F.lit(START_DATE))
    .withColumn("end_date", F.lit(None).cast("date"))
    .withColumn("ownership_pct", F.when(F.col("relationship_type") == "Beneficial Owner", F.lit(25.0) + F.pmod(F.col("id"), F.lit(76))).otherwise(F.lit(None).cast("double")))
    .select(
        "relationship_id",
        "from_party_id",
        "to_party_id",
        "relationship_type",
        "effective_date",
        "end_date",
        F.col("ownership_pct").cast("double"),
    )
)

write_table(
    relationships,
    "party_relationships",
    "Relationships among people, households, and businesses, including beneficial ownership.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Products, branches, employees, and calendar

# COMMAND ----------

product_rows = [
    ("PRD-DEP-CHK-01", "Bigly Everyday Checking", "Retail", "Deposit", "Checking", 0.10, 0.0),
    ("PRD-DEP-CHK-02", "Bigly Premier Checking", "Retail", "Deposit", "Checking", 0.25, 15.0),
    ("PRD-DEP-SAV-01", "Bigly Basic Savings", "Retail", "Deposit", "Savings", 1.25, 0.0),
    ("PRD-DEP-SAV-02", "Bigly High-Yield Savings", "Retail", "Deposit", "Savings", 4.25, 0.0),
    ("PRD-DEP-CD-01", "Bigly 12-Month CD", "Retail", "Deposit", "Certificate of Deposit", 4.85, 0.0),
    ("PRD-CARD-01", "Bigly Cash Rewards Card", "Retail", "Card", "Credit Card", 20.99, 0.0),
    ("PRD-CARD-02", "Bigly Travel Rewards Card", "Retail", "Card", "Credit Card", 19.49, 95.0),
    ("PRD-CARD-03", "Bigly Secured Card", "Retail", "Card", "Credit Card", 24.99, 0.0),
    ("PRD-LOAN-MTG-01", "Bigly 30-Year Fixed Mortgage", "Retail", "Consumer Lending", "Mortgage", 6.75, 0.0),
    ("PRD-LOAN-MTG-02", "Bigly 15-Year Fixed Mortgage", "Retail", "Consumer Lending", "Mortgage", 6.15, 0.0),
    ("PRD-LOAN-AUTO-01", "Bigly Auto Loan", "Retail", "Consumer Lending", "Auto Loan", 6.50, 0.0),
    ("PRD-LOAN-HELOC-01", "Bigly Home Equity Line", "Retail", "Consumer Lending", "HELOC", 8.25, 50.0),
    ("PRD-LOAN-PERS-01", "Bigly Personal Loan", "Retail", "Consumer Lending", "Personal Loan", 11.50, 0.0),
    ("PRD-COMM-DEP-01", "Bigly Business Checking", "Commercial", "Deposit", "Business Checking", 0.15, 20.0),
    ("PRD-COMM-DEP-02", "Bigly Treasury Savings", "Commercial", "Deposit", "Business Savings", 3.25, 0.0),
    ("PRD-COMM-LOC-01", "Bigly Business Line of Credit", "Commercial", "Commercial Lending", "Line of Credit", 9.25, 100.0),
    ("PRD-COMM-TERM-01", "Bigly Commercial Term Loan", "Commercial", "Commercial Lending", "Term Loan", 7.75, 0.0),
    ("PRD-COMM-CRE-01", "Bigly Commercial Real Estate Loan", "Commercial", "Commercial Lending", "CRE Loan", 7.10, 0.0),
    ("PRD-COMM-MERCH-01", "Bigly Merchant Services", "Commercial", "Payments", "Merchant Services", 0.0, 120.0),
    ("PRD-WM-BROK-01", "Bigly Brokerage", "Wealth", "Investment", "Brokerage", 0.0, 0.0),
    ("PRD-WM-MGD-01", "Bigly Managed Portfolio", "Wealth", "Investment", "Managed Account", 0.0, 250.0),
    ("PRD-WM-IRA-01", "Bigly Traditional IRA", "Wealth", "Investment", "Retirement", 0.0, 0.0),
    ("PRD-WM-ROTH-01", "Bigly Roth IRA", "Wealth", "Investment", "Retirement", 0.0, 0.0),
]

products = spark.createDataFrame(
    product_rows,
    "product_id string, product_name string, business_line string, product_category string, product_type string, base_rate_pct double, annual_fee_usd double",
).withColumn("is_active", F.lit(True)).withColumn("launch_date", F.lit(date(2018, 1, 1)))

write_table(products, "products", "Conformed Bigly Bank product catalog across all business lines.")

branch_states = ["NY", "CA", "TX", "FL", "IL", "NJ", "MA", "GA", "NC", "WA"]
branch_regions = regions
branch_cities = [
    "New York",
    "Los Angeles",
    "Dallas",
    "Miami",
    "Chicago",
    "Newark",
    "Boston",
    "Atlanta",
    "Charlotte",
    "Seattle",
]

branches = (
    spark.range(BRANCH_COUNT, numPartitions=4)
    .withColumn("branch_number", F.col("id") + 1)
    .withColumn("geo_index", F.pmod(F.col("id"), F.lit(len(branch_states))))
    .withColumn("branch_id", F.concat(F.lit("BRN-"), F.lpad(F.col("branch_number").cast("string"), 4, "0")))
    .withColumn("branch_name", F.concat(choose(branch_cities, F.col("geo_index")), F.lit(" Branch "), F.col("branch_number")))
    .withColumn("branch_type", F.when(F.pmod(F.col("id"), F.lit(5)) == 0, "Advisory Center").otherwise("Full Service"))
    .withColumn("state", choose(branch_states, F.col("geo_index")))
    .withColumn("region", choose(branch_regions, F.col("geo_index")))
    .withColumn("city", choose(branch_cities, F.col("geo_index")))
    .withColumn("opened_date", F.date_add(F.lit(date(2000, 1, 1)), (F.col("id") * 137).cast("int")))
    .withColumn("is_active", F.lit(True))
    .select("branch_id", "branch_name", "branch_type", "region", "state", "city", "opened_date", "is_active")
)

write_table(branches, "branches", "Shared branch and advisory-center dimension for Bigly Bank.")

employee_roles = [
    "Branch Banker",
    "Relationship Manager",
    "Wealth Advisor",
    "Underwriter",
    "Collector",
    "Fraud Investigator",
    "AML Investigator",
    "Service Specialist",
]

employees = (
    spark.range(EMPLOYEE_COUNT, numPartitions=8)
    .withColumn("employee_number", F.col("id") + 1)
    .withColumn("employee_id", F.concat(F.lit("EMP-"), F.lpad(F.col("employee_number").cast("string"), 5, "0")))
    .withColumn("employee_name", synthetic_employee_name("employee_number"))
    .withColumn("role", choose(employee_roles, F.pmod(F.xxhash64("id", F.lit(SEED)), F.lit(len(employee_roles)))))
    .withColumn("branch_id", F.concat(F.lit("BRN-"), F.lpad((F.pmod(F.col("id"), F.lit(BRANCH_COUNT)) + 1).cast("string"), 4, "0")))
    .withColumn("hire_date", F.date_add(F.lit(date(2010, 1, 1)), F.pmod(F.xxhash64("id", F.lit("hire")), F.lit(4_500)).cast("int")))
    .withColumn("is_active", stable_fraction(F.col("id"), F.lit("employee-active")) < 0.95)
    .select("employee_id", "employee_name", "role", "branch_id", "hire_date", "is_active")
)

write_table(employees, "employees", "Synthetic Bigly Bank employees and their operational roles.")

calendar_days = (AS_OF_DATE - START_DATE).days + 1
bank_calendar = (
    spark.range(calendar_days, numPartitions=4)
    .withColumn("calendar_date", F.date_add(F.lit(START_DATE), F.col("id").cast("int")))
    .withColumn("calendar_year", F.year("calendar_date"))
    .withColumn("calendar_quarter", F.quarter("calendar_date"))
    .withColumn("calendar_month", F.month("calendar_date"))
    .withColumn("month_start", F.trunc("calendar_date", "month"))
    .withColumn("month_end", F.last_day("calendar_date"))
    .withColumn("day_of_week", F.date_format("calendar_date", "EEEE"))
    .withColumn("is_weekend", F.dayofweek("calendar_date").isin(1, 7))
    .withColumn("is_business_day", ~F.col("is_weekend"))
    .select(
        "calendar_date",
        "calendar_year",
        "calendar_quarter",
        "calendar_month",
        "month_start",
        "month_end",
        "day_of_week",
        "is_weekend",
        "is_business_day",
    )
)

write_table(bank_calendar, "bank_calendar", "Shared calendar dimension for the synthetic analysis window.")

spark.sql(
    f"""
    CREATE OR REPLACE VIEW {CORE}.`customers` AS
    SELECT
      party_id AS customer_id,
      party_name AS customer_name,
      party_type,
      customer_segment,
      relationship_tier,
      income_band,
      industry,
      risk_rating,
      state,
      region,
      created_date AS customer_since_date,
      is_active
    FROM {CORE}.`parties`
    WHERE party_type IN ('Person', 'Business')
    """
)

print(f"CORE generation complete: {CATALOG}.{CORE_SCHEMA}")
dbutils.notebook.exit(
    json.dumps(
        {
            "schema": f"{CATALOG}.{CORE_SCHEMA}",
            "parties": PARTY_COUNT,
            "party_relationships": RELATIONSHIP_COUNT,
            "products": len(product_rows),
            "branches": BRANCH_COUNT,
            "employees": EMPLOYEE_COUNT,
            "calendar_days": calendar_days,
        }
    )
)

# Databricks notebook source
# MAGIC %md
# MAGIC # Bigly Bank — Full Data Generation (Single Notebook)
# MAGIC
# MAGIC Generates the complete Bigly Bank synthetic dataset in one notebook:
# MAGIC shared CORE dimensions, every business domain, curated `vw_` views,
# MAGIC governed `mv_` metric views, and cross-domain validation — in dependency
# MAGIC order, one section (cell group) per phase:
# MAGIC
# MAGIC 1. **Shared Core** — parties, relationships, products, branches, employees, calendar
# MAGIC 2. **Retail Deposits** — accounts, ledger, monthly balances, payments, fees
# MAGIC 3. **Credit Cards** — accounts, cards, transactions, statements, disputes, rewards
# MAGIC 4. **Consumer Lending** — applications, decisions, loans, schedules, delinquency, collections
# MAGIC 5. **Commercial Banking** — profiles, deposits, transactions, facilities, loans, covenants, settlements
# MAGIC 6. **Wealth Management** — accounts, portfolios, securities, holdings, trades, fees, goals
# MAGIC 7. **Service & Branch Operations** — incidents, interactions, requests, complaints, staffing
# MAGIC 8. **Fraud, AML & KYC** — alerts, cases, actions, KYC reviews, losses
# MAGIC 9. **Finance & Treasury** (optional) — FTP, provisions, profitability, ledger, liquidity
# MAGIC 10. **Semantic layer** — curated `vw_` views and governed `mv_` metric views
# MAGIC 11. **Validation** — object existence, keys, lifecycles, reconciliations, story checks
# MAGIC
# MAGIC **How to run:** set `DEFAULT_CATALOG` and `DEFAULT_SCHEMA_PREFIX` in the
# MAGIC Configuration cell below, then click **Run All**. Widget values — and any
# MAGIC parameters passed by a job or parent notebook — override those defaults;
# MAGIC clear a widget to fall back to its default.
# MAGIC
# MAGIC **Why a single notebook?** The previous layout ran each domain as a child
# MAGIC notebook via `dbutils.notebook.run`, which wraps every failure in a generic
# MAGIC `WorkflowException` — the real traceback lived in a separate run that had
# MAGIC to be looked up through the API. Here every phase is a cell in this
# MAGIC notebook: an error stops Run All at the failing cell with the full
# MAGIC traceback inline, and intermediate DataFrames stay in memory for
# MAGIC inspection.
# MAGIC
# MAGIC **Compute:** runs on classic clusters (DBR 17.2+) and serverless notebooks
# MAGIC (environment version 5+). Library installs are pinned and restart-free so
# MAGIC the same notebook works on both.
# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
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

from datetime import date
import json

# =============================================================================
# CONFIGURATION — edit these values, then Run All
# Widget values and parameters passed by a job or parent notebook override
# these defaults; clear a widget to fall back to its default.
# =============================================================================
DEFAULT_CATALOG = ""  # REQUIRED: your Unity Catalog, e.g. "my_catalog"
DEFAULT_SCHEMA_PREFIX = ""  # REQUIRED: schema prefix, e.g. "bigly_bank"
DEFAULT_SEED = "42"  # deterministic seed shared by every child notebook
DEFAULT_AS_OF_DATE = "2025-12-31"  # inclusive end date for generated history
DEFAULT_ENABLE_FINANCE = "false"  # "true" adds the Finance & Treasury domain
# =============================================================================

def widget_value(name, default, label, choices=None):
    """Read a widget, falling back to the configuration default above.

    Widgets keep the notebook parameterizable: a job, a parent notebook, or a
    manual entry in the widget panel overrides the defaults.
    """
    try:
        if choices:
            dbutils.widgets.dropdown(name, default, choices, label)
        else:
            dbutils.widgets.text(name, default, label)
    except Exception:
        pass
    try:
        return dbutils.widgets.get(name).strip() or default
    except Exception:
        return default

CATALOG = widget_value("catalog", DEFAULT_CATALOG, "Unity Catalog (required)")
SCHEMA_PREFIX = widget_value(
    "schema_prefix", DEFAULT_SCHEMA_PREFIX, "Schema prefix (required)"
)
# Convert to typed values here, once: every phase below relies on SEED
# being an int and AS_OF_DATE being a date (the former child notebooks each
# performed this conversion themselves).
SEED = int(widget_value("seed", DEFAULT_SEED, "Deterministic seed"))
AS_OF_DATE = date.fromisoformat(
    widget_value("as_of_date", DEFAULT_AS_OF_DATE, "Inclusive as-of date")
)
ENABLE_FINANCE = (
    widget_value(
        "enable_finance",
        DEFAULT_ENABLE_FINANCE,
        "Generate the optional Finance and Treasury domain",
        ["true", "false"],
    ).lower()
    == "true"
)

# Per-phase summaries, filled as each section completes (replaces the old
# dbutils.notebook.exit values of the former child notebooks).
results = {}

if not CATALOG:
    raise ValueError(
        "catalog is required — set DEFAULT_CATALOG in the Configuration cell "
        "or pass the catalog widget/parameter"
    )
if not SCHEMA_PREFIX:
    raise ValueError(
        "schema_prefix is required — set DEFAULT_SCHEMA_PREFIX in the "
        "Configuration cell or pass the schema_prefix widget/parameter"
    )
if "`" in CATALOG or "`" in SCHEMA_PREFIX:
    raise ValueError("catalog and schema_prefix cannot contain backticks")

print(
    f"catalog={CATALOG} schema_prefix={SCHEMA_PREFIX} seed={SEED} "
    f"as_of_date={AS_OF_DATE} enable_finance={ENABLE_FINANCE}"
)
# COMMAND ----------

# MAGIC %md
# MAGIC # Bigly Bank — Shared Core Data
# MAGIC
# MAGIC Generates the conformed dimensions used by every banking domain. Installs
# MAGIC one pinned dependency (`faker`); `pandas` already ships with Databricks
# MAGIC runtimes.
# COMMAND ----------

from datetime import date
import json

import pandas as pd
from pyspark.sql import functions as F

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
results["core"] =         {
            "schema": f"{CATALOG}.{CORE_SCHEMA}",
            "parties": PARTY_COUNT,
            "party_relationships": RELATIONSHIP_COUNT,
            "products": len(product_rows),
            "branches": BRANCH_COUNT,
            "employees": EMPLOYEE_COUNT,
            "calendar_days": calendar_days,
        }
# COMMAND ----------

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
results["retail_deposits"] =         {
            "schema": f"{CATALOG}.{RETAIL_SCHEMA}",
            "deposit_accounts": ACCOUNT_COUNT,
            "deposit_transactions": TRANSACTION_COUNT,
        }
# COMMAND ----------

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
results["credit_cards"] =         {
            "schema": f"{CATALOG}.{RETAIL_SCHEMA}",
            "card_accounts": CARD_ACCOUNT_COUNT,
            "cards": CARD_COUNT,
            "card_transactions": TRANSACTION_COUNT,
        }
# COMMAND ----------

# MAGIC %md
# MAGIC # Bigly Bank — Consumer Lending
# MAGIC
# MAGIC Generates applications, decisions, booked loans, collateral, schedules,
# MAGIC payments, delinquency snapshots, and collection actions in RETAIL.
# COMMAND ----------

from datetime import date
import json

from pyspark.sql import functions as F

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
results["consumer_lending"] =         {
            "schema": f"{CATALOG}.{RETAIL_SCHEMA}",
            "loan_applications": APPLICATION_COUNT,
            "booked_loans": "derived from approved applications",
        }
# COMMAND ----------

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
results["commercial_banking"] =         {
            "schema": f"{CATALOG}.{COMMERCIAL_SCHEMA}",
            "businesses": BUSINESS_COUNT,
            "commercial_transactions": TRANSACTION_COUNT,
            "credit_facilities": FACILITY_COUNT,
            "commercial_loans": COMMERCIAL_LOAN_COUNT,
            "merchant_settlements": MERCHANT_SETTLEMENT_COUNT,
        }
# COMMAND ----------

# MAGIC %md
# MAGIC # Bigly Bank — Wealth Management
# MAGIC
# MAGIC Generates wealth accounts, portfolios, securities, monthly holdings,
# MAGIC trades, advisory fees, and client goals in the WEALTH schema.
# COMMAND ----------

from datetime import date
import json

from pyspark.sql import functions as F

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
results["wealth_management"] =         {
            "schema": f"{CATALOG}.{WEALTH_SCHEMA}",
            "wealth_accounts": WEALTH_ACCOUNT_COUNT,
            "portfolios": PORTFOLIO_COUNT,
            "holding_snapshots_target": HOLDING_SNAPSHOT_COUNT,
            "trades": TRADE_COUNT,
        }
# COMMAND ----------

# MAGIC %md
# MAGIC # Bigly Bank — Service and Branch Operations
# MAGIC
# MAGIC Generates incidents, interactions, service requests, complaints,
# MAGIC staffing snapshots, and branch monthly performance in OPERATIONS.
# COMMAND ----------

from datetime import date
import json

from pyspark.sql import functions as F

CORE_SCHEMA = f"{SCHEMA_PREFIX}_core"
RETAIL_SCHEMA = f"{SCHEMA_PREFIX}_retail"
OPERATIONS_SCHEMA = f"{SCHEMA_PREFIX}_operations"
CORE = f"`{CATALOG}`.`{CORE_SCHEMA}`"
RETAIL = f"`{CATALOG}`.`{RETAIL_SCHEMA}`"
OPERATIONS = f"`{CATALOG}`.`{OPERATIONS_SCHEMA}`"
START_DATE = date(AS_OF_DATE.year - 2, 1, 1)

INCIDENT_COUNT = 25
SERVICE_REQUEST_COUNT = 50_000
INTERACTION_COUNT = 100_000
PERSON_COUNT = 20_000
BRANCH_COUNT = 40
EMPLOYEE_COUNT = 800

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {OPERATIONS}")

def stable_fraction(*columns):
    return F.pmod(F.xxhash64(*columns, F.lit(SEED)), F.lit(1_000_000)) / F.lit(
        1_000_000.0
    )

def choose(values, selector):
    return F.element_at(F.array(*[F.lit(v) for v in values]), selector + F.lit(1))

def write_table(df, table_name, comment):
    full_name = f"{OPERATIONS}.`{table_name}`"
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
# MAGIC ## Operational incidents
# COMMAND ----------

systems = ["Mobile Banking", "Online Banking", "Payments", "Card Authorization", "CRM", "Branch Network"]
incident_types = ["Service Outage", "Performance Degradation", "Release Defect", "Network Failure"]

operational_incidents = (
    spark.range(INCIDENT_COUNT, numPartitions=2)
    .withColumn("incident_number", F.col("id") + 1)
    .withColumn(
        "incident_id",
        F.when(F.col("id") == 0, "INC-MOBILE-20240115").otherwise(F.concat(F.lit("INC-"), F.lpad(F.col("incident_number").cast("string"), 6, "0"))),
    )
    .withColumn(
        "started_at",
        F.when(F.col("id") == 0, F.to_timestamp(F.lit("2024-01-15 08:15:00"))).otherwise(
            F.to_timestamp(
                F.concat_ws(
                    " ",
                    F.date_add(F.lit(START_DATE), F.pmod(F.xxhash64("id", F.lit("incident-date")), F.lit((AS_OF_DATE - START_DATE).days + 1)).cast("int")),
                    F.lit("09:00:00"),
                )
            )
        ),
    )
    .withColumn("duration_minutes", F.when(F.col("id") == 0, 1_080).otherwise(F.lit(30) + F.pmod(F.xxhash64("id", F.lit("duration")), F.lit(540))))
    .withColumn("ended_at", F.expr("timestampadd(MINUTE, duration_minutes, started_at)"))
    .withColumn("affected_system", F.when(F.col("id") == 0, "Mobile Banking").otherwise(choose(systems, F.pmod(F.xxhash64("id", F.lit("system")), F.lit(len(systems))))))
    .withColumn("incident_type", F.when(F.col("id") == 0, "Service Outage").otherwise(choose(incident_types, F.pmod(F.xxhash64("id", F.lit("incident-type")), F.lit(len(incident_types))))))
    .withColumn("severity", F.when(F.col("id") == 0, "Critical").when(stable_fraction("id", F.lit("severity")) < 0.25, "High").when(stable_fraction("id", F.lit("severity")) < 0.70, "Medium").otherwise("Low"))
    .withColumn("root_cause", F.when(F.col("id") == 0, "Authentication service release defect").otherwise("Synthetic infrastructure or application failure"))
    .withColumn("estimated_impact_usd", F.round(F.when(F.col("id") == 0, 2_350_000.0).otherwise(F.lit(10_000.0) + stable_fraction("id", F.lit("impact")) * 350_000.0), 2))
    .withColumn("status", F.lit("Resolved"))
    .select("incident_id", "started_at", "ended_at", "duration_minutes", "affected_system", "incident_type", "severity", "root_cause", "estimated_impact_usd", "status")
)

write_table(operational_incidents, "operational_incidents", "Operational incidents, including the January 2024 mobile-banking outage narrative.")
# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer interactions, cases, and complaints
# COMMAND ----------

party_lookup = spark.table(f"{CORE}.`parties`").select("party_id", "relationship_tier", "region", "state")

service_requests = (
    spark.range(SERVICE_REQUEST_COUNT, numPartitions=16)
    .withColumn("request_number", F.col("id") + 1)
    .withColumn("u_date", stable_fraction("id", F.lit("request-date")))
    .withColumn("u_category", stable_fraction("id", F.lit("request-category")))
    .withColumn("is_outage_cohort", F.col("u_date") < 0.18)
    .withColumn(
        "request_date",
        F.when(
            F.col("is_outage_cohort"),
            F.date_add(F.lit(date(2024, 1, 15)), F.pmod(F.xxhash64("id", F.lit("outage-day")), F.lit(17)).cast("int")),
        ).otherwise(F.date_add(F.lit(START_DATE), F.pmod(F.xxhash64("id", F.lit("normal-day")), F.lit((AS_OF_DATE - START_DATE).days + 1)).cast("int"))),
    )
    .withColumn(
        "party_id",
        F.concat(F.lit("PTY-"), F.lpad((F.pmod(F.xxhash64("id", F.lit("request-party")), F.lit(PERSON_COUNT)) + 1).cast("string"), 6, "0")),
    )
    .join(party_lookup, "party_id")
    .withColumn("request_id", F.concat(F.lit("SRQ-"), F.lpad(F.col("request_number").cast("string"), 8, "0")))
    .withColumn(
        "category",
        F.when(F.col("is_outage_cohort") & (F.col("u_category") < 0.60), "Complaint")
        .when(F.col("is_outage_cohort"), "Technical Issue")
        .when(F.col("u_category") < 0.25, "Account Inquiry")
        .when(F.col("u_category") < 0.43, "Payment Issue")
        .when(F.col("u_category") < 0.58, "Dispute")
        .when(F.col("u_category") < 0.73, "Complaint")
        .when(F.col("u_category") < 0.88, "Product Inquiry")
        .otherwise("Technical Issue"),
    )
    .withColumn(
        "channel",
        F.when(F.col("is_outage_cohort"), F.when(stable_fraction("id", F.lit("outage-channel")) < 0.58, "Phone").otherwise("Chat"))
        .when(stable_fraction("id", F.lit("channel")) < 0.36, "Phone")
        .when(stable_fraction("id", F.lit("channel")) < 0.65, "Chat")
        .when(stable_fraction("id", F.lit("channel")) < 0.84, "App")
        .otherwise("Branch"),
    )
    .withColumn("incident_id", F.when(F.col("is_outage_cohort"), "INC-MOBILE-20240115").cast("string"))
    .withColumn("branch_id", F.concat(F.lit("BRN-"), F.lpad((F.pmod(F.xxhash64("id", F.lit("service-branch")), F.lit(BRANCH_COUNT)) + 1).cast("string"), 4, "0")))
    .withColumn("assigned_employee_id", F.concat(F.lit("EMP-"), F.lpad((F.pmod(F.xxhash64("id", F.lit("service-agent")), F.lit(EMPLOYEE_COUNT)) + 1).cast("string"), 5, "0")))
    .withColumn("priority", F.when(F.col("category") == "Complaint", "High").when(F.col("category").isin("Payment Issue", "Dispute"), "Medium").otherwise("Low"))
    .withColumn("status", F.when(stable_fraction("id", F.lit("request-status")) < 0.80, "Resolved").when(stable_fraction("id", F.lit("request-status")) < 0.93, "Open").otherwise("Escalated"))
    .withColumn(
        "resolution_time_hours",
        F.when(
            F.col("status") == "Resolved",
            F.round(
                F.when(F.col("is_outage_cohort"), F.lit(18.0) + stable_fraction("id", F.lit("outage-resolution")) * 72.0)
                .when(F.col("priority") == "High", F.lit(8.0) + stable_fraction("id", F.lit("high-resolution")) * 48.0)
                .otherwise(F.lit(1.0) + stable_fraction("id", F.lit("normal-resolution")) * 20.0),
                1,
            ),
        ).cast("double"),
    )
    .withColumn(
        "satisfaction_score",
        F.when(
            F.col("status") == "Resolved",
            F.greatest(
                F.lit(1),
                F.least(
                    F.lit(5),
                    F.floor(F.lit(5.0) - F.coalesce("resolution_time_hours", F.lit(0.0)) / 24.0 - F.when(F.col("category") == "Complaint", 1.0).otherwise(0.0)).cast("int"),
                ),
            ),
        ).cast("int"),
    )
    .select("request_id", "request_date", "party_id", "branch_id", "assigned_employee_id", "channel", "category", "priority", "status", "resolution_time_hours", "satisfaction_score", "incident_id", "relationship_tier", "region", "state")
)

write_table(service_requests, "service_requests", "Customer service cases with outage-linked complaints, SLA performance, and satisfaction.")

complaints = (
    spark.table(f"{OPERATIONS}.`service_requests`")
    .filter(F.col("category") == "Complaint")
    .withColumn("complaint_id", F.concat(F.lit("CMP-"), F.substring("request_id", 5, 8)))
    .withColumn("complaint_reason", F.when(F.col("incident_id").isNotNull(), "Digital Access Failure").when(F.col("channel") == "Branch", "Branch Experience").otherwise("Product or Fee Concern"))
    .withColumn("regulatory_category", F.when(F.col("complaint_reason") == "Digital Access Failure", "Managing the Account").otherwise("Fees or Service"))
    .withColumn("remediation_amount_usd", F.when(F.col("status") == "Resolved", F.round(stable_fraction("request_id", F.lit("remediation")) * 150.0, 2)).otherwise(0.0))
    .select("complaint_id", "request_id", "request_date", "party_id", "branch_id", "complaint_reason", "regulatory_category", "status", "resolution_time_hours", "satisfaction_score", "remediation_amount_usd", "incident_id", "region")
)

write_table(complaints, "complaints", "Formal complaints linked to service requests, incidents, and remediation amounts.")

interaction_topics = ["Account Help", "Payment Help", "Card Help", "Loan Help", "Digital Help", "Product Advice"]
customer_interactions = (
    spark.range(INTERACTION_COUNT, numPartitions=16)
    .withColumn("interaction_number", F.col("id") + 1)
    .withColumn("is_outage_cohort", stable_fraction("id", F.lit("interaction-outage")) < 0.12)
    .withColumn(
        "interaction_date",
        F.when(F.col("is_outage_cohort"), F.date_add(F.lit(date(2024, 1, 15)), F.pmod(F.xxhash64("id", F.lit("interaction-outage-day")), F.lit(17)).cast("int")))
        .otherwise(F.date_add(F.lit(START_DATE), F.pmod(F.xxhash64("id", F.lit("interaction-day")), F.lit((AS_OF_DATE - START_DATE).days + 1)).cast("int"))),
    )
    .withColumn("party_id", F.concat(F.lit("PTY-"), F.lpad((F.pmod(F.xxhash64("id", F.lit("interaction-party")), F.lit(PERSON_COUNT)) + 1).cast("string"), 6, "0")))
    .join(party_lookup, "party_id")
    .withColumn("interaction_id", F.concat(F.lit("INT-"), F.lpad(F.col("interaction_number").cast("string"), 9, "0")))
    .withColumn("channel", F.when(F.col("is_outage_cohort"), "Phone").when(stable_fraction("id", F.lit("interaction-channel")) < 0.38, "Phone").when(stable_fraction("id", F.lit("interaction-channel")) < 0.65, "Chat").when(stable_fraction("id", F.lit("interaction-channel")) < 0.85, "Branch").otherwise("Secure Message"))
    .withColumn("topic", F.when(F.col("is_outage_cohort"), "Digital Help").otherwise(choose(interaction_topics, F.pmod(F.xxhash64("id", F.lit("interaction-topic")), F.lit(len(interaction_topics))))))
    .withColumn("branch_id", F.concat(F.lit("BRN-"), F.lpad((F.pmod(F.xxhash64("id", F.lit("interaction-branch")), F.lit(BRANCH_COUNT)) + 1).cast("string"), 4, "0")))
    .withColumn("employee_id", F.concat(F.lit("EMP-"), F.lpad((F.pmod(F.xxhash64("id", F.lit("interaction-employee")), F.lit(EMPLOYEE_COUNT)) + 1).cast("string"), 5, "0")))
    .withColumn("handle_time_minutes", F.round(F.when(F.col("is_outage_cohort"), 18.0 + stable_fraction("id", F.lit("outage-handle")) * 35.0).otherwise(3.0 + stable_fraction("id", F.lit("normal-handle")) * 18.0), 1))
    .withColumn("outcome", F.when(F.col("is_outage_cohort"), "Follow-up Required").when(stable_fraction("id", F.lit("interaction-outcome")) < 0.78, "Resolved").otherwise("Follow-up Required"))
    .withColumn("incident_id", F.when(F.col("is_outage_cohort"), "INC-MOBILE-20240115").cast("string"))
    .select("interaction_id", "interaction_date", "party_id", "branch_id", "employee_id", "channel", "topic", "handle_time_minutes", "outcome", "incident_id", "relationship_tier", "region")
)

write_table(customer_interactions, "customer_interactions", "Omnichannel customer contacts with outage-driven call volume and handle-time degradation.")
# COMMAND ----------

# MAGIC %md
# MAGIC ## Branch staffing and performance
# COMMAND ----------

branches = spark.table(f"{CORE}.`branches`")
months = spark.table(f"{CORE}.`bank_calendar`").select("month_start").distinct()

branch_staffing_snapshots = (
    branches.crossJoin(months)
    .withColumn("snapshot_date", F.last_day("month_start"))
    .withColumn("budgeted_fte", F.lit(12) + F.pmod(F.xxhash64("branch_id", F.lit("budgeted-fte")), F.lit(24)))
    .withColumn("actual_fte", F.greatest(F.lit(5), F.col("budgeted_fte") - F.pmod(F.xxhash64("branch_id", "month_start", F.lit("vacancy")), F.lit(5))))
    .withColumn("overtime_hours", F.round(F.greatest(F.lit(0.0), (F.col("budgeted_fte") - F.col("actual_fte")) * (15.0 + stable_fraction("branch_id", "month_start", F.lit("overtime")) * 25.0)), 1))
    .withColumn("monthly_staff_cost_usd", F.round(F.col("actual_fte") * (6_500.0 + stable_fraction("branch_id", F.lit("staff-cost")) * 2_500.0) + F.col("overtime_hours") * 45.0, 2))
    .select("snapshot_date", "month_start", "branch_id", "budgeted_fte", "actual_fte", "overtime_hours", "monthly_staff_cost_usd")
)

write_table(branch_staffing_snapshots, "branch_staffing_snapshots", "Monthly branch staffing, vacancies, overtime, and labor cost.")

monthly_requests = (
    spark.table(f"{OPERATIONS}.`service_requests`")
    .withColumn("month_start", F.trunc("request_date", "month"))
    .groupBy("branch_id", "month_start")
    .agg(F.count("*").alias("service_request_count"), F.sum(F.when(F.col("category") == "Complaint", 1).otherwise(0)).alias("complaint_count"))
)
monthly_interactions = (
    spark.table(f"{OPERATIONS}.`customer_interactions`")
    .withColumn("month_start", F.trunc("interaction_date", "month"))
    .groupBy("branch_id", "month_start")
    .agg(F.count("*").alias("interaction_count"), F.sum("handle_time_minutes").alias("total_handle_minutes"))
)
monthly_transactions = (
    spark.table(f"{RETAIL}.`deposit_transactions`")
    .withColumn("month_start", F.trunc("transaction_date", "month"))
    .groupBy("branch_id", "month_start")
    .agg(F.count("*").alias("transaction_count"), F.sum("fee_usd").alias("fee_revenue_usd"))
)

branch_monthly_performance = (
    branches.crossJoin(months)
    .join(monthly_requests, ["branch_id", "month_start"], "left")
    .join(monthly_interactions, ["branch_id", "month_start"], "left")
    .join(monthly_transactions, ["branch_id", "month_start"], "left")
    .join(spark.table(f"{OPERATIONS}.`branch_staffing_snapshots`").select("branch_id", "month_start", "actual_fte", "monthly_staff_cost_usd"), ["branch_id", "month_start"], "left")
    .fillna({"service_request_count": 0, "complaint_count": 0, "interaction_count": 0, "total_handle_minutes": 0.0, "transaction_count": 0, "fee_revenue_usd": 0.0})
    .withColumn("branch_visit_count", F.round(F.col("interaction_count") * 0.55 + F.col("transaction_count") * 0.08).cast("long"))
    .withColumn("non_staff_operating_cost_usd", F.round(F.lit(25_000.0) + stable_fraction("branch_id", "month_start", F.lit("branch-cost")) * 65_000.0, 2))
    .withColumn("total_operating_cost_usd", F.round(F.col("monthly_staff_cost_usd") + F.col("non_staff_operating_cost_usd"), 2))
    .withColumn("snapshot_date", F.last_day("month_start"))
    .select("snapshot_date", "month_start", "branch_id", "branch_name", "branch_type", "region", "state", "actual_fte", "branch_visit_count", "transaction_count", "service_request_count", "complaint_count", "interaction_count", "total_handle_minutes", "fee_revenue_usd", "total_operating_cost_usd")
)

write_table(branch_monthly_performance, "branch_monthly_performance", "Monthly branch activity, customer demand, fee revenue, staffing, and operating cost.")

print(f"Service operations generation complete: {CATALOG}.{OPERATIONS_SCHEMA}")
results["service_operations"] =         {
            "schema": f"{CATALOG}.{OPERATIONS_SCHEMA}",
            "operational_incidents": INCIDENT_COUNT,
            "service_requests": SERVICE_REQUEST_COUNT,
            "customer_interactions": INTERACTION_COUNT,
        }
# COMMAND ----------

# MAGIC %md
# MAGIC # Bigly Bank — Fraud, AML, and KYC
# MAGIC
# MAGIC Generates alerts and investigations from actual RETAIL and COMMERCIAL
# MAGIC transactions, plus KYC reviews, case actions, losses, and recoveries.
# COMMAND ----------

from datetime import date
import json

from pyspark.sql import functions as F

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
results["financial_crime"] =         {
            "schema": f"{CATALOG}.{RISK_SCHEMA}",
            "transaction_alerts": "derived from domain transactions",
            "kyc_reviews": KYC_REVIEW_COUNT,
        }
# COMMAND ----------

# MAGIC %md
# MAGIC # Bigly Bank — Finance and Treasury
# MAGIC
# MAGIC Generates reconciled monthly profitability, funds-transfer pricing,
# MAGIC provisions, liquidity, and general-ledger summaries in FINANCE.
# MAGIC
# MAGIC This is the optional final domain. The Configuration cell's
# MAGIC `enable_finance` setting controls whether it runs.
# COMMAND ----------

# MAGIC %md
# MAGIC ## Finance and Treasury (optional — controlled by the enable_finance setting)
# MAGIC
# MAGIC When enabled, this cell generates funds-transfer pricing, credit-loss
# MAGIC provisions, product profitability, general-ledger summaries, and liquidity
# MAGIC snapshots in FINANCE. When disabled it records a skipped summary.
# COMMAND ----------

from datetime import date
import json

from pyspark.sql import Window
from pyspark.sql import functions as F

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

if ENABLE_FINANCE:

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
    results["finance_treasury"] = {"schema": f"{CATALOG}.{FINANCE_SCHEMA}", "status": "complete"}
else:
    print("enable_finance is false — skipping Finance and Treasury generation.")
    results["finance_treasury"] = {"skipped": True, "reason": "enable_finance is false"}
# COMMAND ----------

# MAGIC %md
# MAGIC # Bigly Bank — Curated Views and Metric Views
# MAGIC
# MAGIC Creates one curated `vw_` view and one governed `mv_` metric view for
# MAGIC each planned Genie domain.
# COMMAND ----------

from datetime import date
import json

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

for schema_name in [
    CORE_SCHEMA,
    RETAIL_SCHEMA,
    COMMERCIAL_SCHEMA,
    WEALTH_SCHEMA,
    OPERATIONS_SCHEMA,
    RISK_SCHEMA,
]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{schema_name}`")
if ENABLE_FINANCE:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FINANCE}")
# COMMAND ----------

# MAGIC %md
# MAGIC ## Curated pre-joined views
# COMMAND ----------

curated_views = {
    f"{RETAIL}.`vw_retail_deposits`": f"""
        SELECT
          s.snapshot_date,
          s.month_start,
          s.account_id,
          s.party_id,
          s.product_id,
          p.product_name,
          s.account_type,
          s.branch_id,
          b.branch_name,
          b.region AS branch_region,
          c.customer_segment,
          c.relationship_tier,
          c.state,
          c.region AS customer_region,
          s.average_balance_usd,
          s.ending_balance_usd,
          s.inflow_usd,
          s.outflow_usd,
          s.net_flow_usd,
          s.fee_revenue_usd,
          s.transaction_count
        FROM {RETAIL}.`deposit_balance_snapshots` s
        JOIN {CORE}.`parties` c ON s.party_id = c.party_id
        JOIN {CORE}.`products` p ON s.product_id = p.product_id
        JOIN {CORE}.`branches` b ON s.branch_id = b.branch_id
    """,
    f"{RETAIL}.`vw_credit_cards`": f"""
        WITH dispute_by_transaction AS (
          SELECT
            transaction_id,
            COUNT(*) AS dispute_count,
            SUM(chargeback_loss_usd) AS chargeback_loss_usd
          FROM {RETAIL}.`card_disputes`
          GROUP BY transaction_id
        )
        SELECT
          t.transaction_id,
          t.transaction_timestamp,
          t.transaction_date,
          DATE_TRUNC('MONTH', t.transaction_date) AS month_start,
          t.account_id,
          t.card_id,
          t.party_id,
          t.product_id,
          p.product_name,
          c.customer_segment,
          c.relationship_tier,
          c.region,
          t.transaction_type,
          t.merchant_category,
          t.channel,
          t.amount_usd,
          t.balance_impact_usd,
          t.is_international,
          t.risk_score,
          t.is_suspected_fraud,
          t.authorization_status,
          COALESCE(d.dispute_count, 0) AS dispute_count,
          COALESCE(d.chargeback_loss_usd, 0.0) AS chargeback_loss_usd,
          CASE
            WHEN t.authorization_status = 'Approved' AND COALESCE(s.transaction_count, 0) > 0
              THEN s.statement_balance_usd / s.transaction_count
            ELSE 0.0
          END AS allocated_statement_balance_usd,
          CASE
            WHEN t.authorization_status = 'Approved' AND COALESCE(s.transaction_count, 0) > 0
              THEN s.payment_amount_usd / s.transaction_count
            ELSE 0.0
          END AS allocated_payment_amount_usd,
          a.credit_limit_usd
        FROM {RETAIL}.`card_transactions` t
        JOIN {RETAIL}.`card_accounts` a ON t.account_id = a.account_id
        JOIN {CORE}.`parties` c ON t.party_id = c.party_id
        JOIN {CORE}.`products` p ON t.product_id = p.product_id
        LEFT JOIN {RETAIL}.`card_statements` s
          ON t.account_id = s.account_id
         AND DATE_TRUNC('MONTH', t.transaction_date) = s.month_start
        LEFT JOIN dispute_by_transaction d ON t.transaction_id = d.transaction_id
    """,
    f"{RETAIL}.`vw_consumer_lending`": f"""
        WITH monthly_collections AS (
          SELECT
            loan_id,
            DATE_TRUNC('MONTH', action_date) AS month_start,
            COUNT(*) AS collection_action_count,
            SUM(promise_to_pay_amount_usd) AS promise_to_pay_amount_usd
          FROM {RETAIL}.`collection_actions`
          GROUP BY loan_id, DATE_TRUNC('MONTH', action_date)
        )
        SELECT
          d.snapshot_date,
          d.month_start,
          d.loan_id,
          d.party_id,
          d.product_id,
          p.product_name,
          d.loan_type,
          l.origination_date,
          l.maturity_date,
          l.original_principal_usd,
          l.interest_rate_pct,
          c.customer_segment,
          c.relationship_tier,
          c.risk_rating,
          d.region,
          d.outstanding_principal_usd,
          d.scheduled_payment_usd,
          d.actual_payment_usd,
          d.days_past_due,
          d.delinquency_bucket,
          COALESCE(x.collection_action_count, 0) AS collection_action_count,
          COALESCE(x.promise_to_pay_amount_usd, 0.0) AS promise_to_pay_amount_usd
        FROM {RETAIL}.`delinquency_snapshots` d
        JOIN {RETAIL}.`consumer_loans` l ON d.loan_id = l.loan_id
        JOIN {CORE}.`parties` c ON d.party_id = c.party_id
        JOIN {CORE}.`products` p ON d.product_id = p.product_id
        LEFT JOIN monthly_collections x
          ON d.loan_id = x.loan_id AND d.month_start = x.month_start
    """,
    f"{COMMERCIAL}.`vw_commercial_banking`": f"""
        WITH months AS (
          SELECT DISTINCT month_start FROM {CORE}.`bank_calendar`
        ),
        business_months AS (
          SELECT b.*, m.month_start
          FROM {COMMERCIAL}.`business_profiles` b
          CROSS JOIN months m
          WHERE m.month_start >= DATE_TRUNC('MONTH', b.created_date)
        ),
        txn AS (
          SELECT
            party_id,
            DATE_TRUNC('MONTH', transaction_date) AS month_start,
            SUM(CASE WHEN signed_amount_usd > 0 THEN signed_amount_usd ELSE 0 END) AS cash_inflow_usd,
            SUM(CASE WHEN signed_amount_usd < 0 THEN -signed_amount_usd ELSE 0 END) AS cash_outflow_usd,
            SUM(signed_amount_usd) AS net_cash_flow_usd,
            COUNT(*) AS transaction_count
          FROM {COMMERCIAL}.`commercial_transactions`
          GROUP BY party_id, DATE_TRUNC('MONTH', transaction_date)
        ),
        merchant AS (
          SELECT
            party_id,
            DATE_TRUNC('MONTH', settlement_date) AS month_start,
            SUM(gross_sales_usd) AS merchant_sales_usd,
            SUM(processing_fee_usd) AS processing_fee_usd,
            SUM(chargeback_amount_usd) AS merchant_chargeback_usd
          FROM {COMMERCIAL}.`merchant_settlements`
          GROUP BY party_id, DATE_TRUNC('MONTH', settlement_date)
        ),
        exposure AS (
          SELECT
            party_id,
            SUM(commitment_amount_usd) AS commitment_amount_usd,
            SUM(outstanding_amount_usd) AS outstanding_exposure_usd
          FROM {COMMERCIAL}.`credit_facilities`
          WHERE status = 'Active'
          GROUP BY party_id
        ),
        covenant AS (
          SELECT
            party_id,
            month_start,
            COUNT(*) AS covenant_test_count,
            SUM(CASE WHEN is_in_breach THEN 1 ELSE 0 END) AS covenant_breach_count
          FROM {COMMERCIAL}.`covenant_snapshots`
          GROUP BY party_id, month_start
        )
        SELECT
          bm.month_start,
          bm.party_id,
          bm.business_name,
          bm.customer_segment,
          bm.industry,
          bm.legal_structure,
          bm.annual_revenue_usd,
          bm.employee_count,
          bm.risk_rating,
          bm.relationship_manager_id,
          bm.region,
          COALESCE(t.cash_inflow_usd, 0.0) AS cash_inflow_usd,
          COALESCE(t.cash_outflow_usd, 0.0) AS cash_outflow_usd,
          COALESCE(t.net_cash_flow_usd, 0.0) AS net_cash_flow_usd,
          COALESCE(t.transaction_count, 0) AS transaction_count,
          COALESCE(m.merchant_sales_usd, 0.0) AS merchant_sales_usd,
          COALESCE(m.processing_fee_usd, 0.0) AS processing_fee_usd,
          COALESCE(m.merchant_chargeback_usd, 0.0) AS merchant_chargeback_usd,
          COALESCE(e.commitment_amount_usd, 0.0) AS commitment_amount_usd,
          COALESCE(e.outstanding_exposure_usd, 0.0) AS outstanding_exposure_usd,
          COALESCE(c.covenant_test_count, 0) AS covenant_test_count,
          COALESCE(c.covenant_breach_count, 0) AS covenant_breach_count
        FROM business_months bm
        LEFT JOIN txn t ON bm.party_id = t.party_id AND bm.month_start = t.month_start
        LEFT JOIN merchant m ON bm.party_id = m.party_id AND bm.month_start = m.month_start
        LEFT JOIN exposure e ON bm.party_id = e.party_id
        LEFT JOIN covenant c ON bm.party_id = c.party_id AND bm.month_start = c.month_start
    """,
    f"{WEALTH}.`vw_wealth_management`": f"""
        WITH holding AS (
          SELECT
            month_start,
            portfolio_id,
            wealth_account_id,
            party_id,
            advisor_id,
            strategy,
            region,
            asset_class,
            SUM(market_value_usd) AS assets_under_management_usd,
            SUM(unrealized_gain_loss_usd) AS unrealized_gain_loss_usd
          FROM {WEALTH}.`holding_snapshots`
          GROUP BY month_start, portfolio_id, wealth_account_id, party_id,
                   advisor_id, strategy, region, asset_class
        ),
        portfolio_total AS (
          SELECT month_start, portfolio_id, SUM(assets_under_management_usd) AS portfolio_aum_usd
          FROM holding
          GROUP BY month_start, portfolio_id
        ),
        trade AS (
          SELECT
            DATE_TRUNC('MONTH', trade_date) AS month_start,
            portfolio_id,
            COUNT(*) AS trade_count,
            SUM(CASE WHEN side = 'Buy' THEN trade_amount_usd ELSE -trade_amount_usd END) AS net_trade_flow_usd,
            SUM(commission_usd) AS commission_revenue_usd
          FROM {WEALTH}.`trades`
          GROUP BY DATE_TRUNC('MONTH', trade_date), portfolio_id
        ),
        fee AS (
          SELECT month_start, portfolio_id, SUM(fee_amount_usd) AS advisory_fee_usd
          FROM {WEALTH}.`advisory_fees`
          GROUP BY month_start, portfolio_id
        )
        SELECT
          h.month_start,
          h.portfolio_id,
          h.wealth_account_id,
          h.party_id,
          h.advisor_id,
          h.strategy,
          h.region,
          h.asset_class,
          c.customer_segment,
          c.relationship_tier,
          h.assets_under_management_usd,
          h.unrealized_gain_loss_usd,
          h.assets_under_management_usd / NULLIF(pt.portfolio_aum_usd, 0) * 100.0 AS allocation_pct,
          COALESCE(t.trade_count, 0) * h.assets_under_management_usd / NULLIF(pt.portfolio_aum_usd, 0) AS allocated_trade_count,
          COALESCE(t.net_trade_flow_usd, 0.0) * h.assets_under_management_usd / NULLIF(pt.portfolio_aum_usd, 0) AS allocated_net_trade_flow_usd,
          COALESCE(t.commission_revenue_usd, 0.0) * h.assets_under_management_usd / NULLIF(pt.portfolio_aum_usd, 0) AS allocated_commission_revenue_usd,
          COALESCE(f.advisory_fee_usd, 0.0) * h.assets_under_management_usd / NULLIF(pt.portfolio_aum_usd, 0) AS allocated_advisory_fee_usd
        FROM holding h
        JOIN portfolio_total pt ON h.month_start = pt.month_start AND h.portfolio_id = pt.portfolio_id
        JOIN {CORE}.`parties` c ON h.party_id = c.party_id
        LEFT JOIN trade t ON h.month_start = t.month_start AND h.portfolio_id = t.portfolio_id
        LEFT JOIN fee f ON h.month_start = f.month_start AND h.portfolio_id = f.portfolio_id
    """,
    f"{OPERATIONS}.`vw_service_operations`": f"""
        WITH request_group AS (
          SELECT
            DATE_TRUNC('MONTH', request_date) AS month_start,
            branch_id,
            channel,
            category,
            COUNT(*) AS request_count,
            SUM(CASE WHEN category = 'Complaint' THEN 1 ELSE 0 END) AS complaint_count,
            SUM(CASE WHEN status = 'Resolved' THEN 1 ELSE 0 END) AS resolved_count,
            SUM(CASE WHEN status = 'Escalated' THEN 1 ELSE 0 END) AS escalated_count,
            AVG(resolution_time_hours) AS average_resolution_time_hours,
            AVG(satisfaction_score) AS average_satisfaction_score,
            COUNT(DISTINCT party_id) AS unique_customers
          FROM {OPERATIONS}.`service_requests`
          GROUP BY DATE_TRUNC('MONTH', request_date), branch_id, channel, category
        ),
        request_total AS (
          SELECT month_start, branch_id, SUM(request_count) AS branch_request_count
          FROM request_group
          GROUP BY month_start, branch_id
        )
        SELECT
          r.month_start,
          r.branch_id,
          p.branch_name,
          p.region,
          p.state,
          r.channel,
          r.category,
          r.request_count,
          r.complaint_count,
          r.resolved_count,
          r.escalated_count,
          r.average_resolution_time_hours,
          r.average_satisfaction_score,
          r.unique_customers,
          p.branch_visit_count * r.request_count / NULLIF(rt.branch_request_count, 0) AS allocated_branch_visit_count,
          p.transaction_count * r.request_count / NULLIF(rt.branch_request_count, 0) AS allocated_transaction_count,
          p.fee_revenue_usd * r.request_count / NULLIF(rt.branch_request_count, 0) AS allocated_fee_revenue_usd,
          p.total_operating_cost_usd * r.request_count / NULLIF(rt.branch_request_count, 0) AS allocated_operating_cost_usd
        FROM request_group r
        JOIN request_total rt ON r.month_start = rt.month_start AND r.branch_id = rt.branch_id
        JOIN {OPERATIONS}.`branch_monthly_performance` p
          ON r.month_start = p.month_start AND r.branch_id = p.branch_id
    """,
    f"{RISK}.`vw_financial_crime`": f"""
        SELECT
          a.alert_id,
          a.alert_date,
          DATE_TRUNC('MONTH', a.alert_date) AS month_start,
          a.transaction_id,
          a.source_domain,
          a.party_id,
          a.account_id,
          c.party_type,
          c.customer_segment,
          c.risk_rating AS party_risk_rating,
          c.region,
          a.transaction_amount_usd,
          a.channel,
          a.is_international,
          a.alert_typology,
          a.alert_score,
          a.status AS alert_status,
          f.fraud_case_id,
          f.case_status AS fraud_case_status,
          m.aml_case_id,
          m.case_status AS aml_case_status,
          COALESCE(l.gross_loss_usd, 0.0) AS gross_loss_usd,
          COALESCE(l.recovery_usd, 0.0) AS recovery_usd,
          COALESCE(l.net_loss_usd, 0.0) AS net_loss_usd
        FROM {RISK}.`transaction_alerts` a
        JOIN {CORE}.`parties` c ON a.party_id = c.party_id
        LEFT JOIN {RISK}.`fraud_cases` f ON a.alert_id = f.alert_id
        LEFT JOIN {RISK}.`aml_cases` m ON a.alert_id = m.alert_id
        LEFT JOIN {RISK}.`fraud_loss_events` l ON f.fraud_case_id = l.fraud_case_id
    """,
}

if ENABLE_FINANCE:
    curated_views[f"{FINANCE}.`vw_bank_finance`"] = f"""
        WITH provision AS (
          SELECT month_start, product_id, SUM(provision_amount_usd) AS provision_amount_usd
          FROM {FINANCE}.`credit_loss_provisions`
          GROUP BY month_start, product_id
        )
        SELECT
          p.month_start,
          p.product_id,
          p.product_name,
          p.business_line,
          p.product_category,
          p.account_count,
          p.average_balance_usd,
          p.fee_revenue_usd,
          p.interest_revenue_usd,
          p.total_revenue_usd,
          p.direct_cost_usd,
          p.credit_loss_usd,
          p.net_income_usd,
          f.benchmark_rate_pct,
          f.transfer_rate_pct,
          COALESCE(v.provision_amount_usd, 0.0) AS provision_amount_usd
        FROM {FINANCE}.`product_profitability_monthly` p
        LEFT JOIN {FINANCE}.`funds_transfer_pricing` f
          ON p.product_id = f.product_id AND p.month_start = f.month_start
        LEFT JOIN provision v
          ON p.product_id = v.product_id AND p.month_start = v.month_start
    """

for view_name, select_sql in curated_views.items():
    spark.sql(f"CREATE OR REPLACE VIEW {view_name} AS {select_sql}")
    print(f"Created {view_name}")
# COMMAND ----------

# MAGIC %md
# MAGIC ## Governed metric views
# COMMAND ----------

metric_views = {
    (RETAIL, "mv_retail_deposits"): f"""
version: 1.1
source: '{RETAIL}.`vw_retail_deposits`'
comment: "Retail deposit balances, flows, fees, and channel-independent account health"
dimensions:
  - name: Month
    expr: month_start
  - name: Product
    expr: product_name
  - name: Account Type
    expr: account_type
  - name: Customer Region
    expr: customer_region
  - name: Branch Region
    expr: branch_region
  - name: Relationship Tier
    expr: relationship_tier
measures:
  - name: Ending Deposit Balance
    expr: SUM(ending_balance_usd)
  - name: Average Deposit Balance
    expr: SUM(average_balance_usd)
  - name: Deposit Inflow
    expr: SUM(inflow_usd)
  - name: Deposit Outflow
    expr: SUM(outflow_usd)
  - name: Net Deposit Flow
    expr: SUM(net_flow_usd)
  - name: Fee Revenue
    expr: SUM(fee_revenue_usd)
  - name: Transaction Count
    expr: SUM(transaction_count)
  - name: Unique Customers
    expr: COUNT(DISTINCT party_id)
""",
    (RETAIL, "mv_credit_cards"): f"""
version: 1.1
source: '{RETAIL}.`vw_credit_cards`'
comment: "Credit-card spend, utilization inputs, fraud signals, disputes, and losses"
dimensions:
  - name: Month
    expr: DATE_TRUNC('MONTH', transaction_date)
  - name: Product
    expr: product_name
  - name: Merchant Category
    expr: merchant_category
  - name: Channel
    expr: channel
  - name: Region
    expr: region
  - name: Authorization Status
    expr: authorization_status
measures:
  - name: Card Spend
    expr: SUM(amount_usd) FILTER (WHERE transaction_type = 'Purchase' AND authorization_status = 'Approved')
  - name: Transaction Count
    expr: COUNT(1)
  - name: Approved Transaction Count
    expr: COUNT(1) FILTER (WHERE authorization_status = 'Approved')
  - name: Suspected Fraud Count
    expr: COUNT(1) FILTER (WHERE is_suspected_fraud = TRUE)
  - name: Dispute Count
    expr: SUM(dispute_count)
  - name: Chargeback Loss
    expr: SUM(chargeback_loss_usd)
  - name: Statement Balance
    expr: SUM(allocated_statement_balance_usd)
  - name: Card Payments
    expr: SUM(allocated_payment_amount_usd)
  - name: Unique Cardholders
    expr: COUNT(DISTINCT party_id)
""",
    (RETAIL, "mv_consumer_lending"): f"""
version: 1.1
source: '{RETAIL}.`vw_consumer_lending`'
comment: "Consumer-loan balances, payments, delinquency, and collections"
dimensions:
  - name: Month
    expr: month_start
  - name: Product
    expr: product_name
  - name: Loan Type
    expr: loan_type
  - name: Region
    expr: region
  - name: Risk Rating
    expr: risk_rating
  - name: Delinquency Bucket
    expr: delinquency_bucket
measures:
  - name: Outstanding Principal
    expr: SUM(outstanding_principal_usd)
  - name: Scheduled Payments
    expr: SUM(scheduled_payment_usd)
  - name: Actual Payments
    expr: SUM(actual_payment_usd)
  - name: Loan Count
    expr: COUNT(DISTINCT loan_id)
  - name: Delinquent Loan Count
    expr: COUNT(DISTINCT loan_id) FILTER (WHERE days_past_due >= 30)
  - name: Collection Action Count
    expr: SUM(collection_action_count)
""",
    (COMMERCIAL, "mv_commercial_banking"): f"""
version: 1.1
source: '{COMMERCIAL}.`vw_commercial_banking`'
comment: "Commercial cash flow, merchant activity, credit exposure, and covenants"
dimensions:
  - name: Month
    expr: month_start
  - name: Industry
    expr: industry
  - name: Segment
    expr: customer_segment
  - name: Region
    expr: region
  - name: Risk Rating
    expr: risk_rating
measures:
  - name: Cash Inflow
    expr: SUM(cash_inflow_usd)
  - name: Cash Outflow
    expr: SUM(cash_outflow_usd)
  - name: Net Cash Flow
    expr: SUM(net_cash_flow_usd)
  - name: Merchant Sales
    expr: SUM(merchant_sales_usd)
  - name: Processing Fee Revenue
    expr: SUM(processing_fee_usd)
  - name: Outstanding Exposure
    expr: SUM(outstanding_exposure_usd)
  - name: Covenant Breach Count
    expr: SUM(covenant_breach_count)
  - name: Business Count
    expr: COUNT(DISTINCT party_id)
""",
    (WEALTH, "mv_wealth_management"): f"""
version: 1.1
source: '{WEALTH}.`vw_wealth_management`'
comment: "Wealth assets, allocation, trading, flows, and advisory fees"
dimensions:
  - name: Month
    expr: month_start
  - name: Advisor
    expr: advisor_id
  - name: Strategy
    expr: strategy
  - name: Asset Class
    expr: asset_class
  - name: Region
    expr: region
  - name: Relationship Tier
    expr: relationship_tier
measures:
  - name: Assets Under Management
    expr: SUM(assets_under_management_usd)
  - name: Unrealized Gain Loss
    expr: SUM(unrealized_gain_loss_usd)
  - name: Net Trade Flow
    expr: SUM(allocated_net_trade_flow_usd)
  - name: Advisory Fee Revenue
    expr: SUM(allocated_advisory_fee_usd)
  - name: Commission Revenue
    expr: SUM(allocated_commission_revenue_usd)
  - name: Portfolio Count
    expr: COUNT(DISTINCT portfolio_id)
  - name: Client Count
    expr: COUNT(DISTINCT party_id)
""",
    (OPERATIONS, "mv_service_operations"): f"""
version: 1.1
source: '{OPERATIONS}.`vw_service_operations`'
comment: "Service quality, complaint demand, branch activity, and operating cost"
dimensions:
  - name: Month
    expr: month_start
  - name: Branch
    expr: branch_name
  - name: Region
    expr: region
  - name: Channel
    expr: channel
  - name: Category
    expr: category
measures:
  - name: Request Count
    expr: SUM(request_count)
  - name: Complaint Count
    expr: SUM(complaint_count)
  - name: Resolved Count
    expr: SUM(resolved_count)
  - name: Escalated Count
    expr: SUM(escalated_count)
  - name: Average Resolution Hours
    expr: SUM(average_resolution_time_hours * request_count) / SUM(request_count)
  - name: Average Satisfaction Score
    expr: SUM(average_satisfaction_score * request_count) / SUM(request_count)
  - name: Operating Cost
    expr: SUM(allocated_operating_cost_usd)
  - name: Fee Revenue
    expr: SUM(allocated_fee_revenue_usd)
""",
    (RISK, "mv_financial_crime"): f"""
version: 1.1
source: '{RISK}.`vw_financial_crime`'
comment: "Fraud and AML alerts, investigations, losses, and recoveries"
dimensions:
  - name: Month
    expr: month_start
  - name: Source Domain
    expr: source_domain
  - name: Alert Typology
    expr: alert_typology
  - name: Alert Status
    expr: alert_status
  - name: Region
    expr: region
  - name: Party Risk Rating
    expr: party_risk_rating
measures:
  - name: Alert Count
    expr: COUNT(1)
  - name: Escalated Alert Count
    expr: COUNT(1) FILTER (WHERE alert_status = 'Escalated')
  - name: Fraud Case Count
    expr: COUNT(DISTINCT fraud_case_id)
  - name: AML Case Count
    expr: COUNT(DISTINCT aml_case_id)
  - name: Monitored Transaction Amount
    expr: SUM(transaction_amount_usd)
  - name: Gross Fraud Loss
    expr: SUM(gross_loss_usd)
  - name: Recovery Amount
    expr: SUM(recovery_usd)
  - name: Net Fraud Loss
    expr: SUM(net_loss_usd)
""",
}

if ENABLE_FINANCE:
    metric_views[(FINANCE, "mv_bank_finance")] = f"""
version: 1.1
source: '{FINANCE}.`vw_bank_finance`'
comment: "Product profitability, pricing, provisions, and bank-level financial performance"
dimensions:
  - name: Month
    expr: month_start
  - name: Product
    expr: product_name
  - name: Business Line
    expr: business_line
  - name: Product Category
    expr: product_category
measures:
  - name: Average Balance
    expr: SUM(average_balance_usd)
  - name: Fee Revenue
    expr: SUM(fee_revenue_usd)
  - name: Interest Revenue
    expr: SUM(interest_revenue_usd)
  - name: Total Revenue
    expr: SUM(total_revenue_usd)
  - name: Direct Cost
    expr: SUM(direct_cost_usd)
  - name: Credit Loss
    expr: SUM(credit_loss_usd)
  - name: Provision Amount
    expr: SUM(provision_amount_usd)
  - name: Net Income
    expr: SUM(net_income_usd)
"""

for (schema_ref, metric_view_name), yaml_body in metric_views.items():
    spark.sql(
        f"CREATE OR REPLACE VIEW {schema_ref}.`{metric_view_name}` "
        f"WITH METRICS LANGUAGE YAML AS $$\n{yaml_body.strip()}\n$$"
    )
    print(f"Created {schema_ref}.`{metric_view_name}`")

results["semantic_layer"] =         {
            "curated_views": len(curated_views),
            "metric_views": len(metric_views),
            "finance_enabled": ENABLE_FINANCE,
        }
# COMMAND ----------

# MAGIC %md
# MAGIC # Bigly Bank — Cross-Domain Validation
# MAGIC
# MAGIC Validates object creation, primary and foreign keys, lifecycle dates,
# MAGIC monthly reconciliations, and the connected synthetic business stories.
# MAGIC The notebook raises an error when any required check fails.
# COMMAND ----------

from datetime import date
from functools import reduce
import json

from pyspark.sql import Window
from pyspark.sql import functions as F

SCHEMAS = {
    "core": f"{SCHEMA_PREFIX}_core",
    "retail": f"{SCHEMA_PREFIX}_retail",
    "commercial": f"{SCHEMA_PREFIX}_commercial",
    "wealth": f"{SCHEMA_PREFIX}_wealth",
    "operations": f"{SCHEMA_PREFIX}_operations",
    "risk": f"{SCHEMA_PREFIX}_risk",
    "finance": f"{SCHEMA_PREFIX}_finance",
}

def object_name(schema_alias, object_name):
    return f"`{CATALOG}`.`{SCHEMAS[schema_alias]}`.`{object_name}`"

def table(schema_alias, table_name):
    return spark.table(object_name(schema_alias, table_name))

checks = []

def record(check_type, check_name, passed, observed, details):
    checks.append(
        {
            "check_type": check_type,
            "check_name": check_name,
            "status": "PASS" if passed else "FAIL",
            "observed": str(observed),
            "details": details,
        }
    )
# COMMAND ----------

# MAGIC %md
# MAGIC ## Required tables and semantic objects
# COMMAND ----------

primary_keys = {
    ("core", "parties"): ["party_id"],
    ("core", "party_relationships"): ["relationship_id"],
    ("core", "products"): ["product_id"],
    ("core", "branches"): ["branch_id"],
    ("core", "employees"): ["employee_id"],
    ("core", "bank_calendar"): ["calendar_date"],
    ("retail", "deposit_accounts"): ["account_id"],
    ("retail", "deposit_transactions"): ["transaction_id"],
    ("retail", "deposit_balance_snapshots"): ["account_id", "month_start"],
    ("retail", "payment_events"): ["payment_event_id"],
    ("retail", "fee_events"): ["fee_event_id"],
    ("retail", "card_accounts"): ["account_id"],
    ("retail", "cards"): ["card_id"],
    ("retail", "card_transactions"): ["transaction_id"],
    ("retail", "card_statements"): ["statement_id"],
    ("retail", "card_payments"): ["payment_id"],
    ("retail", "card_disputes"): ["dispute_id"],
    ("retail", "card_reward_events"): ["reward_event_id"],
    ("retail", "loan_applications"): ["application_id"],
    ("retail", "credit_decisions"): ["decision_id"],
    ("retail", "consumer_loans"): ["loan_id"],
    ("retail", "loan_collateral"): ["collateral_id"],
    ("retail", "loan_payment_schedule"): ["schedule_id"],
    ("retail", "loan_payments"): ["payment_id"],
    ("retail", "delinquency_snapshots"): ["loan_id", "month_start"],
    ("retail", "collection_actions"): ["collection_action_id"],
    ("commercial", "business_profiles"): ["business_profile_id"],
    ("commercial", "commercial_deposit_accounts"): ["account_id"],
    ("commercial", "commercial_transactions"): ["transaction_id"],
    ("commercial", "credit_facilities"): ["facility_id"],
    ("commercial", "commercial_loans"): ["loan_id"],
    ("commercial", "covenant_snapshots"): ["covenant_snapshot_id"],
    ("commercial", "merchant_settlements"): ["settlement_id"],
    ("wealth", "wealth_accounts"): ["wealth_account_id"],
    ("wealth", "portfolios"): ["portfolio_id"],
    ("wealth", "securities"): ["security_id"],
    ("wealth", "holding_snapshots"): ["holding_snapshot_id"],
    ("wealth", "trades"): ["trade_id"],
    ("wealth", "advisory_fees"): ["advisory_fee_id"],
    ("wealth", "client_goals"): ["goal_id"],
    ("operations", "operational_incidents"): ["incident_id"],
    ("operations", "service_requests"): ["request_id"],
    ("operations", "complaints"): ["complaint_id"],
    ("operations", "customer_interactions"): ["interaction_id"],
    ("operations", "branch_staffing_snapshots"): ["branch_id", "month_start"],
    ("operations", "branch_monthly_performance"): ["branch_id", "month_start"],
    ("risk", "transaction_alerts"): ["alert_id"],
    ("risk", "fraud_cases"): ["fraud_case_id"],
    ("risk", "aml_cases"): ["aml_case_id"],
    ("risk", "case_actions"): ["case_action_id"],
    ("risk", "kyc_reviews"): ["review_id"],
    ("risk", "fraud_loss_events"): ["loss_event_id"],
}

if ENABLE_FINANCE:
    primary_keys.update(
        {
            ("finance", "funds_transfer_pricing"): ["ftp_record_id"],
            ("finance", "credit_loss_provisions"): ["provision_id"],
            ("finance", "product_profitability_monthly"): ["profitability_id"],
            ("finance", "general_ledger_monthly"): ["ledger_entry_id"],
            ("finance", "liquidity_snapshots"): ["liquidity_snapshot_id"],
        }
    )

semantic_objects = {
    ("retail", "vw_retail_deposits"),
    ("retail", "mv_retail_deposits"),
    ("retail", "vw_credit_cards"),
    ("retail", "mv_credit_cards"),
    ("retail", "vw_consumer_lending"),
    ("retail", "mv_consumer_lending"),
    ("commercial", "vw_commercial_banking"),
    ("commercial", "mv_commercial_banking"),
    ("wealth", "vw_wealth_management"),
    ("wealth", "mv_wealth_management"),
    ("operations", "vw_service_operations"),
    ("operations", "mv_service_operations"),
    ("risk", "vw_financial_crime"),
    ("risk", "mv_financial_crime"),
}
if ENABLE_FINANCE:
    semantic_objects.update(
        {
            ("finance", "vw_bank_finance"),
            ("finance", "mv_bank_finance"),
        }
    )

missing_objects = []
for schema_alias, name in list(primary_keys) + sorted(semantic_objects):
    full_name = object_name(schema_alias, name)
    exists = spark.catalog.tableExists(full_name)
    record("existence", f"{schema_alias}.{name}", exists, exists, full_name)
    if not exists:
        missing_objects.append(full_name)

if missing_objects:
    display(spark.createDataFrame(checks).orderBy("status", "check_type", "check_name"))
    raise AssertionError(f"Missing {len(missing_objects)} required banking objects")

# One aggregation supplies the row count, null-key count, and distinct-key count.
table_row_counts = {}
for (schema_alias, table_name), key_columns in primary_keys.items():
    source = table(schema_alias, table_name)
    null_key_condition = reduce(
        lambda left, right: left | right,
        [F.col(column_name).isNull() for column_name in key_columns],
    )
    health = source.agg(
        F.count(F.lit(1)).alias("row_count"),
        F.countDistinct(F.struct(*[F.col(column_name) for column_name in key_columns])).alias(
            "distinct_key_count"
        ),
        F.sum(F.when(null_key_condition, 1).otherwise(0)).alias("null_key_count"),
    ).first()
    row_count = health["row_count"]
    distinct_key_count = health["distinct_key_count"]
    null_key_count = health["null_key_count"] or 0
    table_row_counts[f"{schema_alias}.{table_name}"] = row_count
    record(
        "row_count",
        f"{schema_alias}.{table_name}",
        row_count > 0,
        row_count,
        "Required generated table must not be empty",
    )
    record(
        "primary_key",
        f"{schema_alias}.{table_name}",
        row_count == distinct_key_count and null_key_count == 0,
        f"rows={row_count}, distinct_keys={distinct_key_count}, null_keys={null_key_count}",
        f"Key columns: {', '.join(key_columns)}",
    )
# COMMAND ----------

# MAGIC %md
# MAGIC ## Referential integrity
# COMMAND ----------

foreign_keys = [
    ("relationship_from_party", "core", "party_relationships", ["from_party_id"], "core", "parties", ["party_id"]),
    ("relationship_to_party", "core", "party_relationships", ["to_party_id"], "core", "parties", ["party_id"]),
    ("employee_branch", "core", "employees", ["branch_id"], "core", "branches", ["branch_id"]),
    ("deposit_account_party", "retail", "deposit_accounts", ["party_id"], "core", "parties", ["party_id"]),
    ("deposit_account_product", "retail", "deposit_accounts", ["product_id"], "core", "products", ["product_id"]),
    ("deposit_account_branch", "retail", "deposit_accounts", ["branch_id"], "core", "branches", ["branch_id"]),
    ("deposit_transaction_account", "retail", "deposit_transactions", ["account_id"], "retail", "deposit_accounts", ["account_id"]),
    ("deposit_snapshot_account", "retail", "deposit_balance_snapshots", ["account_id"], "retail", "deposit_accounts", ["account_id"]),
    ("payment_event_transaction", "retail", "payment_events", ["transaction_id"], "retail", "deposit_transactions", ["transaction_id"]),
    ("fee_event_transaction", "retail", "fee_events", ["transaction_id"], "retail", "deposit_transactions", ["transaction_id"]),
    ("card_account_party", "retail", "card_accounts", ["party_id"], "core", "parties", ["party_id"]),
    ("card_account_product", "retail", "card_accounts", ["product_id"], "core", "products", ["product_id"]),
    ("card_account", "retail", "cards", ["account_id"], "retail", "card_accounts", ["account_id"]),
    ("card_transaction_card", "retail", "card_transactions", ["card_id"], "retail", "cards", ["card_id"]),
    ("card_statement_account", "retail", "card_statements", ["account_id"], "retail", "card_accounts", ["account_id"]),
    ("card_payment_statement", "retail", "card_payments", ["statement_id"], "retail", "card_statements", ["statement_id"]),
    ("card_dispute_transaction", "retail", "card_disputes", ["transaction_id"], "retail", "card_transactions", ["transaction_id"]),
    ("loan_application_party", "retail", "loan_applications", ["party_id"], "core", "parties", ["party_id"]),
    ("credit_decision_application", "retail", "credit_decisions", ["application_id"], "retail", "loan_applications", ["application_id"]),
    ("consumer_loan_application", "retail", "consumer_loans", ["application_id"], "retail", "loan_applications", ["application_id"]),
    ("loan_collateral", "retail", "loan_collateral", ["loan_id"], "retail", "consumer_loans", ["loan_id"]),
    ("loan_schedule", "retail", "loan_payment_schedule", ["loan_id"], "retail", "consumer_loans", ["loan_id"]),
    ("loan_payment_schedule", "retail", "loan_payments", ["schedule_id"], "retail", "loan_payment_schedule", ["schedule_id"]),
    ("delinquency_loan", "retail", "delinquency_snapshots", ["loan_id"], "retail", "consumer_loans", ["loan_id"]),
    ("collection_loan", "retail", "collection_actions", ["loan_id"], "retail", "consumer_loans", ["loan_id"]),
    ("business_profile_party", "commercial", "business_profiles", ["party_id"], "core", "parties", ["party_id"]),
    ("commercial_account_party", "commercial", "commercial_deposit_accounts", ["party_id"], "commercial", "business_profiles", ["party_id"]),
    ("commercial_transaction_account", "commercial", "commercial_transactions", ["account_id"], "commercial", "commercial_deposit_accounts", ["account_id"]),
    ("facility_party", "commercial", "credit_facilities", ["party_id"], "commercial", "business_profiles", ["party_id"]),
    ("commercial_loan_party", "commercial", "commercial_loans", ["party_id"], "commercial", "business_profiles", ["party_id"]),
    ("covenant_facility", "commercial", "covenant_snapshots", ["facility_id"], "commercial", "credit_facilities", ["facility_id"]),
    ("merchant_party", "commercial", "merchant_settlements", ["party_id"], "commercial", "business_profiles", ["party_id"]),
    ("wealth_account_party", "wealth", "wealth_accounts", ["party_id"], "core", "parties", ["party_id"]),
    ("portfolio_account", "wealth", "portfolios", ["wealth_account_id"], "wealth", "wealth_accounts", ["wealth_account_id"]),
    ("holding_portfolio", "wealth", "holding_snapshots", ["portfolio_id"], "wealth", "portfolios", ["portfolio_id"]),
    ("holding_security", "wealth", "holding_snapshots", ["security_id"], "wealth", "securities", ["security_id"]),
    ("trade_portfolio", "wealth", "trades", ["portfolio_id"], "wealth", "portfolios", ["portfolio_id"]),
    ("trade_security", "wealth", "trades", ["security_id"], "wealth", "securities", ["security_id"]),
    ("advisory_fee_portfolio", "wealth", "advisory_fees", ["portfolio_id"], "wealth", "portfolios", ["portfolio_id"]),
    ("goal_account", "wealth", "client_goals", ["wealth_account_id"], "wealth", "wealth_accounts", ["wealth_account_id"]),
    ("service_party", "operations", "service_requests", ["party_id"], "core", "parties", ["party_id"]),
    ("service_branch", "operations", "service_requests", ["branch_id"], "core", "branches", ["branch_id"]),
    ("complaint_request", "operations", "complaints", ["request_id"], "operations", "service_requests", ["request_id"]),
    ("interaction_party", "operations", "customer_interactions", ["party_id"], "core", "parties", ["party_id"]),
    ("staffing_branch", "operations", "branch_staffing_snapshots", ["branch_id"], "core", "branches", ["branch_id"]),
    ("performance_branch", "operations", "branch_monthly_performance", ["branch_id"], "core", "branches", ["branch_id"]),
    ("alert_party", "risk", "transaction_alerts", ["party_id"], "core", "parties", ["party_id"]),
    ("fraud_case_alert", "risk", "fraud_cases", ["alert_id"], "risk", "transaction_alerts", ["alert_id"]),
    ("aml_case_alert", "risk", "aml_cases", ["alert_id"], "risk", "transaction_alerts", ["alert_id"]),
    ("kyc_party", "risk", "kyc_reviews", ["party_id"], "core", "parties", ["party_id"]),
    ("fraud_loss_case", "risk", "fraud_loss_events", ["fraud_case_id"], "risk", "fraud_cases", ["fraud_case_id"]),
]

if ENABLE_FINANCE:
    foreign_keys.extend(
        [
            ("ftp_product", "finance", "funds_transfer_pricing", ["product_id"], "core", "products", ["product_id"]),
            ("provision_product", "finance", "credit_loss_provisions", ["product_id"], "core", "products", ["product_id"]),
            ("profitability_product", "finance", "product_profitability_monthly", ["product_id"], "core", "products", ["product_id"]),
        ]
    )

for (
    check_name,
    child_schema,
    child_table,
    child_columns,
    parent_schema,
    parent_table,
    parent_columns,
) in foreign_keys:
    child = table(child_schema, child_table).alias("child")
    parent = table(parent_schema, parent_table).alias("parent")
    populated_child_key = reduce(
        lambda left, right: left & right,
        [F.col(f"child.{column_name}").isNotNull() for column_name in child_columns],
    )
    join_condition = reduce(
        lambda left, right: left & right,
        [
            F.col(f"child.{child_column}") == F.col(f"parent.{parent_column}")
            for child_column, parent_column in zip(child_columns, parent_columns)
        ],
    )
    orphan_count = (
        child.filter(populated_child_key)
        .join(parent, join_condition, "left_anti")
        .count()
    )
    record(
        "foreign_key",
        check_name,
        orphan_count == 0,
        orphan_count,
        f"{child_schema}.{child_table} -> {parent_schema}.{parent_table}",
    )

# Polymorphic alert links must resolve in the originating domain.
source_transactions = (
    table("retail", "deposit_transactions")
    .select("transaction_id", F.lit("Deposit").alias("source_domain"))
    .unionByName(
        table("retail", "card_transactions").select(
            "transaction_id", F.lit("Card").alias("source_domain")
        )
    )
    .unionByName(
        table("commercial", "commercial_transactions").select(
            "transaction_id", F.lit("Commercial").alias("source_domain")
        )
    )
)
alert_orphans = (
    table("risk", "transaction_alerts")
    .select("transaction_id", "source_domain")
    .join(source_transactions, ["transaction_id", "source_domain"], "left_anti")
    .count()
)
record(
    "foreign_key",
    "alert_originating_transaction",
    alert_orphans == 0,
    alert_orphans,
    "Every alert resolves to its RETAIL or COMMERCIAL transaction",
)
# COMMAND ----------

# MAGIC %md
# MAGIC ## Lifecycle and reconciliation checks
# COMMAND ----------

def record_zero(check_type, check_name, invalid_rows, details):
    record(check_type, check_name, invalid_rows == 0, invalid_rows, details)

invalid_deposit_dates = (
    table("retail", "deposit_transactions")
    .alias("t")
    .join(table("retail", "deposit_accounts").alias("a"), "account_id")
    .filter(
        (F.col("t.transaction_date") < F.col("a.open_date"))
        | (F.col("t.transaction_date") > F.coalesce(F.col("a.close_date"), F.lit(AS_OF_DATE)))
    )
    .count()
)
record_zero("lifecycle", "deposit_transaction_dates", invalid_deposit_dates, "Transactions remain within the deposit-account lifecycle")

invalid_card_dates = (
    table("retail", "card_transactions")
    .alias("t")
    .join(table("retail", "cards").alias("c"), "card_id")
    .join(table("retail", "card_accounts").alias("a"), "account_id")
    .filter(
        (F.col("t.transaction_date") < F.col("c.issued_date"))
        | (F.col("t.transaction_date") > F.col("c.expiration_date"))
        | (F.col("t.transaction_date") > F.coalesce(F.col("a.close_date"), F.lit(AS_OF_DATE)))
    )
    .count()
)
record_zero("lifecycle", "card_transaction_dates", invalid_card_dates, "Card activity remains within card and account lifecycles")

invalid_loan_dates = (
    table("retail", "loan_payment_schedule")
    .alias("s")
    .join(table("retail", "consumer_loans").alias("l"), "loan_id")
    .filter(
        (F.col("s.due_date") < F.col("l.origination_date"))
        | (F.col("s.due_date") > F.col("l.maturity_date"))
        | (F.col("s.due_date") > F.lit(AS_OF_DATE))
    )
    .count()
)
record_zero("lifecycle", "loan_schedule_dates", invalid_loan_dates, "Scheduled payments remain within the loan lifecycle and as-of date")

invalid_commercial_dates = (
    table("commercial", "commercial_transactions")
    .alias("t")
    .join(table("commercial", "commercial_deposit_accounts").alias("a"), "account_id")
    .filter(
        (F.col("t.transaction_date") < F.col("a.open_date"))
        | (F.col("t.transaction_date") > F.lit(AS_OF_DATE))
    )
    .count()
)
record_zero("lifecycle", "commercial_transaction_dates", invalid_commercial_dates, "Commercial transactions remain within the account lifecycle")

invalid_wealth_dates = (
    table("wealth", "trades")
    .alias("t")
    .join(table("wealth", "portfolios").alias("p"), "portfolio_id")
    .filter(
        (F.col("t.trade_date") < F.col("p.inception_date"))
        | (F.col("t.trade_date") > F.coalesce(F.col("p.close_date"), F.lit(AS_OF_DATE)))
    )
    .count()
)
record_zero("lifecycle", "wealth_trade_dates", invalid_wealth_dates, "Trades remain within the portfolio account lifecycle")

invalid_case_dates = (
    table("risk", "fraud_cases")
    .filter(F.col("closed_date").isNotNull() & (F.col("closed_date") < F.col("opened_date")))
    .count()
    + table("risk", "aml_cases")
    .filter(F.col("closed_date").isNotNull() & (F.col("closed_date") < F.col("opened_date")))
    .count()
)
record_zero("lifecycle", "investigation_case_dates", invalid_case_dates, "Cases cannot close before opening")

deposit_window = (
    Window.partitionBy("account_id")
    .orderBy("month_start")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)
deposit_reconciliation_mismatches = (
    table("retail", "deposit_balance_snapshots")
    .join(table("retail", "deposit_accounts").select("account_id", "opening_balance_usd"), "account_id")
    .withColumn(
        "expected_ending_balance_usd",
        F.round(F.col("opening_balance_usd") + F.sum("net_flow_usd").over(deposit_window), 2),
    )
    .filter(F.abs(F.col("ending_balance_usd") - F.col("expected_ending_balance_usd")) > 0.01)
    .count()
)
record_zero("reconciliation", "deposit_ending_balances", deposit_reconciliation_mismatches, "Opening balance plus cumulative monthly flow equals ending balance")

statement_window = (
    Window.partitionBy("account_id")
    .orderBy("month_start")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)
card_reconciliation_mismatches = (
    table("retail", "card_statements")
    .withColumn(
        "expected_statement_balance_usd",
        F.round(
            F.greatest(
                F.lit(0.0),
                F.sum(F.col("net_charges_usd") - F.col("payment_amount_usd")).over(statement_window),
            ),
            2,
        ),
    )
    .filter(F.abs(F.col("statement_balance_usd") - F.col("expected_statement_balance_usd")) > 0.01)
    .count()
)
record_zero("reconciliation", "card_statement_balances", card_reconciliation_mismatches, "Cumulative net charges less payments equals statement balance")

if ENABLE_FINANCE:
    profitability_mismatches = (
        table("finance", "product_profitability_monthly")
        .filter(
            (F.abs(F.col("total_revenue_usd") - F.col("fee_revenue_usd") - F.col("interest_revenue_usd")) > 0.02)
            | (
                F.abs(
                    F.col("net_income_usd")
                    - F.col("total_revenue_usd")
                    + F.col("direct_cost_usd")
                    + F.col("credit_loss_usd")
                )
                > 0.02
            )
        )
        .count()
    )
    record_zero("reconciliation", "finance_product_profitability", profitability_mismatches, "Revenue and net income reconcile to their components")

    liquidity_mismatches = (
        table("finance", "liquidity_snapshots")
        .filter(
            F.abs(
                F.col("total_deposits_usd")
                - F.col("retail_deposits_usd")
                - F.col("commercial_deposits_usd")
            )
            > 0.02
        )
        .count()
    )
    record_zero("reconciliation", "finance_liquidity", liquidity_mismatches, "Retail plus commercial deposits equals total deposits")
# COMMAND ----------

# MAGIC %md
# MAGIC ## Connected business-story checks
# COMMAND ----------

if AS_OF_DATE >= date(2024, 1, 16):
    outage_incidents = table("operations", "operational_incidents").filter(
        F.col("incident_id") == "INC-MOBILE-20240115"
    ).count()
    outage_reversals = table("retail", "deposit_transactions").filter(
        (F.col("incident_id") == "INC-MOBILE-20240115") & (F.col("status") == "Reversed")
    ).count()
    outage_complaints = table("operations", "complaints").filter(
        F.col("incident_id") == "INC-MOBILE-20240115"
    ).count()
    record("story", "mobile_outage_incident", outage_incidents == 1, outage_incidents, "The January 2024 incident exists once")
    record("story", "mobile_outage_reversals", outage_reversals > 0, outage_reversals, "The outage links to reversed retail ledger events")
    record("story", "mobile_outage_complaints", outage_complaints > 0, outage_complaints, "The outage links to formal complaints")
else:
    record("story", "mobile_outage", True, "not applicable", "As-of date precedes the January 2024 story")

if AS_OF_DATE >= date(2025, 12, 31):
    holiday_fraud_transactions = table("retail", "card_transactions").filter(
        F.col("transaction_date").between(date(2025, 11, 1), date(2025, 12, 31))
        & F.col("is_suspected_fraud")
    )
    holiday_fraud_count = holiday_fraud_transactions.count()
    holiday_alert_count = (
        table("risk", "transaction_alerts")
        .filter(F.col("source_domain") == "Card")
        .join(holiday_fraud_transactions.select("transaction_id"), "transaction_id")
        .count()
    )
    fraud_losses = table("risk", "fraud_loss_events")
    fraud_loss_count = fraud_losses.count()
    positive_recoveries = fraud_losses.filter(F.col("recovery_usd") > 0).count()
    record("story", "holiday_card_fraud", holiday_fraud_count > 0, holiday_fraud_count, "Holiday card transactions contain traceable fraud signals")
    record("story", "holiday_fraud_alerts", holiday_alert_count > 0, holiday_alert_count, "Holiday fraud signals generate risk alerts")
    record("story", "fraud_losses", fraud_loss_count > 0, fraud_loss_count, "Confirmed fraud cases produce loss events")
    record("story", "fraud_recoveries", positive_recoveries > 0, positive_recoveries, "Some confirmed losses have recoveries")

    delinquency_2025 = table("retail", "delinquency_snapshots").filter(
        F.year("month_start") == 2025
    )
    southeast_loans = delinquency_2025.filter(F.col("region") == "Southeast")
    other_loans = delinquency_2025.filter(F.col("region") != "Southeast")
    southeast_total = southeast_loans.count()
    other_total = other_loans.count()
    southeast_rate = southeast_loans.filter(F.col("days_past_due") >= 30).count() / max(southeast_total, 1)
    other_rate = other_loans.filter(F.col("days_past_due") >= 30).count() / max(other_total, 1)
    record("story", "southeast_consumer_stress", southeast_total > 0 and southeast_rate > other_rate, f"southeast={southeast_rate:.4f}, other={other_rate:.4f}", "Southeast 30+ day delinquency is higher during 2025")

    covenants_2025 = table("commercial", "covenant_snapshots").filter(
        F.year("month_start") == 2025
    )
    southeast_covenants = covenants_2025.filter(F.col("region") == "Southeast")
    other_covenants = covenants_2025.filter(F.col("region") != "Southeast")
    southeast_covenant_total = southeast_covenants.count()
    other_covenant_total = other_covenants.count()
    southeast_breach_rate = southeast_covenants.filter("is_in_breach").count() / max(southeast_covenant_total, 1)
    other_breach_rate = other_covenants.filter("is_in_breach").count() / max(other_covenant_total, 1)
    record("story", "southeast_commercial_stress", southeast_covenant_total > 0 and southeast_breach_rate > other_breach_rate, f"southeast={southeast_breach_rate:.4f}, other={other_breach_rate:.4f}", "Southeast covenant breaches are higher during 2025")
else:
    record("story", "2025_stress_and_fraud", True, "not applicable", "As-of date precedes the complete 2025 stories")
# COMMAND ----------

validation_results = spark.createDataFrame(checks)
display(validation_results.orderBy("status", "check_type", "check_name"))

failed_checks = sum(1 for check in checks if check["status"] == "FAIL")
if failed_checks:
    raise AssertionError(f"Bigly Bank validation failed: {failed_checks} of {len(checks)} checks failed")

summary = {
    "status": "passed",
    "checks": len(checks),
    "tables": len(primary_keys),
    "semantic_objects": len(semantic_objects),
    "finance_enabled": ENABLE_FINANCE,
    "row_counts": table_row_counts,
}
print(json.dumps(summary, indent=2, sort_keys=True))
results["validation"] = summary
# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# COMMAND ----------

print(
    json.dumps(
        {
            "catalog": CATALOG,
            "schema_prefix": SCHEMA_PREFIX,
            "finance_enabled": ENABLE_FINANCE,
            "phases": results,
        },
        indent=2,
        sort_keys=True,
    )
)
dbutils.notebook.exit(
    json.dumps(
        {
            "catalog": CATALOG,
            "schema_prefix": SCHEMA_PREFIX,
            "finance_enabled": ENABLE_FINANCE,
            "phases": results,
        }
    )
)


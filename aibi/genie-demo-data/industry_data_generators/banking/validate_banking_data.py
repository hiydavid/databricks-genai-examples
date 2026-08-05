# Databricks notebook source

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
try:
    dbutils.widgets.dropdown(
        "enable_finance",
        "false",
        ["true", "false"],
        "Validate the optional Finance and Treasury domain",
    )
except Exception:
    pass

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA_PREFIX = dbutils.widgets.get("schema_prefix").strip()
SEED = int(dbutils.widgets.get("seed"))
AS_OF_DATE = date.fromisoformat(dbutils.widgets.get("as_of_date").strip())
ENABLE_FINANCE = dbutils.widgets.get("enable_finance").strip().lower() == "true"

if not CATALOG:
    raise ValueError("catalog is required")
if not SCHEMA_PREFIX:
    raise ValueError("schema_prefix is required")
if "`" in CATALOG or "`" in SCHEMA_PREFIX:
    raise ValueError("catalog and schema_prefix cannot contain backticks")

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
dbutils.notebook.exit(json.dumps(summary))

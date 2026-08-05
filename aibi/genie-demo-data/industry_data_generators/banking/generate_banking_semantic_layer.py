# Databricks notebook source

# MAGIC %md
# MAGIC # Bigly Bank — Curated Views and Metric Views
# MAGIC
# MAGIC Creates one curated `vw_` view and one governed `mv_` metric view for
# MAGIC each planned Genie domain. Run after all enabled data generators.

# COMMAND ----------

from datetime import date
import json

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
    dbutils.widgets.dropdown("enable_finance", "false", ["true", "false"], "Create finance semantic objects")
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

dbutils.notebook.exit(
    json.dumps(
        {
            "curated_views": len(curated_views),
            "metric_views": len(metric_views),
            "finance_enabled": ENABLE_FINANCE,
        }
    )
)

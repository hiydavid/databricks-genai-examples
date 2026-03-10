# Databricks notebook source

# MAGIC %md
# MAGIC # Horizon Bank — Genie Space Creator
# MAGIC
# MAGIC Creates (or updates) the **Horizon Bank Analytics** Genie space using the Databricks SDK.
# MAGIC
# MAGIC **Prerequisite:** Run `generate_data.py` first to create the 6 Delta tables and 3 metric views.
# MAGIC
# MAGIC **Setup:** Edit `CATALOG` and `SCHEMA` below to match `generate_data.py`, then **Run All**.

# COMMAND ----------

# MAGIC %pip install "databricks-sdk>=0.74.0" --quiet
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

# =============================================================================
# CONFIGURATION — Edit only this section before running
# =============================================================================
CATALOG = "my_catalog"  # Unity Catalog name (must match generate_data.py)
SCHEMA = "horizon_bank"  # Schema name (must match generate_data.py)
WAREHOUSE_ID = ""  # SQL Warehouse ID — leave empty to auto-detect
PARENT_PATH = ""  # Workspace folder for the Genie space — leave empty for current notebook directory

# SPACE_VERSION: 2 = metric views in data_sources.metric_views[] (v2 API).
# If the workspace rejects v2, revert to 1 and move metric views back to tables[].
SPACE_VERSION = 2

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Space Content

# COMMAND ----------

SPACE_TITLE = "Horizon Bank Analytics"
SPACE_DESCRIPTION = (
    "Retail banking analytics across customers, accounts, transactions, branches, and products. "
    "2023–2025 data for Horizon Bank (fictional)."
)

# Raw Delta tables (facts + dimensions)
TABLE_NAMES = [
    "transactions",
    "service_requests",
    "customers",
    "accounts",
    "products",
    "branches",
]

# Metric views (v2: in data_sources.metric_views[], not tables[])
METRIC_VIEW_NAMES = [
    "mv_banking_transactions",
    "mv_customer_health",
    "mv_service_quality",
]

SPACE_INSTRUCTIONS = """\
You are an AI analyst for Horizon Bank, a fictional US retail bank. The dataset covers 2023–2025.

RELATIONSHIP TIERS:
- Standard: mass-market customers
- Preferred: mid-tier, multi-product customers
- Private Client: high-net-worth; 3x higher average balances, 60% mortgage penetration
"""

SAMPLE_QUESTIONS = [
    # Tier 1 — Simple Aggregations & Filtering
    "What was total deposit volume in 2024?",
    # Tier 2 — Time-Series & Period Comparison
    "Show monthly deposit trend from Jan 2023 to Dec 2025",
    # Tier 3 — Multi-Table JOINs & Segmentation
    "What is the average account balance for Private Client customers by state?",
    # Tier 4 — Rankings & Top-N
    "Which 10 branches had the highest deposit volume this year?",
    # Tier 5 — Complex KPIs & Multi-Table Analysis
    "What is fee revenue per customer by relationship tier?",
    # Tier 6 — Agent Mode (Multi-Step Exploration)
    "Explain the deposit trends in 2024 — what drove the Q2 dip and the Q4 recovery?",
    # Tier 7 — Metric View Semantic Layer
    "What was YTD deposit volume as of December 2024?",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Build & Deploy

# COMMAND ----------

import json
import uuid

from databricks.sdk import WorkspaceClient

# COMMAND ----------


def _new_id() -> str:
    return uuid.uuid4().hex


def _build_table_configs(catalog: str, schema: str) -> list:
    cs = f"{catalog}.{schema}"
    return [
        {
            "identifier": f"{cs}.accounts",
            "description": [
                "~2,500 accounts linking customers to products. "
                "current_balance_usd is updated to reflect all transactions."
            ],
            "column_configs": [
                {
                    "column_name": "account_type",
                    "synonyms": ["type", "product type", "account kind"],
                    "enable_entity_matching": True,
                },
                {
                    "column_name": "status",
                    "synonyms": ["account status", "account health"],
                    "enable_entity_matching": True,
                },
                {
                    "column_name": "current_balance_usd",
                    "synonyms": ["balance", "account balance", "current balance"],
                },
                {
                    "column_name": "credit_limit_usd",
                    "synonyms": ["credit limit", "limit"],
                },
                {
                    "column_name": "interest_rate_pct",
                    "synonyms": ["interest rate", "rate", "APR", "APY"],
                },
                {
                    "column_name": "is_primary_account",
                    "synonyms": ["primary account", "main account"],
                },
            ],
        },
        {
            "identifier": f"{cs}.branches",
            "description": [
                "25 branches across 5 US regions. Southeast branches have ~20% higher average transaction values."
            ],
            "column_configs": [
                {
                    "column_name": "branch_name",
                    "synonyms": ["name", "branch"],
                },
                {
                    "column_name": "branch_type",
                    "synonyms": ["type", "branch kind"],
                    "enable_entity_matching": True,
                },
                {
                    "column_name": "region",
                    "synonyms": ["area", "territory", "geography"],
                    "enable_entity_matching": True,
                },
                {
                    "column_name": "state",
                    "synonyms": ["location"],
                    "enable_entity_matching": True,
                },
                {
                    "column_name": "city",
                    "synonyms": ["location"],
                    "enable_entity_matching": True,
                },
                {
                    "column_name": "monthly_operating_cost_usd",
                    "synonyms": ["operating cost", "branch cost", "overhead"],
                },
                {
                    "column_name": "headcount",
                    "synonyms": ["employees", "staff count", "FTE"],
                },
                {
                    "column_name": "is_active",
                    "synonyms": ["active", "open"],
                },
            ],
        },
        {
            "identifier": f"{cs}.customers",
            "description": [
                "1,000 customer records. Relationship tiers: Standard (60%), Preferred (30%), "
                "Private Client (10%). Private Client holds ~35% of total deposit volume and has "
                "3x higher average balances."
            ],
            "column_configs": [
                {
                    "column_name": "customer_name",
                    "synonyms": ["name", "customer"],
                },
                {
                    "column_name": "relationship_tier",
                    "synonyms": ["tier", "customer tier", "segment tier"],
                    "enable_entity_matching": True,
                },
                {
                    "column_name": "customer_segment",
                    "synonyms": ["segment", "customer type"],
                    "enable_entity_matching": True,
                },
                {
                    "column_name": "acquisition_channel",
                    "synonyms": ["how acquired", "acquisition source"],
                    "enable_entity_matching": True,
                },
                {
                    "column_name": "state",
                    "synonyms": ["home state"],
                    "enable_entity_matching": True,
                },
                {
                    "column_name": "region",
                    "synonyms": ["area", "geography"],
                    "enable_entity_matching": True,
                },
                {
                    "column_name": "age_band",
                    "synonyms": ["age", "age group", "age range"],
                    "enable_entity_matching": True,
                },
                {
                    "column_name": "income_band",
                    "synonyms": ["income", "income level", "income range"],
                    "enable_entity_matching": True,
                },
                {
                    "column_name": "credit_score_band",
                    "synonyms": ["credit score", "credit rating"],
                    "enable_entity_matching": True,
                },
                {
                    "column_name": "is_active",
                    "synonyms": ["active", "active customer"],
                },
                {
                    "column_name": "has_checking",
                    "synonyms": ["checking holder"],
                },
                {
                    "column_name": "has_savings",
                    "synonyms": ["savings holder"],
                },
                {
                    "column_name": "has_credit_card",
                    "synonyms": ["credit card holder", "card holder"],
                },
                {
                    "column_name": "has_mortgage",
                    "synonyms": ["mortgage holder", "homeowner"],
                },
                {
                    "column_name": "customer_since_date",
                    "synonyms": ["join date", "tenure", "since date"],
                },
            ],
        },
        {
            "identifier": f"{cs}.products",
            "description": [
                "20-row product catalog across Deposit (9), Credit (4), and Lending (7) product categories."
            ],
            "column_configs": [
                {
                    "column_name": "product_name",
                    "synonyms": ["name", "product"],
                },
                {
                    "column_name": "product_category",
                    "synonyms": ["category", "product line"],
                    "enable_entity_matching": True,
                },
                {
                    "column_name": "product_type",
                    "synonyms": ["type"],
                    "enable_entity_matching": True,
                },
                {
                    "column_name": "annual_fee_usd",
                    "synonyms": ["annual fee", "fee"],
                },
                {
                    "column_name": "base_interest_rate_pct",
                    "synonyms": ["interest rate", "base rate", "APR", "APY"],
                },
                {
                    "column_name": "reward_program",
                    "synonyms": ["rewards", "loyalty program"],
                    "enable_entity_matching": True,
                },
                {
                    "column_name": "min_balance_usd",
                    "synonyms": ["minimum balance", "min balance"],
                },
            ],
        },
        {
            "identifier": f"{cs}.service_requests",
            "description": [
                "Secondary fact table: 3,000 customer service interactions from 2023-2025. "
                "Includes a Jan 2024 complaint spike (+80%) simulating a system outage."
            ],
            "column_configs": [
                {
                    "column_name": "category",
                    "synonyms": ["request type", "inquiry type", "service type"],
                    "enable_entity_matching": True,
                },
                {
                    "column_name": "status",
                    "synonyms": ["request status", "resolution status"],
                    "enable_entity_matching": True,
                },
                {
                    "column_name": "channel",
                    "synonyms": ["contact channel", "support channel"],
                    "enable_entity_matching": True,
                },
                {
                    "column_name": "resolution_time_days",
                    "synonyms": [
                        "resolution time",
                        "time to resolve",
                        "days to resolve",
                    ],
                },
                {
                    "column_name": "satisfaction_score",
                    "synonyms": ["CSAT", "satisfaction", "rating"],
                },
                {
                    "column_name": "is_resolved",
                    "synonyms": ["resolved", "was resolved"],
                },
                {
                    "column_name": "request_year",
                    "synonyms": ["year"],
                },
                {
                    "column_name": "request_month",
                    "synonyms": ["month"],
                },
            ],
        },
        {
            "identifier": f"{cs}.transactions",
            "description": [
                "Primary fact table: 10,000 transactions from 2023-01-01 to 2025-12-31 across "
                "Deposit, Withdrawal, Transfer, Payment, Purchase (credit), Fee, and Interest types."
            ],
            "column_configs": [
                {
                    "column_name": "transaction_type",
                    "synonyms": ["type", "txn type"],
                    "enable_entity_matching": True,
                },
                {
                    "column_name": "channel",
                    "synonyms": ["digital", "in-branch", "mobile channel"],
                    "enable_entity_matching": True,
                },
                {
                    "column_name": "amount_usd",
                    "synonyms": ["amount", "transaction amount", "volume"],
                },
                {
                    "column_name": "fee_usd",
                    "synonyms": ["fee", "fees", "fee revenue"],
                },
                {
                    "column_name": "balance_after_usd",
                    "synonyms": ["balance after", "post-transaction balance"],
                },
                {
                    "column_name": "merchant_category",
                    "synonyms": ["merchant", "category", "spend category"],
                    "enable_entity_matching": True,
                },
                {
                    "column_name": "status",
                    "synonyms": ["transaction status", "settlement status"],
                    "enable_entity_matching": True,
                },
                {
                    "column_name": "is_flagged",
                    "synonyms": ["flagged", "fraud flag"],
                },
                {
                    "column_name": "is_international",
                    "synonyms": ["international", "cross-border", "foreign"],
                },
                {
                    "column_name": "transaction_year",
                    "synonyms": ["year"],
                },
                {
                    "column_name": "transaction_month",
                    "synonyms": ["month"],
                },
                {
                    "column_name": "transaction_quarter",
                    "synonyms": ["quarter", "qtr"],
                },
            ],
        },
    ]


def _build_metric_view_configs(catalog: str, schema: str) -> list:
    cs = f"{catalog}.{schema}"
    return [
        {
            "identifier": f"{cs}.mv_banking_transactions",
            "description": [
                "Transaction KPIs including deposit volume, withdrawal volume, net flow, fee revenue, "
                "digital share percentage, and month-over-month / YTD window measures. "
                "Preferred source for all transaction and channel questions."
            ],
        },
        {
            "identifier": f"{cs}.mv_customer_health",
            "description": [
                "Portfolio KPIs including total balance, average balance, delinquency rate, "
                "mortgage penetration rate, and dormancy rate — segmented by relationship tier, "
                "account type, region, and product."
            ],
        },
        {
            "identifier": f"{cs}.mv_service_quality",
            "description": [
                "Service KPIs including complaint count, resolution rate, escalation rate, "
                "and month-over-month complaint change. "
                "Preferred source for all service request and complaint questions."
            ],
        },
    ]


def _build_example_sqls(catalog: str, schema: str) -> list:
    cs = f"{catalog}.{schema}"
    return [
        # --- Transaction KPIs ---
        {
            "id": _new_id(),
            "question": ["What was total deposit volume in 2024?"],
            "sql": [
                f"SELECT MEASURE(`Deposit Volume`) FROM {cs}.mv_banking_transactions WHERE `Transaction Year` = 2024"
            ],
            "usage_guidance": [
                "Use MEASURE(`Deposit Volume`) from mv_banking_transactions for any deposit aggregation"
            ],
        },
        {
            "id": _new_id(),
            "question": ["What was YTD deposit volume as of December 2024?"],
            "sql": [
                f"SELECT MEASURE(`YTD Deposit Volume`) FROM {cs}.mv_banking_transactions WHERE `Transaction Year` = 2024 AND `Transaction Month` = '2024-12-01'"
            ],
            "usage_guidance": [
                "YTD Deposit Volume is a window measure on mv_banking_transactions — always filter by year and month"
            ],
        },
        {
            "id": _new_id(),
            "question": [
                "Show month-over-month deposit growth by relationship tier for 2024"
            ],
            "sql": [
                f"SELECT `Relationship Tier`, `Transaction Month`, ",
                f"MEASURE(`Month-over-Month Deposit Growth Pct`) ",
                f"FROM {cs}.mv_banking_transactions ",
                f"WHERE `Transaction Year` = 2024",
            ],
            "usage_guidance": [
                "Window measures like MoM growth must use mv_banking_transactions, not the raw transactions table"
            ],
        },
        {
            "id": _new_id(),
            "question": ["What was total withdrawal volume in 2024?"],
            "sql": [
                f"SELECT MEASURE(`Withdrawal Volume`) FROM {cs}.mv_banking_transactions WHERE `Transaction Year` = 2024"
            ],
            "usage_guidance": [
                "Use MEASURE(`Withdrawal Volume`) from mv_banking_transactions for withdrawal aggregations"
            ],
        },
        {
            "id": _new_id(),
            "question": ["What is the net flow by region for 2024?"],
            "sql": [
                f"SELECT `Region`, MEASURE(`Net Flow`) FROM {cs}.mv_banking_transactions WHERE `Transaction Year` = 2024 GROUP BY `Region`"
            ],
            "usage_guidance": [
                "Net Flow = Deposit Volume minus Withdrawal Volume; use mv_banking_transactions"
            ],
        },
        {
            "id": _new_id(),
            "question": ["What is the total fee revenue collected in 2024?"],
            "sql": [
                f"SELECT MEASURE(`Fee Revenue`) FROM {cs}.mv_banking_transactions WHERE `Transaction Year` = 2024"
            ],
            "usage_guidance": [
                "Use MEASURE(`Fee Revenue`) from mv_banking_transactions for fee revenue questions"
            ],
        },
        {
            "id": _new_id(),
            "question": ["How many digital transactions were made in 2024?"],
            "sql": [
                f"SELECT MEASURE(`Digital Transaction Count`) FROM {cs}.mv_banking_transactions WHERE `Transaction Year` = 2024"
            ],
            "usage_guidance": [
                "Digital Transaction Count counts Mobile + Online channel transactions; use mv_banking_transactions"
            ],
        },
        {
            "id": _new_id(),
            "question": ["What is the digital share percentage for 2024?"],
            "sql": [
                f"SELECT MEASURE(`Digital Share Pct`) FROM {cs}.mv_banking_transactions WHERE `Transaction Year` = 2024"
            ],
            "usage_guidance": [
                "Digital Share Pct = Digital Transaction Count / Total Transactions; use mv_banking_transactions"
            ],
        },
        # --- Customer KPIs ---
        {
            "id": _new_id(),
            "question": ["How many active customers do we have?"],
            "sql": [
                f"SELECT COUNT(DISTINCT customer_id) FROM {cs}.customers WHERE is_active = TRUE"
            ],
            "usage_guidance": [
                "Filter customers table with is_active = TRUE for active customer counts"
            ],
        },
        {
            "id": _new_id(),
            "question": ["Who are our high-value customers?"],
            "sql": [
                f"SELECT * FROM {cs}.customers WHERE relationship_tier = 'Private Client' ORDER BY customer_id"
            ],
            "usage_guidance": [
                "High-value customers = relationship_tier = 'Private Client'; they hold ~35% of deposit volume"
            ],
        },
        {
            "id": _new_id(),
            "question": ["Who are our top 10 customers by deposit volume in 2024?"],
            "sql": [
                f"SELECT c.customer_id, c.customer_name, SUM(t.amount_usd) AS deposit_volume ",
                f"FROM {cs}.transactions t ",
                f"JOIN {cs}.customers c ON t.customer_id = c.customer_id ",
                f"WHERE t.transaction_type = 'Deposit' AND t.transaction_year = 2024 ",
                f"GROUP BY c.customer_id, c.customer_name ORDER BY deposit_volume DESC LIMIT 10",
            ],
            "usage_guidance": [
                "Use raw transactions table joined with customers for per-customer rankings (customer_id is not a metric view dimension)"
            ],
        },
        # --- Portfolio KPIs ---
        {
            "id": _new_id(),
            "question": ["What is the average account balance by relationship tier?"],
            "sql": [
                f"SELECT `Relationship Tier`, MEASURE(`Average Balance`) FROM {cs}.mv_customer_health GROUP BY `Relationship Tier`"
            ],
            "usage_guidance": [
                "Use MEASURE(`Average Balance`) from mv_customer_health for balance KPIs segmented by tier"
            ],
        },
        {
            "id": _new_id(),
            "question": ["What is the delinquency rate by relationship tier?"],
            "sql": [
                f"SELECT `Relationship Tier`, MEASURE(`Delinquency Rate Pct`) FROM {cs}.mv_customer_health GROUP BY `Relationship Tier`"
            ],
            "usage_guidance": [
                "Use MEASURE(`Delinquency Rate Pct`) from mv_customer_health for delinquency analysis"
            ],
        },
        {
            "id": _new_id(),
            "question": ["What is the mortgage penetration rate by relationship tier?"],
            "sql": [
                f"SELECT `Relationship Tier`, MEASURE(`Mortgage Penetration Rate Pct`) FROM {cs}.mv_customer_health GROUP BY `Relationship Tier`"
            ],
            "usage_guidance": [
                "Use MEASURE(`Mortgage Penetration Rate Pct`) from mv_customer_health; Private Client = 60%, Standard = 18%"
            ],
        },
        # --- Service KPIs ---
        {
            "id": _new_id(),
            "question": ["What was the complaint count in January 2024?"],
            "sql": [
                f"SELECT MEASURE(`Complaint Count`) FROM {cs}.mv_service_quality WHERE `Request Year` = 2024 AND `Request Month` = '2024-01-01'"
            ],
            "usage_guidance": [
                "Use MEASURE(`Complaint Count`) from mv_service_quality; Jan 2024 had an 80% spike due to system outage"
            ],
        },
        {
            "id": _new_id(),
            "question": ["What is the complaint resolution rate by channel?"],
            "sql": [
                f"SELECT `Channel`, MEASURE(`Resolution Rate Pct`) FROM {cs}.mv_service_quality GROUP BY `Channel`"
            ],
            "usage_guidance": [
                "Use MEASURE(`Resolution Rate Pct`) from mv_service_quality for complaint resolution analysis"
            ],
        },
        {
            "id": _new_id(),
            "question": ["Show month-over-month complaint change for 2024"],
            "sql": [
                f"SELECT `Request Month`, MEASURE(`Month-over-Month Complaint Change Pct`) ",
                f"FROM {cs}.mv_service_quality ",
                f"WHERE `Request Year` = 2024",
            ],
            "usage_guidance": [
                "MoM Complaint Change is a window measure on mv_service_quality; surfaces the Jan 2024 spike"
            ],
        },
        {
            "id": _new_id(),
            "question": ["How many service requests are currently unresolved?"],
            "sql": [
                f"SELECT MEASURE(`Open Count`) + MEASURE(`Escalated Count`) AS unresolved_requests ",
                f"FROM {cs}.mv_service_quality",
            ],
            "usage_guidance": [
                "Unresolved = Open + Escalated; use mv_service_quality with Open Count and Escalated Count measures"
            ],
        },
    ]


def _build_benchmarks(catalog: str, schema: str) -> dict:
    cs = f"{catalog}.{schema}"
    return {
        "questions": [
            # --- Easy (2) ---
            {
                "id": _new_id(),
                "question": ["How many active customers do we have?"],
                "answer": [
                    {
                        "format": "SQL",
                        "content": [
                            f"SELECT COUNT(DISTINCT customer_id) ",
                            f"FROM {cs}.customers ",
                            f"WHERE is_active = TRUE",
                        ],
                    }
                ],
            },
            {
                "id": _new_id(),
                "question": ["What was total deposit volume in 2024?"],
                "answer": [
                    {
                        "format": "SQL",
                        "content": [
                            f"SELECT MEASURE(`Deposit Volume`) ",
                            f"FROM {cs}.mv_banking_transactions ",
                            f"WHERE `Transaction Year` = 2024",
                        ],
                    }
                ],
            },
            # --- Medium (3) ---
            {
                "id": _new_id(),
                "question": ["What are the top 5 products by number of accounts?"],
                "answer": [
                    {
                        "format": "SQL",
                        "content": [
                            f"SELECT p.product_name, COUNT(*) AS account_count ",
                            f"FROM {cs}.accounts a JOIN {cs}.products p ON a.product_id = p.product_id ",
                            f"GROUP BY p.product_name ORDER BY account_count DESC LIMIT 5",
                        ],
                    }
                ],
            },
            {
                "id": _new_id(),
                "question": ["Show monthly deposit volume from Jan 2023 to Dec 2025"],
                "answer": [
                    {
                        "format": "SQL",
                        "content": [
                            f"SELECT `Transaction Month`, `Transaction Year`, MEASURE(`Deposit Volume`) ",
                            f"FROM {cs}.mv_banking_transactions ",
                            f"GROUP BY `Transaction Month`, `Transaction Year` ",
                            f"ORDER BY `Transaction Year`, `Transaction Month`",
                        ],
                    }
                ],
            },
            {
                "id": _new_id(),
                "question": ["Compare Q2 2024 vs Q2 2023 net flow by region"],
                "answer": [
                    {
                        "format": "SQL",
                        "content": [
                            f"SELECT `Region`, `Transaction Year`, MEASURE(`Net Flow`) ",
                            f"FROM {cs}.mv_banking_transactions ",
                            f"WHERE `Transaction Month` BETWEEN '2023-04-01' AND '2023-06-01' ",
                            f"OR `Transaction Month` BETWEEN '2024-04-01' AND '2024-06-01' ",
                            f"GROUP BY `Region`, `Transaction Year`",
                        ],
                    }
                ],
            },
            # --- Hard — Realistic (3) ---
            {
                "id": _new_id(),
                "question": [
                    "What is fee revenue per customer by relationship tier in 2024?"
                ],
                "answer": [
                    {
                        "format": "SQL",
                        "content": [
                            f"SELECT c.relationship_tier, ",
                            f"       SUM(t.fee_usd) AS total_fee_revenue, ",
                            f"       COUNT(DISTINCT c.customer_id) AS customer_count, ",
                            f"       SUM(t.fee_usd) / COUNT(DISTINCT c.customer_id) AS fee_per_customer ",
                            f"FROM {cs}.transactions t ",
                            f"JOIN {cs}.accounts a ON t.account_id = a.account_id ",
                            f"JOIN {cs}.customers c ON a.customer_id = c.customer_id ",
                            f"WHERE t.transaction_year = 2024 ",
                            f"GROUP BY c.relationship_tier",
                        ],
                    }
                ],
            },
            {
                "id": _new_id(),
                "question": [
                    "List Private Client customers who have at least one open service "
                    "request and total account balances above $100,000"
                ],
                "answer": [
                    {
                        "format": "SQL",
                        "content": [
                            f"SELECT c.customer_id, c.customer_name, SUM(a.current_balance_usd) AS total_balance, ",
                            f"       COUNT(sr.request_id) AS open_requests ",
                            f"FROM {cs}.customers c ",
                            f"JOIN {cs}.accounts a ON c.customer_id = a.customer_id ",
                            f"JOIN {cs}.service_requests sr ON c.customer_id = sr.customer_id ",
                            f"WHERE c.relationship_tier = 'Private Client' AND sr.status = 'Open' ",
                            f"GROUP BY c.customer_id, c.customer_name ",
                            f"HAVING SUM(a.current_balance_usd) > 100000",
                        ],
                    }
                ],
            },
            {
                "id": _new_id(),
                "question": [
                    "What is the monthly digital transaction share by region for 2024?"
                ],
                "answer": [
                    {
                        "format": "SQL",
                        "content": [
                            f"SELECT b.region, t.transaction_month, ",
                            f"       COUNT(CASE WHEN t.channel IN ('Mobile', 'Online') THEN 1 END) * 100.0 / COUNT(*) AS digital_share_pct ",
                            f"FROM {cs}.transactions t ",
                            f"JOIN {cs}.branches b ON t.branch_id = b.branch_id ",
                            f"WHERE t.transaction_year = 2024 ",
                            f"GROUP BY b.region, t.transaction_month ",
                            f"ORDER BY b.region, t.transaction_month",
                        ],
                    }
                ],
            },
            # --- Hard — Push the Limits (2) ---
            {
                "id": _new_id(),
                "question": [
                    "Which 5 branches had the largest year-over-year increase in "
                    "deposit volume from 2023 to 2024?"
                ],
                "answer": [
                    {
                        "format": "SQL",
                        "content": [
                            f"WITH branch_deposits AS ( ",
                            f"  SELECT b.branch_name, t.transaction_year, ",
                            f"         SUM(t.amount_usd) AS deposit_volume ",
                            f"  FROM {cs}.transactions t ",
                            f"  JOIN {cs}.branches b ON t.branch_id = b.branch_id ",
                            f"  WHERE t.transaction_type = 'Deposit' AND t.transaction_year IN (2023, 2024) ",
                            f"  GROUP BY b.branch_name, t.transaction_year ",
                            f") ",
                            f"SELECT d24.branch_name, ",
                            f"       d24.deposit_volume AS volume_2024, ",
                            f"       d23.deposit_volume AS volume_2023, ",
                            f"       d24.deposit_volume - d23.deposit_volume AS yoy_increase ",
                            f"FROM branch_deposits d24 ",
                            f"JOIN branch_deposits d23 ON d24.branch_name = d23.branch_name ",
                            f"WHERE d24.transaction_year = 2024 AND d23.transaction_year = 2023 ",
                            f"ORDER BY yoy_increase DESC LIMIT 5",
                        ],
                    }
                ],
            },
            {
                "id": _new_id(),
                "question": [
                    "Who are the top 20 customers with a checking account but no "
                    "credit card, ranked by 2024 deposit volume?"
                ],
                "answer": [
                    {
                        "format": "SQL",
                        "content": [
                            f"SELECT c.customer_id, c.customer_name, c.relationship_tier, ",
                            f"       SUM(t.amount_usd) AS deposit_volume ",
                            f"FROM {cs}.customers c ",
                            f"JOIN {cs}.accounts a ON c.customer_id = a.customer_id ",
                            f"JOIN {cs}.transactions t ON a.account_id = t.account_id ",
                            f"WHERE c.has_checking = TRUE AND c.has_credit_card = FALSE ",
                            f"  AND t.transaction_type = 'Deposit' AND t.transaction_year = 2024 ",
                            f"GROUP BY c.customer_id, c.customer_name, c.relationship_tier ",
                            f"ORDER BY deposit_volume DESC LIMIT 20",
                        ],
                    }
                ],
            },
        ]
    }


def build_serialized_space(
    catalog: str,
    schema: str,
    instructions: str,
    sample_questions: list,
) -> dict:
    # Build raw configs
    tables = _build_table_configs(catalog, schema)
    metric_views = _build_metric_view_configs(catalog, schema)
    example_sqls = _build_example_sqls(catalog, schema)
    benchmarks = _build_benchmarks(catalog, schema)
    sample_qs = [{"id": _new_id(), "question": [q]} for q in sample_questions]

    # --- Enforce sorting rules (see docs: validation-rules-for-serialized_space) ---
    # Tables & metric views sorted by identifier
    tables.sort(key=lambda t: t["identifier"])
    metric_views.sort(key=lambda m: m["identifier"])
    # Column configs sorted by column_name within each table
    for t in tables:
        if "column_configs" in t:
            t["column_configs"].sort(key=lambda c: c["column_name"])
    # All id-bearing collections sorted by id
    sample_qs.sort(key=lambda x: x["id"])
    example_sqls.sort(key=lambda x: x["id"])
    benchmarks["questions"].sort(key=lambda x: x["id"])

    return {
        "version": SPACE_VERSION,
        "data_sources": {
            "tables": tables,
            "metric_views": metric_views,
        },
        "instructions": {
            "text_instructions": [{"id": _new_id(), "content": [instructions]}],
            "example_question_sqls": example_sqls,
            "join_specs": [],  # auto-derived from UC PK/FK constraints
            "sql_snippets": {},  # covered by metric view MEASURE() semantics
        },
        "config": {
            "sample_questions": sample_qs,
        },
        "benchmarks": benchmarks,
    }


# COMMAND ----------


def get_warehouse_id(w: WorkspaceClient, override: str = "") -> str:
    """Return the override ID if set, otherwise auto-detect the first available warehouse."""
    if override.strip():
        return override.strip()
    from databricks.sdk.service.sql import State

    warehouses = list(w.warehouses.list())
    for state in (State.RUNNING, State.STARTING, State.STOPPED):
        for wh in warehouses:
            if wh.state == state:
                return wh.id
    raise RuntimeError("No SQL warehouses found. Create one and re-run.")


# COMMAND ----------


def find_space_by_title(w: WorkspaceClient, title: str):
    """Return the first Genie space matching the given title, or None."""
    response = w.genie.list_spaces()
    for space in response.spaces or []:
        if space.title == title:
            return space
    return None


# COMMAND ----------


def create_or_update_genie_space(
    catalog: str,
    schema: str,
    warehouse_id: str,
    title: str,
    description: str,
    instructions: str,
    sample_questions: list,
    parent_path: str = "",
) -> str:
    """Create a new Genie space or update an existing one with the same title. Returns space_id."""
    w = WorkspaceClient()
    wh_id = get_warehouse_id(w, warehouse_id)
    space_dict = build_serialized_space(catalog, schema, instructions, sample_questions)
    serialized = json.dumps(space_dict, ensure_ascii=False)

    existing = find_space_by_title(w, title)
    if existing:
        print(f"Found existing space '{title}' ({existing.space_id}) — updating...")
        w.genie.update_space(
            space_id=existing.space_id,
            serialized_space=serialized,
            title=title,
            description=description,
            warehouse_id=wh_id,
        )
        return existing.space_id
    else:
        print(f"Creating new Genie space '{title}'...")
        create_kwargs = dict(
            warehouse_id=wh_id,
            serialized_space=serialized,
            title=title,
            description=description,
        )
        if parent_path.strip():
            create_kwargs["parent_path"] = parent_path.strip()
        result = w.genie.create_space(**create_kwargs)
        return result.space_id


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Run

# COMMAND ----------

# Resolve parent_path: use configured value or fall back to notebook's directory
if PARENT_PATH.strip():
    _resolved_parent = PARENT_PATH.strip()
else:
    _ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    _resolved_parent = _ctx.notebookPath().get().rsplit("/", 1)[0]

space_id = create_or_update_genie_space(
    catalog=CATALOG,
    schema=SCHEMA,
    warehouse_id=WAREHOUSE_ID,
    title=SPACE_TITLE,
    description=SPACE_DESCRIPTION,
    instructions=SPACE_INSTRUCTIONS,
    sample_questions=SAMPLE_QUESTIONS,
    parent_path=_resolved_parent,
)

w = WorkspaceClient()
space_url = f"{w.config.host.rstrip('/')}/genie/rooms/{space_id}"
print(f"  Space ID : {space_id}")
print(f"  URL      : {space_url}")
displayHTML(
    f'<a href="{space_url}" target="_blank" style="font-size:16px">Open Horizon Bank Analytics in Genie</a>'
)

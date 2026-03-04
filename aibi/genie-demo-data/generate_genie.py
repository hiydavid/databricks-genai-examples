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

# MAGIC %pip install databricks-sdk --quiet
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

# =============================================================================
# CONFIGURATION — Edit only this section before running
# =============================================================================
CATALOG = "my_catalog"   # Unity Catalog name (must match generate_data.py)
SCHEMA = "horizon_bank"  # Schema name (must match generate_data.py)
WAREHOUSE_ID = ""        # SQL Warehouse ID — leave empty to auto-detect

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
TABLE_NAMES = ["transactions", "service_requests", "customers", "accounts", "products", "branches"]

# Metric views (v2: in data_sources.metric_views[], not tables[])
METRIC_VIEW_NAMES = ["mv_banking_transactions", "mv_customer_health", "mv_service_quality"]

SPACE_INSTRUCTIONS = """\
You are an AI analyst for Horizon Bank, a fictional US retail bank. The dataset covers 2023–2025.

KNOWN DATA PATTERNS (mention these when relevant):
- Nov/Dec each year has ~45% higher transaction volume (holiday season)
- Q2 2024 shows a deposit dip (-15%) due to macro softness
- Mobile channel share grew from 25% (Jan 2023) to 48% (Dec 2025)
- Top 10% of customers (Private Client tier) account for ~35% of total deposit volume
- Southeast region branches have ~20% higher average transaction values
- Jan 2024 saw an 80% spike in Complaint service requests (system outage)

RELATIONSHIP TIERS:
- Standard: mass-market customers
- Preferred: mid-tier, multi-product customers
- Private Client: high-net-worth; 3x higher average balances, 60% mortgage penetration
"""

SAMPLE_QUESTIONS = [
    # Tier 1 — Simple Aggregations & Filtering
    "What was total deposit volume in 2024?",
    "How many active customers do we have?",
    "What are the top 5 products by number of accounts?",
    "Show me all transactions over $5,000 in the Northeast in Q4 2024",
    "What is the total fee revenue collected in 2025?",
    # Tier 2 — Time-Series & Period Comparison
    "Show monthly deposit trend from Jan 2023 to Dec 2025",
    "Compare Q2 2024 vs Q2 2023 net flow by region",
    "How has the mobile channel share changed over time?",
    "Show me a chart of monthly transaction volume by channel for 2024",
    "What months had the highest fee revenue in 2024?",
    # Tier 3 — Multi-Table JOINs & Segmentation
    "What is the average account balance for Private Client customers by state?",
    "What is the delinquency rate by relationship tier and region?",
    "Break down transaction volume by account type and year",
    "Which acquisition channel produces the highest-balance customers?",
    "What percentage of Preferred customers have a mortgage?",
    # Tier 4 — Rankings & Top-N
    "Which 10 branches had the highest deposit volume this year?",
    "Who are our top 20 customers by total deposits in 2024?",
    "Which merchant categories drive the most credit card spending?",
    "Rank regions by average transaction value",
    # Tier 5 — Complex KPIs & Multi-Table Analysis
    "What is fee revenue per customer by relationship tier?",
    "Do customers with unresolved service complaints have lower average balances?",
    "Which states have the highest concentration of delinquent accounts?",
    "Compare customer lifetime deposit value by acquisition channel",
    "What is the complaint resolution time trend in 2024 vs 2023?",
    # Tier 6 — Agent Mode (Multi-Step Exploration)
    "Analyze branch profitability — which branches are underperforming vs. their operating cost?",
    "Which customer segments are most at risk of churning based on recent activity?",
    "Explain the deposit trends in 2024 — what drove the Q2 dip and the Q4 recovery?",
    "Build me a complete profile of our Private Client customers",
    "Are there any unusual patterns in the January 2024 service requests?",
    # Tier 7 — Metric View Semantic Layer
    "What was YTD deposit volume as of December 2024?",
    "Show month-over-month deposit growth by relationship tier for 2024",
    "Show the month-over-month complaint change for 2024 — when was the worst spike?",
    "How has digital share changed by relationship tier over the last 3 years?",
    "What is fee revenue per customer for Private Client vs Standard customers?",
    "What is the trailing 3-month resolution rate trend for escalated vs non-escalated channels?",
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
            "id": _new_id(),
            "identifier": f"{cs}.transactions",
            "description": (
                "Primary fact table: 10,000 transactions from 2023-01-01 to 2025-12-31 across "
                "Deposit, Withdrawal, Transfer, Payment, Purchase (credit), Fee, and Interest types."
            ),
            "column_configs": [
                {
                    "column_name": "transaction_type",
                    "synonyms": ["type", "txn type"],
                    "enable_entity_matching": True,
                },
                {
                    "column_name": "channel",
                    "description": "Mobile/Branch/ATM/Online/Wire. Mobile grew from 25% (Jan 2023) to 48% (Dec 2025).",
                    "synonyms": ["digital", "in-branch", "mobile channel"],
                    "enable_entity_matching": True,
                },
                {
                    "column_name": "amount_usd",
                    "description": "Transaction amount in USD; always positive regardless of direction.",
                    "synonyms": ["amount", "transaction amount", "volume"],
                },
                {
                    "column_name": "fee_usd",
                    "description": "Separate fee charged on the transaction (wire, overdraft, or service fees). Zero for non-fee transactions.",
                    "synonyms": ["fee", "fees", "fee revenue"],
                },
                {
                    "column_name": "merchant_category",
                    "synonyms": ["merchant", "category", "spend category"],
                    "enable_entity_matching": True,
                },
                {
                    "column_name": "is_flagged",
                    "description": "TRUE for ~0.5% of transactions flagged for fraud or AML monitoring.",
                },
            ],
        },
        {
            "id": _new_id(),
            "identifier": f"{cs}.service_requests",
            "description": (
                "Secondary fact table: 3,000 customer service interactions from 2023-2025. "
                "Includes a Jan 2024 complaint spike (+80%) simulating a system outage."
            ),
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
                    "column_name": "satisfaction_score",
                    "description": "Customer satisfaction rating 1-5. NULL if request is unresolved. Complaints skew low (1-2); resolved inquiries skew high (4-5).",
                },
            ],
        },
        {
            "id": _new_id(),
            "identifier": f"{cs}.customers",
            "description": (
                "1,000 customer records. Relationship tiers: Standard (60%), Preferred (30%), "
                "Private Client (10%). Private Client holds ~35% of total deposit volume and has "
                "3x higher average balances."
            ),
            "column_configs": [
                {
                    "column_name": "relationship_tier",
                    "description": "Standard / Preferred / Private Client. Private Client = high-net-worth, 60% mortgage penetration.",
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
                    "column_name": "is_active",
                    "description": "TRUE for ~94% of customers.",
                    "synonyms": ["active", "active customer"],
                },
            ],
        },
        {
            "id": _new_id(),
            "identifier": f"{cs}.accounts",
            "description": (
                "~2,500 accounts linking customers to products. "
                "current_balance_usd is updated to reflect all transactions."
            ),
            "column_configs": [
                {
                    "column_name": "account_type",
                    "synonyms": ["type", "product type", "account kind"],
                    "enable_entity_matching": True,
                },
                {
                    "column_name": "status",
                    "description": "Active (78%) / Dormant (10%) / Closed (8%) / Delinquent (4%).",
                    "synonyms": ["account status", "account health"],
                    "enable_entity_matching": True,
                },
                {
                    "column_name": "current_balance_usd",
                    "synonyms": ["balance", "account balance", "current balance"],
                },
            ],
        },
        {
            "id": _new_id(),
            "identifier": f"{cs}.products",
            "description": "20-row product catalog across Deposit (9), Credit (4), and Lending (7) product categories.",
            "column_configs": [
                {
                    "column_name": "product_category",
                    "synonyms": ["category", "product type", "product line"],
                    "enable_entity_matching": True,
                },
            ],
        },
        {
            "id": _new_id(),
            "identifier": f"{cs}.branches",
            "description": "25 branches across 5 US regions. Southeast branches have ~20% higher average transaction values.",
            "column_configs": [
                {
                    "column_name": "region",
                    "description": "Northeast / Southeast / Midwest / West / Southwest. Southeast branches average 20% higher transaction amounts.",
                    "synonyms": ["area", "territory", "geography"],
                    "enable_entity_matching": True,
                },
                {
                    "column_name": "branch_type",
                    "synonyms": ["type", "branch kind"],
                    "enable_entity_matching": True,
                },
            ],
        },
    ]


def _build_metric_view_configs(catalog: str, schema: str) -> list:
    cs = f"{catalog}.{schema}"
    return [
        {
            "id": _new_id(),
            "identifier": f"{cs}.mv_banking_transactions",
            "description": (
                "Transaction KPIs including deposit volume, withdrawal volume, net flow, fee revenue, "
                "digital share percentage, and month-over-month / YTD window measures. "
                "Preferred source for all transaction and channel questions."
            ),
        },
        {
            "id": _new_id(),
            "identifier": f"{cs}.mv_customer_health",
            "description": (
                "Portfolio KPIs including total balance, average balance, delinquency rate, "
                "mortgage penetration rate, and dormancy rate — segmented by relationship tier, "
                "account type, region, and product."
            ),
        },
        {
            "id": _new_id(),
            "identifier": f"{cs}.mv_service_quality",
            "description": (
                "Service KPIs including complaint count, resolution rate, escalation rate, "
                "and month-over-month complaint change. "
                "Preferred source for all service request and complaint questions."
            ),
        },
    ]


def _build_example_sqls(catalog: str, schema: str) -> list:
    cs = f"{catalog}.{schema}"
    return [
        # --- Transaction KPIs ---
        {
            "id": _new_id(),
            "question": ["What was total deposit volume in 2024?"],
            "sql": [f"SELECT MEASURE(`Deposit Volume`) FROM {cs}.mv_banking_transactions WHERE transaction_year = 2024"],
            "usage_guidance": ["Use MEASURE(`Deposit Volume`) from mv_banking_transactions for any deposit aggregation"],
        },
        {
            "id": _new_id(),
            "question": ["What was YTD deposit volume as of December 2024?"],
            "sql": [f"SELECT MEASURE(`YTD Deposit Volume`) FROM {cs}.mv_banking_transactions WHERE transaction_year = 2024 AND transaction_month = 12"],
            "usage_guidance": ["YTD Deposit Volume is a window measure on mv_banking_transactions — always filter by year and month"],
        },
        {
            "id": _new_id(),
            "question": ["Show month-over-month deposit growth by relationship tier for 2024"],
            "sql": [
                f"SELECT `Relationship Tier`, `Transaction Month`, ",
                f"MEASURE(`Month-over-Month Deposit Growth Pct`) ",
                f"FROM {cs}.mv_banking_transactions ",
                f"WHERE transaction_year = 2024",
            ],
            "usage_guidance": ["Window measures like MoM growth must use mv_banking_transactions, not the raw transactions table"],
        },
        {
            "id": _new_id(),
            "question": ["What was total withdrawal volume in 2024?"],
            "sql": [f"SELECT MEASURE(`Withdrawal Volume`) FROM {cs}.mv_banking_transactions WHERE transaction_year = 2024"],
            "usage_guidance": ["Use MEASURE(`Withdrawal Volume`) from mv_banking_transactions for withdrawal aggregations"],
        },
        {
            "id": _new_id(),
            "question": ["What is the net flow by region for 2024?"],
            "sql": [f"SELECT Region, MEASURE(`Net Flow`) FROM {cs}.mv_banking_transactions WHERE transaction_year = 2024 GROUP BY Region"],
            "usage_guidance": ["Net Flow = Deposit Volume minus Withdrawal Volume; use mv_banking_transactions"],
        },
        {
            "id": _new_id(),
            "question": ["What is the total fee revenue collected in 2024?"],
            "sql": [f"SELECT MEASURE(`Fee Revenue`) FROM {cs}.mv_banking_transactions WHERE transaction_year = 2024"],
            "usage_guidance": ["Use MEASURE(`Fee Revenue`) from mv_banking_transactions for fee revenue questions"],
        },
        {
            "id": _new_id(),
            "question": ["How many digital transactions were made in 2024?"],
            "sql": [f"SELECT MEASURE(`Digital Transaction Count`) FROM {cs}.mv_banking_transactions WHERE transaction_year = 2024"],
            "usage_guidance": ["Digital Transaction Count counts Mobile + Online channel transactions; use mv_banking_transactions"],
        },
        {
            "id": _new_id(),
            "question": ["What is the digital share percentage for 2024?"],
            "sql": [f"SELECT MEASURE(`Digital Share Pct`) FROM {cs}.mv_banking_transactions WHERE transaction_year = 2024"],
            "usage_guidance": ["Digital Share Pct = Digital Transaction Count / Total Transactions; use mv_banking_transactions"],
        },
        # --- Customer KPIs ---
        {
            "id": _new_id(),
            "question": ["How many active customers do we have?"],
            "sql": [f"SELECT COUNT(DISTINCT customer_id) FROM {cs}.customers WHERE is_active = TRUE"],
            "usage_guidance": ["Filter customers table with is_active = TRUE for active customer counts"],
        },
        {
            "id": _new_id(),
            "question": ["Who are our high-value customers?"],
            "sql": [f"SELECT * FROM {cs}.customers WHERE relationship_tier = 'Private Client' ORDER BY customer_id"],
            "usage_guidance": ["High-value customers = relationship_tier = 'Private Client'; they hold ~35% of deposit volume"],
        },
        {
            "id": _new_id(),
            "question": ["Who are our top 10 customers by deposit volume in 2024?"],
            "sql": [
                f"SELECT customer_id, MEASURE(`Deposit Volume`) AS deposit_volume ",
                f"FROM {cs}.mv_banking_transactions ",
                f"WHERE transaction_year = 2024 ",
                f"GROUP BY customer_id ORDER BY deposit_volume DESC LIMIT 10",
            ],
            "usage_guidance": ["Rank customers by MEASURE(`Deposit Volume`) from mv_banking_transactions for deposit-based rankings"],
        },
        # --- Portfolio KPIs ---
        {
            "id": _new_id(),
            "question": ["What is the average account balance by relationship tier?"],
            "sql": [f"SELECT `Relationship Tier`, MEASURE(`Average Balance`) FROM {cs}.mv_customer_health GROUP BY `Relationship Tier`"],
            "usage_guidance": ["Use MEASURE(`Average Balance`) from mv_customer_health for balance KPIs segmented by tier"],
        },
        {
            "id": _new_id(),
            "question": ["What is the delinquency rate by relationship tier?"],
            "sql": [f"SELECT `Relationship Tier`, MEASURE(`Delinquency Rate Pct`) FROM {cs}.mv_customer_health GROUP BY `Relationship Tier`"],
            "usage_guidance": ["Use MEASURE(`Delinquency Rate Pct`) from mv_customer_health for delinquency analysis"],
        },
        {
            "id": _new_id(),
            "question": ["What is the mortgage penetration rate by relationship tier?"],
            "sql": [f"SELECT `Relationship Tier`, MEASURE(`Mortgage Penetration Rate Pct`) FROM {cs}.mv_customer_health GROUP BY `Relationship Tier`"],
            "usage_guidance": ["Use MEASURE(`Mortgage Penetration Rate Pct`) from mv_customer_health; Private Client = 60%, Standard = 18%"],
        },
        # --- Service KPIs ---
        {
            "id": _new_id(),
            "question": ["What was the complaint count in January 2024?"],
            "sql": [f"SELECT MEASURE(`Complaint Count`) FROM {cs}.mv_service_quality WHERE request_year = 2024 AND request_month = 1"],
            "usage_guidance": ["Use MEASURE(`Complaint Count`) from mv_service_quality; Jan 2024 had an 80% spike due to system outage"],
        },
        {
            "id": _new_id(),
            "question": ["What is the complaint resolution rate by channel?"],
            "sql": [f"SELECT Channel, MEASURE(`Resolution Rate Pct`) FROM {cs}.mv_service_quality GROUP BY Channel"],
            "usage_guidance": ["Use MEASURE(`Resolution Rate Pct`) from mv_service_quality for complaint resolution analysis"],
        },
        {
            "id": _new_id(),
            "question": ["Show month-over-month complaint change for 2024"],
            "sql": [
                f"SELECT `Request Month`, MEASURE(`Month-over-Month Complaint Change Pct`) ",
                f"FROM {cs}.mv_service_quality ",
                f"WHERE request_year = 2024",
            ],
            "usage_guidance": ["MoM Complaint Change is a window measure on mv_service_quality; surfaces the Jan 2024 spike"],
        },
        {
            "id": _new_id(),
            "question": ["How many service requests are currently unresolved?"],
            "sql": [
                f"SELECT MEASURE(`Open Count`) + MEASURE(`Escalated Count`) AS unresolved_requests ",
                f"FROM {cs}.mv_service_quality",
            ],
            "usage_guidance": ["Unresolved = Open + Escalated; use mv_service_quality with Open Count and Escalated Count measures"],
        },
    ]


def _build_benchmarks(catalog: str, schema: str) -> dict:
    cs = f"{catalog}.{schema}"
    return {
        "questions": [
            {
                "id": _new_id(),
                "question": ["What was total deposit volume in 2024?"],
                "answer": [{"format": "SQL", "content": [
                    f"SELECT MEASURE(`Deposit Volume`) ",
                    f"FROM {cs}.mv_banking_transactions ",
                    f"WHERE transaction_year = 2024",
                ]}],
            },
            {
                "id": _new_id(),
                "question": ["How many active customers do we have?"],
                "answer": [{"format": "SQL", "content": [
                    f"SELECT COUNT(DISTINCT customer_id) ",
                    f"FROM {cs}.customers ",
                    f"WHERE is_active = TRUE",
                ]}],
            },
            {
                "id": _new_id(),
                "question": ["What are the top 5 products by number of accounts?"],
                "answer": [{"format": "SQL", "content": [
                    f"SELECT p.product_name, COUNT(*) AS account_count ",
                    f"FROM {cs}.accounts a JOIN {cs}.products p ON a.product_id = p.product_id ",
                    f"GROUP BY p.product_name ORDER BY account_count DESC LIMIT 5",
                ]}],
            },
            {
                "id": _new_id(),
                "question": ["What is the total fee revenue collected in 2025?"],
                "answer": [{"format": "SQL", "content": [
                    f"SELECT MEASURE(`Fee Revenue`) ",
                    f"FROM {cs}.mv_banking_transactions ",
                    f"WHERE transaction_year = 2025",
                ]}],
            },
            {
                "id": _new_id(),
                "question": ["Show monthly deposit volume from Jan 2023 to Dec 2025"],
                "answer": [{"format": "SQL", "content": [
                    f"SELECT `Transaction Month`, `Transaction Year`, MEASURE(`Deposit Volume`) ",
                    f"FROM {cs}.mv_banking_transactions ",
                    f"GROUP BY `Transaction Month`, `Transaction Year` ",
                    f"ORDER BY `Transaction Year`, `Transaction Month`",
                ]}],
            },
            {
                "id": _new_id(),
                "question": ["Compare Q2 2024 vs Q2 2023 net flow by region"],
                "answer": [{"format": "SQL", "content": [
                    f"SELECT Region, `Transaction Year`, MEASURE(`Net Flow`) ",
                    f"FROM {cs}.mv_banking_transactions ",
                    f"WHERE transaction_quarter = 2 AND transaction_year IN (2023, 2024) ",
                    f"GROUP BY Region, `Transaction Year`",
                ]}],
            },
            {
                "id": _new_id(),
                "question": ["What is digital share as of December 2025?"],
                "answer": [{"format": "SQL", "content": [
                    f"SELECT MEASURE(`Digital Share Pct`) ",
                    f"FROM {cs}.mv_banking_transactions ",
                    f"WHERE transaction_year = 2025 AND transaction_month = 12",
                ]}],
            },
            {
                "id": _new_id(),
                "question": ["What is the average account balance for Private Client customers?"],
                "answer": [{"format": "SQL", "content": [
                    f"SELECT MEASURE(`Average Balance`) ",
                    f"FROM {cs}.mv_customer_health ",
                    f"WHERE `Relationship Tier` = 'Private Client'",
                ]}],
            },
            {
                "id": _new_id(),
                "question": ["What is the delinquency rate by relationship tier?"],
                "answer": [{"format": "SQL", "content": [
                    f"SELECT `Relationship Tier`, MEASURE(`Delinquency Rate Pct`) ",
                    f"FROM {cs}.mv_customer_health ",
                    f"GROUP BY `Relationship Tier`",
                ]}],
            },
            {
                "id": _new_id(),
                "question": ["Which 10 branches had the highest deposit volume in 2024?"],
                "answer": [{"format": "SQL", "content": [
                    f"SELECT Branch, MEASURE(`Deposit Volume`) AS deposit_volume ",
                    f"FROM {cs}.mv_banking_transactions ",
                    f"WHERE transaction_year = 2024 ",
                    f"GROUP BY Branch ORDER BY deposit_volume DESC LIMIT 10",
                ]}],
            },
            {
                "id": _new_id(),
                "question": ["What was YTD deposit volume as of December 2024?"],
                "answer": [{"format": "SQL", "content": [
                    f"SELECT MEASURE(`YTD Deposit Volume`) ",
                    f"FROM {cs}.mv_banking_transactions ",
                    f"WHERE transaction_year = 2024 AND transaction_month = 12",
                ]}],
            },
            {
                "id": _new_id(),
                "question": ["Show month-over-month deposit growth for 2024"],
                "answer": [{"format": "SQL", "content": [
                    f"SELECT `Transaction Month`, MEASURE(`Month-over-Month Deposit Growth Pct`) ",
                    f"FROM {cs}.mv_banking_transactions ",
                    f"WHERE transaction_year = 2024 ",
                    f"GROUP BY `Transaction Month`",
                ]}],
            },
            {
                "id": _new_id(),
                "question": ["What was the complaint count in January 2024?"],
                "answer": [{"format": "SQL", "content": [
                    f"SELECT MEASURE(`Complaint Count`) ",
                    f"FROM {cs}.mv_service_quality ",
                    f"WHERE request_year = 2024 AND request_month = 1",
                ]}],
            },
            {
                "id": _new_id(),
                "question": ["Show the month-over-month complaint change for 2024"],
                "answer": [{"format": "SQL", "content": [
                    f"SELECT `Request Month`, MEASURE(`Month-over-Month Complaint Change Pct`) ",
                    f"FROM {cs}.mv_service_quality ",
                    f"WHERE request_year = 2024 ",
                    f"GROUP BY `Request Month`",
                ]}],
            },
            {
                "id": _new_id(),
                "question": ["What is mortgage penetration rate by relationship tier?"],
                "answer": [{"format": "SQL", "content": [
                    f"SELECT `Relationship Tier`, MEASURE(`Mortgage Penetration Rate Pct`) ",
                    f"FROM {cs}.mv_customer_health ",
                    f"GROUP BY `Relationship Tier`",
                ]}],
            },
        ]
    }


def build_serialized_space(
    catalog: str,
    schema: str,
    instructions: str,
    sample_questions: list,
) -> dict:
    return {
        "version": SPACE_VERSION,
        "data_sources": {
            "tables": _build_table_configs(catalog, schema),
            "metric_views": _build_metric_view_configs(catalog, schema),
        },
        "instructions": {
            "text_instructions": [{"id": _new_id(), "content": [instructions]}],
            "example_question_sqls": _build_example_sqls(catalog, schema),
            "join_specs": [],        # auto-derived from UC PK/FK constraints
            "sql_snippets": {},      # covered by metric view MEASURE() semantics
        },
        "config": {
            "sample_questions": [{"id": _new_id(), "question": [q]} for q in sample_questions],
        },
        "benchmarks": _build_benchmarks(catalog, schema),
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
    for space in w.genie.list_spaces():
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
        result = w.genie.create_space(
            warehouse_id=wh_id,
            serialized_space=serialized,
            title=title,
            description=description,
        )
        return result.space_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Run

# COMMAND ----------

space_id = create_or_update_genie_space(
    catalog=CATALOG,
    schema=SCHEMA,
    warehouse_id=WAREHOUSE_ID,
    title=SPACE_TITLE,
    description=SPACE_DESCRIPTION,
    instructions=SPACE_INSTRUCTIONS,
    sample_questions=SAMPLE_QUESTIONS,
)

w = WorkspaceClient()
space_url = f"{w.config.host.rstrip('/')}/genie/rooms/{space_id}"
print(f"  Space ID : {space_id}")
print(f"  URL      : {space_url}")
displayHTML(f'<a href="{space_url}" target="_blank" style="font-size:16px">Open Horizon Bank Analytics in Genie</a>')

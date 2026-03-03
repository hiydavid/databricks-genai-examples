# Genie Space Configuration — Horizon Bank Analytics

This document contains everything needed to configure the **Horizon Bank Analytics** Genie space after the dataset has been generated.

---

## 1. Space Setup

**Space name:** Horizon Bank Analytics
**Description:** Retail banking analytics across customers, accounts, transactions, branches, and products. 2023–2025 data for Horizon Bank (fictional).

### Tables to include

| Object | Type | Notes |
|---|---|---|
| `transactions` | Table | Primary fact table — add first |
| `service_requests` | Table | Secondary fact table |
| `customers` | Table | Customer dimension |
| `accounts` | Table | Account dimension |
| `products` | Table | Product catalog |
| `branches` | Table | Branch dimension |
| `mv_banking_transactions` | Metric View | Transaction KPIs with deposit/fee/digital measures + window measures |
| `mv_customer_health` | Metric View | Portfolio KPIs: balances, delinquency, mortgage penetration by tier |
| `mv_service_quality` | Metric View | Service KPIs: complaint rate, escalation rate, MoM complaint window |

---

## 2. Space Instructions

Paste the following into the **Space Instructions** field:

```
You are an AI analyst for Horizon Bank, a fictional US retail bank. The dataset covers 2023–2025.

METRIC VIEWS:
The metric views (mv_banking_transactions, mv_customer_health, mv_service_quality) encode all business logic for deposits, fees, net flow, digital share, and complaint trends as named measures. Prefer these views for transaction, customer, and service questions.

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

```

---

## 3. Key Synonyms / Verified Answers

Add the following as **Verified Answers** or **Synonyms** in the Genie knowledge store:

### Business Term → Metric View Measure

| Business Term | Metric View Measure |
|---|---|
| **deposits** / **deposit volume** | `MEASURE(\`Deposit Volume\`)` from `mv_banking_transactions` |
| **withdrawals** / **withdrawal volume** | `MEASURE(\`Withdrawal Volume\`)` from `mv_banking_transactions` |
| **net flow** | `MEASURE(\`Net Flow\`)` from `mv_banking_transactions` |
| **fee revenue** | `MEASURE(\`Fee Revenue\`)` from `mv_banking_transactions` |
| **digital transactions** | `MEASURE(\`Digital Transaction Count\`)` from `mv_banking_transactions` |
| **digital share** | `MEASURE(\`Digital Share Pct\`)` from `mv_banking_transactions` |
| **YTD deposits** | `MEASURE(\`YTD Deposit Volume\`)` from `mv_banking_transactions` |
| **MoM deposit growth** | `MEASURE(\`Month-over-Month Deposit Growth Pct\`)` from `mv_banking_transactions` |
| **average balance** | `MEASURE(\`Average Balance\`)` from `mv_customer_health` |
| **delinquency rate** | `MEASURE(\`Delinquency Rate Pct\`)` from `mv_customer_health` |
| **mortgage penetration** | `MEASURE(\`Mortgage Penetration Rate Pct\`)` from `mv_customer_health` |
| **complaint count** | `MEASURE(\`Complaint Count\`)` from `mv_service_quality` |
| **resolution rate** | `MEASURE(\`Resolution Rate Pct\`)` from `mv_service_quality` |
| **MoM complaint change** | `MEASURE(\`Month-over-Month Complaint Change Pct\`)` from `mv_service_quality` |
| **active customers** | `COUNT(DISTINCT customer_id) WHERE is_active = TRUE` from `customers` table |
| **high-value customers** | customers WHERE relationship_tier = 'Private Client' |
| **top customers** | rank by `Deposit Volume` or `Total Balance` descending |
| **unresolved requests** | `MEASURE(\`Open Count\`) + MEASURE(\`Escalated Count\`)` from `mv_service_quality` |

---

## 4. Demo Question Bank

Use these questions in order during a demo to build from simple to complex.

### Tier 1 — Simple Aggregations & Filtering

| Question | Capability Demonstrated |
|---|---|
| "What was total deposit volume in 2024?" | Basic aggregation |
| "How many active customers do we have?" | Simple filter + count |
| "What are the top 5 products by number of accounts?" | Aggregation + ranking |
| "Show me all transactions over $5,000 in the Northeast in Q4 2024" | Multi-filter |
| "What is the total fee revenue collected in 2025?" | Aggregation on separate column |

### Tier 2 — Time-Series & Period Comparison

| Question | Capability Demonstrated |
|---|---|
| "Show monthly deposit trend from Jan 2023 to Dec 2025" | Time-series |
| "Compare Q2 2024 vs Q2 2023 net flow by region" | YoY / period comparison |
| "How has the mobile channel share changed over time?" | Trend with segmentation |
| "Show me a chart of monthly transaction volume by channel for 2024" | Visualization prompt |
| "What months had the highest fee revenue in 2024?" | Time-based ranking |

### Tier 3 — Multi-Table JOINs & Segmentation

| Question | Capability Demonstrated |
|---|---|
| "What is the average account balance for Private Client customers by state?" | Multi-join + segmentation |
| "Break down transaction volume by account type and year" | Segmentation |
| "Which acquisition channel produces the highest-balance customers?" | Join + ranking |
| "What percentage of Preferred customers have a mortgage?" | Cohort filter |
| "Show me credit utilization rate by relationship tier" | Derived KPI + segmentation |

### Tier 4 — Rankings & Top-N

| Question | Capability Demonstrated |
|---|---|
| "Which 10 branches had the highest deposit volume this year?" | Top-N |
| "Who are our top 20 customers by total deposits in 2024?" | Customer ranking |
| "Which merchant categories drive the most credit card spending?" | Top-N with filter |
| "Rank regions by average transaction value" | Ranking across dimension |

### Tier 5 — Complex KPIs & Multi-Table Analysis

| Question | Capability Demonstrated |
|---|---|
| "What is fee revenue per customer by relationship tier?" | KPI by segment |
| "Do customers with unresolved service complaints have lower average balances?" | Cross-domain join (transactions + service_requests) |
| "Which states have the highest concentration of delinquent accounts?" | Multi-table + geo filter |
| "Compare customer lifetime deposit value by acquisition channel" | LTV-style KPI |
| "What is the complaint resolution time trend in 2024 vs 2023?" | Time comparison on secondary fact |

### Tier 6 — Agent Mode (Multi-Step Exploration)

| Question | Capability Demonstrated |
|---|---|
| "Analyze branch profitability — which branches are underperforming vs. their operating cost?" | Agent: multi-step analysis |
| "Which customer segments are most at risk of churning based on recent activity?" | Agent: churn analysis |
| "Explain the deposit trends in 2024 — what drove the Q2 dip and the Q4 recovery?" | Agent: narrative + data |
| "Build me a complete profile of our Private Client customers" | Agent: 360-degree summary |
| "Are there any unusual patterns in the January 2024 service requests?" | Agent: anomaly detection |

### Tier 7 — Metric View Semantic Layer

| Question | Metric View Capability |
|---|---|
| "What was YTD deposit volume as of December 2024?" | Window measure: `YTD Deposit Volume` |
| "Show month-over-month deposit growth by relationship tier for 2024" | Window + dimension: `Month-over-Month Deposit Growth Pct` × `Relationship Tier` |
| "How has digital share changed by relationship tier over the last 3 years?" | Filtered measure: `Digital Share Pct` × `Relationship Tier` × `Transaction Month` |
| "What is fee revenue per customer for Private Client vs Standard customers?" | Ratio measure: `Fee Revenue per Customer` by `Relationship Tier` |
| "Show mortgage penetration rate by relationship tier" | Filtered ratio: `Mortgage Penetration Rate Pct` (surfaces 60% Private Client vs 18% Standard) |
| "What is the delinquency rate by region and account type?" | Filtered measure: `Delinquency Rate Pct` × `Region` × `Account Type` |
| "Show the month-over-month complaint change for 2024 — when was the worst spike?" | Window measure: `Month-over-Month Complaint Change Pct` (surfaces Jan 2024 +80%) |
| "What is the trailing 3-month resolution rate trend for escalated vs non-escalated channels?" | Window measure: `Trailing 3-Month Resolution Rate Pct` × `Channel` |

---

## 5. Suggested Visualizations

| Metric View | Recommended Chart | Fields |
|---|---|---|
| `mv_banking_transactions` | Line chart | x=`Transaction Month`, y=`Deposit Volume`, color=`Region` |
| `mv_banking_transactions` | Stacked bar | x=`Transaction Month`, y=`Digital Share Pct` by `Channel` |
| `mv_banking_transactions` | Bar chart | x=`Transaction Year`, y=`Net Flow` by `Channel Type` |
| `mv_banking_transactions` | Line chart | x=`Transaction Month`, y=`YTD Deposit Volume` by `Transaction Year` |
| `mv_banking_transactions` | Line chart | x=`Transaction Month`, y=`Month-over-Month Deposit Growth Pct` |
| `mv_customer_health` | Bar chart | x=`Relationship Tier`, y=`Average Balance per Customer` |
| `mv_customer_health` | Bar chart | x=`Relationship Tier`, y=`Mortgage Penetration Rate Pct` |
| `mv_customer_health` | Heatmap | x=`Account Type`, y=`Region`, value=`Delinquency Rate Pct` |
| `mv_service_quality` | Line chart | x=`Request Month`, y=`Complaint Count` + `Month-over-Month Complaint Change Pct` |
| `mv_service_quality` | Bar chart | x=`Category`, y=`Escalation Rate Pct` by `Relationship Tier` |
| `transactions` | Heatmap | x=`transaction_month`, y=`transaction_year`, value=COUNT(*) |

---

## 6. Injected Patterns Reference

These patterns are embedded in the data and should surface naturally in demo queries:

| Pattern | Data Signal | Best Query to Surface |
|---|---|---|
| Nov/Dec volume spike | +45% transaction count in months 11-12 | "Show monthly transaction volume by month for 2024" |
| Q2 2024 deposit dip | -15% avg deposit amount in Apr/May/Jun 2024 | "Compare Q2 2024 vs Q1 2024 deposit volume" |
| Mobile channel growth | 25% share (Jan 2023) → 48% (Dec 2025) | "Show digital share by month over the full period" |
| Fee revenue growth | +20% fee amounts per year | "Compare total fee revenue 2023 vs 2024 vs 2025" |
| Top 10% concentration | Private Client = ~35% of deposit volume | "What % of deposit volume comes from Private Client customers?" |
| Southeast premium | +20% avg txn value at SE branches | "Compare average transaction value by region for branch transactions" |
| Jan 2024 complaint spike | 80% more complaints in Jan 2024 | "Show monthly complaint volume for 2023–2024" |
| Private Client mortgage | 60% mortgage penetration vs 18% Standard | "What % of each tier has a mortgage?" |

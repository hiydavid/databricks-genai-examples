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
| `vw_monthly_transactions` | View | Pre-aggregated trend data |
| `vw_branch_performance` | View | Branch scorecard |
| `vw_customer_summary` | View | Customer 360 |

---

## 2. Space Instructions

Paste the following into the **Space Instructions** field:

```
You are an AI analyst for Horizon Bank, a fictional US retail bank. The dataset covers 2023–2025.

IMPORTANT RULES FOR AMOUNTS:
- Transaction amounts (amount_usd) are ALWAYS stored as positive values.
- The direction of money flow is determined by transaction_type:
  - "Deposit" = money coming into the account (inflow)
  - "Withdrawal" = money going out of the account (outflow)
  - "Transfer" = internal movement (treat as outflow from source account)
  - "Payment" = outflow (bill pay, loan payment)
  - "Purchase" = credit card spending (outflow)
  - "Fee" = bank charge (amount_usd = 0; fee captured in fee_usd)
  - "Interest" = interest credit or charge (direction depends on account type)
- fee_usd is a SEPARATE column from amount_usd. Total cost to customer = amount_usd + fee_usd.

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

For "deposits" always filter WHERE transaction_type = 'Deposit'.
For "fee revenue" use SUM(fee_usd) across all transactions.
For "net flow" compute SUM deposits - SUM withdrawals.
For "digital transactions" use WHERE channel IN ('Online', 'Mobile').
For "YTD" use WHERE transaction_date >= DATE_TRUNC('year', CURRENT_DATE()).
```

---

## 3. Key Synonyms / Verified Answers

Add the following as **Verified Answers** or **Synonyms** in the Genie knowledge store:

### Business Term → SQL Mapping

| Business Term | SQL Mapping |
|---|---|
| **deposits** | `SUM(amount_usd) WHERE transaction_type = 'Deposit'` |
| **deposit volume** | `SUM(amount_usd) WHERE transaction_type = 'Deposit'` |
| **withdrawals** | `SUM(amount_usd) WHERE transaction_type = 'Withdrawal'` |
| **net flow** | `SUM(CASE WHEN transaction_type='Deposit' THEN amount_usd ELSE 0 END) - SUM(CASE WHEN transaction_type='Withdrawal' THEN amount_usd ELSE 0 END)` |
| **fee revenue** | `SUM(fee_usd)` |
| **active customers** | `COUNT(DISTINCT customer_id) WHERE is_active = TRUE` (from customers table) |
| **digital transactions** | `COUNT(*) WHERE channel IN ('Online', 'Mobile')` |
| **digital share** | `COUNT(digital) / COUNT(total) * 100` |
| **average balance** | `AVG(current_balance_usd)` from accounts table |
| **credit utilization** | `current_balance_usd / credit_limit_usd` for credit accounts only |
| **YTD** | `WHERE transaction_date >= DATE_TRUNC('year', CURRENT_DATE())` |
| **last quarter** | `WHERE transaction_quarter = QUARTER(CURRENT_DATE()) - 1 AND transaction_year = YEAR(CURRENT_DATE())` |
| **high-value customers** | customers WHERE relationship_tier = 'Private Client' |
| **top customers** | rank by total deposits or total balance descending |
| **branch profitability** | total_deposits vs monthly_operating_cost_usd from vw_branch_performance |
| **churn risk** | churn_risk column in vw_customer_summary |
| **unresolved requests** | service_requests WHERE status IN ('Open', 'Escalated') |

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

---

## 5. Suggested Visualizations

| View / Table | Recommended Chart | Fields |
|---|---|---|
| `vw_monthly_transactions` | Line chart | x=month_start_date, y=total_deposits, color=region |
| `vw_monthly_transactions` | Stacked bar | x=month_start_date, y=digital_pct by channel |
| `vw_monthly_transactions` | Bar chart | x=txn_quarter, y=net_flow, facet=txn_year |
| `vw_branch_performance` | Scatter plot | x=operating_cost_usd, y=total_deposits, size=unique_customers |
| `vw_customer_summary` | Bar chart | x=relationship_tier, y=avg total_balance |
| `vw_customer_summary` | Pie chart | x=churn_risk, y=count |
| `transactions` | Heatmap | x=transaction_month, y=transaction_year, value=COUNT(*) |
| `service_requests` | Line chart | x=request_date (monthly), y=complaint_count |

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

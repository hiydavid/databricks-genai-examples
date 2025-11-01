# Claude Financial Analyst

A Claude Code agent specialized in querying and analyzing financial data from Databricks. This agent combines professional financial analysis methodologies with direct database access to provide comprehensive insights into company financial health.

## Overview

The Claude Financial Analyst uses a skills-based architecture to perform sophisticated financial analysis on real company data. It can:

- **Query financial data** from Databricks Unity Catalog (stock prices, income statements, balance sheets, cash flows)
- **Perform financial analysis** using CFA-level methodologies (profitability, liquidity, leverage, efficiency, valuation)
- **Calculate key metrics** like ROE, current ratio, debt-to-equity, free cash flow, and more
- **Identify trends** across multiple reporting periods
- **Provide actionable insights** with industry benchmark comparisons

## Features

### Financial Analysis Capabilities

- **Comprehensive Analysis**: Overall company financial health assessment
- **Targeted Analysis**: Focus on specific areas (profitability, liquidity, leverage, efficiency)
- **Valuation Analysis**: DCF models, P/E ratios, EV/EBITDA multiples
- **Quality Analysis**: Earnings quality and cash flow sustainability
- **Trend Analysis**: Multi-period comparisons and growth rates
- **Benchmark Comparisons**: Industry-standard benchmarks for all metrics

### Data Access

- Direct SQL queries to Databricks Unity Catalog via MCP server
- Four core financial data tables:
  - `cfa_stock_price`: Daily OHLC price data with volume
  - `cfa_balance_sheet`: Quarterly/annual balance sheet statements
  - `cfa_income_statement`: Quarterly/annual income statements
  - `cfa_cashflow`: Quarterly/annual cash flow statements

### Skill System

Three specialized Claude Code skills:

1. **financial-analysis**: Professional financial analysis workflows with calculation formulas
2. **data-querying**: Database schema and query patterns for financial data
3. **skill-creator**: Tools for building and packaging new skills

## Architecture

```
claude-financial-analyst/
├── .claude/
│   ├── skills/
│   │   ├── financial-analysis/     # Financial analysis methodologies
│   │   │   ├── SKILL.md
│   │   │   └── references/
│   │   │       └── financial_metrics.md
│   │   ├── data-querying/          # Database schema and queries
│   │   │   ├── SKILL.md
│   │   │   └── references/
│   │   │       └── schema.md
│   │   └── skill-creator/          # Skill development tools
│   │       ├── SKILL.md
│   │       └── scripts/
│   │           ├── init_skill.py
│   │           └── package_skill.py
├── notebooks/
│   └── get_financial_data.ipynb    # Data ingestion notebook
├── .mcp.json                        # MCP server configuration
├── CLAUDE.md                        # Project instructions
└── README.md                        # This file
```

## Prerequisites

- [Claude Code](https://docs.claude.com/en/docs/claude-code) installed
- Databricks workspace with Unity Catalog
- Databricks SQL warehouse for querying
- [Alpha Vantage API key](https://www.alphavantage.co/support/#api-key) (for loading financial statement data)
- Python 3.8+ with `yfinance` library (for loading stock price data)

## Setup

### 1. Clone the Repository

```bash
git clone <repository-url>
cd claude-financial-analyst
```

### 2. Configure MCP Server

The project uses the `dbsql-mcp` MCP server to query Databricks. Configuration is in `.mcp.json`:

```json
{
  "mcpServers": {
    "dbsql-mcp": {
      "command": "npx",
      "args": [
        "mcp-remote",
        "https://your-workspace.cloud.databricks.com/api/2.0/mcp/sql",
        "--header",
        "Authorization: Bearer your-databricks-pat-token"
      ],
    }
  }
}
```

**Required environment variables:**
- `DATABRICKS_HOST`: Your Databricks workspace URL
- `DATABRICKS_TOKEN`: Personal access token with warehouse execution permissions

**Note**: Store sensitive credentials securely. Consider using environment variables or a secrets manager instead of hardcoding in `.mcp.json`.

**Important**: The Databricks managed MCP feature is currently in beta and is subject to breaking changes from Databricks. Monitor the [Databricks documentation](https://docs.databricks.com/aws/en/generative-ai/mcp/managed-mcp) for updates and changes to the MCP SQL API.

### 3. Load Financial Data

Financial data must be loaded into Databricks Unity Catalog before analysis:

1. Open `notebooks/get_financial_data.ipynb` in your Databricks workspace
2. Configure the notebook variables:
   ```python
   catalog = "your_catalog"  # Unity Catalog name
   schema = "your_schema"    # Schema name
   ALPHAVANTAGE_API_KEY = "your_api_key"  # Alpha Vantage API key
   ```
3. Run all cells using Serverless compute
4. Data will be written to:
   - `{catalog}.{schema}.cfa_stock_price`
   - `{catalog}.{schema}.cfa_income_statement`
   - `{catalog}.{schema}.cfa_balance_sheet`
   - `{catalog}.{schema}.cfa_cashflow`

**Default companies included:**
- NVDA, AAPL, MSFT, GOOGL, AMZN, META, AVGO, BRK-B, TSLA, JPM

**Note**:
- Alpha Vantage free tier limits: 25 requests/day, 5 requests/minute
- The notebook includes rate limiting (12 seconds between requests)
- Stock price data: Last 3 years from Yahoo Finance
- Financial statements: Annual reports via Alpha Vantage

## Usage

### Starting the Agent

Launch Claude Code in the project directory:

```bash
claude
```

The agent automatically loads the three skills and has access to your financial data via the MCP server.

### Example Queries

**Comprehensive Analysis:**
```
Analyze Apple's overall financial health for the latest quarter
```

**Specific Metrics:**
```
What is Microsoft's free cash flow trend over the last 4 quarters?
```

**Comparative Analysis:**
```
Compare the profitability margins of NVDA and AMD
```

**Valuation:**
```
Calculate Tesla's P/E ratio and EV/EBITDA using the latest data
```

**Trend Analysis:**
```
Show me Amazon's revenue growth and operating margin trends over the past 3 years
```

### Query Patterns

The agent uses these common SQL patterns (fully qualified table names):

```sql
-- Latest stock price
SELECT ticker, date, close, volume
FROM {catalog}.{schema}.cfa_stock_price
WHERE ticker = 'AAPL'
ORDER BY date DESC
LIMIT 1

-- Financial snapshot
SELECT
  bs.ticker,
  bs.reporting_date,
  bs.totalassets,
  bs.totalshareholderequity,
  inc.totalrevenue,
  inc.netincome,
  cf.operatingcashflow
FROM {catalog}.{schema}.cfa_balance_sheet bs
JOIN {catalog}.{schema}.cfa_income_statement inc
  ON bs.ticker = inc.ticker AND bs.reporting_date = inc.reporting_date
JOIN {catalog}.{schema}.cfa_cashflow cf
  ON bs.ticker = cf.ticker AND bs.reporting_date = cf.reporting_date
WHERE bs.ticker = 'AAPL'
ORDER BY bs.reporting_date DESC

-- Profitability margins
SELECT
  ticker,
  reporting_date,
  grossprofit / totalrevenue AS gross_margin,
  operatingincome / totalrevenue AS operating_margin,
  netincome / totalrevenue AS net_margin
FROM {catalog}.{schema}.cfa_income_statement
WHERE ticker = 'AAPL' AND totalrevenue > 0
ORDER BY reporting_date DESC
```

## Troubleshooting

### MCP Server Connection Issues

If queries fail with connection errors:

1. Verify `.mcp.json` environment variables are correct
2. Check that the SQL warehouse is running in Databricks
3. Confirm your personal access token has warehouse execution permissions
4. Test connectivity: `databricks sql query execute "SELECT 1"`

### Data Not Found

If queries return no results:

1. Verify the notebook has been run and data loaded
2. Check that catalog/schema names match in queries and `.mcp.json`
3. Confirm permissions to read from the Unity Catalog tables
4. Use `SHOW TABLES IN {catalog}.{schema}` to list available tables

## Acknowledgments

- Financial data from [Yahoo Finance](https://finance.yahoo.com/) via `yfinance`
- Financial statements from [Alpha Vantage](https://www.alphavantage.co/)
- Powered by [Claude Code](https://claude.com/code) and [Databricks](https://databricks.com/)

# Claude Code Financial Analyst

> Transform Claude Code into an expert financial analyst using Skills (context engineering) and Databricks DBSQL MCP Server

## Overview

This project demonstrates how to build a sophisticated financial analysis system using Claude Code's Skills feature combined with Databricks' DBSQL MCP Server. The result is an AI agent capable of performing comprehensive, multi-dimensional financial statement analysis with industry-specific expertise.

**Key capabilities:**

- Query financial data from Databricks (stock prices, income statements, balance sheets, cash flow statements)
- Perform 7-domain financial analysis: Profitability, Liquidity, Leverage, Efficiency, Cash Flow, Quality, and Valuation
- Apply industry-specific interpretation and benchmarks
- Generate peer comparisons and trend analysis
- Identify financial red flags and anomalies
- Provide strategic insights and actionable recommendations

## Architecture

### Skills-Based Context Engineering

The system uses **Skills** to provide Claude Code with specialized domain knowledge:

> **Note**: Skills must be located in `.claude/skills/` for Claude Code to discover them. The `.claude` directory is gitignored, so skills are not committed to this repository. See [docs/skills/](docs/skills/) for setup instructions.

1. **financial_analysis** - Comprehensive financial analysis framework
   - 7 analysis domains with detailed reference documentation
   - Industry-specific interpretation guidelines
   - Multi-period trend analysis workflows
   - Red flag detection patterns
   - Setup guide: [docs/skills/financial_analysis.md](docs/skills/financial_analysis.md)

2. **data_querying** - Databricks data access guidelines
   - READ-ONLY SQL query interface
   - Table schemas and data dictionaries
   - Query best practices and constraints
   - Setup guide: [docs/skills/data_querying.md](docs/skills/data_querying.md)

### Databricks Integration

Connect to Databricks via the **DBSQL Managed MCP Server** for seamless data access:

- Unity Catalog tables
- 4 core tables: stock prices, income statements, balance sheets, cash flow statements
- Data coverage: Top 10 S&P 500 companies (NVDA, AAPL, MSFT, GOOGL, AMZN, META, AVGO, BRK-B, TSLA, JPM)
- Historical data: ~10 years stock prices, ~10 years financial statements

## Getting Started

### Prerequisites

- [Claude Code](https://claude.ai/code) installed
- Databricks workspace with Unity Catalog access
- Databricks Personal Access Token (PAT)
- Python 3.8+ with `yfinance` (for data collection)
- Alpha Vantage API key (optional, for financial statements)

### Step 1: Collect Financial Data

Use the provided notebook to download and upload financial data to Databricks:

```bash
# See notebooks/get_financial_data.ipynb
# This notebook downloads:
# - Stock prices from Yahoo Finance (yfinance)
# - Financial statements from Alpha Vantage API
# - Uploads to Databricks Unity Catalog
```

**Data sources:**

- **Stock prices**: [yfinance](https://pypi.org/project/yfinance/) - 10 years of daily OHLCV data
- **Financial statements**: [Alpha Vantage](https://www.alphavantage.co/) - Annual income statements, balance sheets, cash flow statements

**Tables created:**

- `{your_catalog}.{your_schema}.cfa_stock_price` - Historical stock prices
- `{your_catalog}.{your_schema}.cfa_income_statement` - Annual income statements
- `{your_catalog}.{your_schema}.cfa_balance_sheet` - Annual balance sheets
- `{your_catalog}.{your_schema}.cfa_cashflow` - Annual cash flow statements

### Step 2: Configure DBSQL MCP Server

Add the DBSQL Managed MCP server to your project:

1. Create `.mcp.json` in project root:

```json
{
  "mcpServers": {
    "dbsql-mcp": {
      "command": "npx",
      "args": [
        "mcp-remote",
        "https://YOUR_WORKSPACE.cloud.databricks.com/api/2.0/mcp/sql",
        "--header",
        "Authorization: Bearer YOUR_PAT_TOKEN"
      ]
    }
  }
}
```

1. Configure Claude Code settings in `.claude/settings.local.json`:

```json
{
  "enableAllProjectMcpServers": true,
  "enabledMcpjsonServers": ["dbsql-mcp"]
}
```

**Documentation:**

- [Databricks MCP Server Setup](https://docs.databricks.com/aws/en/generative-ai/mcp/connect-external-services#connect-claude-desktop-using-pat)

### Step 3: Set Up Skills

**CRITICAL**: Skills MUST be in `.claude/skills/` for Claude Code to discover them.

Create the skills using the setup guides:

1. **Financial Analysis Skill**: Follow [docs/skills/financial_analysis.md](docs/skills/financial_analysis.md)
   - Creates `.claude/skills/financial_analysis/SKILL.md`
   - Optionally creates reference docs in `references/` subdirectory

2. **Data Querying Skill**: Follow [docs/skills/data_querying.md](docs/skills/data_querying.md)
   - Creates `.claude/skills/data_querying/SKILL.md`

The setup guides provide instructions for using Claude Code to generate skills from the [financial_analysis_guide.md](docs/financial_analysis_guide.md).

### Step 4: Use the Skills

Once created in `.claude/skills/`, the Skills are automatically available in Claude Code. Example usage:

```text
"Analyze Apple's profitability trends over the last 3 years and compare to Microsoft and Google"

"Perform a comprehensive financial health assessment for Tesla including all 7 analysis domains"

"What are the liquidity concerns for JPMorgan? Show key metrics and trends"

"Compare the valuation multiples for the mega-cap tech companies (AAPL, MSFT, GOOGL, META, NVDA)"
```

Claude Code will:

1. Invoke the `data_querying` skill to fetch relevant data from Databricks
2. Invoke the `financial_analysis` skill to calculate metrics and interpret results
3. Apply industry-specific context and benchmarks
4. Generate comprehensive analysis with insights

## Project Structure

```text
claude-financial-analyst/
├── .claude/                                # GITIGNORED - Create locally using setup guides
│   ├── skills/
│   │   ├── financial_analysis/
│   │   │   ├── SKILL.md                    # Main financial analysis skill
│   │   │   └── references/                 # Detailed domain references (optional)
│   │   │       ├── profitability_analysis.md
│   │   │       ├── liquidity_analysis.md
│   │   │       ├── leverage_analysis.md
│   │   │       ├── efficiency_analysis.md
│   │   │       ├── cashflow_analysis.md
│   │   │       ├── quality_analysis.md
│   │   │       └── valuation_analysis.md
│   │   └── data_querying/
│   │       └── SKILL.md                    # Data access guidelines
│   └── settings.local.json                 # Claude Code MCP configuration
├── docs/
│   ├── skills/                             # Skills setup documentation
│   │   ├── README.md                       # Skills overview and setup
│   │   ├── financial_analysis.md           # Financial Analysis skill setup guide
│   │   └── data_querying.md                # Data Querying skill setup guide
│   ├── financial_analysis_guide.md         # Comprehensive financial analysis literature
│   └── report_generation_recommendations.md # Report generation evaluation
├── notebooks/
│   └── get_financial_data.ipynb            # Data collection pipeline
├── .gitignore                              # Ignores .claude/ and .mcp.json
├── .mcp.json                               # MCP server configuration (gitignored)
├── CLAUDE.md                               # Context for Claude Code (gitignored)
└── README.md                               # This file
```

## Example Analyses

### Comprehensive Company Analysis

```text
"Perform a full financial analysis of Apple (AAPL) covering all 7 domains"
```

Claude Code will analyze:

- Profitability: Margins, returns (ROE, ROIC, ROA)
- Liquidity: Current ratio, quick ratio, working capital
- Leverage: Debt ratios, coverage ratios
- Efficiency: Asset turnover, inventory/receivables cycles
- Cash Flow: Free cash flow, cash conversion
- Quality: Earnings quality, accruals
- Valuation: P/E, P/B, EV/EBITDA vs. peers

### Peer Comparison

```text
"Compare profitability metrics for AAPL, MSFT, and GOOGL over the last 3 years"
```

Claude Code will:

- Calculate gross, operating, net, EBITDA margins
- Compute ROE, ROIC, ROA for each company
- Identify trends and relative positioning
- Apply tech industry context

### Red Flag Detection

```text
"Are there any liquidity or quality concerns with Tesla?"
```

Claude Code will:

- Analyze liquidity ratios and trends
- Assess earnings quality indicators
- Detect financial reporting red flags
- Provide risk assessment

## Current Limitations

- **Data scope**: Limited to 10 S&P 500 companies (can be expanded)
- **Historical data**: ~3 years stock prices, ~5 years financial statements
- **Database access**: READ-ONLY via MCP (cannot write results back)
- **Data freshness**: Requires manual refresh via notebook
- **Alpha Vantage API**: Rate limits on free tier (25 requests/day)

## Resources

- [Claude Code Documentation](https://docs.claude.com/en/docs/claude-code)
- [Claude Code Skills Guide](https://docs.claude.com/en/docs/claude-code/skills)
- [Databricks MCP Server](https://docs.databricks.com/aws/en/generative-ai/mcp/)
- [Model Context Protocol (MCP)](https://modelcontextprotocol.io/)
- [Financial Analysis Literature](docs/financial_analysis_guide.md)

## License

This project is for educational and demonstration purposes.

## Acknowledgments

- Financial analysis methodologies based on industry-standard practices
- Data sources: Yahoo Finance (yfinance), Alpha Vantage
- Built with Claude Code and Databricks

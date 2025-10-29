# Data Querying Skill - Setup Guide

> **NOTE**: This is NOT the actual skill file. This is documentation to help you create the skill.
>
> **The actual skill must be located at**: `.claude/skills/data_querying/SKILL.md`

## What This Document Is

This guide helps you create the Data Querying skill for Claude Code. The skill itself must live in the `.claude` directory (which is gitignored) for Claude Code to discover and use it.

## What the Skill Does

Once created, the Data Querying skill provides Claude Code with read-only SQL querying capabilities to Databricks financial datasets via the DBSQL MCP server.

## Creating the Skill

### Step 1: Create the Directory Structure

```bash
mkdir -p .claude/skills/data_querying
```

### Step 2: Create SKILL.md

Create `.claude/skills/data_querying/SKILL.md` with the following structure:

#### Required Frontmatter

```yaml
---
name: data-querying
description: Querying data from Databricks using the DBSQL Managed MCP server in order to perform financial analysis.
---
```

#### Content Sections to Include

1. **Overview** - Brief description of the skill's purpose
2. **Critical Constraints** - READ-ONLY access, limited scope, no schema exploration
3. **Available Datasets** - Document the 4 financial tables
4. **Query Guidelines** - What TO do and what NOT to do
5. **Best Practices** - Query optimization and data handling tips
6. **Error Handling** - Troubleshooting common issues

### Step 3: Document Available Tables

The skill should document access to these four tables:

#### 1. Stock Price History

- **Table**: `your_catalog.your_schema.cfa_stock_price`
- **Content**: Historical daily stock prices (3 years)
- **Columns**: date, ticker, open, high, low, close, volume
- **Use**: Price trends, volatility analysis, technical indicators

#### 2. Income Statements

- **Table**: `your_catalog.your_schema.cfa_income_statement`
- **Content**: Annual income statement data
- **Use**: Revenue analysis, profitability metrics, growth trends

#### 3. Balance Sheets

- **Table**: `your_catalog.your_schema.cfa_balance_sheet`
- **Content**: Annual balance sheet data
- **Use**: Financial position, liquidity ratios, asset analysis

#### 4. Cash Flow Statements

- **Table**: `your_catalog.your_schema.cfa_cashflow`
- **Content**: Annual cash flow data
- **Use**: Cash generation, investment analysis, financing activities

**Data Coverage**: Top 10 S&P 500 companies (NVDA, AAPL, MSFT, GOOGL, AMZN, META, AVGO, BRK-B, TSLA, JPM)

### Step 4: Define Critical Constraints

The skill MUST emphasize these constraints:

#### READ-ONLY Access

- **ONLY** `SELECT` queries permitted
- **NO** data modification: INSERT, UPDATE, DELETE, MERGE
- **NO** schema changes: CREATE, ALTER, DROP
- **NO** permission changes: GRANT, REVOKE

#### Limited Scope

- Access to ONLY the 4 documented tables
- **NO** catalog/schema exploration (SHOW CATALOGS, SHOW SCHEMAS, SHOW TABLES)
- Work exclusively with documented tables

#### Schema Discovery

- Use `DESCRIBE TABLE <table_name>` to view schemas
- Use `DESCRIBE EXTENDED <table_name>` for additional metadata
- Always verify column names before complex queries

### Step 5: Query Guidelines

Include best practices:

**What TO Do:**

- Write SELECT statements with specific columns
- Use WHERE clauses for date ranges and filters
- Join tables for multi-dimensional analysis
- Use aggregate functions (SUM, AVG, COUNT, MAX, MIN)
- Apply GROUP BY for dimensional analysis
- Create calculated fields in SELECT clause

**What NOT To Do:**

- Avoid SELECT * when possible
- Never attempt data modification
- Don't use temporary tables with modifications
- Don't explore schemas beyond documented tables

**Best Practices:**

- Be specific with date filters for performance
- Select only needed columns
- Handle NULL values appropriately (COALESCE, IFNULL)
- Validate ticker symbols exist
- Check data freshness before complex queries

## Integration with Financial Analysis

This skill works together with the [Financial Analysis skill](./financial_analysis.md):

1. **Data Querying** retrieves financial data from Databricks
2. **Financial Analysis** applies analytical frameworks to the data
3. Combined, they enable end-to-end financial analysis workflows

## Example Skill Content Template

```markdown
---
name: data-querying
description: Querying data from Databricks using the DBSQL Managed MCP server in order to perform financial analysis.
---

# Data Querying Skill

[Overview section]

## Critical Constraints

**READ-ONLY ACCESS**: You can ONLY execute SELECT queries...

[Detailed constraints]

## Available Datasets

### 1. Stock Price History (users.david_huang.cfa_stock_price)

[Table documentation]

### 2-4. [Other tables]

## Query Guidelines

### What TO Do
[Allowed operations]

### What NOT To Do
[Prohibited operations]

## Best Practices

[Performance and data handling guidance]

## Error Handling

[Troubleshooting tips]
```

## MCP Server Requirement

This skill requires the DBSQL MCP server to be configured. See main [README.md](../../README.md) for MCP setup instructions.

Without the MCP server configured, the skill can still provide query guidance, but cannot execute queries.

## Why Skills Must Be in `.claude`

**Critical**: Claude Code only discovers skills in the `.claude/skills/` directory. Skills placed elsewhere will not work.

- The `.claude` directory is scanned automatically by Claude Code
- The `SKILL.md` naming convention is required
- The frontmatter format enables proper skill registration
- Skills outside `.claude/skills/` are invisible to Claude Code

## Invoking the Skill

Once created, invoke with:

- `/skill data_querying` - Explicit invocation
- Automatic - Claude Code may invoke automatically when data queries are needed

## Customization Options

When creating your skill, consider:

- **Table schemas** - Document your specific table structures
- **Data coverage** - Update company lists, date ranges
- **Query patterns** - Add common query templates
- **Performance tips** - Include database-specific optimizations
- **Error messages** - Add troubleshooting for common issues

## Additional Resources

- **Skills overview**: [README.md](./README.md) - General skills documentation
- **Financial Analysis**: [financial_analysis.md](./financial_analysis.md) - Complementary skill
- **Usage examples**: [skills_example.md](../skills_example.md) - How to use both skills together

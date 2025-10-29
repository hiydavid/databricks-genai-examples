# Claude Code Skills Documentation

This directory contains documentation for the Claude Code Skills used in this project. Skills enable Claude Code to perform specialized tasks by providing domain-specific context and frameworks.

## What are Claude Code Skills?

Skills are a context engineering pattern that transforms Claude Code into a specialized agent with deep expertise in specific domains. When invoked, a skill loads comprehensive instructions, frameworks, and best practices that guide Claude Code's analysis and responses.

## Critical Requirement: `.claude` Directory Structure

**IMPORTANT**: For Claude Code to discover and use Skills, they MUST be located in the `.claude/skills/` directory structure within your project:

```text
.claude/
└── skills/
    ├── financial_analysis/
    │   ├── SKILL.md           # Main skill definition (required)
    │   └── references/         # Optional supporting documentation
    │       ├── profitability_analysis.md
    │       ├── liquidity_analysis.md
    │       └── ...
    └── data_querying/
        └── SKILL.md           # Main skill definition (required)
```

### Why `.claude` is Required

- Claude Code automatically scans `.claude/skills/` for available skills
- The directory structure and `SKILL.md` naming are conventions that Claude Code recognizes
- Skills placed outside `.claude/skills/` will NOT be discovered
- The frontmatter in `SKILL.md` (name, description) defines how the skill appears in Claude Code's UI

### Skill File Structure

Each skill requires a `SKILL.md` file with YAML frontmatter:

```markdown
---
name: skill-name
description: Brief description of what the skill does
---

# Skill Name

[Detailed skill instructions and context...]
```

## Skills in This Project

This project uses two primary skills:

### 1. Financial Analysis Skill

**Location**: `.claude/skills/financial_analysis/SKILL.md`

Transforms Claude Code into a comprehensive financial analyst with expertise across seven analysis domains:

1. **Profitability Analysis** - Margins, returns, asset efficiency
2. **Liquidity Analysis** - Short-term obligations, cash management
3. **Leverage Analysis** - Debt levels, capital structure, financial risk
4. **Efficiency Analysis** - Asset deployment, working capital management
5. **Cash Flow Analysis** - Cash generation quality, sustainability
6. **Quality Analysis** - Earnings quality, financial reporting integrity
7. **Valuation Analysis** - Market multiples, fundamental valuation

**Capabilities:**

- Multi-dimensional company analysis
- Industry-specific interpretation and benchmarks
- Peer comparison frameworks
- Trend identification and red flag detection
- Strategic insights and actionable recommendations

**Supporting Documentation**: Seven detailed reference documents in `.claude/skills/financial_analysis/references/` provide in-depth metric definitions, formulas, interpretation guidelines, and best practices for each domain.

**Full Documentation**: See [financial_analysis.md](./financial_analysis.md)

### 2. Data Querying Skill

**Location**: `.claude/skills/data_querying/SKILL.md`

Provides read-only SQL querying interface to Databricks financial datasets via the DBSQL MCP server.

**Data Access:**

- `your_catalog.your_schema.cfa_stock_price` - Historical stock prices (3 years, daily)
- `your_catalog.your_schema.cfa_income_statement` - Annual income statements
- `your_catalog.your_schema.cfa_balance_sheet` - Annual balance sheets
- `your_catalog.your_schema.cfa_cashflow` - Annual cash flow statements

**Coverage**: Top 10 S&P 500 companies (NVDA, AAPL, MSFT, GOOGL, AMZN, META, AVGO, BRK-B, TSLA, JPM)

**Constraints:**

- READ-ONLY: Only SELECT queries permitted
- Limited scope: Only the 4 documented tables
- No schema exploration (SHOW CATALOGS/SCHEMAS/TABLES)

**Full Documentation**: See [data_querying.md](./data_querying.md)

## Setting Up Skills Locally

To use these skills in your own Claude Code environment:

### Step 1: Create the `.claude` Directory Structure

```bash
mkdir -p .claude/skills/financial_analysis/references
mkdir -p .claude/skills/data_querying
```

### Step 2: Copy Skill Definitions

You'll need to create the `SKILL.md` files based on the documentation in this directory:

1. Review [financial_analysis.md](./financial_analysis.md) and create `.claude/skills/financial_analysis/SKILL.md`
2. Review [data_querying.md](./data_querying.md) and create `.claude/skills/data_querying/SKILL.md`
3. Optionally, copy reference documentation to `.claude/skills/financial_analysis/references/`

### Step 3: Verify Skills are Discovered

Open Claude Code in your project. Skills should appear in:

- The `/skill` command autocomplete
- The Skill tool when Claude Code suggests using specialized skills

### Step 4: Configure MCP Server (for data_querying skill)

The data_querying skill requires the DBSQL MCP server. See the main [README.md](../../README.md) for setup instructions.

## Invoking Skills

When working with Claude Code, skills are automatically invoked when relevant to your task. You can also explicitly invoke them:

- Type `/skill financial_analysis` to load the financial analysis skill
- Type `/skill data_querying` to load the data querying skill
- Skills can be combined - both can be active simultaneously

## Customizing Skills

Skills are highly customizable. You can:

1. **Modify existing skills**: Edit `SKILL.md` files to adjust frameworks, add company-specific context, or change analysis approaches
2. **Create new skills**: Add new skill directories under `.claude/skills/`
3. **Add reference documentation**: Place supporting materials in subdirectories
4. **Combine skills**: Skills work together - financial_analysis + data_querying enables end-to-end analysis

## Benefits of Skills-Based Architecture

1. **Separation of concerns**: Domain knowledge separate from conversational AI
2. **Reusability**: Skills can be shared across projects and teams
3. **Maintainability**: Update domain expertise without changing application code
4. **Scalability**: Add new skills without modifying core system
5. **Transparency**: Domain logic is visible and auditable in markdown files
6. **Version control**: Skills can be versioned independently (when not gitignored)

# Multi-Agent System with Agent Bricks

# Introduction
This is a tutorial on how to create your first Multi-Agent Supervisor with Databricks Agent Bricks. Most of this is done via the UI, since some of these features do not yet have API support. I will update this tutorial with API code once that is available.

By the end of this, you will have created a multi-agent system on Databricks with Agent Bricks, including the following assets:
- A **Genie Space** with two datasets `balance-sheet` and `income-statement`
- A **Vector Search Index** of a chunked SEC 10K dataset `sec-10k-chunked`
- A **Knowledge Assistant** that's connected to the Vector Search Index
- A **Multi-Agent Supervisor** that's a coordinator between the **Genie Space** and the **Knowledge Assistant**

# Prerequisites
- Databricks workspace with Unity Catalog enabled
- Cluster with ML runtime (for creating Vector Search Index)
- Permissions to create schemas, tables, and vector search endpoints
- Agent Bricks (Beta) is enabled in the worksapce

# Get Started

## Step 1: Ingest Data as Delta Tables
First, make sure the parquet files are in a Unity Catalog Volumes directory. Then, run `ingest-data` notebook to create the Delta tables.

1. Update the catalog and schema names in the notebook configuration
2. Execute all cells to create three Delta tables:
   - `{catalog}.{schema}.balance_sheet`
   - `{catalog}.{schema}.income_statement`
   - `{catalog}.{schema}.sec_10k_chunked`

## Step 2: Create Vector Search Index (UI)
Create a vector search index for the SEC 10K chunked data:

1. Make sure a Vector Search endpoint has been create. You will use this endpoint to create a Vector Search index. 
   - To create a VS endpoint, navigate to **Compute**, and then **Vector Search**, then **Create Endpoint**
2. Then, go to the `{catalog}.{schema}.sec_10k_chunked` Delta table in Unity Catalog
3. To the upper-right corner, click **Create**, and select **Vector search index**
   - Follow this [documentation](https://docs.databricks.com/aws/en/generative-ai/create-query-vector-search) for more details on how to create an index

## Step 3: Create Genie Space (UI Only)
Create a Genie Space for the financial data:

1. Navigate to **Genie** in the Databricks workspace
2. Click **New** to the upper right corner
3. Connect your data by selecting the tables `{catalog}.{schema}.balance_sheet` and `{catalog}.{schema}.income_statement`
4. Configure the space:
   - **Name**: `SEC Financial Data Genie`
   - **Description**: `Analysis of balance sheet and income statement data for APPL, BAC, and AXP, from years 2003 through 2022.`
5. Optimize your Genie space in order to get the best out of Genie:
   - Add data and column descriptions (you have the option to use AI Suggestion deccription in Unity Catalog)
   - Add to General Instructions -- example:
      * **General Instructions**:
         ```
         # Guidelines
         1. **Fully qualify** each column with its table alias (`bs`, `is`, `cf`, etc.).
         2. **Filter** by `company_id` (ticker) and explicit period(s) given by the user.
         3. If the user omits **time period, filing type, or company**, respond with exactly **one** follow-up question that asks for the missing detail, then stop.
         4. Use `ROUND(metric_value, 4)` for ratio outputs unless the user requests raw numbers.
         5. Default sort order: descending by fiscal_year, fiscal_quarter.
         6. Ensure divisions use `NULLIF(denominator,0)` to avoid divide-by-zero errors.
         7. For multi-period comparisons (e.g., YoY), use CTEs or self-joins that clearly label current vs. prior periods (`cur`, `prev`).

         # SUPPORTED METRICS & RATIOS
         - Liquidity
            * Current Ratio: bs.total_current_assets / bs.total_current_liabilities
            * Quick Ratio: (bs.cash_and_equivalents + bs.short_term_investments + bs.accounts_receivable) / bs.total_current_liabilities
         - Solvency
            * Debt-to-Equity: (bs.short_term_debt + bs.long_term_debt) / bs.shareholders_equity
            * Interest Coverage: is.ebit / NULLIF(is.interest_expense,0)
         - Profitability
            * Gross Margin: (is.gross_profit / is.total_revenue)
            * Net Profit Margin: is.net_income / is.total_revenue
            * Return on Assets (ROA): is.net_income / bs.total_assets
            * Return on Equity (ROE): is.net_income / bs.shareholders_equity
         - Efficiency
            * Asset Turnover: is.total_revenue / bs.total_assets
         - Growth
            * Revenue Growth % YoY: (cur.total_revenue - prev.total_revenue) / prev.total_revenue
         - Cash Flow
            * Free Cash Flow: cf.operating_cash_flow - cf.capital_expenditure

         Remember: you are a precise, no-nonsense financial analyst SQL generator. Ask clarifying questions only when essential information is missing; otherwise, return the optimized SQL query immediately.
         ```
   - Add examples to SQL Queries -- example:
      * **Q: What is the debt to equity ratio for APPL for 2022**
         ```
         SELECT
            ROUND(
                  try_divide((`short_term_debt` + `long_term_debt`), NULLIF(`total_equity`, 0)), 4
            ) AS `debt_to_equity_ratio`
         FROM
            `catalog`.`schema`.`genie_balance_sheet`
         WHERE
            `TICKER` = :company
            AND `YEAR` = :year
         ;
         ```
   - Follow this [documentation](https://docs.databricks.com/aws/en/genie/best-practices) for more details on how to cruate an effective Genie space


## Step 4: Create Knowledge Assistant Agent Brick (UI Only)
```
Note: This is a Beta feature.
```
Create a Knowledge Assistant connected to the vector search index:

1. Navigate to **Agent** in the Databricks workspace
2. Select the **Knowledge Assistant** template and Click **Build**
3. Configure the agent:
   - **Name**: `SEC 10K Knowledge Assistant`
   - **Description**: `Answers questions about SEC 10K document queries`
4 Configure Knowledge Sources: 
   - For the first knowledge source:
      - **Type**: select **Vector Search Index**
      - **Source**: select `sec_10k_chunked_vsindex`, or the name you've chosen for the index durin index creation
      - **Doc URI Column**: `chunk_id`, or which ever column you'd like to be included in citations
      - **Text Column**: `doc_content`
      - **Describe the Content**: ```The Vector Search Index contains text chunks from 10K filings, for Apple and American Express, from years 2020 through 2022```
   - Add more knowledge sources if you have more, but for this tutorial you only have one vector search index
   - Under **Optional**, you can add further **Insturctions** for the Knowledge Assistant agent 
4. Click **Create Agent**
5. Test the agent, and try to improve quality by adding example questions and guidelines in the **Improve Quality** tab

## Step 5: Create Multi-Agent Supervisor (UI Only)
```
Note: This is a Beta feature.
```
Create a supervisor agent to coordinate between the Genie Space and Knowledge Assistant:

1. Navigate to **Agent** in the Databricks workspace
2. Select the **Multi-Agent Supervisor** template and Click **Build**
3. Configure the supervisor:
   - **Name**: `Financial Analysis Multi-Agent Supervisor`
   - **Description**: `Coordinates between financial data analysis and SEC document queries`
4. Configure Agents:
   - For the first agent:
      - **Type**: `Genie Space`
      - **Genie Space**: `SEC Financial Data Genie`
      - **Agent Name**: `genie-agent`
      - **Describe the Content**: `Analysis of balance sheet and income statement data for APPL, BAC, and AXP, from years 2003 through 2022.`
   - Add a second Agent:
      - **Type**: `Agent Endpoint`
      - **Agent Endpoint**: select the endpoint name of the Knowledge Assistant created in the previous step
      - **Agent Name**: `ka-agent`
      - **Describe the Content**: `Answers questions about SEC 10K document queries`
   - Under **Optional**, you can add further **Insturctions** for the Multi-Agent Supervisor 
5. Click **Create Agent**

## Step 6: Test the Multi-Agent System
Test your complete multi-agent system:

1. **Genie Queries**: 
   - "What was the total revenue last quarter?"
   - "Show me the balance sheet trends"

2. **SEC Document Queries**:
   - "What are the main risk factors mentioned in the 10K?"
   - "Summarize the business overview section"

3. **Combined Queries**:
   - "How do the financial results compare to the risks mentioned in the 10K?"
   - "Analyze the company's financial health based on both datasets"

## Architecture Overview

```
Multi-Agent Supervisor
├── Genie Space (Financial Data Analysis)
│   ├── balance_sheet table
│   └── income_statement table
└── Knowledge Assistant (SEC 10K Analysis)
    └── sec_10k_vector_index
```

The supervisor agent intelligently routes queries to the appropriate sub-agent based on the type of information requested, and can coordinate responses from multiple agents for complex analytical tasks. 
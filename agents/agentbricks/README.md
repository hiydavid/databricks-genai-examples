# Multi-Agent System with Agent Bricks

# Introduction
Create a multi-agent system on Databricks with Agent Bricks. You will create
* A `Genie Space` with two datasets `balance-sheet` and `income-statement`
* A `Vector Search Index` of a chunked SEC 10K dataset `sec-10k-chunked`
* A `Knowledge Assistant` that's connected to the Vector Search Index
* A `Multi-Agent Supervisor` that's a coordinator between the `Genie Space` and the `Knowledge Assistant`

# Prerequisites
- Databricks workspace with Unity Catalog enabled
- Cluster with ML runtime (for vector search capabilities)
- Permissions to create schemas, tables, and vector search endpoints

# Get Started

## Step 1: Ingest Data to Delta Tables
Run the `00_ingest_data.ipynb` notebook to create Delta tables from the local parquet files in the `data/` directory.

1. Update the catalog and schema names in the notebook configuration
2. Execute all cells to create three Delta tables:
   - `{catalog}.{schema}.balance_sheet`
   - `{catalog}.{schema}.income_statement`
   - `{catalog}.{schema}.sec_10k_chunked`

## Step 2: Create Vector Search Index
Create a vector search index for the SEC 10K chunked data:

1. Navigate to **Catalog** > **Vector Search** in the Databricks workspace
2. Click **Create vector search endpoint**
3. Configure the endpoint:
   - **Name**: `agentbricks-vector-search-endpoint`
   - **Compute**: Select appropriate compute size
   - Click **Create**
4. Once endpoint is active, click **Create vector search index**
5. Configure the index:
   - **Name**: `sec_10k_vector_index`
   - **Endpoint**: Select the endpoint created above
   - **Source table**: `{catalog}.{schema}.sec_10k_chunked`
   - **Primary key**: Select the appropriate ID column
   - **Text column**: Select the column containing the text chunks
   - **Embedding model**: Choose a supported embedding model (e.g., `databricks-bge-large-en`)
   - **Sync mode**: **Triggered** (recommended for this demo)
6. Click **Create** and wait for the index to be built

## Step 3: Create Genie Space (UI Only)
Create a Genie Space for the financial data:

1. Navigate to **Genie** in the Databricks workspace
2. Click **Create Genie Space**
3. Configure the space:
   - **Name**: `Financial Data Analysis`
   - **Description**: `Analysis of balance sheet and income statement data`
4. Add datasets:
   - Click **Add Data**
   - Select `{catalog}.{schema}.balance_sheet`
   - Click **Add Data** again
   - Select `{catalog}.{schema}.income_statement`
5. Configure data descriptions and column meanings if needed
6. Click **Create Space**

## Step 4: Create Knowledge Assistant Agent Brick (UI Only)
Create a Knowledge Assistant connected to the vector search index:

1. Navigate to **Agent Bricks** in the Databricks workspace
2. Click **Create Agent Brick**
3. Select **Knowledge Assistant** template
4. Configure the agent:
   - **Name**: `SEC 10K Knowledge Assistant`
   - **Description**: `Assistant for SEC 10K document queries`
   - **Vector Search Index**: Select `sec_10k_vector_index`
   - **System Prompt**: Customize the prompt for financial document analysis
5. Test the agent with sample queries
6. Click **Save** when satisfied

## Step 5: Create Multi-Agent Supervisor (UI Only)
Create a supervisor agent to coordinate between the Genie Space and Knowledge Assistant:

1. Navigate to **Agent Bricks** in the Databricks workspace
2. Click **Create Agent Brick**
3. Select **Multi-Agent** or **Supervisor** template
4. Configure the supervisor:
   - **Name**: `Financial Analysis Supervisor`
   - **Description**: `Coordinates between financial data analysis and SEC document queries`
5. Add sub-agents:
   - Add the Genie Space as a tool/agent
   - Add the SEC 10K Knowledge Assistant as a tool/agent
6. Configure routing logic:
   - Route balance sheet/income statement queries to Genie Space
   - Route SEC document queries to Knowledge Assistant
   - Handle complex queries that need both agents
7. Set system prompt for coordination logic
8. Test with various query types
9. Click **Save** when satisfied

## Step 6: Test the Multi-Agent System
Test your complete multi-agent system:

1. **Financial Data Queries**: 
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
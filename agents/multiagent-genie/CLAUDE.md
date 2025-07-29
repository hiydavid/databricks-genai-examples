# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Environment Setup
- Install dependencies: `pip install -r requirements.txt`
- The project uses Python with Databricks-specific libraries (MLflow, LangChain, LangGraph)
- No traditional build, test, or lint commands - this is a Databricks notebook-based project

### Running the Agent
- Main entry point: `driver.py` (Databricks notebook format)
- Core agent implementation: `multiagent-genie.py`
- Configuration: `configs.yaml` (requires manual setup of Databricks resources)

## Architecture Overview

This is a **multi-agent system** built with LangGraph for financial data analysis using Databricks Genie:

### Core Components

1. **Supervisor Agent** (`supervisor_agent` function in multiagent-genie.py:100)
   - Routes queries between agents based on complexity
   - Uses structured output to determine routing strategy
   - Implements iteration limits (max 3 iterations)

2. **Genie Agent** (multiagent-genie.py:52)
   - Databricks GenieAgent for SQL-based financial data queries
   - Accesses SEC financial data (2003-2017) for AAPL, BAC, AXP
   - Primary data source for Income Statement and Balance Sheet metrics

3. **Research Planner Agent** (`research_planner_node` function in multiagent-genie.py:146)
   - Executes parallel queries for complex multi-step analysis
   - Uses ThreadPoolExecutor for concurrent Genie queries
   - Synthesizes results from multiple data sources

### Data Scope
- **Time Range**: SEC financial data from 2003-2017 only
- **Companies**: Apple Inc. (AAPL), Bank of America Corp (BAC), American Express (AXP)
- **Data Types**: Income Statement and Balance Sheet metrics
- **Supported Metrics**: See `data/genie_instruction.txt` for full list of financial ratios and calculations

### Workflow Pattern
```
User Query → Supervisor → [Research Planning OR Direct Genie] → Supervisor → Final Answer
```

### Key Technical Details

- **State Management**: Uses LangGraph's `AgentState` with typed state including research plans and results
- **MLflow Integration**: All agent calls are traced with `@mlflow.trace` decorators
- **Chat Interface**: Wrapped in `LangGraphChatAgent` class implementing MLflow's `ChatAgent` interface
- **Streaming Support**: Both `predict` and `predict_stream` methods available with status updates to prevent timeouts
- **Configuration**: Extensive YAML-based configuration for Databricks resources
- **Parallel Execution**: Research Planner uses ThreadPoolExecutor (max 3 workers) for concurrent Genie queries
- **Error Handling**: Comprehensive error handling in parallel query execution with graceful degradation
- **Authentication**: Uses Databricks PAT stored in secrets for Genie space access

### Configuration Requirements

Before running, update `configs.yaml` with:
- Databricks workspace details (catalog, schema, workspace_url)
- Genie space ID and SQL warehouse ID
- MLflow experiment name
- Authentication tokens (stored in Databricks secrets)
- LLM endpoint configuration

### Data Files
- `data/balance_sheet.parquet` and `data/income_statement.parquet`: Financial datasets
- `data/genie_instruction.txt`: SQL query guidelines and supported financial metrics
- `data/ingest-genie-data.py`: Data ingestion script

### Agent Decision Logic

The Supervisor Agent uses structured output with the following routing strategy:

1. **Simple Questions**: Route directly to Genie for single-metric queries
2. **Complex Analysis**: Route to ResearchPlanner for:
   - Multi-company comparisons
   - Multiple financial metrics
   - Year-over-year trend analysis
   - Complex financial ratios requiring multiple data points

**Iteration Limit**: Maximum 3 iterations to prevent infinite loops

### Supported Financial Metrics

The system supports comprehensive financial analysis including:
- **Liquidity**: Current Ratio, Quick Ratio
- **Solvency**: Debt-to-Equity, Interest Coverage
- **Profitability**: Gross Margin, Net Profit Margin, ROA, ROE
- **Efficiency**: Asset Turnover
- **Growth**: Revenue Growth YoY
- **Cash Flow**: Free Cash Flow

See `data/genie_instruction.txt` for complete SQL formulas and implementation details.

### Deployment Architecture

The system is designed for deployment as a Databricks model serving endpoint with:
- **Automatic Authentication Passthrough**: For Databricks resources (Genie spaces, SQL warehouses, UC tables)
- **Environment Variables**: Secrets-based configuration for PAT tokens
- **Resource Dependencies**: Declared upfront for automatic credential management
- **MLflow Model Serving**: Compatible with Databricks model serving infrastructure
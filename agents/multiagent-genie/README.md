# Multi-Agent System with Databricks Genie

A multi-agent system built with LangGraph, leveraging Databricks Genie for natural language to SQL query generation. This system intelligently routes queries between specialized agents to provide comprehensive analysis of financial metrics.

I used SEC income statement and balance sheet data for three companies as examples. But this will work with any structured data, provided that a Genie Space has been created and optimized on the dataset. [See this documentation](https://docs.databricks.com/aws/en/genie/set-up) for how to create a Genie Space.

## What This System Does

This multi-agent system provides intelligent financial data analysis by:

1. **Intelligent Query Routing**: A supervisor agent analyzes incoming questions and routes them to the most appropriate agent based on complexity
2. **Natural Language SQL Generation**: Uses Databricks Genie to convert financial questions into SQL queries against SEC filing data
3. **Parallel Query Execution**: For complex analyses, executes multiple related queries concurrently to gather comprehensive data
4. **Comprehensive Analysis**: Synthesizes results from multiple data sources to provide detailed financial insights

## Architecture Overview

### Multi-Agent System Design

```txt
User Query → Supervisor Agent → [Genie Agent OR Parallel Executor] → Final Answer
```

#### Core Components

1. **Supervisor Agent** (`multiagent-genie.py:100`)
   - Routes queries based on complexity analysis
   - Uses structured output with Pydantic models for decision-making
   - Implements 3-iteration limit to prevent infinite loops
   - Handles follow-up questions from Genie agent

2. **Genie Agent** (`multiagent-genie.py:52`)
   - Databricks GenieAgent for direct SQL-based financial queries
   - Accesses SEC financial data through natural language interface
   - Handles single-metric and straightforward financial questions

3. **Parallel Executor Agent** (`multiagent-genie.py:146`)
   - Executes multiple concurrent queries for complex analysis using asyncio
   - Uses `asyncio.gather()` with `asyncio.to_thread()` for MLflow context preservation
   - Maximum 3 concurrent queries for optimal resource usage
   - Synthesizes results from parallel data sources
   - Handles multi-company comparisons and complex financial ratios

### Sample Data Scope and Coverage

- **Time Period**: SEC financial data from 2003-2022
- **Companies**: Apple Inc. (AAPL), Bank of America Corp (BAC), American Express (AXP)
- **Data Types**: Income Statement and Balance Sheet metrics

### Technical Stack

- **Framework**: LangGraph for agent orchestration
- **Data Access**: Databricks Genie for natural language SQL generation
- **LLM**: ChatDatabricks with configurable endpoints (Claude Sonnet models)
- **Observability**: MLflow tracing with `@mlflow.trace` decorators
- **State Management**: Typed state management with Pydantic models
- **Temporal Context**: Automatic fiscal year/quarter awareness with timezone support
- **Async Execution**: asyncio-based parallel processing with MLflow context preservation
- **Evaluation Framework**: Integrated MLflow evaluation with curated test datasets
- **Deployment**: Databricks model serving endpoints

## Getting Started

### Prerequisites

1. **Databricks Environment**: Workspace with cluster compute access
2. **Genie Space**: Created and configured with SEC financial data
3. **Authentication**: Databricks Personal Access Token (PAT)
4. **Permissions**:
   - `CAN RUN` on Genie Space
   - `CAN USE` on SQL Warehouse
   - `SELECT` on Unity Catalog tables
   - `EXECUTE` on Unity Catalog functions

### Setup Instructions

1. **Install Dependencies**

   ```bash
   pip install -r requirements.txt
   ```

2. **Configure Settings**
   Update `configs.yaml` with your Databricks resources:
   - Catalog and schema details
   - Genie space ID and SQL warehouse ID
   - Workspace URL and authentication tokens
   - MLflow experiment configuration

3. **Set Environment Variables**

   ```bash
   export DB_MODEL_SERVING_HOST_URL="your-databricks-workspace-url"
   export DATABRICKS_GENIE_PAT="your-personal-access-token"
   ```

4. **Load Data** (if needed)

   ```python
   # Run the data ingestion script
   %run ./data/sec/ingest-sec-data
   ```

5. **Run the System**
   Execute cells in `driver.py` sequentially to test, log, register, and deploy the agent.

### Testing the System

Use the sample questions provided in `driver.py` (cells 141-143):

- **Simple Question**: "What was AAPL's revenue in 2015?"
- **Complex Analysis**: "Compare the profitability ratios of AAPL, BAC, and AXP for 2020-2022"
- **Trend Analysis**: "Analyze AAPL's debt-to-equity ratio trend from 2018 to 2022"
- **Temporal Context Examples**:
  - "How does this fiscal quarter's performance compare to last quarter?"
  - "What are the year-to-date financial metrics for the current fiscal year?"
  - "Compare current fiscal year performance to the same period last year"

## System Optimization

This system includes comprehensive optimization capabilities across multiple layers:

### 1. Genie Space Optimization (Foundation Layer)

The foundation of system performance relies on proper Genie space configuration:

- **Table Descriptions**: Comprehensive descriptions for all financial tables
- **General Instructions**: Clear SQL guidelines and financial calculation formulas (see `data/sec/genie_instruction.md`)
- **Trusted Assets**: Validated example SQL queries for complex financial metrics
- **Consistency**: Synchronized instructions between Genie space and multi-agent prompts

### 2. Prompt Engineering Optimization

The system uses sophisticated prompt engineering for optimal routing:

#### Supervisor Agent Prompts (`configs.yaml`)

- **System Prompt**: Controls routing logic and decision-making criteria
- **Research Prompt**: Determines when to use parallel query execution
- **Final Answer Prompt**: Formats responses based on query complexity

#### Optimization Strategy

- **Bias Toward Genie**: Default to direct Genie routing for simple queries (reduces latency)
- **Clear Thresholds**: Specific criteria for complex analysis (3+ separate queries)
- **Data-Aware Examples**: Examples matching actual dataset scope (2003-2022 SEC data)

### 3. Performance Monitoring

- **MLflow Integration**: All agent interactions traced for performance analysis
- **Routing Decisions**: Monitor supervisor agent routing accuracy
- **Async Execution**: Track asyncio performance and MLflow context preservation
- **Query Performance**: Individual Genie query execution times
- **Evaluation Metrics**: Automated scoring with Correctness, RelevanceToQuery, and Safety

## Recent Updates

- Asyncio Parallel Execution: The system now uses asyncio-based parallel execution instead of ThreadPoolExecutor, providing significant improvements
- Temporal Context Integration: The system now includes automatic temporal context awareness that provides real-time date and fiscal information to enhance financial analysis

## File Structure

```txt
├── README.md                             # This file
├── CLAUDE.md                             # Development guidance
├── docs/
│   └── optimization-guide.md             # System optimization strategies
├── driver.py                             # Main Databricks notebook entry point
├── multiagent-genie.py                   # Core agent implementation
├── configs.yaml                          # System configuration
├── requirements.txt                      # Python dependencies
└── data/
    ├── sec/                              # SEC financial data
    │   ├── balance_sheet.parquet         # Balance sheet data (2003-2022)
    │   ├── income_statement.parquet      # Income statement data (2003-2022)
    │   ├── genie_instruction.md          # SQL guidelines for Genie
    │   └── ingest-sec-data.py            # Data ingestion script
    ├── evals/                            # Evaluation datasets
    │   └── eval-questions.json           # Curated test questions with expected responses
    └── graphs/                           # Architecture diagrams
        ├── arch-drawing.png
        └── arch-graph.png
```

## Deployment Architecture

The system is designed for production deployment on Databricks:

- **Model Serving Endpoints**: Compatible with Databricks model serving infrastructure
- **Automatic Authentication**: Passthrough for Databricks resources
- **Environment Variables**: Secrets-based configuration for secure token management
- **Resource Dependencies**: Declared upfront for automatic credential management

## Support and Documentation

- **Development Guide**: See `CLAUDE.md` for detailed development instructions
- **Optimization**: Refer to `docs/optimization-guide.md` for performance tuning
- **Data Guidelines**: Check `data/sec/genie_instruction.md` for financial metrics and SQL patterns

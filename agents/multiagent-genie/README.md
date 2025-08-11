# Multi-Agent Financial Analysis System with Databricks Genie

A sophisticated multi-agent system built with LangGraph for SEC financial data analysis, leveraging Databricks Genie for natural language to SQL query generation. This system intelligently routes financial queries between specialized agents to provide comprehensive analysis of financial metrics.

## What This System Does

This multi-agent system provides intelligent financial data analysis by:

1. **Intelligent Query Routing**: A supervisor agent analyzes incoming financial questions and routes them to the most appropriate agent based on complexity
2. **Natural Language SQL Generation**: Uses Databricks Genie to convert financial questions into SQL queries against SEC filing data
3. **Parallel Query Execution**: For complex analyses, executes multiple related queries concurrently to gather comprehensive data
4. **Comprehensive Analysis**: Synthesizes results from multiple data sources to provide detailed financial insights

### Supported Analysis Types

- **Simple Queries**: Single financial metrics (e.g., "What was AAPL's revenue in 2015?")
- **Complex Comparative Analysis**: Multi-company comparisons across different financial ratios
- **Trend Analysis**: Year-over-year financial performance tracking
- **Financial Ratio Calculations**: Liquidity, solvency, profitability, and efficiency metrics

## Architecture Overview

### Multi-Agent System Design

```
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
   - Executes multiple concurrent queries for complex analysis
   - Uses ThreadPoolExecutor with max 3 workers
   - Synthesizes results from parallel data sources
   - Handles multi-company comparisons and complex financial ratios

### Data Scope and Coverage

- **Time Period**: SEC financial data from 2003-2022
- **Companies**: Apple Inc. (AAPL), Bank of America Corp (BAC), American Express (AXP)
- **Data Types**: Income Statement and Balance Sheet metrics
- **Financial Metrics**: 15+ ratios including liquidity, solvency, profitability, efficiency, and growth

### Technical Stack

- **Framework**: LangGraph for agent orchestration
- **Data Access**: Databricks Genie for natural language SQL generation
- **LLM**: ChatDatabricks with configurable endpoints (Claude Sonnet models)
- **Observability**: MLflow tracing with `@mlflow.trace` decorators
- **State Management**: Typed state management with Pydantic models
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

## System Optimization

This system includes comprehensive optimization capabilities across multiple layers:

### 1. Genie Space Optimization (Foundation Layer)

The foundation of system performance relies on proper Genie space configuration:

- **Table Descriptions**: Comprehensive metadata for all financial tables
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
- **Parallel Execution**: Track ThreadPoolExecutor performance and bottlenecks
- **Query Performance**: Individual Genie query execution times

## Future Development Plans

### Genie Metadata Integration Plan

A comprehensive plan has been developed to enhance the supervisor agent's contextual awareness:

#### Proposed Implementation

1. **Metadata API Integration**
   - Retrieve Genie space metadata via REST APIs (`GET /api/2.0/genie/spaces/{space_id}`)
   - Access Unity Catalog table metadata for column descriptions and data types
   - Implement caching system to minimize API overhead

2. **Enhanced Context Awareness**
   - Include table/column descriptions in supervisor agent system prompt
   - Provide metadata about available tables, data types, and relationships
   - Expose trusted assets information for better routing decisions

3. **Benefits**
   - Improved routing accuracy with full data context
   - Reduced trial-and-error queries
   - Better understanding of available data for complex analysis

#### Implementation Approaches

**Option 1: Direct API Integration**
- Real-time metadata access via Databricks REST APIs
- Automatic synchronization with Genie space changes
- Comprehensive metadata coverage

**Option 2: Configuration-Based Solution**
- Manual metadata management in `configs.yaml`
- Full control over exposed information
- No API call overhead during execution

See `docs/genie-metadata-implementation.md` for complete technical details and implementation roadmap.

## File Structure

```
├── README.md                               # This file
├── CLAUDE.md                              # Development guidance
├── docs/
│   ├── genie-metadata-implementation.md   # Future metadata integration plan
│   └── optimization-guide.md              # System optimization strategies
├── driver.py                             # Main Databricks notebook entry point
├── multiagent-genie.py                   # Core agent implementation
├── configs.yaml                          # System configuration
├── requirements.txt                      # Python dependencies
└── data/
    ├── sec/                              # SEC financial data
    │   ├── balance_sheet.parquet         # Balance sheet data (2003-2022)
    │   ├── income_statement.parquet      # Income statement data (2003-2022)
    │   ├── genie_instruction.md          # SQL guidelines for Genie
    │   └── ingest-sec-data.py           # Data ingestion script
    └── graphs/                           # Architecture diagrams
        ├── arch-drawing.png
        └── arch-graph.png
```

## Key Technical Features

### State Management
- **LangGraph AgentState**: Typed state management with conversation history
- **Iteration Control**: Maximum 3 iterations to prevent infinite loops
- **Message Filtering**: Only final answers returned to prevent noise

### Error Handling
- **Graceful Degradation**: Comprehensive error handling in parallel execution
- **Query Validation**: Structured output validation for routing decisions
- **Authentication**: Robust PAT token management for Databricks resources

### Streaming Support
- **predict()**: Synchronous prediction for simple queries
- **predict_stream()**: Streaming responses with status updates for complex analysis
- **Timeout Prevention**: Status updates during long-running operations

## Deployment Architecture

The system is designed for production deployment on Databricks:

- **Model Serving Endpoints**: Compatible with Databricks model serving infrastructure
- **Automatic Authentication**: Passthrough for Databricks resources
- **Environment Variables**: Secrets-based configuration for secure token management
- **Resource Dependencies**: Declared upfront for automatic credential management

## Contributing and Development

### Development Workflow

1. **Environment Setup**: Configure Databricks workspace and Genie space
2. **Local Testing**: Use sample questions to validate functionality
3. **Performance Monitoring**: Review MLflow traces for optimization opportunities
4. **Prompt Tuning**: Adjust routing criteria in `configs.yaml`
5. **Deployment**: Register and deploy via Databricks model serving

### Optimization Guidelines

- **Monitor Routing Decisions**: Use MLflow traces to validate supervisor logic
- **Test Across Complexity Levels**: Ensure proper routing for simple vs. complex queries
- **Synchronize Genie Instructions**: Keep Genie space and agent prompts aligned
- **Performance Tuning**: Optimize parallel execution worker counts based on usage patterns

## Support and Documentation

- **Development Guide**: See `CLAUDE.md` for detailed development instructions
- **Optimization**: Refer to `docs/optimization-guide.md` for performance tuning
- **Future Plans**: Review `docs/genie-metadata-implementation.md` for upcoming features
- **Data Guidelines**: Check `data/sec/genie_instruction.md` for financial metrics and SQL patterns

This multi-agent system represents a sophisticated approach to financial data analysis, combining the power of natural language processing with intelligent agent orchestration to provide comprehensive financial insights.
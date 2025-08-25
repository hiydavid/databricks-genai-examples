# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Environment Setup

- Install dependencies: `pip install -r requirements.txt`
- Install Databricks CLI: `pip install databricks-cli`
- The project uses Python with Databricks-specific libraries (MLflow, LangChain, LangGraph)
- **Key Dependencies**:
  - `mlflow[databricks]==3.2.0`
  - `databricks-langchain==0.6.0`
  - `databricks-agents==1.4.0`
  - `langgraph==0.5.4`
  - `pydantic<2.12.0`
  - `nest-asyncio==1.6.0`
- No traditional build, test, or lint commands - this is a Databricks notebook-based project

### Databricks Asset Bundle (DAB) Commands

**Authentication**:
```bash
databricks auth login  # Follow prompts to authenticate
```

**Bundle Operations**:
```bash
# Validate bundle configuration
databricks bundle validate --profile your-profile

# Deploy bundle to Databricks (creates experiments, jobs, resources)
databricks bundle deploy --profile your-profile

# Run deployment job (test, log, register, deploy agent)
databricks bundle run agent_deploy_job --profile your-profile

# Run evaluation job
databricks bundle run agent_evaluation_job --profile your-profile

# Show bundle status and resources
databricks bundle summary --profile your-profile
```

### Databricks Notebook Structure

- **src/deployment.py**: Main Databricks notebook with `# Databricks notebook source` magic commands
- **src/multiagent_genie.py**: Core Python module imported via `%run ./multiagent_genie`
- Notebook cells are separated by `# COMMAND ----------` markers
- Development workflow requires Databricks environment with cluster compute

### Environment Variables Required

- `DB_MODEL_SERVING_HOST_URL`: Databricks workspace URL for model serving
- `DATABRICKS_GENIE_PAT`: Personal Access Token for Genie space authentication
- Configure in Databricks secrets for production deployment

### Running the Agent

- Main entry point: `src/deployment.py` (Databricks notebook format with `# Databricks notebook source` magic commands)
- Core agent implementation: `src/multiagent_genie.py` (Python module loaded via `%run ./multiagent_genie`)
- Configuration: `configs.yaml` (requires manual setup of Databricks resources)
- Test agent: Use sample questions in `src/deployment.py` cells for testing different complexity levels

### Development Workflow

**Initial Setup**:
1. Update `src/configs.yaml` TODO placeholders with your Databricks resources
2. Update `databricks.yml` workspace host if needed  
3. Create Genie space and obtain space ID from Databricks workspace
4. Set up Databricks PAT token in secrets (required for Genie space access)
5. Load data using `data/sec/ingest-sec-data.py` if needed

**Development Cycle**:
1. **Local Testing**: Test agent logic in `src/multiagent_genie.py` 
2. **DAB Deployment**: Use `databricks bundle deploy` to create infrastructure
3. **Agent Testing**: Run `databricks bundle run agent_deploy_job` to deploy and test
4. **Evaluation**: Run `databricks bundle run agent_evaluation_job` to validate performance
5. **Iteration**: Modify prompts/logic in `src/configs.yaml` and repeat

**Key Development Pattern**:
- Configuration changes in `src/configs.yaml` (prompts, agent descriptions)
- Core logic changes in `src/multiagent_genie.py`
- Infrastructure changes in `databricks.yml`
- Use DAB jobs for consistent deployment and testing

### Testing the Agent

- **Simple Questions**: Test with single metric queries (e.g., "What was AAPL's revenue in 2015?")
- **Complex Questions**: Test multi-company comparisons and trend analysis
- **Temporal Context Questions**: Test date-aware queries (e.g., "What is the current fiscal quarter performance?")
- **Sample Test Cases**: Use `sample_questions` in `src/deployment.py`
- **Response Testing**: Both `predict()` and `predict_stream()` methods available

## Architecture Overview

This is a **multi-agent system** built with LangGraph for financial data analysis using Databricks Genie:

### Core Components

1. **Supervisor Agent** (`supervisor_agent` function in src/multiagent_genie.py:142)
   - Routes queries between agents based on complexity
   - Uses structured output to determine routing strategy
   - Implements iteration limits (max 3 iterations)

2. **Genie Agent** (src/multiagent_genie.py:56)
   - Databricks GenieAgent for SQL-based financial data queries
   - Accesses SEC financial data (2003-2022) for AAPL, BAC, AXP
   - Primary data source for Income Statement and Balance Sheet metrics

3. **Parallel Executor Agent** (`research_planner_node` function in src/multiagent_genie.py:219)
   - Executes parallel queries for complex multi-step analysis using asyncio
   - Uses `asyncio.gather()` with `asyncio.to_thread()` for concurrent Genie queries
   - Preserves MLflow context during parallel execution (eliminates tracing warnings)
   - Synthesizes results from multiple data sources
   - Renamed from "Research Planner" to "Parallel Executor" for clarity

### Data Scope

- **Time Range**: SEC financial data from 2003-2022 (updated from original 2003-2017)
- **Companies**: Apple Inc. (AAPL), Bank of America Corp (BAC), American Express (AXP)
- **Data Types**: Income Statement and Balance Sheet metrics
- **Supported Metrics**: See `src/data/sec/genie_instruction.md` for full list of financial ratios and calculations

### Workflow Pattern

```text
User Query → Supervisor → [Parallel Executor OR Direct Genie] → Supervisor → Final Answer
```

### Key Technical Details

- **State Management**: Uses LangGraph's `AgentState` with typed state including research plans and results
- **MLflow Integration**: All agent calls are traced with `@mlflow.trace` decorators
- **Chat Interface**: Wrapped in `LangGraphChatAgent` class implementing MLflow's `ChatAgent` interface
- **Streaming Support**: Both `predict` and `predict_stream` methods available with status updates to prevent timeouts
- **Configuration**: Extensive YAML-based configuration for Databricks resources
- **Async Parallel Execution**: Uses asyncio with `asyncio.to_thread()` for concurrent Genie queries (max 3)
- **MLflow Context Preservation**: Eliminates "Failed to get Databricks request ID" warnings
- **Error Handling**: Individual query failures don't cancel other parallel queries (`return_exceptions=True`)
- **Authentication**: Uses Databricks PAT stored in secrets for Genie space access  
- **Temporal Context**: Automatic fiscal year/quarter awareness with real-time date injection into prompts
- **Event Loop Compatibility**: Uses `nest-asyncio` for Databricks environment compatibility

### Implementation Details

- **Graph Structure**: Entry point is `supervisor` → workers (`Genie`, `ParallelExecutor`) → `supervisor` → `final_answer`
- **State Schema**: `AgentState` includes messages, next_node, iteration_count, research_plan, and research_results
- **LLM Configuration**: Uses ChatDatabricks with configurable endpoint (default: `databricks-claude-3-7-sonnet`)
- **Structured Output**: Supervisor uses Pydantic models (`NextNode`, `ResearchPlanOutput`) for routing decisions
- **Message Handling**: Only final_answer node messages are returned to prevent intermediate output noise
- **UUID Generation**: All messages get unique IDs for proper MLflow tracing

### Configuration Requirements

Before running, update `src/configs.yaml` with your Databricks resources:

**Critical TODO Items**:
- `databricks_configs.catalog`: Your Unity Catalog name  
- `databricks_configs.schema`: Your schema name
- `databricks_configs.workspace_url`: Your Databricks workspace URL
- `databricks_configs.sql_warehouse_id`: Your SQL warehouse ID
- `agent_configs.genie_agent.space_id`: Your Genie space ID
- `agent_configs.llm.endpoint_name`: LLM endpoint (e.g., "databricks-claude-3-7-sonnet")
- `databricks_configs.databricks_pat`: Secret scope/key for PAT token

**Bundle Configuration**:
Update `databricks.yml`:
- `targets.dev.workspace.host`: Your workspace URL
- Adjust experiment name patterns if needed

### Data Files

- `data/sec/balance_sheet.parquet` and `data/sec/income_statement.parquet`: SEC financial datasets (2003-2022)
- `data/sec/genie_instruction.md`: SQL query guidelines and supported financial metrics for SEC data
- `data/sec/ingest-sec-data.py`: SEC data ingestion script

### Agent Decision Logic

The Supervisor Agent uses structured output with the following routing strategy:

1. **Simple Questions**: Route directly to Genie for single-metric queries
2. **Complex Analysis**: Route to ParallelExecutor for:
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

See `src/data/sec/genie_instruction.md` for complete SQL formulas and implementation details.

### Databricks Asset Bundle (DAB) Architecture

This project uses Databricks Asset Bundles for Infrastructure-as-Code deployment:

**Bundle Structure** (`databricks.yml`):
- **Resources**: Defines MLflow experiments and Databricks jobs
  - `agents_experiment`: MLflow experiment for agent runs  
  - `agent_deploy_job`: Job to deploy and test the agent
  - `agent_evaluation_job`: Job to run agent evaluations
- **Variables**: Configurable parameters (experiment names, etc.)
- **Targets**: Environment-specific configurations (dev/staging/prod)

**Job Configuration**:
- `agent_deploy_job`: Runs `src/deployment.py` notebook with mlflow_experiment parameter
- `agent_evaluation_job`: Runs `src/evaluation.py` notebook  
- Both jobs have timeout protection and concurrency limits
- Jobs run as the current user with proper authentication passthrough

**Deployment Model**:
- **MLflow Model Registration**: Automatic model versioning in Unity Catalog
- **Model Serving Endpoints**: Compatible with Databricks serving infrastructure  
- **Secrets Management**: PAT tokens stored in Databricks secrets

### Code Structure and Key Functions

**src/multiagent-genie.py Functions:**

- `get_temporal_context()` (line 80): Returns current date, fiscal year, and fiscal quarter context
- `supervisor_agent()` (line 142): Main routing logic with temporal context injection and structured output
- `research_planner_node()` (line 219): Parallel query execution with asyncio
- `agent_node()` (line 347): Generic wrapper for individual agents
- `final_answer()` (line 407): Formats final response using configured prompt

**LangGraphChatAgent Class:**

- `predict()` (line 477): Synchronous prediction with message filtering
- `predict_stream()` (line 576): Streaming prediction with status updates

### Financial Data Analysis Capabilities

The system is specifically designed for SEC financial data analysis with predefined formulas in `data/sec/genie_instruction.md`:

- **Data Coverage**: 2003-2022 SEC filings for AAPL, BAC, AXP only
- **Table Aliases**: `bs` (balance sheet), `is` (income statement), `cf` (cash flow)
- **Query Guidelines**: Fully qualified columns, explicit filtering, NULLIF for division safety
- **Supported Calculations**: 15+ financial ratios including liquidity, solvency, profitability, efficiency, and growth metrics

## Critical Development Notes

### Databricks Asset Bundle Prerequisites

**Required Permissions**:
- Workspace admin or sufficient privileges to create MLflow experiments
- Access to create and run Databricks jobs
- Unity Catalog access for model registration
- Genie space access (`CAN RUN` permission)
- SQL warehouse access (`CAN USE` permission)

**Authentication Setup**:
- **CLI Authentication**: Run `databricks auth login` and configure profile
- **Genie Space Access**: Create PAT token and store in Databricks secrets  
- **Secret Configuration**: Update `src/configs.yaml` with secret scope/key details
- **Model Serving**: PAT token automatically passed through for serving endpoints

### Common Issues and Solutions

**DAB Deployment Issues**:
- **Authentication Errors**: Use `--profile your-profile` flag with bundle commands
- **Task Type Mismatch**: Ensure `.py` notebook files use `notebook_task` in `databricks.yml`  
- **Dependencies Not Found**: Keep `requirements.txt` in root directory, not `src/`
- **MLflow Experiment Issues**: Verify bundle created experiment at configured path

**Agent Runtime Issues**:  
- **Streaming Timeouts**: Use `predict_stream()` for long-running queries to prevent client timeouts
- **MLflow Context Warnings**: Fixed by asyncio implementation using `asyncio.to_thread()`
- **Event Loop Conflicts**: Resolved with `nest-asyncio` dependency for Databricks compatibility
- **Parallel Execution Issues**: Monitor asyncio performance via MLflow traces; individual failures don't cancel other queries
- **Routing Accuracy**: Use structured output validation to ensure proper agent selection
- **Temporal Context**: Ensure relative time terms are converted to explicit dates in parallel subqueries

### MLflow Tracing Integration

- All agent interactions traced with manual `@mlflow.trace` decorators for clean output
- **MLflow Autolog Disabled**: `mlflow.langchain.autolog()` commented out to prevent verbose LangChain state capture
- Monitor supervisor routing decisions, Genie query performance, and parallel execution coordination
- Enhanced Genie tracing with explicit query parameters for better trace visibility
- Use traces to identify optimization opportunities and performance bottlenecks

### Structured Output Schema

- `NextNode`: Controls agent routing with Literal type constraints
- `ResearchPlan`: Defines parallel query structure and rationale
- `AgentState`: Manages conversation state across iterations (max 3)

## Temporal Context Integration (Recent Enhancement)

The system now includes automatic temporal context awareness that provides real-time fiscal calendar information to enhance financial analysis:

### Temporal Context Features

1. **Automatic Date Context**
   - Current date in ISO format (America/New_York timezone)
   - Automatically injected into supervisor agent system prompts
   - Enables date-aware financial queries and analysis

2. **Fiscal Year Awareness**
   - Fiscal year calculation following Sep 1 → Aug 31 calendar
   - Labeled by end year (e.g., FY2025 runs Sep 2024 → Aug 2025)
   - Supports fiscal year-based financial comparisons and "current fiscal year" queries

3. **Fiscal Quarter Context**  
   - Q1: Sep-Nov, Q2: Dec-Feb, Q3: Mar-May, Q4: Jun-Aug
   - Enables quarterly financial analysis and reporting
   - Supports quarter-over-quarter trend analysis and "current quarter" queries

### Implementation Architecture

**Function Location**: `get_temporal_context()` in src/multiagent_genie.py:80

```python
def get_temporal_context() -> Dict[str, str]:
    """Return current date, fiscal year, and fiscal quarter.
    
    Fiscal year runs Sep 1 -> Aug 31, labeled by end year.
    Quarters: Q1=Sep-Nov, Q2=Dec-Feb, Q3=Mar-May, Q4=Jun-Aug
    """
```

**Context Injection**: Automatically prepended to supervisor system prompts:

```text
- The current date is: {today_iso}
- The current fiscal year is: {fy}  
- The current fiscal quarter is: {fq}
```

### Key Benefits

- **Date-Aware Analysis**: Supports queries like "How does this quarter compare to last quarter?"
- **Fiscal Context**: Enables fiscal year-based financial reporting aligned with business standards
- **Temporal Trends**: Better context for year-over-year and quarter-over-quarter analysis
- **Real-Time Updates**: Context automatically reflects current date without manual configuration
- **Enhanced Routing**: Supervisor can make better decisions based on temporal context

### Usage Examples

With temporal context, the system can now handle queries like:

- "What is the current fiscal year performance for AAPL?"
- "Compare this quarter's results to the same quarter last year"
- "How has performance changed since the beginning of the fiscal year?"

## Prompt Optimization

The system includes comprehensive prompt optimization capabilities documented in `src/docs/optimization-guide.md`:

### Key Optimization Areas

- **Supervisor Agent System Prompt**: Controls routing logic and decision-making
- **Research Planning Prompt**: Determines when to use parallel query execution  
- **Final Answer Prompt**: Formats responses for simple vs. complex queries

### Configuration Location

All prompts are configured in `configs.yaml` under `agent_configs.supervisor_agent`:

- `system_prompt`: Main supervisor routing logic
- `research_prompt`: Parallel execution decision criteria
- `final_answer_prompt`: Response formatting guidelines

### Optimization Guidelines

- **Bias Toward Genie**: Default to direct Genie routing for simple queries to reduce latency
- **Clear Thresholds**: Define specific criteria for complex analysis (e.g., "3+ separate queries")
- **Data-Aware Examples**: Use examples matching your actual dataset scope
- **Performance Monitoring**: Use MLflow tracing to monitor routing decisions and response quality

See `src/docs/optimization-guide.md` for detailed prompt customization strategies and testing approaches.

## File Structure and Dependencies

### Core Files

- `src/deployment.py`: Databricks notebook entry point with cell-based structure
- `src/evaluation.py`: Evaluation notebook for testing agent performance
- `src/multiagent_genie.py`: Core agent implementation with LangGraph workflow
- `configs.yaml`: Complete system configuration including prompts
- `requirements.txt`: Python dependencies for Databricks environment

### Data Directory Structure

- `data/sec/`: SEC financial data and instructions
  - `balance_sheet.parquet`, `income_statement.parquet`: Financial datasets (2003-2022)
  - `genie_instruction.md`: SQL guidelines and financial formulas for Genie space
  - `ingest-sec-data.py`: Data ingestion script
- `data/evals/`: Evaluation datasets
  - `eval-questions.json`: Curated test questions with expected responses for automated evaluation

### Configuration Management

- All settings managed through `configs.yaml` using `mlflow.models.ModelConfig`
- Must update TODO placeholders before running
- Prompts are configurable via YAML for easy optimization
- Environment variables loaded from `os.getenv()` for runtime configuration

### Notebook Development Pattern

- Cells use `# MAGIC` commands for markdown documentation
- Python code cells separated by `# COMMAND ----------`
- Autoreload enabled for development: `%load_ext autoreload`, `%autoreload 2`
- Library installation via `%pip install` followed by `dbutils.library.restartPython()`

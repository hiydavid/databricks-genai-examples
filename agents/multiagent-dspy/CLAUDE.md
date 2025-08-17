# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Environment Setup
- Install dependencies: `pip install -r requirements.txt`
- The project uses Python with Databricks-specific libraries and DSPy framework
- **Key Dependencies**: 
  - `mlflow-skinny[databricks]==3.2.0`
  - `databricks-langchain==0.6.0`
  - `databricks-agents==1.1.0`
  - `dspy-ai>=3.0.0` (upgraded from 2.6.27 for enhanced optimization capabilities and improved MLflow integration)
  - `pydantic<2.12.0`
- **Python Version**: Requires Python 3.10+ (for DSPy 3.0 compatibility)
- No traditional build, test, or lint commands - this is a Databricks notebook-based project

### Running the Agent
- Main entry point: `driver.py` (Databricks notebook format with `# Databricks notebook source` magic commands)
- Core agent implementation: `agent.py` (Python module loaded via `%run ./agent`)
- Configuration: `configs.yaml` (requires manual setup of Databricks resources)
- Test agent: Use sample questions in `driver.py` cells 141-143 for testing different complexity levels

### Development Workflow
1. Update `configs.yaml` with your Databricks resources (catalog, schema, workspace_url, etc.)
2. Create Genie space and obtain space ID from Databricks workspace
3. Set up Databricks PAT token in secrets (required for Genie space access)
4. Load data using `data/sec/ingest-sec-data.py` if needed
5. Run cells in `driver.py` sequentially to test, log, register, and deploy the agent
6. Use sample questions in cells 141-143 for testing different complexity levels

### Testing the Agent
- **Simple Questions**: Test with single metric queries (e.g., "What was AAPL's revenue in 2015?")
- **Complex Questions**: Test multi-company comparisons and trend analysis
- **Sample Test Cases**: Use `sample_questions` array in `driver.py` cells 141-143
- **Response Testing**: Both `predict()` and `predict_stream()` methods available

## Architecture Overview

This is a **multi-agent system** built with DSPy framework for financial data analysis using Databricks Genie:

### Core Components

1. **SupervisorModule** (`SupervisorModule` class in agent.py:98)
   - DSPy module that routes queries between agents based on complexity
   - Uses `dspy.ChainOfThought(RoutingDecision)` for structured routing decisions
   - Implements iteration limits (max 3 iterations)

2. **GenieModule** (agent.py:124)
   - DSPy module wrapping Databricks GenieAgent for SQL-based financial data queries
   - Accesses SEC financial data (2003-2022) for AAPL, BAC, AXP
   - Primary data source for Income Statement and Balance Sheet metrics

3. **ParallelExecutorModule** (`ParallelExecutorModule` class in agent.py:138)
   - DSPy module for executing parallel queries for complex multi-step analysis
   - Uses ThreadPoolExecutor for concurrent Genie queries
   - Synthesizes results from multiple data sources

4. **FinalAnswerModule** (`FinalAnswerModule` class in agent.py:194)
   - DSPy module for generating comprehensive final responses
   - Uses `dspy.ChainOfThought(FinalAnswer)` for response formatting

### DSPy Architecture Advantages

- **Modular Programming**: Each agent is a DSPy module with clear signatures
- **Automatic Optimization**: DSPy can automatically optimize prompts across the entire pipeline
- **Declarative Signatures**: Clear input/output field definitions with `dspy.InputField` and `dspy.OutputField`
- **Structured Output**: Built-in support for structured predictions
- **System Message Context**: Uses `dspy.context()` for system prompts instead of manual prompt engineering

### DSPy Signatures (Interfaces)

1. **RoutingDecision** (agent.py:65): Supervisor routing logic
2. **GenieQuery** (agent.py:75): Genie agent interface
3. **ParallelResearch** (agent.py:81): Parallel executor interface  
4. **FinalAnswer** (agent.py:87): Final response generation interface

### Data Scope
- **Time Range**: SEC financial data from 2003-2022
- **Companies**: Apple Inc. (AAPL), Bank of America Corp (BAC), American Express (AXP)
- **Data Types**: Income Statement and Balance Sheet metrics
- **Supported Metrics**: See `data/sec/genie_instruction.md` for full list of financial ratios and calculations

### Workflow Pattern
```
User Query → SupervisorModule → [ParallelExecutorModule OR GenieModule] → SupervisorModule → FinalAnswerModule
```

### Key Technical Details

- **State Management**: Uses DSPy module composition instead of graph-based state
- **MLflow Integration**: All module calls are traced with `@mlflow.trace` decorators
- **Chat Interface**: Wrapped in `DSPyChatAgent` class implementing MLflow's `ChatAgent` interface
- **Streaming Support**: Both `predict` and `predict_stream` methods available with status updates
- **Configuration**: Extensive YAML-based configuration for Databricks resources
- **Parallel Execution**: ParallelExecutorModule uses ThreadPoolExecutor (max 3 workers) for concurrent Genie queries
- **Error Handling**: Comprehensive error handling in parallel query execution with graceful degradation
- **Authentication**: Uses Databricks PAT stored in secrets for Genie space access

### Implementation Details

- **Module Composition**: SupervisorModule orchestrates GenieModule, ParallelExecutorModule, and FinalAnswerModule
- **DSPy LM Integration**: Custom `DatabricksLM` class extends `dspy.LM` for Databricks ChatDatabricks compatibility
- **Signature Polymorphism**: DSPy signatures provide flexible input/output interfaces
- **Context Management**: Uses `dspy.context(lm=dspy_lm.copy(system_message=...))` for system prompts
- **Structured Prediction**: All modules use DSPy's structured output capabilities
- **Message Handling**: Only final answer messages are returned to prevent intermediate output noise
- **UUID Generation**: All messages get unique IDs for proper MLflow tracing

### Configuration Requirements

Before running, update `configs.yaml` with:
- Databricks workspace details (catalog, schema, workspace_url)
- Genie space ID and SQL warehouse ID  
- MLflow experiment name
- Authentication tokens (stored in Databricks secrets)
- LLM endpoint configuration

### DSPy vs LangGraph Differences

**DSPy Advantages:**
- **Declarative Programming**: Program with modules and signatures instead of prompts
- **Automatic Optimization**: Built-in prompt and weight optimization capabilities
- **Type Safety**: Strong typing with input/output field definitions
- **Composability**: Easy module composition and reuse
- **Research-Backed**: Based on Stanford NLP research with proven optimization techniques

**Key Changes from LangGraph:**
- Replaced `StateGraph` with `DSPyMultiAgentSystem` module
- Replaced `add_conditional_edges` with module composition logic
- Replaced Pydantic models with `dspy.Signature` definitions
- Replaced manual prompt engineering with DSPy context management
- Replaced graph-based state with module-based state passing

### Agent Decision Logic

The SupervisorModule uses structured DSPy signatures with the following routing strategy:

1. **Simple Questions**: Route directly to GenieModule for single-metric queries
2. **Complex Analysis**: Route to ParallelExecutorModule for:
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

See `data/sec/genie_instruction.md` for complete SQL formulas and implementation details.

### Deployment Architecture

The system is designed for deployment as a Databricks model serving endpoint with:
- **Automatic Authentication Passthrough**: For Databricks resources (Genie spaces, SQL warehouses, UC tables)
- **Environment Variables**: Secrets-based configuration for PAT tokens
- **Resource Dependencies**: Declared upfront for automatic credential management
- **MLflow Model Serving**: Compatible with Databricks model serving infrastructure

### Code Structure and Key Classes

**agent.py Classes:**
- `DatabricksLM()` (line 53): Custom DSPy LM for Databricks integration
- `SupervisorModule()` (line 98): Main routing logic with DSPy signatures
- `GenieModule()` (line 124): DSPy wrapper for Genie agent
- `ParallelExecutorModule()` (line 138): Parallel query execution with synthesis
- `FinalAnswerModule()` (line 194): Final response formatting
- `DSPyMultiAgentSystem()` (line 213): Main orchestrator module
- `DSPyChatAgent()` (line 286): MLflow ChatAgent wrapper

### Financial Data Analysis Capabilities

The system is specifically designed for SEC financial data analysis with predefined formulas:
- **Data Coverage**: 2003-2022 SEC filings for AAPL, BAC, AXP only
- **Table Aliases**: `bs` (balance sheet), `is` (income statement), `cf` (cash flow)
- **Query Guidelines**: Fully qualified columns, explicit filtering, NULLIF for division safety
- **Supported Calculations**: 15+ financial ratios including liquidity, solvency, profitability, efficiency, and growth metrics

## Critical Development Notes

### DSPy-Specific Requirements
- **DSPy Configuration**: Must call `dspy.configure(lm=dspy_lm)` to set up the language model
- **Signature Design**: Use clear input/output field descriptions for better optimization
- **Module Composition**: Design modules to be reusable and composable
- **Context Management**: Use `dspy.context()` for system prompts and special configurations

### Authentication Requirements
- **Genie Space Access**: Requires `DATABRICKS_GENIE_PAT` environment variable for Genie space authentication
- **Model Serving**: Configure PAT token in secrets for deployment (`{{secrets/scope/genie-pat}}`)
- **Permissions**: Ensure PAT has access to Genie space, SQL warehouse, and Unity Catalog tables

### Common Issues and Solutions
- **DSPy LM Integration**: Ensure `DatabricksLM` properly implements `basic_request` method
- **Signature Validation**: DSPy validates input/output fields - ensure proper field definitions
- **Module State**: DSPy modules are stateless - pass all required state through forward() method
- **Optimization Readiness**: Design signatures and modules for future DSPy optimization

### MLflow Tracing Integration
- All module interactions automatically traced with `@mlflow.trace` decorators
- Monitor supervisor routing decisions, Genie query performance, and parallel execution coordination
- Use traces to identify optimization opportunities and performance bottlenecks

### DSPy Optimization Capabilities (DSPy 3.0)
- **GEPA Optimizer**: Genetic-Pareto approach using natural language reflection with multi-objective evolutionary search
- **SIMBA Optimizer**: Stochastic Introspective Mini-Batch Ascent - LLM analyzes its own performance for improvement
- **GRPO Optimizer**: Group Relative Policy Optimization - reinforcement learning-based approach
- **Enhanced MLflow Integration**: Better observability and tracing for optimization workflows
- **Example-Based Tuning**: Support for optimizing based on example question-answer pairs
- **Multi-Metric Optimization**: Optimize across accuracy, latency, and cost metrics simultaneously

## Prompt Optimization

The system includes comprehensive prompt optimization capabilities through DSPy:

### Key Optimization Areas
- **Supervisor Routing Signatures**: DSPy can optimize routing decision prompts automatically
- **Research Planning Logic**: Automatic optimization of when to use parallel execution
- **Final Answer Generation**: Optimization of response formatting for different query types

### Configuration Location
All prompts are configured in `configs.yaml` under `agent_configs.supervisor_agent`:
- `system_prompt`: Main supervisor routing logic
- `research_planning_prompt`: Parallel execution decision criteria
- `final_answer_prompt`: Response formatting guidelines

### DSPy Optimization Guidelines
- **Signature-Based Optimization**: DSPy optimizes based on signature definitions
- **Example Data**: Provide example inputs/outputs for better optimization
- **Multi-Objective**: Optimize for accuracy, latency, and consistency simultaneously
- **Automatic Prompt Engineering**: Let DSPy handle prompt refinement automatically

### Implementing DSPy 3.0 Optimizers

**GEPA Example** (Recommended for financial analysis):
```python
from dspy.teleprompt import GEPA

# Optimize SupervisorModule routing decisions
optimizer = GEPA(metric=financial_accuracy_metric, max_demos=4)
optimized_supervisor = optimizer.compile(
    SupervisorModule(), 
    trainset=financial_examples
)
```

**SIMBA Example** (Good for complex queries):
```python
from dspy.teleprompt import SIMBA

optimizer = SIMBA(
    metric=financial_accuracy_metric,
    max_demos=4,  # Ideal for financial use cases
    num_candidates=6
)
optimized_system = optimizer.compile(dspy_multi_agent, trainset=examples)
```

### Future Optimization Enhancements
- Implement automatic evaluation metrics for financial accuracy
- Set up continuous optimization pipeline with new financial examples  
- Explore multi-objective optimization (accuracy + latency + cost)
- Integrate human-in-the-loop optimization workflows

The DSPy 3.0 framework provides significant advantages over traditional prompt engineering by enabling automatic optimization across the entire multi-agent pipeline, with enhanced MLflow integration and improved optimization efficiency.
# Multi-Agent System with DSPy

> **🚧 UNDER CONSTRUCTION / WORK IN PROGRESS 🚧**
> 
> This project is currently under active development and is not yet production-ready. Features, APIs, and documentation may change significantly. Use for experimentation and development purposes only.

A multi-agent financial data analysis system built with the DSPy framework, designed as an alternative to the LangGraph-based `multiagent-genie` implementation.

## Overview

This project demonstrates how to build a sophisticated multi-agent system using DSPy's module-based architecture instead of traditional graph-based orchestration. The system analyzes SEC financial data (2003-2022) for Apple Inc. (AAPL), Bank of America Corp (BAC), and American Express (AXP).

### Key Components

- **SupervisorModule**: Routes queries between agents using DSPy signatures
- **GenieModule**: Wraps Databricks GenieAgent for SQL-based financial queries
- **ParallelExecutorModule**: Executes parallel research queries for complex analysis
- **FinalAnswerModule**: Generates comprehensive responses using DSPy chain-of-thought

### DSPy Advantages Over LangGraph

- **Modular Programming**: Program with modules instead of prompts
- **Automatic Optimization**: Built-in prompt and weight optimization (GEPA, SIMBA, GRPO optimizers)
- **Declarative Signatures**: Clear input/output interfaces between modules
- **Type Safety**: Strong typing with field definitions
- **Research-Backed**: Based on Stanford NLP optimization techniques
- **Enhanced Performance**: DSPy 3.0 provides improved async support and native MLflow integration

## Prerequisites

⚠️ **Important Setup Requirements**

- **Python 3.10+** (required for DSPy 3.0)
- Databricks workspace with Genie Space configured
- Personal Access Token (PAT) stored as Databricks secret
- SQL warehouse with appropriate permissions
- Unity Catalog tables with SEC financial data

## Installation

```bash
pip install -r requirements.txt
```

### Key Dependencies

- `dspy-ai>=3.0.0` - Core DSPy framework (upgraded from 2.6.27)
- `databricks-langchain==0.6.0` - Databricks integrations
- `mlflow-skinny[databricks]==3.2.0` - Model serving and tracing
- `databricks-agents==1.1.0` - Agent deployment

## Configuration

1. **Update `configs.yaml`** with your Databricks resources:
   ```yaml
   databricks_configs:
     catalog: your_catalog
     schema: your_schema  
     workspace_url: https://your-workspace.databricks.com
     sql_warehouse_id: your_warehouse_id
   ```

2. **Set up authentication** in Databricks secrets for Genie space access

3. **Configure Genie Space ID** in the agent configs section

## Usage

### Local Development

```python
# Load the DSPy multi-agent system
%run ./agent

# Test with sample questions
input_example = {
    "messages": [
        {
            "role": "user",
            "content": "What's the debt-to-asset ratio for American Express from 2012 to 2021?"
        }
    ]
}

response = AGENT.predict(input_example)
print(response.messages[-1].content)
```

### Deployment

Use the provided `driver.py` notebook to:
1. Test the agent locally
2. Log as MLflow model  
3. Register to Unity Catalog
4. Deploy to model serving endpoint

## Supported Financial Analysis

### Data Scope
- **Time Range**: 2003-2022 SEC filings
- **Companies**: AAPL, BAC, AXP only
- **Data Types**: Income Statement and Balance Sheet metrics

### Financial Metrics
- **Liquidity**: Current Ratio, Quick Ratio
- **Solvency**: Debt-to-Equity, Interest Coverage  
- **Profitability**: Gross Margin, Net Profit Margin, ROA, ROE
- **Efficiency**: Asset Turnover
- **Growth**: Revenue Growth YoY

## Sample Questions

```python
sample_questions = [
    "What's the debt-to-asset ratio for American Express from 2012 to 2021, compare to that of Bank of America?",
    "Give me an executive summary comparing year-on-year revenue growth from 2012 to 2021 between AAPL and BAC?",
    "Why is BAC's revenue growth so volatile between the years 2012 to 2021?",
]
```

## Architecture

```
User Query → SupervisorModule → [GenieModule | ParallelExecutorModule] → FinalAnswerModule
```

### Decision Logic
- **Simple Questions**: Route directly to GenieModule
- **Complex Analysis**: Route to ParallelExecutorModule for parallel execution
- **Iteration Limit**: Maximum 3 iterations to prevent loops

## Project Status

### ✅ Completed Features
- [x] Core DSPy module implementation
- [x] Databricks LLM integration via custom `DatabricksLM`
- [x] MLflow ChatAgent wrapper for compatibility
- [x] Parallel query execution with ThreadPoolExecutor
- [x] Comprehensive configuration system
- [x] MLflow tracing integration

### 🚧 In Progress
- [ ] DSPy GEPA/SIMBA optimizer integration for enhanced financial analysis accuracy
- [ ] Enhanced error handling and recovery

### 📋 Planned Features
- [x] DSPy 3.0 upgrade (with enhanced optimization capabilities and native MLflow integration)
- [ ] DSPy GEPA/SIMBA optimizer integration for automatic prompt optimization
- [ ] Extended data source integration

## DSPy Optimization Features

### Available Optimizers (DSPy 3.0)

- **GEPA (Genetic-Pareto)**: Natural language reflection with multi-objective evolutionary search (35x fewer rollouts)
- **SIMBA (Stochastic Introspective Mini-Batch Ascent)**: LLM self-analysis for performance improvement with introspective reasoning
- **GRPO (Group Relative Policy Optimization)**: Reinforcement learning-based optimization (outperformed by GEPA)

### Future Optimization Integration

```python
# Example: Optimizing the SupervisorModule for better routing decisions
from dspy.teleprompt import GEPA

optimizer = GEPA(metric=financial_accuracy_metric)
optimized_supervisor = optimizer.compile(
    SupervisorModule(), 
    trainset=financial_examples
)
```

## Development Notes

See `CLAUDE.md` for detailed development guidelines, architecture details, and DSPy-specific implementation notes.

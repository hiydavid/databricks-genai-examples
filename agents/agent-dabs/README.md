# AI Agent Development Tutorial: Local to Databricks

Learn how to develop AI agent systems locally and deploy them to Databricks using Databricks Asset Bundles (DABs). This tutorial demonstrates building a multi-agent financial analysis system that routes queries intelligently and leverages Databricks Genie for natural language to SQL conversion.

**Tutorial Goal**: Show the complete workflow from local development → evaluation → production deployment using modern MLOps practices with Databricks.

## What You'll Build

A production-ready multi-agent system featuring:

- **Local Development**: Build and test agents using standard Python tooling
- **Intelligent Routing**: A supervisor agent that routes queries based on complexity  
- **Databricks Integration**: Leverage Genie spaces for natural language SQL generation
- **MLOps Pipeline**: Evaluation, model registration, and deployment via DABs
- **Production Deployment**: Model serving endpoints with proper authentication

## Development Workflow

This tutorial follows a modern MLOps workflow:

```txt
Local Development → Testing & Evaluation → DAB Deployment → Model Serving
```

### Tutorial Architecture

**Agent System**: Multi-agent financial analysis with intelligent routing  
**Data Source**: SEC financial data (2003-2022) via Databricks Genie  
**Framework**: LangGraph + MLflow + Databricks Asset Bundles  

**Key Components**:
- **Supervisor Agent**: Routes queries based on complexity
- **Genie Agent**: Handles simple SQL queries via natural language
- **Parallel Executor**: Manages complex multi-step analysis

### Technology Stack

- **Local Development**: Python + LangGraph + MLflow
- **Data Access**: Databricks Genie spaces for natural language SQL
- **Deployment**: Databricks Asset Bundles (DABs)
- **Serving**: Databricks model serving endpoints
- **Evaluation**: MLflow evaluation framework

## Tutorial Steps

### Step 1: Local Development Setup

1. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   pip install databricks-cli
   ```

2. **Configure Databricks CLI**
   ```bash
   databricks auth login
   # Follow prompts to authenticate with your workspace
   ```

3. **Set Up Your Environment**
   Update `configs.yaml` with your Databricks resources:
   - Catalog and schema details
   - Genie space ID and SQL warehouse ID  
   - Workspace URL and MLflow experiment configuration

### Step 2: Local Development & Testing

1. **Develop Your Agent Logic**
   - Core implementation in `src/multiagent_genie.py`
   - Configuration via `configs.yaml`
   - Local testing with sample questions

2. **Test Agent Responses**
   ```python
   # Test different query types locally
   simple_query = "What was AAPL's revenue in 2015?"
   complex_query = "Compare profitability ratios of AAPL, BAC, and AXP"
   ```

### Step 3: Evaluation & Validation

1. **Run Evaluation Notebook**
   Execute `src/evaluation.py` to validate agent performance with curated test questions

2. **Review MLflow Traces**
   Monitor agent routing decisions and performance metrics

### Step 4: Production Deployment via DABs

1. **Configure Databricks Bundle**
   Update `databricks.yml` with your workspace settings

2. **Deploy via Asset Bundles**
   ```bash
   # Validate configuration
   databricks bundle validate --profile your-profile
   
   # Deploy to Databricks
   databricks bundle deploy --profile your-profile
   
   # Run deployment job
   databricks bundle run agent_deploy_job --profile your-profile
   ```

3. **Model Serving**
   The system automatically creates model serving endpoints for production use

## What You'll Learn

By completing this tutorial, you'll understand:

- **Local Agent Development**: Building multi-agent systems with LangGraph
- **MLflow Integration**: Tracing, evaluation, and model management
- **Databricks Asset Bundles**: Infrastructure-as-code for ML deployments
- **Production MLOps**: End-to-end workflow from development to serving

### Sample Questions to Test

- **Simple**: "What was AAPL's revenue in 2015?"
- **Complex**: "Compare profitability ratios of AAPL, BAC, and AXP for 2020-2022"  
- **Trend Analysis**: "Analyze AAPL's debt-to-equity ratio trend from 2018 to 2022"

## Prerequisites

Before starting this tutorial, ensure you have:

1. **Databricks Workspace Access**
   - Workspace with compute access
   - Permissions to create MLflow experiments
   - Access to Unity Catalog

2. **Genie Space Setup**  
   - Created Genie space with financial data
   - `CAN RUN` permissions on the space
   - `CAN USE` permissions on SQL Warehouse

3. **Authentication**
   - Databricks Personal Access Token (PAT)
   - Configured Databricks CLI profile

## Tutorial File Structure

```txt
├── src/
│   ├── deployment.py                     # Main tutorial notebook - start here
│   ├── evaluation.py                     # Agent evaluation and testing
│   └── multiagent_genie.py               # Core agent implementation
├── databricks.yml                        # DAB configuration for deployment  
├── configs.yaml                          # Agent and system configuration
├── requirements.txt                      # Python dependencies
└── data/                                 # Sample financial data and evaluation sets
    ├── sec/                              # SEC financial data (2003-2022)  
    └── evals/                            # Evaluation question sets
```

## Key Tutorial Concepts

### Databricks Asset Bundles (DABs)

Learn how DABs enable Infrastructure-as-Code for ML projects:

- **Declarative Configuration**: Define resources in `databricks.yml`
- **Environment Management**: Separate dev/staging/prod environments  
- **Automated Deployment**: One command to deploy experiments, jobs, and models
- **Resource Management**: Handle permissions, secrets, and dependencies

### MLflow Integration Patterns

Understand production MLflow usage:

- **Agent Tracing**: Monitor multi-agent interactions and routing decisions
- **Model Registration**: Automated model versioning in Unity Catalog
- **Evaluation Framework**: Systematic agent performance testing
- **Production Serving**: Model endpoints with proper authentication

## Next Steps

After completing this tutorial, you'll be ready to:

- **Adapt for Your Data**: Replace SEC data with your own structured datasets
- **Customize Agents**: Modify routing logic and agent behaviors
- **Scale Deployment**: Use DAB environments for dev/staging/prod workflows
- **Monitor Production**: Implement comprehensive agent performance monitoring

## Additional Resources

- **Development Guide**: See `CLAUDE.md` for detailed development instructions  
- **Databricks Asset Bundles**: [Official Documentation](https://docs.databricks.com/dev-tools/bundles/index.html)
- **Genie Spaces**: [Setup Guide](https://docs.databricks.com/aws/en/genie/set-up)
- **MLflow Agents**: [Framework Documentation](https://mlflow.org/docs/latest/llms/index.html)

## Common Issues

**Authentication**: Use `databricks bundle deploy --profile your-profile` for specific profiles  
**Dependencies**: Ensure `requirements.txt` is in the root directory  
**MLflow Experiments**: Verify DAB deployment created the experiment path correctly

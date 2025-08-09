# Agent Development Tutorials for Databricks

This directory contains **hands-on tutorials** that teach you how to build multi-agent systems on the Databricks platform. Each tutorial demonstrates different approaches and methodologies for creating, orchestrating, and deploying intelligent agent workflows.

## Tutorial Overview

These tutorials use **financial data analysis** as a practical domain to demonstrate agent development concepts. You'll learn different patterns for multi-agent coordination, from no-code UI-driven approaches to sophisticated programmatic orchestration using popular frameworks.

| Tutorial | Framework | Approach | Learning Objectives |
|---|---|---|---|
| **agentbricks/** | Databricks Agent Bricks | UI-First | Learn no-code agent creation via Databricks UI |
| **multiagent-genie/** | Genie + LangGraph | Code-First | Build SQL-focused agents with programmatic coordination |
| **multiagent-langgraph/** | LangGraph + Vector Search + UC Tools | Code-First | Create document retrieval systems with advanced orchestration |

## Tutorials

### 🧱 Tutorial 1: Agent Bricks (`agentbricks/`)
**Learn to build agents using Databricks' no-code interface**

**What you'll learn:**
- How to create Genie Spaces for structured data analysis
- Building Knowledge Assistants with Vector Search integration
- Configuring Multi-Agent Supervisors to coordinate between agents
- Best practices for prompt engineering in the Databricks UI

**Note**: Agent Bricks is currently in **Beta**. See the [official documentation](https://docs.databricks.com/aws/en/generative-ai/agent-bricks/multi-agent-supervisor) for latest updates.

**Prerequisites:** Basic familiarity with Databricks workspace navigation

### 🤖 Tutorial 2: Multi-Agent with Genie (`multiagent-genie/`)
**Build programmatic agents that leverage Databricks Genie for SQL generation**

**What you'll learn:**
- LangGraph fundamentals for multi-agent orchestration
- Integrating Databricks Genie for natural language to SQL
- Implementing supervisor patterns for agent coordination
- Parallel query execution and result synthesis
- MLflow integration for agent deployment and monitoring

**Prerequisites:** Python programming, basic SQL knowledge, familiarity with LangChain concepts

### 🔍 Tutorial 3: Advanced LangGraph System (`multiagent-langgraph/`)
**Create sophisticated document retrieval and research agents**

**What you'll learn:**
- Advanced LangGraph patterns for complex workflows  
- Multi-index vector search implementation
- Self-querying retrievers with metadata filtering
- Agent handoff patterns and state management
- Parallel document processing and synthesis techniques

**Prerequisites:** Intermediate Python, understanding of vector embeddings, experience with document processing

## Setup Requirements

All tutorials require access to:

- **Databricks Workspace** with Unity Catalog enabled
- **ML Runtime Cluster** (for vector search and model operations)
- **Databricks Model Serving** (for production deployment tutorials)
- **Vector Search Endpoints** (for document-based tutorials)
- **Appropriate workspace permissions** for creating schemas, tables, and endpoints

## How to Use These Tutorials

1. **Choose Your Learning Path**:
   - **New to agent development?** Start with Tutorial 1 (`agentbricks/`)
   - **Want to learn programmatic coordination?** Begin with Tutorial 2 (`multiagent-genie/`) 
   - **Ready for advanced patterns?** Jump to Tutorial 3 (`multiagent-langgraph/`)

2. **Follow the Tutorial Structure**:
   - Each tutorial includes step-by-step instructions
   - Sample data and ingestion scripts are provided
   - Configuration templates guide your setup

3. **Practice and Experiment**:
   - Modify the provided examples to understand concepts
   - Try different prompts and configurations
   - Deploy your agents for hands-on experience

## What You'll Learn About

### Agent Architecture Patterns
Through these tutorials, you'll master three key patterns:

**Supervisor Coordination**
```
User Query → Supervisor → [Specialized Agents] → Supervisor → Response
```
*Learned in: Tutorial 2 (Genie) and Tutorial 1 (Agent Bricks)*

**Linear Pipeline Processing**
```
User Query → Validator → Planner → Executor → Response  
```
*Learned in: Tutorial 3 (LangGraph)*

**UI-Driven Configuration**
```
User Query → Multi-Agent Supervisor → [Configured Agents] → Response
```
*Learned in: Tutorial 1 (Agent Bricks)*

### Core Databricks Technologies
These tutorials will teach you to work with:

- **LangGraph**: Multi-agent orchestration and state management
- **Databricks Genie**: Natural language to SQL conversion  
- **Vector Search**: Document retrieval and semantic search
- **MLflow**: Model tracking, serving, and deployment
- **Unity Catalog**: Data governance and access control
- **Agent Bricks**: Databricks native agent framework (Beta)

## Tutorial Structure

```
agents/
├── agentbricks/           # Tutorial 1: No-code agent development
│   ├── README.md         # Step-by-step tutorial guide
│   ├── data/             # Sample datasets for hands-on practice
│   └── ingest-data.py    # Data preparation walkthrough
├── multiagent-genie/     # Tutorial 2: Programmatic agents with Genie
│   ├── CLAUDE.md         # Development environment setup
│   ├── OPTIMIZATION_GUIDE.md  # Advanced prompt engineering
│   ├── multiagent-genie.py    # Core tutorial implementation
│   ├── driver.py         # Interactive tutorial notebook
│   ├── configs.yaml      # Configuration examples
│   └── data/             # Tutorial datasets and instructions
└── multiagent-langgraph/ # Tutorial 3: Advanced multi-agent systems
    ├── README.md         # Tutorial overview and concepts
    ├── CLAUDE.md         # Development best practices
    ├── OPTIMIZATION_GUIDE.md  # Advanced prompt engineering
    ├── agent.py          # Advanced implementation examples
    ├── driver.py         # Hands-on exercise notebook
    ├── configs.yaml      # Advanced configuration patterns
    └── tools/            # Custom tool development examples
```

Each tutorial is self-contained with detailed instructions, sample code, and practical exercises to reinforce learning.
# Agent Development Tutorials for Databricks

This directory contains **hands-on tutorials** that teach you how to build multi-agent systems on the Databricks platform. Each tutorial demonstrates different approaches and methodologies for creating, orchestrating, and deploying intelligent agent workflows.

## Tutorial Overview

These tutorials use **financial data analysis** as a practical domain to demonstrate agent development concepts. You'll learn different patterns for multi-agent coordination, from no-code UI-driven approaches to sophisticated programmatic orchestration using popular frameworks.

| Tutorial | Framework | Approach | Learning Objectives |
|---|---|---|---|
| **agentbricks/** | Databricks Agent Bricks | UI-First | Learn multi-agent creation via Databricks UI |
| **agent-dabs/** | LangGraph + MLflow + DABs | MLOps-First | Master local development to production deployment workflow |
| **multiagent-genie/** | Genie + LangGraph | Code-First | Build SQL-focused agents with programmatic coordination |
| **multiagent-langgraph/** | LangGraph + Vector Search + UC Tools | Code-First | Create document retrieval systems with advanced orchestration |
| **multiagent-openai/** | OpenAI Response API + Genie + UC Tools | API-First | Master OpenAI-compatible agent architecture with Databricks integration |

## Tutorials

### 🧱 Tutorial 1: Agent Bricks (`agentbricks/`)

Learn to build agents using Databricks' no-code interface

**What you'll learn:**

- How to create Genie Spaces for structured data analysis
- Building Knowledge Assistants with Vector Search integration
- Configuring Multi-Agent Supervisors to coordinate between agents
- Best practices for prompt engineering in the Databricks UI

**Note**: Agent Bricks is currently in **Beta**. See the [official documentation](https://docs.databricks.com/aws/en/generative-ai/agent-bricks/multi-agent-supervisor) for latest updates.

**Prerequisites:** Basic familiarity with Databricks workspace navigation

### 🚀 Tutorial 2: Agent Development with DABs (`agent-dabs/`)

Learn the complete MLOps workflow from local development to production deployment using Databricks Asset Bundles

**What you'll learn:**

- Local agent development with Python and LangGraph
- MLflow integration for tracing, evaluation, and model management
- Databricks Asset Bundles (DABs) for Infrastructure-as-Code
- Production deployment via model serving endpoints
- End-to-end workflow: Development → Testing → Evaluation → Deployment

**Prerequisites:** Python programming, Databricks CLI, Unity Catalog access, Genie space setup

### 🤖 Tutorial 3: Multi-Agent with Genie (`langgraph-genie-deep-research/`)

Build programmatic agents that leverage Databricks Genie for SQL generation

**What you'll learn:**

- LangGraph fundamentals for multi-agent orchestration
- Integrating Databricks Genie for natural language to SQL
- Implementing supervisor patterns for agent coordination
- Parallel query execution and result synthesis
- MLflow integration for agent deployment and monitoring

**Prerequisites:** Python programming, basic SQL knowledge, familiarity with LangChain concepts

### 🔍 Tutorial 4: Advanced LangGraph System (`langgraph-knowledge-assistant/`)

Create sophisticated document retrieval and research agents

**What you'll learn:**

- Advanced LangGraph patterns for complex workflows  
- Multi-index vector search implementation
- Self-querying retrievers with metadata filtering
- Agent handoff patterns and state management
- Parallel document processing and synthesis techniques

**Prerequisites:** Intermediate Python, understanding of vector embeddings, experience with document processing

### 🤖 Tutorial 5: OpenAI-Compatible Agent (`openai-genie/`)

Build production-ready agents using the OpenAI Response API format with comprehensive Databricks integration

**What you'll learn:**

- OpenAI Response API implementation patterns for agent development
- Unity Catalog function integration as agent tools
- Databricks Genie integration for natural language data queries
- MLflow-based agent tracing and observability
- Streaming agent responses and real-time interactions
- Tool-calling architectures with automatic function execution
- Production deployment using Databricks Asset Bundles

**Prerequisites:** Python programming, familiarity with OpenAI API patterns, Unity Catalog access, Genie space setup

## Setup Requirements

All tutorials require access to:

- **Databricks Workspace** with Unity Catalog enabled
- **ML Runtime Cluster** (for vector search and model operations)
- **Databricks Model Serving** (for production deployment tutorials)
- **Vector Search Endpoints** (for document-based tutorials)
- **Appropriate workspace permissions** for creating schemas, tables, and endpoints

## Core Databricks Technologies

These tutorials will teach you to work with:

- **LangGraph**: Multi-agent orchestration and state management
- **Databricks Genie**: Natural language to SQL conversion  
- **Vector Search**: Document retrieval and semantic search
- **MLflow**: Model tracking, serving, and deployment
- **Unity Catalog**: Data governance and access control
- **Agent Bricks**: Databricks native agent framework (Beta)
- **Databricks Asset Bundles (DABs)**: Infrastructure-as-Code for ML deployments

Each tutorial is self-contained with detailed instructions, sample code, and practical exercises to reinforce learning.

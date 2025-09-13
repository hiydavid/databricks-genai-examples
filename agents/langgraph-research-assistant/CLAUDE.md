# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **multi-agent LangGraph system** built for Databricks that performs financial research and analysis. The system uses specialized agents to validate companies, plan research tasks, and retrieve & synthesize documents into final responses.

## Key Dependencies & Commands

### Installation

```bash
pip install -r requirements.txt
```

### Core Dependencies

- `mlflow[databricks]==3.1.1` - MLflow integration for Databricks
- `databricks-langchain==0.6.0` - Databricks LangChain integration
- `databricks-agents==1.1.0` - Databricks agent framework
- `langgraph==0.5.4` - Multi-agent orchestration framework

### Running the System

The system is designed to run in Databricks notebooks:

1. **Setup**: Run `driver.py` notebook which installs dependencies and sets up environment
2. **Main Agent**: The core agent logic is in `agent.py`
3. **Testing**: Use the test questions in `driver.py` cells to validate functionality

## Architecture Overview

### Streamlined 3-Agent Graph Flow

The system implements a streamlined multi-agent workflow:

1. **validator_agent**
   - Validates company existence using UC functions
   - Uses `databricks-claude-3-7-sonnet` model
   - Tools: `lookup_company_info_by_name`, `lookup_company_info_by_ticker`

2. **planner_agent**
   - Decomposes user questions into actionable research plans
   - Uses `databricks-claude-sonnet-4` model
   - Creates comprehensive search strategies across multiple data sources

3. **document_retrieval_agent** (FINAL AGENT)
   - Executes parallel searches across SEC filings and earnings transcripts
   - Uses `databricks-claude-sonnet-4` model (upgraded for synthesis capabilities)
   - Tools: `search_sec_business_section`, `search_sec_other_sections`, `search_earnings_calls`
   - **Synthesizes findings and provides final user response** (no handoff needed)

### Key Components

#### Vector Search Integration

- **Retrievers** (`agent.py:264-289`): Self-querying retrievers for SEC 10K business sections, other sections, and earnings calls
- **Indexes**: `sec_10k_business_vsindex`, `sec_10k_others_vsindex`, `earnings_call_transcripts_vsindex`
- **Embedding Model**: `databricks-gte-large-en`

#### Agent Routing (`agent.py:160-190`)

The `route_after_agent` function handles inter-agent transitions based on handoff tool calls:

- Analyzes recent messages for `transfer_to_*` tool calls
- Routes between validator → planner → document_retrieval → END
- **Simplified flow**: document_retrieval_agent directly routes to END (no supervisor handoff)
- Includes debug logging for troubleshooting

#### MLflow Integration (`agent.py:370-430`)

- `MultiAgentResearchAssistant` class wraps the LangGraph for MLflow deployment
- Implements both `predict` and `predict_stream` methods
- Handles message conversion between MLflow and LangGraph formats

## Configuration Management

### Main Configuration (`configs.yaml`)

- **databricks_configs**: Catalog, schema, experiment names
- **agent_configs**: Individual agent prompts and LLM settings
- **tool_configs**: Retriever settings, handoff definitions, UC tools

### Key Configuration Sections

- **LLM Endpoints**: Different Claude models for different agents (Sonnet 4 for planning/synthesis, 3.7 for validation)
- **Vector Search**: Endpoint names, index configurations, embedding models
- **Handoff Tools**: Simplified agent transition definitions (no supervisor handoff)
- **UC Functions**: Company lookup tool configurations

## Data Sources

The system searches across three main knowledge bases:

1. **SEC 10K Business Sections** - Company strategy, operations, competitive landscape
2. **SEC 10K Other Sections** - Risks, legal proceedings, financial policies  
3. **Earnings Call Transcripts** - Recent performance, management guidance

## Development Notes

### File Structure

- `agent.py` - Core multi-agent logic and graph definition
- `driver.py` - Driver notebook for setup, testing, and deployment
- `configs.yaml` - All agent and tool configurations
- `tools/uc-functions.py` - Unity Catalog function definitions
- `tools/vector-search-indexes.py` - Vector search index setup

### Critical Implementation Details

- **Parallel Tool Execution**: The retrieval agent calls multiple search tools simultaneously for comprehensive research
- **Unified Retrieval & Synthesis**: Document retrieval agent now handles both data gathering AND final response synthesis (eliminated supervisor agent)
- **Simplified Routing**: Streamlined graph flow eliminates handoff issues between retrieval and supervisor
- **Self-Query Retrieval**: Uses structured metadata filtering for precise document retrieval
- **MLflow Tracing**: All agent interactions are traced for monitoring and debugging

### Recent Architecture Changes

- **Merged supervisor functionality** into document_retrieval_agent for simplified workflow
- **Upgraded retrieval agent** to Claude Sonnet 4 for better synthesis capabilities
- **Eliminated handoff bugs** by making document_retrieval_agent the terminal node
- **Reduced system complexity** and improved response latency

### Testing

Use the predefined `test_questions` in `driver.py` to validate system functionality across different financial research scenarios.

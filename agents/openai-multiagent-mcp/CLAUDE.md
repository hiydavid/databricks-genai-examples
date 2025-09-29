# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Environment Setup
```bash
# Install dependencies using uv (preferred) or pip
uv pip install -r requirements.txt
# OR
pip install -r requirements.txt
```

### Databricks Bundle Deployment
```bash
# Deploy to development environment (default)
databricks bundle deploy

# Deploy to specific environment
databricks bundle deploy --target dev
```

### Configuration Setup
```bash
# Copy template files and customize
cp databricks.template.yml databricks.yml
cp src/config.template.yaml src/config.yaml
```

## Architecture Overview

This is a **multi-agent system** built on Databricks that integrates **MCP (Model Context Protocol)** tools with MLflow's ResponsesAgent framework. The system combines multiple data analysis capabilities through managed MCP servers.

### Core Architecture Components

1. **MCPToolCallingAgent**: Main agent class that extends MLflow's ResponsesAgent to work with MCP tools
2. **MCP Tool Integration**: Dynamically loads tools from both managed and custom MCP servers
3. **Multi-Modal Data Access**: Unity Catalog functions, Genie natural language querying, and vector search
4. **MLflow Integration**: Full tracing, logging, and model serving capabilities

### MCP Server Configuration

The system connects to three types of managed MCP servers:
- **Unity Catalog Functions**: `/api/2.0/mcp/functions/{schema_name}` - Python code execution
- **Vector Search**: `/api/2.0/mcp/vector-search/users/{user_name}` - Document search capabilities
- **Genie**: `/api/2.0/mcp/genie/{space_id}` - Natural language data querying

Custom MCP servers can be added via OAuth authentication using service principals.

### Configuration System

**Two-layer configuration**:
1. `databricks.yml`: Databricks bundle configuration (workspace, targets)
2. `src/config.yaml`: Agent-specific configuration (LLM endpoints, tool settings, system prompts)

Both have corresponding `.template` files for easy setup.

### Key Files

- [`src/agent.py`](src/agent.py): Main agent implementation and MCP tool integration
- [`src/config.yaml`](src/config.yaml): Agent configuration (LLM, tools, prompts)
- [`databricks.yml`](databricks.yml): Databricks bundle configuration
- [`src/driver.ipynb`](src/driver.ipynb): Driver notebook for testing and deployment

### Tool Integration Pattern

Tools are loaded dynamically using the `ToolInfo` class which wraps:
- Tool name and OpenAI-compatible specification
- Execution function that calls the appropriate MCP server
- Automatic error handling and retry logic with backoff

### Authentication & Deployment

- Uses Databricks workspace authentication for managed MCP servers
- OAuth with service principals required for custom MCP servers
- Deployed as MLflow model with automatic logging enabled
- Supports multiple deployment targets (dev/staging/prod)

### Agent Capabilities

The agent combines multiple data analysis tools:
- **Genie**: Natural language to SQL conversion and execution
- **Vector Search**: Financial document retrieval and analysis
- **Python Execution**: Advanced calculations and data processing via Unity Catalog functions
- **Streaming Responses**: Real-time tool execution with progress tracking
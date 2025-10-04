# Multi-Agent System with Databricks Managed MCP Tools

An intelligent data analysis agent built on Databricks that integrates MCP (Model Context Protocol) tools with MLflow's ResponsesAgent framework.

[Documentation](https://docs.databricks.com/aws/en/notebooks/source/generative-ai/openai-mcp-tool-calling-agent.html) | [Architecture Diagram](architecture.md)

## Features

### MCP Tool Integration

- **Genie**: Natural language to SQL conversion and execution
- **Vector Search**: Financial document retrieval with automatic filtering
- **Unity Catalog Functions**: Python code execution and web search capabilities in sandboxed environment
  - `system.ai.python_exec`: Execute Python code for advanced calculations
  - `users.first_last.search_web`: Search the web for current information not available in existing knowledge bases

### MLflow Integration

- Full request/response tracing with MLflow Traces
- Model serving deployment via Databricks Model Serving
- Automatic logging and experiment tracking

### Agent Capabilities

- Streaming responses with real-time tool execution
- Multi-turn conversations with tool chaining
- Automatic retry logic with backoff on rate limits
- Support for both managed and custom MCP servers

## Project Structure

- `src/agent.py` - Main agent implementation and MCP tool integration
- `src/config.yaml` - Agent configuration (LLM endpoint, tools, system prompt)
- `src/driver.ipynb` - Interactive testing notebook
- `databricks.yml` - Databricks bundle deployment configuration

See [CLAUDE.md](CLAUDE.md) for detailed architecture and development guidance.

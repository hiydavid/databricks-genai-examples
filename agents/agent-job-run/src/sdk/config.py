"""Configuration for OpenAI Agents SDK with Databricks."""

import mlflow
from agents import set_default_openai_api, set_default_openai_client
from agents.tracing import set_trace_processors
from databricks_openai import AsyncDatabricksOpenAI
from databricks_openai.agents import McpServer


def configure_sdk():
    """Configure OpenAI Agents SDK for Databricks endpoints.

    Call this once at startup before creating any agents.
    """
    set_default_openai_client(AsyncDatabricksOpenAI())
    set_default_openai_api("chat_completions")
    set_trace_processors([])  # Disable SDK tracing, use MLflow instead
    mlflow.openai.autolog()  # Auto-trace all LLM calls


async def init_mcp_server(host: str, connection_name: str) -> McpServer:
    """Initialize MCP server for external tools (e.g., web search).

    Args:
        host: Databricks workspace host URL
        connection_name: Name of the MCP connection in Unity Catalog

    Returns:
        McpServer instance to use with Agent

    Usage:
        async with await init_mcp_server(host, "my-connection") as mcp:
            agent = Agent(..., mcp_servers=[mcp])
    """
    return McpServer(
        url=f"{host}/api/2.0/mcp/external/{connection_name}",
        name=f"{connection_name}-mcp",
    )

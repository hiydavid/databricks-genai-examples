"""Configuration for OpenAI Agents SDK with Databricks."""

import mlflow

# Import agents SDK with fallback for different versions
try:
    from agents import set_default_openai_api, set_default_openai_client
except ImportError as e:
    # Try alternative import paths
    try:
        from openai_agents import set_default_openai_api, set_default_openai_client
    except ImportError:
        raise ImportError(
            f"Could not import from 'agents' or 'openai_agents'. "
            f"Make sure openai-agents is installed: pip install openai-agents\n"
            f"Original error: {e}"
        )

try:
    from agents.tracing import set_trace_processors
except ImportError:
    # Fallback if tracing module not available
    def set_trace_processors(processors):
        pass

from databricks_openai import AsyncDatabricksOpenAI
from databricks_openai.agents import McpServer


def configure_sdk(experiment_name: str | None = None):
    """Configure OpenAI Agents SDK for Databricks endpoints.

    Call this once at startup before creating any agents.

    Args:
        experiment_name: Optional MLflow experiment name. If provided, sets
            the tracking URI to Databricks and configures the experiment.
    """
    # Configure MLflow BEFORE autolog so traces go to the right experiment
    if experiment_name:
        mlflow.set_tracking_uri("databricks")
        mlflow.set_experiment(experiment_name)

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

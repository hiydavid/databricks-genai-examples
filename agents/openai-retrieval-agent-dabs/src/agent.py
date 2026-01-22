"""
Retrieval Agent with MCP Tool Calling

This agent uses the OpenAI Chat Completions API with Databricks MCP (Model Context Protocol)
to perform retrieval from a Vector Search index.
"""

import json
import os
from typing import Any, Callable, Generator, Optional

import backoff
import mlflow
import openai
from databricks.sdk import WorkspaceClient
from databricks_mcp import DatabricksMCPClient
from mlflow.entities import SpanType
from mlflow.pyfunc import ChatAgent
from mlflow.types.agent import (
    ChatAgentChunk,
    ChatAgentMessage,
    ChatAgentResponse,
    ChatContext,
)
from pydantic import BaseModel

# Set up MLflow
mlflow.set_registry_uri("databricks-uc")
mlflow.set_tracking_uri("databricks")

# Load configuration
config = mlflow.models.ModelConfig(development_config="./configs.yaml")
databricks_config = config.get("databricks_configs")
agent_config = config.get("agent_configs")

# Extract configuration values
WORKSPACE_URL = databricks_config.get("workspace_url")
LLM_ENDPOINT = agent_config.get("llm", {}).get("endpoint_name", "databricks-claude-sonnet-4")
TEMPERATURE = agent_config.get("llm", {}).get("temperature", 0.1)
MAX_TOKENS = agent_config.get("llm", {}).get("max_tokens", 4096)

# Vector Search MCP configuration
CATALOG = databricks_config.get("catalog")
SCHEMA = databricks_config.get("schema")
VS_MCP_URL = f"{WORKSPACE_URL.rstrip('/')}/api/2.0/mcp/vector-search/{CATALOG}/{SCHEMA}"

# System prompt for the retrieval agent
SYSTEM_PROMPT = """You are a helpful assistant that answers questions about user guides and documentation.

When answering questions:
1. Use the search_index tool to find relevant information from the documentation
2. Base your answers on the retrieved context
3. If the retrieved information doesn't contain the answer, say so clearly
4. Cite the source documents when possible
5. Be concise and accurate

Always search the documentation before answering questions about the product or system."""


class ToolInfo(BaseModel):
    """Container for tool information."""

    name: str
    spec: dict  # OpenAI-compatible tool specification
    exec_fn: Callable  # Function that executes the tool


def create_mcp_tools(ws: WorkspaceClient, mcp_server_url: str) -> list[ToolInfo]:
    """Create tools from MCP server."""
    tools = []

    try:
        mcp_client = DatabricksMCPClient(server_url=mcp_server_url, workspace_client=ws)
        mcp_tools = mcp_client.list_tools()

        for mcp_tool in mcp_tools:
            # Convert MCP tool to OpenAI tool spec
            tool_spec = {
                "type": "function",
                "function": {
                    "name": mcp_tool.name,
                    "description": mcp_tool.description,
                    "parameters": mcp_tool.inputSchema,
                },
            }

            # Create execution function with closure
            def create_exec_fn(server_url: str, tool_name: str, workspace_client: WorkspaceClient):
                def exec_fn(**kwargs) -> str:
                    client = DatabricksMCPClient(
                        server_url=server_url, workspace_client=workspace_client
                    )
                    response = client.call_tool(tool_name, kwargs)
                    return "".join([c.text for c in response.content])

                return exec_fn

            exec_fn = create_exec_fn(mcp_server_url, mcp_tool.name, ws)
            tools.append(ToolInfo(name=mcp_tool.name, spec=tool_spec, exec_fn=exec_fn))

        print(f"Loaded {len(tools)} tools from MCP server: {[t.name for t in tools]}")

    except Exception as e:
        print(f"Error loading MCP tools from {mcp_server_url}: {e}")
        raise

    return tools


class RetrievalAgent(ChatAgent):
    """
    Retrieval agent using OpenAI Chat Completions API with MCP tools.

    This agent:
    1. Receives user questions
    2. Uses Vector Search MCP tools to retrieve relevant context
    3. Generates answers based on retrieved context
    """

    def __init__(self, llm_endpoint: str, tools: list[ToolInfo]):
        self.llm_endpoint = llm_endpoint
        self.workspace_client = WorkspaceClient()

        # Get OpenAI-compatible client from Databricks SDK
        self.client = self.workspace_client.serving_endpoints.get_open_ai_client()

        # Store tools
        self._tools_dict = {tool.name: tool for tool in tools}

    def get_tool_specs(self) -> list[dict]:
        """Get OpenAI-compatible tool specifications."""
        return [tool_info.spec for tool_info in self._tools_dict.values()]

    @mlflow.trace(span_type=SpanType.TOOL)
    def execute_tool(self, tool_name: str, args: dict) -> Any:
        """Execute a tool by name with given arguments."""
        if tool_name not in self._tools_dict:
            return f"Error: Unknown tool '{tool_name}'"

        try:
            result = self._tools_dict[tool_name].exec_fn(**args)
            return result
        except Exception as e:
            return f"Error executing tool {tool_name}: {str(e)}"

    @backoff.on_exception(backoff.expo, openai.RateLimitError, max_tries=3)
    @mlflow.trace(span_type=SpanType.LLM)
    def call_llm(self, messages: list[dict[str, Any]]) -> openai.types.chat.ChatCompletion:
        """Call the LLM with messages and tools."""
        tool_specs = self.get_tool_specs() if self._tools_dict else None

        return self.client.chat.completions.create(
            model=self.llm_endpoint,
            messages=messages,
            tools=tool_specs,
            temperature=TEMPERATURE,
            max_tokens=MAX_TOKENS,
        )

    @backoff.on_exception(backoff.expo, openai.RateLimitError, max_tries=3)
    @mlflow.trace(span_type=SpanType.LLM)
    def call_llm_stream(
        self, messages: list[dict[str, Any]]
    ) -> Generator[dict[str, Any], None, None]:
        """Call the LLM with streaming."""
        tool_specs = self.get_tool_specs() if self._tools_dict else None

        for chunk in self.client.chat.completions.create(
            model=self.llm_endpoint,
            messages=messages,
            tools=tool_specs,
            temperature=TEMPERATURE,
            max_tokens=MAX_TOKENS,
            stream=True,
        ):
            yield chunk.to_dict()

    def _process_tool_calls(
        self, assistant_message: dict, messages: list[dict]
    ) -> list[dict]:
        """Process tool calls from assistant message and return tool results."""
        tool_results = []

        for tool_call in assistant_message.get("tool_calls", []):
            tool_name = tool_call["function"]["name"]
            tool_args = json.loads(tool_call["function"]["arguments"])

            print(f"Executing tool: {tool_name} with args: {tool_args}")
            result = self.execute_tool(tool_name, tool_args)

            tool_results.append(
                {
                    "role": "tool",
                    "tool_call_id": tool_call["id"],
                    "content": str(result),
                }
            )

        return tool_results

    @mlflow.trace(span_type=SpanType.AGENT)
    def predict(
        self,
        messages: list[ChatAgentMessage],
        context: Optional[ChatContext] = None,
        custom_inputs: Optional[dict[str, Any]] = None,
    ) -> ChatAgentResponse:
        """
        Generate a response for the given messages.

        This implements an agent loop that:
        1. Calls the LLM
        2. If the LLM wants to use tools, executes them
        3. Continues until the LLM provides a final response
        """
        # Convert messages to dict format
        working_messages = [{"role": "system", "content": SYSTEM_PROMPT}]
        for msg in messages:
            working_messages.append(msg.model_dump(exclude_none=True))

        max_iterations = 10
        for _ in range(max_iterations):
            # Call LLM
            response = self.call_llm(working_messages)
            assistant_message = response.choices[0].message

            # Add assistant message to history
            working_messages.append(assistant_message.model_dump(exclude_none=True))

            # Check if there are tool calls
            if assistant_message.tool_calls:
                # Execute tools and add results
                tool_results = self._process_tool_calls(
                    assistant_message.model_dump(exclude_none=True), working_messages
                )
                working_messages.extend(tool_results)
            else:
                # No tool calls, return the response
                return ChatAgentResponse(
                    messages=[
                        ChatAgentMessage(
                            role="assistant",
                            content=assistant_message.content or "",
                        )
                    ]
                )

        # If we hit max iterations, return whatever we have
        return ChatAgentResponse(
            messages=[
                ChatAgentMessage(
                    role="assistant",
                    content="I apologize, but I was unable to complete the request within the allowed iterations.",
                )
            ]
        )

    @mlflow.trace(span_type=SpanType.AGENT)
    def predict_stream(
        self,
        messages: list[ChatAgentMessage],
        context: Optional[ChatContext] = None,
        custom_inputs: Optional[dict[str, Any]] = None,
    ) -> Generator[ChatAgentChunk, None, None]:
        """
        Generate a streaming response for the given messages.
        """
        # For simplicity, use non-streaming predict and yield the result
        # A full implementation would stream the LLM response
        response = self.predict(messages, context, custom_inputs)

        for msg in response.messages:
            yield ChatAgentChunk(
                delta=ChatAgentMessage(role=msg.role, content=msg.content)
            )


# Initialize the agent
print("Initializing Retrieval Agent...")
print(f"LLM Endpoint: {LLM_ENDPOINT}")
print(f"Vector Search MCP URL: {VS_MCP_URL}")

workspace_client = WorkspaceClient()
mcp_tools = create_mcp_tools(workspace_client, VS_MCP_URL)

AGENT = RetrievalAgent(llm_endpoint=LLM_ENDPOINT, tools=mcp_tools)

# Register the agent with MLflow
mlflow.models.set_model(AGENT)

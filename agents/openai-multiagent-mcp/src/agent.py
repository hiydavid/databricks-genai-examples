import asyncio
import json
import os
import warnings
from typing import Any, Callable, Generator, List, Optional
from uuid import uuid4

import backoff
import mlflow
import nest_asyncio
import openai
from databricks.sdk import WorkspaceClient
from databricks_mcp import DatabricksMCPClient, DatabricksOAuthClientProvider
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client as connect
from mlflow.entities import SpanType
from mlflow.pyfunc import ResponsesAgent
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
)
from openai import OpenAI
from pydantic import BaseModel

nest_asyncio.apply()

mlflow.set_registry_uri("databricks-uc")
mlflow.set_tracking_uri("databricks")


############################################################
## Load variables
workspace_client = WorkspaceClient()
host = workspace_client.config.host

config = mlflow.models.ModelConfig(development_config="./config.yaml")
databricks_config = config.get("databricks")
agent_config = config.get("agent")
tools_conifg = agent_config.get("tools")

# Databricks configuration
CATALOG = databricks_config.get("catalog")
SCHEMA = databricks_config.get("schema")

# Load system prompt from MLflow Prompt Registry
PROMPT_REGISTRY = agent_config.get("system_prompt").get("prompt_registry")
SYSTEM_PROMPT = mlflow.genai.load_prompt(
    name_or_uri=f"{CATALOG}.{SCHEMA}.{PROMPT_REGISTRY["name"]}",
    version=f"{PROMPT_REGISTRY["version"]}",
).template

# Load other resources
LLM_ENDPOINT_NAME = agent_config.get("llm").get("endpoint_name")
UC_FUNCTIONS = tools_conifg.get("uc_functions")
VECTOR_SEARCH_CATALOG = tools_conifg.get("vector_search").get("catalog")
VECTOR_SEARCH_SCHEMA = tools_conifg.get("vector_search").get("schema")
GENIE_SPACE_ID = tools_conifg.get("genie").get("space_id")


############################################################
## Configure MCP Servers for your agent
MANAGED_MCP_SERVER_URLS = []

# Add UC function MCP servers
if UC_FUNCTIONS:
    for uc_func in UC_FUNCTIONS:
        schema_name = uc_func.get("schema_name")
        if schema_name:
            MANAGED_MCP_SERVER_URLS.append(
                f"{host}/api/2.0/mcp/functions/{schema_name}"
            )

# Add vector search MCP server
if VECTOR_SEARCH_CATALOG and VECTOR_SEARCH_SCHEMA:
    MANAGED_MCP_SERVER_URLS.append(
        f"{host}/api/2.0/mcp/vector-search/{VECTOR_SEARCH_CATALOG}/{VECTOR_SEARCH_SCHEMA}"
    )

# Add Genie MCP server
if GENIE_SPACE_ID:
    MANAGED_MCP_SERVER_URLS.append(f"{host}/api/2.0/mcp/genie/{GENIE_SPACE_ID}")

# ----- Advanced (optional): Custom MCP Server with OAuth -----
# For Databricks Apps hosting custom MCP servers, OAuth with a service principal is required.
# Uncomment and fill in your settings ONLY if connecting to a custom MCP server.
#
# import os
# workspace_client = WorkspaceClient(
#     host="<DATABRICKS_WORKSPACE_URL>",
#     client_id=os.getenv("DATABRICKS_CLIENT_ID"),
#     client_secret=os.getenv("DATABRICKS_CLIENT_SECRET"),
#     auth_type="oauth-m2m",   # Enables machine-to-machine OAuth
# )

# Custom MCP Servers: Add URLs below if needed (requires custom setup and OAuth above)
CUSTOM_MCP_SERVER_URLS = [
    # Example: "https://<custom-mcp-url>/mcp"
]


class ToolInfo(BaseModel):
    """
    Class representing a tool for the agent.
    - "name" (str): The name of the tool.
    - "spec" (dict): JSON description of the tool (matches OpenAI Responses format)
    - "exec_fn" (Callable): Function that implements the tool logic
    """

    name: str
    spec: dict
    exec_fn: Callable


async def _run_custom_async(
    server_url: str, tool_name: str, ws: WorkspaceClient, **kwargs
) -> str:
    """Executes a tool from a custom MCP server asynchronously using OAuth."""
    async with connect(server_url, auth=DatabricksOAuthClientProvider(ws)) as (
        read_stream,
        write_stream,
        _,
    ):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            response = await session.call_tool(tool_name, kwargs)
            return "".join([c.text for c in response.content])


async def get_custom_mcp_tools(ws: WorkspaceClient, server_url: str):
    """Retrieves the list of tools available from a custom MCP server."""
    async with connect(server_url, auth=DatabricksOAuthClientProvider(ws)) as (
        read_stream,
        write_stream,
        _,
    ):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            tools_response = await session.list_tools()
            return tools_response.tools


async def create_mcp_tools(
    ws: WorkspaceClient,
    managed_server_urls: List[str] = None,
    custom_server_urls: List[str] = None,
) -> List[ToolInfo]:
    """Aggregates all available tools from both managed and custom MCP servers into OpenAI-compatible ToolInfo objects."""
    tools = []

    if managed_server_urls:
        for server_url in managed_server_urls:
            try:
                mcp_client = DatabricksMCPClient(
                    server_url=server_url, workspace_client=ws
                )
                mcp_tools = mcp_client.list_tools()

                for mcp_tool in mcp_tools:
                    tool_spec = {
                        "type": "function",
                        "function": {
                            "name": mcp_tool.name,
                            "parameters": mcp_tool.inputSchema,
                        },
                        "description": mcp_tool.description or f"Tool: {mcp_tool.name}",
                    }

                    def create_managed_exec_fn(server_url, tool_name, ws):
                        def exec_fn(**kwargs):
                            client = DatabricksMCPClient(
                                server_url=server_url, workspace_client=ws
                            )
                            response = client.call_tool(tool_name, kwargs)
                            return "".join([c.text for c in response.content])

                        return exec_fn

                    exec_fn = create_managed_exec_fn(server_url, mcp_tool.name, ws)

                    tools.append(
                        ToolInfo(name=mcp_tool.name, spec=tool_spec, exec_fn=exec_fn)
                    )
            except Exception as e:
                print(f"Error loading tools from managed server {server_url}: {e}")

    if custom_server_urls:
        for server_url in custom_server_urls:
            try:
                mcp_tools = await get_custom_mcp_tools(ws, server_url)

                for mcp_tool in mcp_tools:
                    tool_spec = {
                        "type": "function",
                        "function": {
                            "name": mcp_tool.name,
                            "parameters": mcp_tool.inputSchema,
                        },
                        "description": mcp_tool.description or f"Tool: {mcp_tool.name}",
                    }

                    def create_custom_exec_fn(server_url, tool_name, ws):
                        def exec_fn(**kwargs):
                            return asyncio.run(
                                _run_custom_async(server_url, tool_name, ws, **kwargs)
                            )

                        return exec_fn

                    exec_fn = create_custom_exec_fn(server_url, mcp_tool.name, ws)

                    tools.append(
                        ToolInfo(name=mcp_tool.name, spec=tool_spec, exec_fn=exec_fn)
                    )
            except Exception as e:
                print(f"Error loading tools from custom server {server_url}: {e}")

    return tools


############################################################
## Setu MCP tool-calling agent
class MCPToolCallingAgent(ResponsesAgent):
    def __init__(self, llm_endpoint: str, tools: list[ToolInfo]):
        """Initializes the MCP Tool Calling Agent."""
        self.llm_endpoint = llm_endpoint
        self.workspace_client = WorkspaceClient()
        self.model_serving_client = (
            self.workspace_client.serving_endpoints.get_open_ai_client()
        )
        self._tools_dict = {tool.name: tool for tool in tools}

    def get_tool_specs(self) -> list[dict]:
        """Returns tool specifications in the format OpenAI expects."""
        return [tool_info.spec for tool_info in self._tools_dict.values()]

    @mlflow.trace(span_type=SpanType.TOOL)
    def execute_tool(self, tool_name: str, args: dict) -> Any:
        """Executes the specified tool with the given arguments."""
        return self._tools_dict[tool_name].exec_fn(**args)

    @backoff.on_exception(backoff.expo, openai.RateLimitError)
    @mlflow.trace(span_type=SpanType.LLM)
    def call_llm(
        self, messages: list[dict[str, Any]]
    ) -> Generator[dict[str, Any], None, None]:
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore", message="PydanticSerializationUnexpectedValue"
            )
            for chunk in self.model_serving_client.chat.completions.create(
                model=self.llm_endpoint,
                messages=self.prep_msgs_for_cc_llm(messages),
                tools=self.get_tool_specs(),
                stream=True,
            ):
                yield chunk.to_dict()

    def handle_tool_call(
        self, tool_call: dict[str, Any], messages: list[dict[str, Any]]
    ) -> ResponsesAgentStreamEvent:
        """
        Execute tool calls, add them to the running message history, and return a ResponsesStreamEvent w/ tool output
        """
        args = json.loads(tool_call["arguments"])
        result = str(self.execute_tool(tool_name=tool_call["name"], args=args))

        tool_call_output = self.create_function_call_output_item(
            tool_call["call_id"], result
        )
        messages.append(tool_call_output)
        return ResponsesAgentStreamEvent(
            type="response.output_item.done", item=tool_call_output
        )

    def call_and_run_tools(
        self,
        messages: list[dict[str, Any]],
        max_iter: int = 10,
    ) -> Generator[ResponsesAgentStreamEvent, None, None]:
        for _ in range(max_iter):
            last_msg = messages[-1]
            if last_msg.get("role", None) == "assistant":
                return
            elif last_msg.get("type", None) == "function_call":
                yield self.handle_tool_call(last_msg, messages)
            else:
                yield from self.output_to_responses_items_stream(
                    chunks=self.call_llm(messages), aggregator=messages
                )

        yield ResponsesAgentStreamEvent(
            type="response.output_item.done",
            item=self.create_text_output_item(
                "Max iterations reached. Stopping.", str(uuid4())
            ),
        )

    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        outputs = [
            event.item
            for event in self.predict_stream(request)
            if event.type == "response.output_item.done"
        ]
        return ResponsesAgentResponse(
            output=outputs, custom_outputs=request.custom_inputs
        )

    def predict_stream(
        self, request: ResponsesAgentRequest
    ) -> Generator[ResponsesAgentStreamEvent, None, None]:
        messages = [{"role": "system", "content": SYSTEM_PROMPT}] + [
            i.model_dump() for i in request.input
        ]

        yield from self.call_and_run_tools(messages)


############################################################
## Create MCP tools from the configured servers
mcp_tools = asyncio.run(
    create_mcp_tools(
        ws=workspace_client,
        managed_server_urls=MANAGED_MCP_SERVER_URLS,
        custom_server_urls=CUSTOM_MCP_SERVER_URLS,
    )
)


############################################################
## Log the model using MLflow
mlflow.openai.autolog()
AGENT = MCPToolCallingAgent(llm_endpoint=LLM_ENDPOINT_NAME, tools=mcp_tools)
mlflow.models.set_model(AGENT)

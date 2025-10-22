import json
import os
import warnings
from typing import Any, Callable, Generator, List
from uuid import uuid4

import backoff
import mlflow
import nest_asyncio
import openai
from databricks.sdk import WorkspaceClient
from databricks_mcp import DatabricksMCPClient
from mlflow.entities import SpanType
from mlflow.pyfunc import ResponsesAgent
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
)
from pydantic import BaseModel

nest_asyncio.apply()

mlflow.set_registry_uri("databricks-uc")
mlflow.set_tracking_uri("databricks")

############################################################
## Load variables
workspace_client = WorkspaceClient()
host = workspace_client.config.host

config = mlflow.models.ModelConfig(development_config="./config.yml")

databricks_config = config.get("databricks")
agent_config = config.get("agent")
tools_config = config.get("tools")

# Databricks configuration
CATALOG = databricks_config.get("catalog")
SCHEMA = databricks_config.get("schema")

# Agent configuration
LLM_ENDPOINT = agent_config.get("llm").get("endpoint")
MAX_ITERATIONS = agent_config.get(
    "max_iterations", 10
)  # Default to 10 if not specified

# Load system prompt from MLflow Prompt Registry
PROMPT_REGISTRY = agent_config.get("system_prompt").get("prompt_registry")
SYSTEM_PROMPT = mlflow.genai.load_prompt(
    name_or_uri=f"{CATALOG}.{SCHEMA}.{PROMPT_REGISTRY["name"]}",
    version=f"{PROMPT_REGISTRY["version"]}",
).template

# Tools configuration
UC_FUNCTIONS_MCP_SCHEMA = f"{CATALOG}/{SCHEMA}"


############################################################
## Configure MCP Server for UC Functions
MANAGED_MCP_SERVER_URL = f"{host}/api/2.0/mcp/functions/{UC_FUNCTIONS_MCP_SCHEMA}"


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


def create_mcp_tools(
    ws: WorkspaceClient,
    server_url: str,
) -> List[ToolInfo]:
    """Retrieves UC Function tools from the managed MCP server and converts them to OpenAI-compatible ToolInfo objects."""
    tools = []

    try:
        mcp_client = DatabricksMCPClient(server_url=server_url, workspace_client=ws)
        mcp_tools = mcp_client.list_tools()

        for mcp_tool in mcp_tools:
            try:
                tool_spec = {
                    "type": "function",
                    "function": {
                        "name": mcp_tool.name,
                        "parameters": mcp_tool.inputSchema,
                        "description": mcp_tool.description or f"Tool: {mcp_tool.name}",
                    },
                }

                def create_exec_fn(server_url, tool_name, ws):
                    def exec_fn(**kwargs):
                        client = DatabricksMCPClient(
                            server_url=server_url, workspace_client=ws
                        )
                        response = client.call_tool(tool_name, kwargs)
                        return "".join([c.text for c in response.content])

                    return exec_fn

                exec_fn = create_exec_fn(server_url, mcp_tool.name, ws)

                tools.append(
                    ToolInfo(name=mcp_tool.name, spec=tool_spec, exec_fn=exec_fn)
                )
            except Exception as e:
                print(f"Error loading tool '{mcp_tool.name}': {e}")
                print(f"  Tool description: {mcp_tool.description}")
                print(f"  Tool inputSchema: {mcp_tool.inputSchema}")
                continue
    except Exception as e:
        print(f"Error loading tools from MCP server {server_url}: {e}")
        import traceback

        traceback.print_exc()

    return tools


############################################################
## Setup MCP tool-calling agent
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
        # Parse arguments - handle both dict and string
        if isinstance(tool_call["arguments"], dict):
            args = tool_call["arguments"]
        else:
            try:
                args = json.loads(tool_call["arguments"])
            except json.JSONDecodeError as e:
                # Fallback: If JSON parsing fails due to concatenated objects (parallel tool calls),
                # parse just the first valid JSON object
                print(
                    f"Warning: JSONDecodeError for tool '{tool_call.get('name')}' - attempting to parse first JSON object only"
                )
                decoder = json.JSONDecoder()
                try:
                    args, idx = decoder.raw_decode(tool_call["arguments"])
                    print(
                        f"  Successfully parsed first JSON object. Ignored {len(tool_call['arguments']) - idx} chars of extra data."
                    )
                except json.JSONDecodeError:
                    print(
                        f"  Failed to parse arguments: {tool_call['arguments'][:200]}"
                    )
                    raise e

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
    ) -> Generator[ResponsesAgentStreamEvent, None, None]:
        for _ in range(MAX_ITERATIONS):
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
## Create MCP tools from UC Functions
mcp_tools = create_mcp_tools(
    ws=workspace_client,
    server_url=MANAGED_MCP_SERVER_URL,
)


############################################################
## Log the model using MLflow
mlflow.openai.autolog()
AGENT = MCPToolCallingAgent(llm_endpoint=LLM_ENDPOINT, tools=mcp_tools)
mlflow.models.set_model(AGENT)

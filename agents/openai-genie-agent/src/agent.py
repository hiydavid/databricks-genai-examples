# Databricks notebook source
import json
import time
from pathlib import Path
from typing import Any, Callable, Generator, Optional
from uuid import uuid4

import backoff
import mlflow
import openai
import yaml
from databricks.sdk import WorkspaceClient
from databricks_ai_bridge.genie import Genie
from databricks_openai import UCFunctionToolkit, VectorSearchRetrieverTool
from mlflow.entities import SpanType
from mlflow.pyfunc import ResponsesAgent
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
)
from openai import OpenAI
from pydantic import BaseModel
from unitycatalog.ai.core.base import get_uc_function_client

# Load configuration from config.yaml
config = mlflow.models.ModelConfig(development_config="./config.yaml")
agent_config = config.get("agent")
LLM_ENDPOINT_NAME = agent_config.get("llm").get("endpoint_name")
SYSTEM_PROMPT = agent_config.get("system").get("prompt")
UC_TOOL_NAMES = agent_config.get("tools").get("uc_tool_names")
GENIE_SPACE_ID = agent_config.get("tools").get("genie", {}).get("space_id")


## Define tools for your agent, enabling it to retrieve data or take actions beyond text generation
## To create and see usage examples of more tools, see
## https://docs.databricks.com/en/generative-ai/agent-framework/agent-tool.html
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


def create_tool_info(tool_spec, exec_fn_param: Optional[Callable] = None):
    """
    Factory function to create ToolInfo objects from a given tool spec
    and (optionally) a custom execution function.
    """
    # Remove 'strict' property, as Claude models do not support it in tool specs.
    tool_spec["function"].pop("strict", None)
    tool_name = tool_spec["function"]["name"]
    # Converts tool name with double underscores to UDF dot notation.
    udf_name = tool_name.replace("__", ".")

    # Define a wrapper that accepts kwargs for the UC tool call,
    # then passes them to the UC tool execution client
    def exec_fn(**kwargs):
        function_result = uc_function_client.execute_function(udf_name, kwargs)
        # Return error message if execution fails, result value if not.
        if function_result.error is not None:
            return function_result.error
        else:
            return function_result.value

    return ToolInfo(name=tool_name, spec=tool_spec, exec_fn=exec_fn_param or exec_fn)


def create_genie_tool(space_id: str):
    """
    Create a Genie query tool for natural language data analysis.
    """
    tool_spec = {
        "type": "function",
        "function": {
            "name": "query_data_with_genie",
            "description": (
                "Query and analyze data using natural language. "
                "Use this tool when you need to retrieve, analyze, or explore data "
                "from the data warehouse. Genie can answer questions about data trends, "
                "generate insights, create visualizations, and perform complex data "
                "analysis using plain English queries."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": (
                            "The natural language question or query about the data. "
                            "Be specific and clear about what data or insights you're looking for."
                        ),
                    }
                },
                "required": ["query"],
            },
        },
    }

    def exec_genie_query(query: str) -> str:
        """Execute a Genie query and return the results."""
        try:
            # Initialize Genie client
            genie_client = Genie(space_id=space_id)

            # Ask the question using the simplified API
            response = genie_client.ask_question(query)

            # Format the response
            results = []

            # Add the main result (could be string or DataFrame)
            if hasattr(response, "result") and response.result is not None:
                if hasattr(response.result, "to_string"):  # DataFrame
                    results.append(f"Data Result:\n{response.result.to_string()}")
                else:  # String or other
                    results.append(f"Response: {response.result}")

            # Add the generated SQL query if available
            if hasattr(response, "query") and response.query:
                results.append(f"Generated SQL: {response.query}")

            # Add description if available
            if hasattr(response, "description") and response.description:
                results.append(f"Description: {response.description}")

            if results:
                return "\n\n".join(results)
            else:
                return "Genie query completed but returned no results."

        except Exception as e:
            return f"Error executing Genie query: {str(e)}"

    return ToolInfo(
        name="query_data_with_genie", spec=tool_spec, exec_fn=exec_genie_query
    )


# List to store information about all tools available to the agent.
TOOL_INFOS = []

# UDFs in Unity Catalog can be exposed as agent tools.
uc_function_client = get_uc_function_client()
uc_toolkit = UCFunctionToolkit(function_names=UC_TOOL_NAMES)
for tool_spec in uc_toolkit.tools:
    TOOL_INFOS.append(create_tool_info(tool_spec))

# Add Genie tool if space_id is configured
if GENIE_SPACE_ID:
    genie_tool = create_genie_tool(GENIE_SPACE_ID)
    TOOL_INFOS.append(genie_tool)

# List to store vector search tool instances for unstructured retrieval.
VECTOR_SEARCH_TOOLS = []

# To add vector search retriever tools,
# use VectorSearchRetrieverTool and create_tool_info,
# then append the result to TOOL_INFOS.
# Example:
# VECTOR_SEARCH_TOOLS.append(
#     VectorSearchRetrieverTool(
#         index_name="",
#         # filters="..."
#     )
# )

for vs_tool in VECTOR_SEARCH_TOOLS:
    TOOL_INFOS.append(create_tool_info(vs_tool.tool, vs_tool.execute))


class MultiAgent(ResponsesAgent):
    """
    Class representing a tool-calling Agent.
    Handles both tool execution via exec_fn and LLM interactions via model serving.
    """

    def __init__(self, llm_endpoint: str, tools: list[ToolInfo]):
        """Initializes the MultiAgent with tools."""
        self.llm_endpoint = llm_endpoint
        self.workspace_client = WorkspaceClient()
        self.model_serving_client: OpenAI = (
            self.workspace_client.serving_endpoints.get_open_ai_client()
        )
        # Internal message list holds conversation state in completion-message format
        self.messages: list[dict[str, Any]] = None
        self._tools_dict = {tool.name: tool for tool in tools}

    def get_tool_specs(self) -> list[dict]:
        """Returns tool specifications in the format OpenAI expects."""
        return [tool_info.spec for tool_info in self._tools_dict.values()]

    @mlflow.trace(span_type=SpanType.TOOL)
    def execute_tool(self, tool_name: str, args: dict) -> Any:
        """Executes the specified tool with the given arguments."""
        return self._tools_dict[tool_name].exec_fn(**args)

    def _responses_to_cc(self, message: dict[str, Any]) -> list[dict[str, Any]]:
        """Convert from a Responses API output item to a list of ChatCompletion messages."""
        msg_type = message.get("type")
        if msg_type == "function_call":
            return [
                {
                    "role": "assistant",
                    "content": "tool call",  # empty content is not supported by claude models
                    "tool_calls": [
                        {
                            "id": message["call_id"],
                            "type": "function",
                            "function": {
                                "arguments": message["arguments"],
                                "name": message["name"],
                            },
                        }
                    ],
                }
            ]
        elif msg_type == "message" and isinstance(message.get("content"), list):
            return [
                {"role": message["role"], "content": content["text"]}
                for content in message["content"]
            ]
        elif msg_type == "reasoning":
            return [{"role": "assistant", "content": json.dumps(message["summary"])}]
        elif msg_type == "function_call_output":
            return [
                {
                    "role": "tool",
                    "content": message["output"],
                    "tool_call_id": message["call_id"],
                }
            ]
        compatible_keys = ["role", "content", "name", "tool_calls", "tool_call_id"]
        filtered = {k: v for k, v in message.items() if k in compatible_keys}
        return [filtered] if filtered else []

    def prep_msgs_for_llm(self, messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Filter out message fields that are not compatible with LLM message formats and convert from Responses API to ChatCompletion compatible"""
        chat_msgs = []
        for msg in messages:
            chat_msgs.extend(self._responses_to_cc(msg))
        return chat_msgs

    @backoff.on_exception(backoff.expo, openai.RateLimitError)
    def call_llm(self) -> Generator[dict[str, Any], None, None]:
        # Using OpenAI ChatCompletion API since Databricks hosted models are not compatible with the OpenAI Response API yet
        for chunk in self.model_serving_client.chat.completions.create(
            model=self.llm_endpoint,
            messages=self.prep_msgs_for_llm(self.messages),
            tools=self.get_tool_specs(),
            stream=True,
        ):
            yield chunk.to_dict()

    def handle_tool_calls(
        self, tool_calls: list[dict[str, Any]]
    ) -> Generator[ResponsesAgentStreamEvent, None, None]:
        """
        Execute tool calls, add them to the running message history, and return a ResponsesStreamEvent w/ tool output
        """
        for tool_call in tool_calls:
            function = tool_call["function"]
            args = json.loads(function["arguments"])
            # Cast tool result to a string, since not all tools return as tring
            result = str(self.execute_tool(tool_name=function["name"], args=args))
            self.messages.append(
                {"role": "tool", "content": result, "tool_call_id": tool_call["id"]}
            )
            yield ResponsesAgentStreamEvent(
                type="response.output_item.done",
                item=self.create_function_call_output_item(
                    tool_call["id"],
                    result,
                ),
            )

    def call_and_run_tools(
        self,
        max_iter: int = 10,
    ) -> Generator[ResponsesAgentStreamEvent, None, None]:
        for _ in range(max_iter):
            last_msg = self.messages[-1]
            if tool_calls := last_msg.get("tool_calls", None):
                yield from self.handle_tool_calls(tool_calls)
            elif last_msg.get("role", None) == "assistant":
                return
            else:
                # aggregate the chat completions stream to add to internal state
                llm_content = ""
                tool_calls = []
                msg_id = None
                for chunk in self.call_llm():
                    delta = chunk["choices"][0]["delta"]
                    msg_id = chunk.get("id", None)
                    content = delta.get("content", None)
                    if tc := delta.get("tool_calls"):
                        if (
                            not tool_calls
                        ):  # only accomodate for single tool call right now
                            tool_calls = tc
                        else:
                            tool_calls[0]["function"]["arguments"] += tc[0]["function"][
                                "arguments"
                            ]
                    elif content is not None:
                        llm_content += content
                        yield ResponsesAgentStreamEvent(
                            **self.create_text_delta(content, item_id=msg_id)
                        )
                llm_output = {
                    "role": "assistant",
                    "content": llm_content,
                    "tool_calls": tool_calls,
                }
                self.messages.append(llm_output)

                # yield an `output_item.done` `output_text` event that aggregates the stream
                # which enables tracing and payload logging
                if llm_output["content"]:
                    yield ResponsesAgentStreamEvent(
                        type="response.output_item.done",
                        item=self.create_text_output_item(
                            llm_output["content"], msg_id
                        ),
                    )
                # yield an `output_item.done` `function_call` event for each tool call
                if tool_calls := llm_output.get("tool_calls", None):
                    for tool_call in tool_calls:
                        yield ResponsesAgentStreamEvent(
                            type="response.output_item.done",
                            item=self.create_function_call_item(
                                str(uuid4()),
                                tool_call["id"],
                                tool_call["function"]["name"],
                                tool_call["function"]["arguments"],
                            ),
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
        self.messages = [{"role": "system", "content": SYSTEM_PROMPT}] + [
            i.model_dump() for i in request.input
        ]
        yield from self.call_and_run_tools()


# Log the model using MLflow
mlflow.openai.autolog()
AGENT = MultiAgent(llm_endpoint=LLM_ENDPOINT_NAME, tools=TOOL_INFOS)
mlflow.models.set_model(AGENT)

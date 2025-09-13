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
GENIE_SPACE_ID = agent_config.get("tools").get("genie").get("space_id")
VECTOR_SEARCH_CONFIGS = agent_config.get("tools").get("vector_search")
PARSING_PROMPT = (
    VECTOR_SEARCH_CONFIGS.get("parsing_prompt") if VECTOR_SEARCH_CONFIGS else None
)


# Define tools for your agent, enabling it to retrieve data or take actions beyond text generation
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


def create_self_query_retriever_tool(
    llm_endpoint: str,
    tool_name: str,
    tool_description: str,
    endpoint_name: str,
    index_name: str,
    vs_schema: dict,
):
    """
    Create a self-querying retriever tool that can dynamically construct filters
    based on natural language queries without using LangChain.
    """
    tool_spec = {
        "type": "function",
        "function": {
            "name": tool_name,
            "description": tool_description,
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": (
                            "The natural language question or search query. "
                            "Include specific company names, tickers, or years when relevant. "
                            "Examples: 'Netflix market share growth 2023', "
                            "'What are Tesla's main challenges in 2024?'"
                        ),
                    }
                },
                "required": ["query"],
            },
        },
    }

    def parse_query_and_filters(query: str) -> dict:
        """Use the LLM to parse natural language query into search terms and filters."""

        try:
            workspace_client = WorkspaceClient()
            model_client = workspace_client.serving_endpoints.get_open_ai_client()

            response = model_client.chat.completions.create(
                model=llm_endpoint,
                messages=[
                    {"role": "system", "content": PARSING_PROMPT},
                    {"role": "user", "content": f"Parse this query: {query}"},
                ],
                temperature=0.1,
                max_tokens=500,
            )

            llm_response = response.choices[0].message.content

            # Extract JSON from markdown code blocks if present
            if "```json" in llm_response:
                json_start = llm_response.find("```json") + 7
                json_end = llm_response.find("```", json_start)
                json_content = llm_response[json_start:json_end].strip()
            elif "```" in llm_response:
                json_start = llm_response.find("```") + 3
                json_end = llm_response.find("```", json_start)
                json_content = llm_response[json_start:json_end].strip()
            else:
                json_content = llm_response.strip()

            result = json.loads(json_content)
            return result
        except Exception as e:
            print(f"DEBUG: LLM parsing failed: {e}")
            return {"search_query": query, "filters": {}}

    def construct_vector_search_filter(filters: dict) -> dict:
        """Convert filters dict to Databricks Vector Search filter format."""
        if not filters:
            return {}

        # Get available filter fields from the schema
        available_fields = set(vs_schema.keys())

        filter_dict = {}
        for key, value in filters.items():
            # Only use fields that exist in the schema
            if key in available_fields:
                if isinstance(value, list):
                    if key == "year":
                        # Convert years to integers
                        filter_dict[key] = [int(v) for v in value]
                    else:
                        # Keep other fields as strings
                        filter_dict[key] = [str(v) for v in value]
                else:
                    # Single value
                    if key == "year":
                        filter_dict[key] = int(value)
                    else:
                        filter_dict[key] = str(value)

        return filter_dict

    def exec_self_query_search(query: str) -> str:
        """Execute self-querying retrieval with dynamic filters."""
        try:
            # Parse the query to extract search terms and filters
            parsed = parse_query_and_filters(query)
            search_query = parsed.get("search_query", query)
            filters = parsed.get("filters", {})

            # Construct the filter for vector search
            filter_dict = construct_vector_search_filter(filters)

            # Create vector search tool with dynamic filters
            workspace_client = WorkspaceClient()
            vs_tool = VectorSearchRetrieverTool(
                index_name=index_name,
                tool_name="temp_search_tool",
                tool_description="Temporary search tool",
                filters=filter_dict if filter_dict else None,
                num_results=3,
                query_type="HYBRID",
                workspace_client=workspace_client,
            )

            # Execute the search
            search_results = vs_tool.execute(query=search_query)

            # Format results with filter information
            result_parts = []
            if filter_dict:
                result_parts.append(f"Applied filters: {filter_dict}")
            result_parts.append(f"Search query: {search_query}")
            result_parts.append(f"Results:\n{search_results}")

            return "\n\n".join(result_parts)

        except Exception as e:
            return f"Error executing self-query search: {str(e)}"

    return ToolInfo(name=tool_name, spec=tool_spec, exec_fn=exec_self_query_search)


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

# Add Vector Search tool if configured
if VECTOR_SEARCH_CONFIGS:
    vector_search_tool = create_self_query_retriever_tool(
        llm_endpoint=LLM_ENDPOINT_NAME,
        tool_name=VECTOR_SEARCH_CONFIGS.get("tool_name"),
        tool_description=VECTOR_SEARCH_CONFIGS.get("tool_description"),
        endpoint_name=VECTOR_SEARCH_CONFIGS.get("endpoint_name"),
        index_name=VECTOR_SEARCH_CONFIGS.get("index_name"),
        vs_schema=VECTOR_SEARCH_CONFIGS.get("vs_schema", {}),
    )
    TOOL_INFOS.append(vector_search_tool)


# Wrap agent in MLflow
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

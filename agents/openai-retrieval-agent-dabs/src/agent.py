"""
Retrieval Agent with MCP Tool Calling

This agent uses the OpenAI Chat Completions API with Databricks MCP (Model Context Protocol)
to perform retrieval from a Vector Search index.
"""

import json
import uuid
from typing import Any, Generator, Optional

import backoff
import mlflow
import nest_asyncio

# Allow nested event loops (needed when running inside MLflow model validation)
nest_asyncio.apply()
import openai
from databricks.sdk import WorkspaceClient
from databricks_openai import McpServerToolkit
from mlflow.entities import Document, SpanType
from mlflow.pyfunc import ChatAgent
from mlflow.types.agent import (
    ChatAgentChunk,
    ChatAgentMessage,
    ChatAgentResponse,
    ChatContext,
)

# Set up MLflow
mlflow.set_registry_uri("databricks-uc")
mlflow.set_tracking_uri("databricks")

# Load configuration
config = mlflow.models.ModelConfig(development_config="./configs.yaml")
databricks_config = config.get("databricks_configs")
agent_config = config.get("agent_configs")

# Extract configuration values
LLM_ENDPOINT = agent_config.get("llm", {}).get("endpoint_name", "databricks-claude-sonnet-4")
TEMPERATURE = agent_config.get("llm", {}).get("temperature", 0.1)
MAX_TOKENS = agent_config.get("llm", {}).get("max_tokens", 4096)
SYSTEM_PROMPT = agent_config.get("system_prompt", "You are a helpful assistant.")

# Vector Search configuration
CATALOG = databricks_config.get("catalog")
SCHEMA = databricks_config.get("schema")


class RetrievalAgent(ChatAgent):
    """Retrieval agent using OpenAI Chat Completions API with MCP tools."""

    def __init__(self, llm_endpoint: str, mcp_servers: list[McpServerToolkit]):
        self.llm_endpoint = llm_endpoint
        self.workspace_client = WorkspaceClient()
        self.client = self.workspace_client.serving_endpoints.get_open_ai_client()

        # Collect tools from MCP servers
        self._tools_dict = {}
        for mcp_server in mcp_servers:
            for tool in mcp_server.get_tools():
                self._tools_dict[tool.name] = tool

        print(f"Loaded tools: {list(self._tools_dict.keys())}")

    def get_tool_specs(self) -> list[dict]:
        """Get OpenAI-compatible tool specifications."""
        return [tool.spec for tool in self._tools_dict.values()]

    @mlflow.trace(span_type="RETRIEVER")
    def execute_tool(self, tool_name: str, args: dict) -> list[Document]:
        """Execute a tool and return Documents for MLflow retrieval scorers."""
        if tool_name not in self._tools_dict:
            return [Document(page_content=f"Error: Unknown tool '{tool_name}'", metadata={"source": tool_name})]

        try:
            tool = self._tools_dict[tool_name]
            result = tool.execute(**args)
            return [Document(page_content=str(result), metadata={"source": tool_name})]
        except Exception as e:
            return [Document(page_content=f"Error: {str(e)}", metadata={"source": tool_name})]

    @backoff.on_exception(backoff.expo, openai.RateLimitError, max_tries=3)
    @mlflow.trace(span_type=SpanType.LLM)
    def call_llm(self, messages: list[dict[str, Any]]) -> openai.types.chat.ChatCompletion:
        """Call the LLM with messages and tools."""
        return self.client.chat.completions.create(
            model=self.llm_endpoint,
            messages=messages,
            tools=self.get_tool_specs() if self._tools_dict else None,
            temperature=TEMPERATURE,
            max_tokens=MAX_TOKENS,
        )

    def _process_tool_calls(self, assistant_message: dict) -> list[dict]:
        """Process tool calls and return results."""
        tool_results = []
        for tool_call in assistant_message.get("tool_calls", []):
            tool_name = tool_call["function"]["name"]
            tool_args = json.loads(tool_call["function"]["arguments"])
            print(f"Executing tool: {tool_name} with args: {tool_args}")
            docs = self.execute_tool(tool_name, tool_args)
            # Extract text from Documents for LLM
            content = "\n\n".join([doc.page_content for doc in docs])
            tool_results.append({
                "role": "tool",
                "tool_call_id": tool_call["id"],
                "content": content,
            })
        return tool_results

    @mlflow.trace(span_type=SpanType.AGENT)
    def predict(
        self,
        messages: list[ChatAgentMessage],
        context: Optional[ChatContext] = None,
        custom_inputs: Optional[dict[str, Any]] = None,
    ) -> ChatAgentResponse:
        """Generate a response for the given messages."""
        working_messages = [{"role": "system", "content": SYSTEM_PROMPT}]
        for msg in messages:
            working_messages.append(msg.model_dump(exclude_none=True))

        for _ in range(10):  # max iterations
            response = self.call_llm(working_messages)
            assistant_message = response.choices[0].message
            working_messages.append(assistant_message.model_dump(exclude_none=True))

            if assistant_message.tool_calls:
                tool_results = self._process_tool_calls(
                    assistant_message.model_dump(exclude_none=True)
                )
                working_messages.extend(tool_results)
            else:
                return ChatAgentResponse(
                    messages=[
                        ChatAgentMessage(
                            id=str(uuid.uuid4()),
                            role="assistant",
                            content=assistant_message.content or "",
                        )
                    ]
                )

        return ChatAgentResponse(
            messages=[
                ChatAgentMessage(
                    id=str(uuid.uuid4()),
                    role="assistant",
                    content="Unable to complete request within allowed iterations.",
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
        """Generate a streaming response."""
        response = self.predict(messages, context, custom_inputs)
        for msg in response.messages:
            yield ChatAgentChunk(
                delta=ChatAgentMessage(id=msg.id, role=msg.role, content=msg.content)
            )


# Initialize the agent
print("Initializing Retrieval Agent...")
print(f"LLM Endpoint: {LLM_ENDPOINT}")
print(f"Vector Search: {CATALOG}.{SCHEMA}")

mcp_servers = [
    McpServerToolkit.from_vector_search(catalog=CATALOG, schema=SCHEMA),
]

AGENT = RetrievalAgent(llm_endpoint=LLM_ENDPOINT, mcp_servers=mcp_servers)
mlflow.models.set_model(AGENT)

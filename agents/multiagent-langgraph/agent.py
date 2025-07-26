# Standard library imports
from typing import Any, Dict, Generator, List, Optional
import os

# Databricks imports
from databricks_langchain import (
    ChatDatabricks,
    UCFunctionToolkit,
    DatabricksVectorSearch,
)

# MLflow imports
import mlflow
from mlflow.pyfunc import ChatAgent
from mlflow.entities import SpanType
from mlflow.types.agent import (
    ChatAgentChunk,
    ChatAgentMessage,
    ChatAgentResponse,
    ChatContext,
)

# LangChain core imports
from langchain_core.tools import tool

# LangChain community imports
from langchain.chains.query_constructor.schema import AttributeInfo
from langchain.retrievers.self_query.base import SelfQueryRetriever
from langchain_community.query_constructors.databricks_vector_search import (
    DatabricksVectorSearchTranslator,
)
from langchain.chains.query_constructor.base import (
    load_query_constructor_runnable,
)

# LangGraph imports
from langgraph.prebuilt import create_react_agent
from langgraph.graph import StateGraph, START, MessagesState, END


# Create util functions
def create_handoff_tool(*, agent_name: str, description: str | None = None):
    """
    Create a handoff tool for transferring control between agents in a LangGraph.

    Args:
        agent_name: Name of the agent to transfer control to
        description: Optional description of when to use this tool

    Returns:
        LangChain tool that can transfer control to the specified agent
    """
    name = f"transfer_to_{agent_name}"
    description = description or f"Ask {agent_name} for help."

    @tool(name, description=description)
    def handoff_tool() -> str:
        return f"Successfully transferred to {agent_name}"

    return handoff_tool


def load_self_querying_retriever(
    model, endpoint_name: str, index_name: str, vs_schema: dict
):
    """
    Load a self-querying retriever for financial documents.

    Args:
        model: LLM model to use for query construction 
        endpoint_name: Vector search endpoint name
        index_name: Vector search index name
        vs_schema: Vector search schema configuration

    Returns:
        SelfQueryRetriever: Configured self-querying retriever
    """
    # Define metadata fields for filtering
    metadata_field_info = [
        AttributeInfo(
            name="name",
            description="The name of the company being analyzed.",
            type="string",
        ),
        AttributeInfo(
            name="ticker",
            description="The ticker of the company being analyzed.",
            type="string",
        ),
        AttributeInfo(
            name="year",
            description="Year of the document REPRESENTED AS A STRING",
            type="string",
        ),
    ]

    # Define example queries for few-shot learning
    examples = [
        (
            "Is Netflix's market share growth accelerating or decelerator?",
            {
                "query": "Netflix 2023 market share growth drivers or inhibitors",
                "filter": 'and(eq("ticker", "NFLX"), eq("year", "2023"))',
            },
        ),
        (
            "What was the key agenda of the AMCOR's business strategy in FY 2024?",
            {
                "query": "2024 Amcor 8k key agenda",
                "filter": 'and(eq("ticker", "AMCR"), eq("year", 2024))',
            },
        ),
        (
            "What challenges or opportunities are Walmart executives expecting from Trump's tariffs?",
            {
                "query": "Latest expected tariff impact on Walmart 2025",
                "filter": 'and(eq("ticker", "WMT"), eq("year", 2025))',
            },
        ),
    ]

    # Create vector store
    vector_store = DatabricksVectorSearch(
        endpoint=endpoint_name,
        index_name=index_name,
        columns=[
            vs_schema.get("primary_key"),
            vs_schema.get("text"),
            vs_schema.get("source"),
            vs_schema.get("date"),
            vs_schema.get("year"),
            vs_schema.get("name"),
            vs_schema.get("ticker"),
            vs_schema.get("sedol"),
        ],
    )

    # Build query constructor
    doc_contents = "Excerpts from the knowledge bases pertaining to companies in the context of investment research."
    query_constructor = load_query_constructor_runnable(
        model, doc_contents, metadata_field_info, examples=examples, fix_invalid=True
    )

    # Create self-query retriever
    self_query_retriever = SelfQueryRetriever(
        query_constructor=query_constructor,
        vectorstore=vector_store,
        structured_query_translator=DatabricksVectorSearchTranslator(),
        search_kwargs={"k": 3, "query_type": "hybrid"},
    )

    return self_query_retriever


def create_named_retrieval_tools(retriever, tool_name, tool_description):
    named_tool = retriever.as_tool()
    named_tool.name = tool_name
    named_tool.description = tool_description
    return named_tool


def route_after_agent(state):
    """Route to the next agent based on handoff tool messages"""
    # Look through the last few messages to find handoff tool calls or tool messages
    messages = state["messages"]
    
    for message in reversed(messages[-5:]):  # Check last 5 messages
        # Check for AI messages with handoff tool calls
        if hasattr(message, 'tool_calls') and message.tool_calls:
            for tool_call in message.tool_calls:
                if tool_call["name"] == "transfer_to_planner_agent":
                    print("DEBUG: Routing to planner_agent via tool_call")
                    return "planner_agent"
                elif tool_call["name"] == "transfer_to_document_retrieval_agent":
                    print("DEBUG: Routing to document_retrieval_agent via tool_call")
                    return "document_retrieval_agent" 
                elif tool_call["name"] == "transfer_to_supervisor_agent":
                    print("DEBUG: Routing to supervisor_agent via tool_call")
                    return "supervisor_agent"
        
        # Also check for ToolMessages from handoff tools
        if hasattr(message, 'name') and hasattr(message, 'role') and message.role == "tool":
            if message.name == "transfer_to_planner_agent":
                print("DEBUG: Routing to planner_agent via tool_message")
                return "planner_agent"
            elif message.name == "transfer_to_document_retrieval_agent":
                print("DEBUG: Routing to document_retrieval_agent via tool_message")
                return "document_retrieval_agent"
            elif message.name == "transfer_to_supervisor_agent": 
                print("DEBUG: Routing to supervisor_agent via tool_message")
                return "supervisor_agent"
    
    print("DEBUG: No handoff detected, routing to END")            
    return END


# Set MLflow auto-logging
mlflow.set_tracking_uri("databricks")
mlflow.langchain.autolog()


# Load main configs
configs = mlflow.models.ModelConfig(development_config="./configs.yaml")

databricks_configs = configs.get("databricks_configs")
agent_configs = configs.get("agent_configs")
tool_configs = configs.get("tool_configs")


# Load agent configs
# Get UC configs
catalog = databricks_configs.get("catalog")
schema = databricks_configs.get("schema")

# Get retriever configs
retriever_configs = tool_configs.get("retrievers")
vector_search_endpoint = retriever_configs.get("endpoint_name")
vector_search_schema = retriever_configs.get("schema")
vector_search_indexes = retriever_configs.get("indexes")

# Get handoff configs
handoff_configs = tool_configs.get("handoffs")

# Get UC tool configs
uc_tool_configs = tool_configs.get("uc_tools")

# Get all agent configs
validator_agent_configs = agent_configs.get("validator_agent")
planning_agent_configs = agent_configs.get("planning_agent")
retrieval_agent_configs = agent_configs.get("retrieval_agent")
supervisor_agent_configs = agent_configs.get("supervisor_agent")


# Set models for agents
validator_agent_model = ChatDatabricks(
    endpoint=validator_agent_configs.get("llm").get("llm_endpoint_name"),
    extra_params=validator_agent_configs.get("llm").get("llm_parameters"),
)

planning_agent_model = ChatDatabricks(
    endpoint=planning_agent_configs.get("llm").get("llm_endpoint_name"),
    extra_params=planning_agent_configs.get("llm").get("llm_parameters"),
)

retrieval_agent_model = ChatDatabricks(
    endpoint=retrieval_agent_configs.get("llm").get("llm_endpoint_name"),
    extra_params=retrieval_agent_configs.get("llm").get("llm_parameters"),
)

supervisor_agent_model = ChatDatabricks(
    endpoint=supervisor_agent_configs.get("llm").get("llm_endpoint_name"),
    extra_params=supervisor_agent_configs.get("llm").get("llm_parameters"),
)


# Create handoff tools
assign_to_planner = create_handoff_tool(
    agent_name=handoff_configs.get("to_planner").get("name"),
    description=handoff_configs.get("to_planner").get("description")
)

assign_to_supervisor = create_handoff_tool(
    agent_name=handoff_configs.get("to_supervisor").get("name"),
    description=handoff_configs.get("to_supervisor").get("description")
)

assign_to_retriever = create_handoff_tool(
    agent_name=handoff_configs.get("to_retriever").get("name"),
    description=handoff_configs.get("to_retriever").get("description")
)

planner_agent_tools = [assign_to_retriever]
supervisor_agent_tools = []


# Create validation tools
validator_agent_tool_names = [
    f"{catalog}.{schema}.{uc_tool_configs.get("validator").get("by_name")}",
    f"{catalog}.{schema}.{uc_tool_configs.get("validator").get("by_ticker")}",
]
validator_agent_toolkit = UCFunctionToolkit(function_names=validator_agent_tool_names)
validator_agent_tools = validator_agent_toolkit.tools
validator_agent_tools.append(assign_to_planner)


# Create retrievers
sec_10k_business_retriever = load_self_querying_retriever(
    model=retrieval_agent_model,
    endpoint_name=retriever_configs.get("endpoint_name"),
    index_name=(
        f"{catalog}.{schema}.{vector_search_indexes.get("sec_10k_business").get("index_name")}"
    ),
    vs_schema=vector_search_schema,
)

sec_10k_others_retriever = load_self_querying_retriever(
    model=retrieval_agent_model,
    endpoint_name=retriever_configs.get("endpoint_name"),
    index_name=(
        f"{catalog}.{schema}.{vector_search_indexes.get("sec_10k_others").get("index_name")}"
    ),
    vs_schema=vector_search_schema,
)

earnings_call_retriever = load_self_querying_retriever(
    model=retrieval_agent_model,
    endpoint_name=retriever_configs.get("endpoint_name"),
    index_name=(
        f"{catalog}.{schema}.{vector_search_indexes.get("earnings_call").get("index_name")}"
    ),
    vs_schema=vector_search_schema,
)

# Create retriever tools
sec_business_tool = create_named_retrieval_tools(
    retriever=sec_10k_business_retriever,
    tool_name=vector_search_indexes.get("sec_10k_business").get("tool_name"),
    tool_description=vector_search_indexes.get("sec_10k_business").get("tool_description")
)

sec_others_tool = create_named_retrieval_tools(
    retriever=sec_10k_others_retriever,
    tool_name=vector_search_indexes.get("sec_10k_others").get("tool_name"),
    tool_description=vector_search_indexes.get("sec_10k_others").get("tool_description")
)

earnings_tool = create_named_retrieval_tools(
    retriever=earnings_call_retriever,
    tool_name=vector_search_indexes.get("earnings_call").get("tool_name"),
    tool_description=vector_search_indexes.get("earnings_call").get("tool_description")
)

document_retrieval_agent_tools = [
    sec_business_tool,
    sec_others_tool,
    earnings_tool,
    assign_to_supervisor,
]


# Create agents
validator_agent = create_react_agent(
    model=validator_agent_model,
    tools=validator_agent_tools,
    prompt=validator_agent_configs.get("prompt"),
    name="validator_agent",
)

planner_agent = create_react_agent(
    model=planning_agent_model,
    tools=planner_agent_tools,
    prompt=planning_agent_configs.get("prompt"),
    name="planner_agent",
)

document_retrieval_agent = create_react_agent(
    model=retrieval_agent_model,
    tools=document_retrieval_agent_tools,
    prompt=retrieval_agent_configs.get("prompt"),
    name="document_retrieval_agent",
)

supervisor_agent = create_react_agent(
    model=supervisor_agent_model,
    tools=supervisor_agent_tools,
    prompt=supervisor_agent_configs.get("prompt"),
    name="supervisor_agent",
)


# Define multi-agent graph
multi_agent_graph = (
    StateGraph(MessagesState)
    # add agent nodes
    .add_node("validator_agent", validator_agent)
    .add_node("planner_agent", planner_agent)
    .add_node("supervisor_agent", supervisor_agent)
    .add_node("document_retrieval_agent", document_retrieval_agent)
    # define the flow
    .add_edge(START, "validator_agent")  # entry point
    .add_conditional_edges("validator_agent", route_after_agent)
    .add_conditional_edges("planner_agent", route_after_agent)
    .add_conditional_edges("document_retrieval_agent", route_after_agent)
    .add_edge("supervisor_agent", END)  # exit point - supervisor provides final response
    .compile()
)


# Wrap in MLflow ChatAgent
class MultiAgentResearchAssistant(ChatAgent):
    def __init__(self, agent):
        self.agent = agent

    def get_last_valid_message(self, messages: list):
        for message in reversed(messages):
            if message.content != "":
                return message

    @mlflow.trace(span_type=SpanType.AGENT)
    def predict(
        self,
        messages: list[ChatAgentMessage],
        context: Optional[ChatContext] = None,
        custom_inputs: Optional[dict[str, Any]] = None,
    ) -> ChatAgentResponse:
        
        if type(messages[0]) == mlflow.types.agent.ChatAgentMessage:
            request = {"messages": [m.model_dump() for m in messages]}

        agent_response = self.agent.invoke(request)

        last_valid_agent_message = self.get_last_valid_message(agent_response["messages"])

        response = [
            {
                "role": "assistant",
                "id": last_valid_agent_message.id,
                "content": last_valid_agent_message.content,
            }
        ]

        return ChatAgentResponse(messages=response)

    @mlflow.trace(span_type=SpanType.AGENT)
    def predict_stream(
        self,
        messages: list[ChatAgentMessage],
        context: Optional[ChatContext] = None,
        custom_inputs: Optional[dict[str, Any]] = None,
    ) -> Generator[ChatAgentChunk, None, None]:

        request = {"messages": [m.model_dump() for m in messages]}

        for event in self.agent.stream(request):
            agent_key = list(event.keys())[-1]
            try:
                if len(event[agent_key][-1]["messages"][-1]["content"]) > 0:
                    if event[agent_key][-1]["messages"][-1]["role"] == "tool":
                        yield ChatAgentChunk(
                            delta=ChatAgentMessage(
                                content=event[agent_key][-1]["messages"][-1]["content"],
                                role="assistant",
                                id=event[agent_key][-1]["messages"][-1]["tool_call_id"],
                            )
                        )
                    else:
                        yield ChatAgentChunk(
                                delta=ChatAgentMessage(
                                    content=event[agent_key][-1]["messages"][-1]["content"],
                                    role="assistant",
                                    id=event[agent_key][-1]["messages"][-1]["id"],
                                )
                            )
            except:
                if len(event[agent_key]["messages"][-1].content) > 0:
                    yield ChatAgentChunk(
                                delta=ChatAgentMessage(
                                    content=event[agent_key]["messages"][-1].content,
                                    role="assistant",
                                    id=event[agent_key]["messages"][-1].id,
                                )
                            )


# Set ChatAgent model
mlflow.langchain.autolog()
AGENT = MultiAgentResearchAssistant(multi_agent_graph)
mlflow.models.set_model(AGENT)
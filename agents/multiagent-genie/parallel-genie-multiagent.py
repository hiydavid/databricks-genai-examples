# Databricks notebook source
import functools
import os
import uuid
from typing import Any, Generator, Literal, Optional, List, Dict
from typing_extensions import TypedDict, Annotated
import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed

import mlflow
from databricks.sdk import WorkspaceClient
from databricks_langchain import (
    ChatDatabricks,
    UCFunctionToolkit,
)
from databricks_langchain.genie import GenieAgent
from langchain_core.runnables import RunnableLambda
from langgraph.graph import END, StateGraph, add_messages
from langgraph.graph.state import CompiledStateGraph
from langgraph.prebuilt import create_react_agent
from mlflow.entities import SpanType
from mlflow.pyfunc import ChatAgent
from mlflow.types.agent import (
    ChatAgentChunk,
    ChatAgentMessage,
    ChatAgentResponse,
    ChatContext,
)
from pydantic import BaseModel

###################################################
## Create a GenieAgent with access to a Genie Space
###################################################

# TODO add GENIE_SPACE_ID and a description for this space
# You can find the ID in the URL of the genie room /genie/rooms/<GENIE_SPACE_ID>
# Example description: This Genie agent can answer questions based on a database containing tables related to enterprise software sales, including accounts, opportunities, opportunity history, fiscal periods, quotas, targets, teams, and users. Use Genie to fetch and analyze data from these tables by specifying the relevant columns and filters. Genie can execute SQL queries to provide precise data insights based on your questions.
GENIE_SPACE_ID = "01f0627099691651968d0a92a26b06e9"
genie_agent_description = (
    "This genie agent can answer financial metric questions relevant to "
    "SEC 10k filing Income Statement and Balance Sheet "
    "for Apple Inc. (AAPL), Bank of America Corp (BAC), and American Express (AXP)."
)

genie_agent = GenieAgent(
    genie_space_id=GENIE_SPACE_ID,
    genie_agent_name="Genie",
    description=genie_agent_description,
    client=WorkspaceClient(
        host=os.getenv("DB_MODEL_SERVING_HOST_URL"),
        token=os.getenv("DATABRICKS_GENIE_PAT"),
    ),
)


############################################
# Define your LLM endpoint and system prompt
############################################

# TODO: Replace with your model serving endpoint
# multi-agent Genie works best with claude 3.7 or gpt 4o models.
# LLM_ENDPOINT_NAME = "databricks-claude-3-7-sonnet"
LLM_ENDPOINT_NAME = "databricks-claude-sonnet-4"
llm = ChatDatabricks(endpoint=LLM_ENDPOINT_NAME)


############################################################
# Create a code agent
# You can also create agents with access to additional tools
############################################################
tools = []

# TODO if desired, add additional tools and update the description of this agent
uc_tool_names = ["system.ai.*"]
uc_toolkit = UCFunctionToolkit(function_names=uc_tool_names)
tools.extend(uc_toolkit.tools)
code_agent_description = (
    "The Coder agent specializes in solving programming challenges, generating code snippets, debugging issues, and explaining complex coding concepts.",
)
code_agent = create_react_agent(llm, tools=tools)

#############################
# Define the supervisor agent with research planning capabilities
#############################

research_planner_description = "Plans and executes parallel research queries using the Genie agent to gather comprehensive information for complex questions."

# TODO update the max number of iterations between supervisor and worker nodes before returning to the user
MAX_ITERATIONS = 3

worker_descriptions = {
    "Genie": genie_agent_description,
    "Coder": code_agent_description,
    "ResearchPlanner": research_planner_description,
}

formatted_descriptions = "\n".join(
    f"- {name}: {desc}" for name, desc in worker_descriptions.items()
)

# Enhanced supervisor prompt with research planning capabilities
system_prompt = f"""
You are a supervisor managing a multi-agent system for complex business analysis with advanced research planning capabilities.

For complex questions, you have multiple strategies:

1. **Simple Routing**: For straightforward questions, route directly to the appropriate agent
2. **Research Planning**: For complex questions requiring multiple data points, use the ResearchPlanner to:
   - Break down the question into multiple specific research queries
   - Execute parallel searches via Genie agent
   - Gather comprehensive data from different angles
3. **Code Analysis**: Route to Coder for programming-related tasks

Available agents:
{formatted_descriptions}

**Decision Logic**:
- Use ResearchPlanner for complex analytical questions that would benefit from multiple parallel data queries
- Use Genie directly for single, focused financial/data questions
- Use Coder for programming/technical implementation questions
- Use FINISH when you have sufficient information to provide a comprehensive answer

When using ResearchPlanner, the system will automatically handle parallel execution and result synthesis.

Always provide comprehensive, well-structured responses that synthesize insights from multiple data sources when available.
"""

options = ["FINISH"] + list(worker_descriptions.keys())
FINISH = {"next_node": "FINISH"}


class NextNode(BaseModel):
    next_node: Literal[tuple(options)]


class ResearchPlan(BaseModel):
    queries: List[str]
    rationale: str


class ResearchPlanOutput(BaseModel):
    should_plan_research: bool
    research_plan: Optional[ResearchPlan] = None
    next_node: Literal[tuple(options)]


def supervisor_agent(state):
    count = state.get("iteration_count", 0) + 1
    if count > MAX_ITERATIONS:
        return FINISH

    # Check if we should plan research or route normally
    preprocessor = RunnableLambda(
        lambda state: [{"role": "system", "content": system_prompt}] + state["messages"]
    )

    # First, determine if we need research planning
    research_prompt = (
        "Analyze the user's question. Should this be handled with parallel research queries via ResearchPlanner? "
        "Respond with should_plan_research=True if the question is complex and would benefit from multiple parallel data queries. "
        "If planning research, provide 2-4 specific, focused queries that will gather comprehensive information. "
        "Otherwise, route to the appropriate single agent."
    )

    enhanced_preprocessor = RunnableLambda(
        lambda state: [
            {"role": "system", "content": system_prompt + "\n\n" + research_prompt}
        ]
        + state["messages"]
    )

    supervisor_chain = enhanced_preprocessor | llm.with_structured_output(
        ResearchPlanOutput
    )
    decision = supervisor_chain.invoke(state)

    # if routed back to the same node, exit the loop
    if state.get("next_node") == decision.next_node:
        return FINISH

    result = {"iteration_count": count, "next_node": decision.next_node}

    # If research planning is needed, store the research plan
    if decision.should_plan_research and decision.research_plan:
        result["research_plan"] = {
            "queries": decision.research_plan.queries,
            "rationale": decision.research_plan.rationale,
        }

    return result


#######################################
# Research Planner Node for Parallel Execution
#######################################


def research_planner_node(state):
    """Execute multiple Genie queries in parallel based on the research plan."""
    research_plan = state.get("research_plan")

    if not research_plan or not research_plan.get("queries"):
        return {
            "messages": [
                {
                    "role": "assistant",
                    "content": "No research plan found. Unable to execute parallel queries.",
                    "name": "ResearchPlanner",
                }
            ]
        }

    queries = research_plan["queries"]
    rationale = research_plan.get("rationale", "")

    def execute_genie_query(query: str) -> Dict[str, Any]:
        """Execute a single Genie query."""
        try:
            # Create a state with just this query
            query_state = {"messages": [{"role": "user", "content": query}]}
            result = genie_agent.invoke(query_state)
            return {
                "query": query,
                "success": True,
                "response": (
                    result["messages"][-1].content
                    if result.get("messages")
                    else "No response"
                ),
                "error": None,
            }
        except Exception as e:
            return {"query": query, "success": False, "response": None, "error": str(e)}

    # Execute queries in parallel
    results = []
    with ThreadPoolExecutor(max_workers=min(len(queries), 4)) as executor:
        # Submit all queries
        future_to_query = {
            executor.submit(execute_genie_query, query): query for query in queries
        }

        # Collect results as they complete
        for future in as_completed(future_to_query):
            result = future.result()
            results.append(result)

    # Sort results to maintain query order
    query_to_result = {r["query"]: r for r in results}
    ordered_results = [query_to_result[query] for query in queries]

    # Format the consolidated response
    response_parts = [f"Research Plan: {rationale}\n"]
    response_parts.append("Parallel Research Results:\n")

    for i, result in enumerate(ordered_results, 1):
        response_parts.append(f"\n{i}. Query: {result['query']}")
        if result["success"]:
            response_parts.append(f"   Result: {result['response']}")
        else:
            response_parts.append(f"   Error: {result['error']}")

    # Add synthesis
    response_parts.append(
        f"\n\nSynthesis: The parallel research has gathered comprehensive data from {len(queries)} different angles. This information can now be used to provide a complete answer to the original question."
    )

    consolidated_response = "\n".join(response_parts)

    return {
        "messages": [
            {
                "role": "assistant",
                "content": consolidated_response,
                "name": "ResearchPlanner",
            }
        ],
        "research_results": ordered_results,  # Store detailed results for potential further processing
    }


#######################################
# Define our multiagent graph structure
#######################################


def agent_node(state, agent, name):
    result = agent.invoke(state)
    return {
        "messages": [
            {
                "role": "assistant",
                "content": result["messages"][-1].content,
                "name": name,
            }
        ]
    }


def final_answer(state):
    prompt = "Using only the content in the messages, respond to the previous user question using the answer given by the other assistant messages. Provide a comprehensive, well-structured response that synthesizes all available information."
    preprocessor = RunnableLambda(
        lambda state: state["messages"] + [{"role": "user", "content": prompt}]
    )
    final_answer_chain = preprocessor | llm
    return {"messages": [final_answer_chain.invoke(state)]}


class AgentState(TypedDict):
    messages: Annotated[list, add_messages]
    next_node: str
    iteration_count: int
    research_plan: Optional[Dict[str, Any]]
    research_results: Optional[List[Dict[str, Any]]]


code_node = functools.partial(agent_node, agent=code_agent, name="Coder")
genie_node = functools.partial(agent_node, agent=genie_agent, name="Genie")

workflow = StateGraph(AgentState)
workflow.add_node("Genie", genie_node)
workflow.add_node("Coder", code_node)
workflow.add_node("ResearchPlanner", research_planner_node)
workflow.add_node("supervisor", supervisor_agent)
workflow.add_node("final_answer", final_answer)

workflow.set_entry_point("supervisor")
# We want our workers to ALWAYS "report back" to the supervisor when done
for worker in ["Genie", "Coder", "ResearchPlanner"]:
    workflow.add_edge(worker, "supervisor")

# Let the supervisor decide which next node to go
workflow.add_conditional_edges(
    "supervisor",
    lambda x: x["next_node"],
    {**{k: k for k in ["Genie", "Coder", "ResearchPlanner"]}, "FINISH": "final_answer"},
)
workflow.add_edge("final_answer", END)
multi_agent = workflow.compile()

###################################
# Wrap our multi-agent in ChatAgent
###################################


class LangGraphChatAgent(ChatAgent):
    def __init__(self, agent: CompiledStateGraph):
        self.agent = agent

    @mlflow.trace(span_type=SpanType.AGENT)
    def predict(
        self,
        messages: list[ChatAgentMessage],
        context: Optional[ChatContext] = None,
        custom_inputs: Optional[dict[str, Any]] = None,
    ) -> ChatAgentResponse:
        request = {
            "messages": [m.model_dump_compat(exclude_none=True) for m in messages]
        }

        messages = []
        for event in self.agent.stream(request, stream_mode="updates"):
            for node_data in event.values():
                for msg in node_data.get("messages", []):
                    # Convert message to dict if it's an AIMessage object
                    if hasattr(msg, "model_dump_compat"):
                        msg_dict = msg.model_dump_compat(exclude_none=True)
                    elif isinstance(msg, dict):
                        msg_dict = msg
                    else:
                        # Fallback: convert to dict manually
                        msg_dict = {
                            "role": getattr(msg, "role", "assistant"),
                            "content": getattr(msg, "content", str(msg)),
                        }
                        # Add name if present
                        if hasattr(msg, "name") and msg.name:
                            msg_dict["name"] = msg.name

                    # Ensure message has an ID
                    if "id" not in msg_dict or not msg_dict["id"]:
                        msg_dict["id"] = str(uuid.uuid4())

                    messages.append(ChatAgentMessage(**msg_dict))
        return ChatAgentResponse(messages=messages)

    @mlflow.trace(span_type=SpanType.AGENT)
    def predict_stream(
        self,
        messages: list[ChatAgentMessage],
        context: Optional[ChatContext] = None,
        custom_inputs: Optional[dict[str, Any]] = None,
    ) -> Generator[ChatAgentChunk, None, None]:
        request = {
            "messages": [m.model_dump_compat(exclude_none=True) for m in messages]
        }
        for event in self.agent.stream(request, stream_mode="updates"):
            for node_data in event.values():
                for msg in node_data.get("messages", []):
                    # Convert message to dict if it's an AIMessage object
                    if hasattr(msg, "model_dump_compat"):
                        msg_dict = msg.model_dump_compat(exclude_none=True)
                    elif isinstance(msg, dict):
                        msg_dict = msg
                    else:
                        # Fallback: convert to dict manually
                        msg_dict = {
                            "role": getattr(msg, "role", "assistant"),
                            "content": getattr(msg, "content", str(msg)),
                        }
                        # Add name if present
                        if hasattr(msg, "name") and msg.name:
                            msg_dict["name"] = msg.name

                    # Ensure message has an ID
                    if "id" not in msg_dict or not msg_dict["id"]:
                        msg_dict["id"] = str(uuid.uuid4())

                    yield ChatAgentChunk(**{"delta": msg_dict})


# Create the agent object, and specify it as the agent object to use when
# loading the agent back for inference via mlflow.models.set_model()
mlflow.langchain.autolog()
AGENT = LangGraphChatAgent(multi_agent)
mlflow.models.set_model(AGENT)

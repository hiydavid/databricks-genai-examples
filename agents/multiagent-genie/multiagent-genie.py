import asyncio
import functools
import os
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, Generator, List, Literal, Optional
from zoneinfo import ZoneInfo

import mlflow
from databricks.sdk import WorkspaceClient
from databricks_langchain import ChatDatabricks
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
from typing_extensions import Annotated, TypedDict

mlflow.set_tracking_uri("databricks")

######################################
## Load variables from the config file
######################################

# TODO make sure you update the config file
configs = mlflow.models.ModelConfig(development_config="./configs.yaml")
agent_configs = configs.get("agent_configs")

LLM_ENDPOINT_NAME = agent_configs.get("llm_endpoint_name")
GENIE_SPACE_ID = agent_configs.get("genie_agent").get("space_id")
GENIE_DESCRIPTION = agent_configs.get("genie_agent").get("description")
PARALLEL_EXECUTOR_DESCRIPTION = agent_configs.get("parallel_executor_agent").get(
    "description"
)

MAX_ITERATIONS = agent_configs.get("supervisor_agent").get("max_iterations")

SYSTEM_PROMPT = agent_configs.get("supervisor_agent").get("system_prompt")
RESEARCH_PROMPT = agent_configs.get("supervisor_agent").get("research_prompt")
FINAL_ANSWER_PROMPT = agent_configs.get("supervisor_agent").get("final_answer_prompt")

###################################################
## Create a GenieAgent with access to a Genie Space
###################################################

genie_agent = GenieAgent(
    genie_space_id=GENIE_SPACE_ID,
    genie_agent_name="Genie",
    description=GENIE_DESCRIPTION,
    client=WorkspaceClient(
        host=os.getenv("DB_MODEL_SERVING_HOST_URL"),
        token=os.getenv("DATABRICKS_GENIE_PAT"),
    ),
)

############################################
# Define your LLM endpoint and system prompt
############################################
llm = ChatDatabricks(endpoint=LLM_ENDPOINT_NAME)


################################################
# Create calendar functions for temporal context
################################################


def get_temporal_context() -> Dict[str, str]:
    """Return current date, fiscal year, and fiscal quarter.

    Fiscal year runs Sep 1 -> Aug 31, labeled by end year.
    Quarters: Q1=Sep-Nov, Q2=Dec-Feb, Q3=Mar-May, Q4=Jun-Aug
    """
    now = datetime.now(ZoneInfo("America/New_York"))
    today_iso = now.date().isoformat()

    # Fiscal year calculation (Sep-Aug, labeled by end year)
    fy_end_year = now.year + 1 if now.month >= 9 else now.year
    fy = f"FY{fy_end_year}"

    # Fiscal quarter calculation
    if now.month in (9, 10, 11):
        fq = "Q1"
    elif now.month in (12, 1, 2):
        fq = "Q2"
    elif now.month in (3, 4, 5):
        fq = "Q3"
    else:  # Jun, Jul, Aug
        fq = "Q4"

    return {
        "today_iso": today_iso,
        "fy": fy,
        "fq": fq,
    }


#################################################################
# Define the supervisor agent with research planning capabilities
#################################################################

worker_descriptions = {
    "Genie": GENIE_DESCRIPTION,
    "ParallelExecutor": PARALLEL_EXECUTOR_DESCRIPTION,
}

formatted_descriptions = "\n".join(
    f"- {name}: {desc}" for name, desc in worker_descriptions.items()
)

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


# @mlflow.trace(span_type=SpanType.AGENT, name="supervisor_routing")
def supervisor_agent(state):
    """Supervisor agent node that prepends temporal org context to the system prompt
    and then decides whether to plan research or route normally.

    The injected context includes:
      - Current date (America/New_York)
      - Current fiscal year (FY named by end year, Sep→Aug)
      - Current fiscal quarter (Q1..Q4, with Q1 starting in September)
    """
    try:
        count = state.get("iteration_count", 0) + 1
        if count > MAX_ITERATIONS:
            return FINISH

        # Build dynamic system prompt
        temporal_ctx = get_temporal_context()

        # Keep the context compact and machine-friendly at the very top of the system prompt.
        temporal_prefix = (
            "Below is information on the current date and fiscal year/quarter information. You may or may not use this in your analysis.\n"
            f"- The current date is: {temporal_ctx['today_iso']}\n"
            f"- The current fiscal year is: {temporal_ctx['fy']}\n"
            f"- The current fiscal quarter is: {temporal_ctx['fq']}\n\n"
        )

        SYSTEM_PROMPT_WITH_CONTEXT = temporal_prefix + SYSTEM_PROMPT
        SYSTEM_PROMPT_WITH_CONTEXT_AND_RESEARCH = (
            temporal_prefix + SYSTEM_PROMPT + "\n\n" + RESEARCH_PROMPT
        )

        # Preprocessors that include the dynamic prefix
        preprocessor = RunnableLambda(
            lambda state: [{"role": "system", "content": SYSTEM_PROMPT_WITH_CONTEXT}]
            + state["messages"]
        )

        enhanced_preprocessor = RunnableLambda(
            lambda state: [
                {"role": "system", "content": SYSTEM_PROMPT_WITH_CONTEXT_AND_RESEARCH}
            ]
            + state["messages"]
        )

        # Decide routing / research planning as before
        supervisor_chain = enhanced_preprocessor | llm.with_structured_output(
            ResearchPlanOutput
        )
        decision = supervisor_chain.invoke(state)

        # If routed back to the same node, finish to prevent loops
        if state.get("next_node") == decision.next_node:
            return FINISH

        result = {"iteration_count": count, "next_node": decision.next_node}

        # Persist research plan if needed
        if decision.should_plan_research and decision.research_plan:
            result["research_plan"] = {
                "queries": decision.research_plan.queries,
                "rationale": decision.research_plan.rationale,
            }

        return result

    except Exception as e:
        # If supervisor fails, finish gracefully with error
        error_message = f"Supervisor routing failed: {str(e)}"
        print(f"[ERROR] {error_message}")
        return FINISH


##############################################
# Research Planner Node for Parallel Execution
##############################################


# @mlflow.trace(span_type=SpanType.AGENT, name="research_planner")  # Commented out - using autolog only
def research_planner_node(state):
    """Execute multiple Genie queries in parallel based on the research plan."""
    try:
        research_plan = state.get("research_plan")

        if not research_plan or not research_plan.get("queries"):
            return {
                "messages": [
                    {
                        "role": "assistant",
                        "content": "No research plan found. Unable to execute parallel queries.",
                        "name": "ParallelExecutor",
                    }
                ]
            }

        queries = research_plan["queries"]
        rationale = research_plan.get("rationale", "")

        # @mlflow.trace(span_type=SpanType.AGENT, name="execute_genie_query")  # Commented out to reduce memory overhead
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
                return {
                    "query": query,
                    "success": False,
                    "response": None,
                    "error": str(e),
                }

        # Execute queries in parallel with memory-efficient processing
        results = []
        with ThreadPoolExecutor(max_workers=min(len(queries), 3)) as executor:
            # Submit all queries
            future_to_query = {
                executor.submit(execute_genie_query, query): query for query in queries
            }

            # Process results immediately to free memory with error handling
            for future in as_completed(future_to_query):
                try:
                    result = future.result()
                    results.append(result)
                    del result  # Explicit cleanup of result object
                except Exception as e:
                    # If individual future fails, create error result
                    query = future_to_query[future]
                    error_result = {
                        "query": query,
                        "success": False,
                        "response": None,
                        "error": str(e),
                    }
                    results.append(error_result)
                    print(
                        f"[ERROR] Parallel execution failed for query '{query}': {str(e)}"
                    )

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

        consolidated_response = "\n\n".join(response_parts)

        # Explicit cleanup of large objects
        del response_parts
        del query_to_result
        import gc

        gc.collect()

        return {
            "messages": [
                {
                    "role": "assistant",
                    "content": consolidated_response,
                    "name": "ParallelExecutor",
                }
            ],
            "research_results": ordered_results,  # Store detailed results for potential further processing
        }

    except Exception as e:
        # If entire parallel research fails, return error message
        error_message = f"Parallel research execution failed: {str(e)}"
        print(f"[ERROR] {error_message}")

        return {
            "messages": [
                {
                    "role": "assistant",
                    "content": f"I encountered an error during parallel research execution: {error_message}. Please try rephrasing your question or ask something simpler.",
                    "name": "ParallelExecutor",
                }
            ]
        }


#######################################
# Define our multiagent graph structure
#######################################


# @mlflow.trace(span_type=SpanType.AGENT)  # Commented out to reduce memory overhead
def agent_node(state, agent, name):
    """Agent node wrapper with error handling to prevent supervisor failures."""
    try:
        result = agent.invoke(state)

        # Validate result structure
        if not result or "messages" not in result or not result["messages"]:
            raise ValueError(f"Invalid result structure from {name} agent")

        return {
            "messages": [
                {
                    "role": "assistant",
                    "content": result["messages"][-1].content,
                    "name": name,
                }
            ]
        }
    except Exception as e:
        # Log error but don't crash the supervisor
        error_message = f"Error in {name} agent: {str(e)}"
        print(f"[ERROR] {error_message}")

        # Return error message as agent response so supervisor can continue
        return {
            "messages": [
                {
                    "role": "assistant",
                    "content": f"I encountered an error while processing your request: {error_message}. Please try rephrasing your question or ask something different.",
                    "name": name,
                }
            ]
        }


# @mlflow.trace(span_type=SpanType.AGENT, name="final_answer")
def final_answer(state):
    """Generate final answer with error handling."""
    try:
        preprocessor = RunnableLambda(
            lambda state: state["messages"]
            + [{"role": "user", "content": FINAL_ANSWER_PROMPT}]
        )
        final_answer_chain = preprocessor | llm
        return {"messages": [final_answer_chain.invoke(state)]}

    except Exception as e:
        # If final answer generation fails, provide fallback response
        error_message = f"Final answer generation failed: {str(e)}"
        print(f"[ERROR] {error_message}")

        # Generate a fallback response based on available messages
        try:
            messages = state.get("messages", [])
            if messages:
                last_message = messages[-1]
                fallback_content = f"I apologize, but I encountered an error while generating my final response. Based on the information gathered, here's what I found: {last_message.get('content', 'Unable to retrieve previous response.')} Please try asking your question again."
            else:
                fallback_content = "I apologize, but I encountered an error and couldn't process your request. Please try asking your question again."
        except:
            fallback_content = "I apologize, but I encountered an error and couldn't process your request. Please try asking your question again."

        return {"messages": [{"role": "assistant", "content": fallback_content}]}


class AgentState(TypedDict):
    messages: Annotated[list, add_messages]
    next_node: str
    iteration_count: int
    research_plan: Optional[Dict[str, Any]]
    research_results: Optional[List[Dict[str, Any]]]


genie_node = functools.partial(agent_node, agent=genie_agent, name="Genie")

workflow = StateGraph(AgentState)
workflow.add_node("Genie", genie_node)
workflow.add_node("ParallelExecutor", research_planner_node)
workflow.add_node("supervisor", supervisor_agent)
workflow.add_node("final_answer", final_answer)

workflow.set_entry_point("supervisor")
# We want our workers to ALWAYS "report back" to the supervisor when done
for worker in ["Genie", "ParallelExecutor"]:
    workflow.add_edge(worker, "supervisor")

# Let the supervisor decide which next node to go
workflow.add_conditional_edges(
    "supervisor",
    lambda x: x["next_node"],
    {**{k: k for k in ["Genie", "ParallelExecutor"]}, "FINISH": "final_answer"},
)
workflow.add_edge("final_answer", END)
multi_agent = workflow.compile()


###################################
# Wrap our multi-agent in ChatAgent
###################################


class LangGraphChatAgent(ChatAgent):
    def __init__(self, agent: CompiledStateGraph):
        self.agent = agent

    # @mlflow.trace(span_type=SpanType.AGENT, name="user_interaction")
    def predict(
        self,
        messages: list[ChatAgentMessage],
        context: Optional[ChatContext] = None,
        custom_inputs: Optional[dict[str, Any]] = None,
    ) -> ChatAgentResponse:
        # Truncate message history to prevent memory accumulation
        MAX_MESSAGES = 7
        if len(messages) > MAX_MESSAGES:
            messages = messages[-MAX_MESSAGES:]

        request = {
            "messages": [m.model_dump_compat(exclude_none=True) for m in messages]
        }

        messages = []
        for event in self.agent.stream(request, stream_mode="updates"):
            for node_name, node_data in event.items():
                # Only include messages from the final_answer node
                if node_name == "final_answer":
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

        # Explicit cleanup to free memory
        import gc

        gc.collect()

        return ChatAgentResponse(messages=messages)

    # @mlflow.trace(span_type=SpanType.AGENT, name="user_interaction_stream")
    def predict_stream(
        self,
        messages: list[ChatAgentMessage],
        context: Optional[ChatContext] = None,
        custom_inputs: Optional[dict[str, Any]] = None,
    ) -> Generator[ChatAgentChunk, None, None]:
        # Truncate message history to prevent memory accumulation
        MAX_MESSAGES = 7
        if len(messages) > MAX_MESSAGES:
            messages = messages[-MAX_MESSAGES:]

        request = {
            "messages": [m.model_dump_compat(exclude_none=True) for m in messages]
        }

        # Track which nodes we've seen to provide status updates
        seen_nodes = set()

        for event in self.agent.stream(request, stream_mode="updates"):
            for node_name, node_data in event.items():
                # Provide status updates for intermediate nodes to prevent timeout
                if node_name not in seen_nodes and node_name != "final_answer":
                    seen_nodes.add(node_name)
                    status_msg = {
                        "role": "assistant",
                        "content": f"Processing with {node_name}...",
                        "id": str(uuid.uuid4()),
                    }
                    # Yield status update as a chunk but don't include in final response
                    yield ChatAgentChunk(**{"delta": status_msg})

                # Only include actual messages from the final_answer node
                if node_name == "final_answer":
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

        # Explicit cleanup after streaming completes
        import gc

        gc.collect()


# Create the agent object
mlflow.langchain.autolog()
AGENT = LangGraphChatAgent(multi_agent)
mlflow.models.set_model(AGENT)

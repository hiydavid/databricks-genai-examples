# Databricks notebook source
import functools
import os
from typing import Any, Generator, Literal, Optional

import mlflow
from databricks.sdk import WorkspaceClient
from databricks_langchain import (
    ChatDatabricks,
    UCFunctionToolkit,
)
from databricks_langchain.genie import GenieAgent
from langchain_core.runnables import RunnableLambda
from langgraph.graph import END, StateGraph
from langgraph.graph.state import CompiledStateGraph
from langgraph.prebuilt import create_react_agent
from mlflow.langchain.chat_agent_langgraph import ChatAgentState
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

LLM_ENDPOINT_NAME = "databricks-claude-3-7-sonnet"
# LLM_ENDPOINT_NAME = "databricks-claude-sonnet-4"
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
# Define the supervisor agent
#############################

# TODO update the max number of iterations between supervisor and worker nodes
# before returning to the user
MAX_ITERATIONS = 3

worker_descriptions = {
    "Genie": genie_agent_description,
    "Coder": code_agent_description,
}

formatted_descriptions = "\n".join(
    f"- {name}: {desc}" for name, desc in worker_descriptions.items()
)

# # OLD SUPERVISOR PROMPT
# system_prompt = (
#     f"Decide between routing between the following workers or ending the conversation if an answer is provided. \n"
#     "Financial metrics questions should be passed on to the Genie Agent. \n"
#     "{formatted_descriptions}"
# )

# # NEW SUPERVISOR PROMPT
system_prompt = f"""
You are a supervisor managing a multi-agent system for complex business analysis.

For complex questions:
1. First, break down the question into logical steps
2. Route to appropriate agents (Genie for data, Coder for analysis)
3. Receive and review results from your worker agents. 
4. If the outputs from either of the agents are not sufficient to answer the question, try asking a different question to the agents.
4. If the outputs are sufficient, use them to synthesize results into comprehensive insights

Available agents:
{formatted_descriptions}

For financial metrics & data questions, route to Genie first. If Genie is unsure of the question and asks for clarification, try your best to guide Genie. For example, if Genie asks you to confirm or clarify user's intention, try to answer Genie on behalf of the user.

Finally, when you're ready to provide an answer, make sure your response is concise, succinct, and to the point, but include important facts from your worker agents to support your conclusion.
"""

options = ["FINISH"] + list(worker_descriptions.keys())
FINISH = {"next_node": "FINISH"}

def supervisor_agent(state):
    count = state.get("iteration_count", 0) + 1
    if count > MAX_ITERATIONS:
        return FINISH
    
    class nextNode(BaseModel):
        next_node: Literal[tuple(options)]

    preprocessor = RunnableLambda(
        lambda state: [{"role": "system", "content": system_prompt}] + state["messages"]
    )
    supervisor_chain = preprocessor | llm.with_structured_output(nextNode)
    next_node = supervisor_chain.invoke(state).next_node
    
    # if routed back to the same node, exit the loop
    if state.get("next_node") == next_node:
        return FINISH
    return {
        "iteration_count": count,
        "next_node": next_node
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
    prompt = "Using only the content in the messages, respond to the previous user question using the answer given by the other assistant messages."
    preprocessor = RunnableLambda(
        lambda state: state["messages"] + [{"role": "user", "content": prompt}]
    )
    final_answer_chain = preprocessor | llm
    return {"messages": [final_answer_chain.invoke(state)]}


class AgentState(ChatAgentState):
    next_node: str
    iteration_count: int


code_node = functools.partial(agent_node, agent=code_agent, name="Coder")
genie_node = functools.partial(agent_node, agent=genie_agent, name="Genie")

workflow = StateGraph(AgentState)
workflow.add_node("Genie", genie_node)
workflow.add_node("Coder", code_node)
workflow.add_node("supervisor", supervisor_agent)
workflow.add_node("final_answer", final_answer)

workflow.set_entry_point("supervisor")
# We want our workers to ALWAYS "report back" to the supervisor when done
for worker in worker_descriptions.keys():
    workflow.add_edge(worker, "supervisor")

# Let the supervisor decide which next node to go
workflow.add_conditional_edges(
    "supervisor",
    lambda x: x["next_node"],
    {**{k: k for k in worker_descriptions.keys()}, "FINISH": "final_answer"},
)
workflow.add_edge("final_answer", END)
multi_agent = workflow.compile()

###################################
# Wrap our multi-agent in ChatAgent
###################################


class LangGraphChatAgent(ChatAgent):
    def __init__(self, agent: CompiledStateGraph):
        self.agent = agent

    # @mlflow.trace(span_type=SpanType.AGENT)
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
                messages.extend(
                    ChatAgentMessage(**msg) for msg in node_data.get("messages", [])
                )
        return ChatAgentResponse(messages=messages)

    # @mlflow.trace(span_type=SpanType.AGENT)
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
                yield from (
                    ChatAgentChunk(**{"delta": msg})
                    for msg in node_data.get("messages", [])
                )


# Create the agent object, and specify it as the agent object to use when
# loading the agent back for inference via mlflow.models.set_model()
mlflow.langchain.autolog()
AGENT = LangGraphChatAgent(multi_agent)
mlflow.models.set_model(AGENT)

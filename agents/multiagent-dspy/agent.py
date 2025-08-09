import asyncio
import functools
import os
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Generator, List, Literal, Optional

import dspy
import mlflow
from databricks.sdk import WorkspaceClient
from databricks_langchain import ChatDatabricks
from databricks_langchain.genie import GenieAgent
from mlflow.entities import SpanType
from mlflow.pyfunc import ChatAgent
from mlflow.types.agent import (
    ChatAgentChunk,
    ChatAgentMessage,
    ChatAgentResponse,
    ChatContext,
)
from pydantic import BaseModel

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
RESEARCH_PLANNING_PROMPT = agent_configs.get("supervisor_agent").get("research_planning_prompt")
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
# Set up DSPy LLM and configure
############################################

# Configure DSPy to use Databricks
class DatabricksLM(dspy.LM):
    def __init__(self, endpoint_name: str, **kwargs):
        super().__init__(model=endpoint_name, **kwargs)
        self.endpoint_name = endpoint_name
        self.chat_databricks = ChatDatabricks(endpoint=endpoint_name)
    
    def basic_request(self, prompt: str, **kwargs) -> str:
        # Convert prompt to messages format expected by ChatDatabricks
        messages = [{"role": "user", "content": prompt}]
        response = self.chat_databricks.invoke(messages)
        return response.content

# Set up DSPy with Databricks LLM
dspy_lm = DatabricksLM(endpoint_name=LLM_ENDPOINT_NAME)
dspy.configure(lm=dspy_lm)

#################################################################
# Define DSPy Signatures for the multi-agent system
#################################################################

class RoutingDecision(dspy.Signature):
    """Analyze a financial question and decide which agent should handle it."""
    
    question = dspy.InputField(desc="The user's financial question")
    conversation_history = dspy.InputField(desc="Previous messages in the conversation")
    iteration_count = dspy.InputField(desc="Number of iterations so far")
    
    should_plan_research = dspy.OutputField(desc="Whether to use parallel research planning")
    next_agent = dspy.OutputField(desc="Which agent to route to: 'Genie', 'ParallelExecutor', or 'FINISH'")
    research_queries = dspy.OutputField(desc="List of queries for parallel execution (if planning research)")
    rationale = dspy.OutputField(desc="Explanation for the routing decision")

class GenieQuery(dspy.Signature):
    """Query the Genie agent for SEC financial data."""
    
    query = dspy.InputField(desc="Financial question to ask the Genie agent")
    
    response = dspy.OutputField(desc="Genie agent's response with financial data")

class ParallelResearch(dspy.Signature):
    """Execute parallel research queries and synthesize results."""
    
    research_queries = dspy.InputField(desc="List of queries to execute in parallel")
    rationale = dspy.InputField(desc="Reasoning for the parallel research approach")
    
    synthesis = dspy.OutputField(desc="Synthesized results from all parallel queries")

class FinalAnswer(dspy.Signature):
    """Generate final response based on agent interactions."""
    
    original_question = dspy.InputField(desc="The user's original question")
    agent_responses = dspy.InputField(desc="All responses from the agents")
    
    final_response = dspy.OutputField(desc="Comprehensive final answer to the user's question")

#################################################################
# Define DSPy Modules for the multi-agent system
#################################################################

class SupervisorModule(dspy.Module):
    def __init__(self):
        super().__init__()
        self.routing_chain = dspy.ChainOfThought(RoutingDecision)
    
    @mlflow.trace(span_type=SpanType.AGENT, name="supervisor_routing")
    def forward(self, question: str, conversation_history: List[Dict], iteration_count: int = 0):
        # Check iteration limit
        if iteration_count >= MAX_ITERATIONS:
            return dspy.Prediction(
                should_plan_research=False,
                next_agent="FINISH",
                research_queries=[],
                rationale=f"Maximum iterations ({MAX_ITERATIONS}) reached."
            )
        
        # Format conversation history
        history_text = "\n".join([
            f"{msg.get('role', 'unknown')}: {msg.get('content', '')}"
            for msg in conversation_history
        ])
        
        # Get routing decision
        with dspy.context(lm=dspy_lm.copy(system_message=SYSTEM_PROMPT + "\n\n" + RESEARCH_PLANNING_PROMPT)):
            decision = self.routing_chain(
                question=question,
                conversation_history=history_text,
                iteration_count=str(iteration_count)
            )
        
        return decision

class GenieModule(dspy.Module):
    def __init__(self, genie_agent):
        super().__init__()
        self.genie_agent = genie_agent
    
    @mlflow.trace(span_type=SpanType.AGENT, name="genie_agent")
    def forward(self, query: str):
        # Create state for GenieAgent
        state = {"messages": [{"role": "user", "content": query}]}
        
        try:
            result = self.genie_agent.invoke(state)
            response = result["messages"][-1].content if result.get("messages") else "No response from Genie"
            return dspy.Prediction(response=response)
        except Exception as e:
            return dspy.Prediction(response=f"Error querying Genie: {str(e)}")

class ParallelExecutorModule(dspy.Module):
    def __init__(self, genie_agent):
        super().__init__()
        self.genie_agent = genie_agent
    
    @mlflow.trace(span_type=SpanType.AGENT, name="parallel_executor")
    def forward(self, research_queries: List[str], rationale: str):
        if not research_queries:
            return dspy.Prediction(synthesis="No research queries provided.")
        
        @mlflow.trace(span_type=SpanType.AGENT, name="execute_genie_query")
        def execute_genie_query(query: str) -> Dict[str, Any]:
            """Execute a single Genie query."""
            try:
                query_state = {"messages": [{"role": "user", "content": query}]}
                result = self.genie_agent.invoke(query_state)
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
        with ThreadPoolExecutor(max_workers=min(len(research_queries), 3)) as executor:
            future_to_query = {
                executor.submit(execute_genie_query, query): query 
                for query in research_queries
            }
            
            for future in as_completed(future_to_query):
                result = future.result()
                results.append(result)
        
        # Sort results to maintain query order
        query_to_result = {r["query"]: r for r in results}
        ordered_results = [query_to_result[query] for query in research_queries]
        
        # Format the synthesis
        synthesis_parts = [f"Research Plan: {rationale}\n"]
        synthesis_parts.append("Parallel Research Results:\n")
        
        for i, result in enumerate(ordered_results, 1):
            synthesis_parts.append(f"\n{i}. Query: {result['query']}")
            if result["success"]:
                synthesis_parts.append(f"   Result: {result['response']}")
            else:
                synthesis_parts.append(f"   Error: {result['error']}")
        
        synthesis_parts.append(
            f"\n\nSynthesis: The parallel research has gathered comprehensive data from {len(research_queries)} different angles. This information can now be used to provide a complete answer to the original question."
        )
        
        synthesis = "\n".join(synthesis_parts)
        return dspy.Prediction(synthesis=synthesis)

class FinalAnswerModule(dspy.Module):
    def __init__(self):
        super().__init__()
        self.answer_chain = dspy.ChainOfThought(FinalAnswer)
    
    @mlflow.trace(span_type=SpanType.AGENT, name="final_answer")
    def forward(self, original_question: str, agent_responses: List[str]):
        responses_text = "\n\n".join([
            f"Agent Response {i+1}: {response}"
            for i, response in enumerate(agent_responses)
        ])
        
        with dspy.context(lm=dspy_lm.copy(system_message=FINAL_ANSWER_PROMPT)):
            result = self.answer_chain(
                original_question=original_question,
                agent_responses=responses_text
            )
        
        return result

#################################################################
# Main Multi-Agent DSPy System
#################################################################

class DSPyMultiAgentSystem(dspy.Module):
    def __init__(self):
        super().__init__()
        self.supervisor = SupervisorModule()
        self.genie = GenieModule(genie_agent)
        self.parallel_executor = ParallelExecutorModule(genie_agent)
        self.final_answer = FinalAnswerModule()
    
    @mlflow.trace(span_type=SpanType.AGENT, name="multiagent_system")
    def forward(self, question: str):
        conversation_history = [{"role": "user", "content": question}]
        agent_responses = []
        iteration_count = 0
        
        while iteration_count < MAX_ITERATIONS:
            # Get routing decision from supervisor
            decision = self.supervisor(
                question=question,
                conversation_history=conversation_history,
                iteration_count=iteration_count
            )
            
            if decision.next_agent == "FINISH":
                break
            elif decision.next_agent == "Genie":
                # Route to Genie agent
                genie_result = self.genie(query=question)
                agent_responses.append(genie_result.response)
                conversation_history.append({
                    "role": "assistant", 
                    "content": genie_result.response, 
                    "name": "Genie"
                })
            elif decision.next_agent == "ParallelExecutor" and decision.should_plan_research:
                # Route to ParallelExecutor
                research_queries = decision.research_queries
                if isinstance(research_queries, str):
                    # Handle case where research_queries might be a string
                    research_queries = [q.strip() for q in research_queries.split('\n') if q.strip()]
                
                executor_result = self.parallel_executor(
                    research_queries=research_queries,
                    rationale=decision.rationale
                )
                agent_responses.append(executor_result.synthesis)
                conversation_history.append({
                    "role": "assistant", 
                    "content": executor_result.synthesis, 
                    "name": "ParallelExecutor"
                })
            else:
                # Default to Genie for safety
                genie_result = self.genie(query=question)
                agent_responses.append(genie_result.response)
                conversation_history.append({
                    "role": "assistant", 
                    "content": genie_result.response, 
                    "name": "Genie"
                })
            
            iteration_count += 1
        
        # Generate final answer
        final_result = self.final_answer(
            original_question=question,
            agent_responses=agent_responses
        )
        
        return dspy.Prediction(final_response=final_result.final_response)

###################################
# Wrap DSPy system in ChatAgent
###################################

class DSPyChatAgent(ChatAgent):
    def __init__(self, dspy_system: DSPyMultiAgentSystem):
        self.dspy_system = dspy_system
    
    @mlflow.trace(span_type=SpanType.AGENT)
    def predict(
        self,
        messages: list[ChatAgentMessage],
        context: Optional[ChatContext] = None,
        custom_inputs: Optional[dict[str, Any]] = None,
    ) -> ChatAgentResponse:
        # Extract the latest user message
        user_messages = [msg for msg in messages if msg.role == "user"]
        if not user_messages:
            return ChatAgentResponse(messages=[
                ChatAgentMessage(
                    role="assistant",
                    content="No user question found.",
                    id=str(uuid.uuid4())
                )
            ])
        
        question = user_messages[-1].content
        
        # Run the DSPy system
        result = self.dspy_system(question=question)
        
        # Return as ChatAgentMessage
        response_message = ChatAgentMessage(
            role="assistant",
            content=result.final_response,
            id=str(uuid.uuid4())
        )
        
        return ChatAgentResponse(messages=[response_message])
    
    @mlflow.trace(span_type=SpanType.AGENT)
    def predict_stream(
        self,
        messages: list[ChatAgentMessage],
        context: Optional[ChatContext] = None,
        custom_inputs: Optional[dict[str, Any]] = None,
    ) -> Generator[ChatAgentChunk, None, None]:
        # For streaming, we'll provide status updates
        user_messages = [msg for msg in messages if msg.role == "user"]
        if not user_messages:
            yield ChatAgentChunk(delta={
                "role": "assistant",
                "content": "No user question found.",
                "id": str(uuid.uuid4())
            })
            return
        
        question = user_messages[-1].content
        
        # Provide status update
        yield ChatAgentChunk(delta={
            "role": "assistant",
            "content": "Processing with DSPy multi-agent system...",
            "id": str(uuid.uuid4())
        })
        
        # Run the DSPy system
        result = self.dspy_system(question=question)
        
        # Yield final result
        yield ChatAgentChunk(delta={
            "role": "assistant",
            "content": result.final_response,
            "id": str(uuid.uuid4())
        })

# Create the agent object
mlflow.langchain.autolog()
dspy_multi_agent = DSPyMultiAgentSystem()
AGENT = DSPyChatAgent(dspy_multi_agent)
mlflow.models.set_model(AGENT)
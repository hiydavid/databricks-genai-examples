# Multi-Agent System Optimization Guide

This comprehensive guide covers how to optimize the multi-agent financial analysis system at both the system and prompt level. The system uses multiple components that work together to provide effective financial data analysis.

## Overview of Optimization Layers

The multi-agent system has several optimization layers:

1. **Genie Space Optimization** - Foundation data access layer
2. **System Configuration** - LLM, endpoints, and deployment settings
3. **Prompt Engineering** - Agent routing and response logic
4. **Development Workflow** - Testing, debugging, and evaluation

## 1. Genie Space Optimization

### Foundation Layer

The quality of the multi-agent system fundamentally depends on Genie's ability to answer questions. This is your foundation layer that must be optimized first.

**Key Areas:**

- **Table Descriptions**: Ensure comprehensive, accurate metadata for all financial tables
- **General Instructions**: Add clear SQL guidelines and financial calculation formulas
- **Trusted Assets**: Include validated example SQL queries for complex financial metrics
 **Consistency Check**: Eliminate conflicting instructions across the Genie space

**Important Limitation**: The Supervisor agent and Genie do not share contextual information automatically. Custom instructions in the Genie Space must be manually synchronized with multi-agent system prompts.

### Data Scope Alignment

Ensure your Genie space instructions align with actual data availability:

- **Time Range**: SEC financial data from 2003-2022 (update from 2003-2017 if needed)
- **Companies**: Apple Inc. (AAPL), Bank of America Corp (BAC), American Express (AXP)
- **Data Types**: Income Statement, Balance Sheet, and derived financial ratios
- **Query Guidelines**: Reference `data/sec/genie_instruction.md` for supported calculations

## 2. System Configuration Optimization

### LLM Endpoint Configuration

**Location**: `configs.yaml` → `agent_configs.supervisor_agent.llm_endpoint_name`

**Options:**

- **Claude 3.7 Sonnet** (default): Balanced performance and cost
- **Claude 4 Sonnet**: Higher quality but potential rate limits and higher cost
- **Custom Endpoints**: Your own fine-tuned models

**Considerations:**

- Claude 4 faces industry resource constraints - coordinate with Databricks for higher rate limits
- Provisioned throughput versions expected later this quarter
- Reference [Anthropic's prompt engineering guide](https://docs.anthropic.com/claude/docs/prompt-engineering) for Claude-specific optimization

### Iteration Control

**Location**: `configs.yaml` → `agent_configs.supervisor_agent.max_iterations`

**Default**: 3 iterations (recommended starting point)

**Trade-offs**:

- Higher numbers allow more complex multi-step reasoning
- Increased latency and token costs
- Risk of infinite loops or degraded responses

### Endpoint Sizing for Deployment

When deploying with `agents.deploy()`, consider concurrency needs:

```python
agents.deploy(
    model_name,
    model_version,
    tags,
    environment_vars,
    workload_size="Small|Medium|Large",  # 4|16|64 concurrent requests
    scale_to_zero=True,  # Cost optimization
    endpoint_name
)
```

**Sizing Guidelines:**

- **Small (4 concurrent)**: Development, small teams
- **Medium (16 concurrent)**: Production, moderate usage
- **Large (64 concurrent)**: High-traffic production environments

Note: Sizing affects concurrency, not per-request latency.

## 3. Prompt Engineering Optimization

The system has three main prompt components in `configs.yaml` that need to be customized for your specific dataset and use cases:

### Supervisor Agent System Prompt

**Location**: `agent_configs.supervisor_agent.system_prompt`

**Key areas to customize:**

- **Data scope and limitations** - Update to reflect your actual data availability, time ranges, entities, and data types
- **Routing criteria** - Define when to route directly to Genie (simple queries) vs ParallelExecutor (complex multi-step analysis)
- **Agent descriptions** - Update individual agent descriptions to match your use case

### Research Planning Prompt

**Location**: `agent_configs.supervisor_agent.research_prompt`

**Key areas to customize:**

- **Routing bias** - Set default preference toward Genie for performance, only using ParallelExecutor when truly needed
- **Examples** - Provide examples of simple vs complex queries specific to your domain
- **Planning constraints** - Define guidelines for generating research queries within your data limitations

### Final Answer Prompt

**Location**: `agent_configs.supervisor_agent.final_answer_prompt`

**Key areas to customize:**

- **Response formatting** - Define different styles for simple vs complex query responses
- **Domain-specific structure** - Tailor output format to your business domain and user expectations

### Best Practices for Prompt Customization

- **Start conservative** - Bias toward direct Genie routing to minimize latency
- **Be domain-specific** - Include terminology, entities, and examples from your actual data
- **Test iteratively** - Use representative queries to validate routing decisions
- **Maintain consistency** - Ensure prompts align with your Genie space instructions

## 4. Development and Debugging Workflow

### MLflow Tracing Integration

All agent interactions are automatically traced with MLflow 3.0+ integration.

**Setup Requirements:**

- MLflow 3.0+ installed in notebook environment
- Proper experiment configuration in `configs.yaml`
- Token permissions for MLflow experiment access
- Use `agents.deploy()` for automatic tracing in Model Serving

**Debugging Workflow:**

1. Work in notebooks during development (avoid frequent deployments)
2. Use `driver.py` for testing agent configurations
3. Review MLflow traces for routing decisions and performance
4. Iterate on prompts based on tracing insights
5. Deploy only when ready for user review or full evaluation

**Key Tracing Data Points:**

- Supervisor routing decisions (Genie vs ParallelExecutor)
- Individual Genie query performance
- Parallel execution coordination
- Final answer synthesis quality

### Testing Your Prompt Changes

After updating prompts, test with the sample questions in `driver.py` cells 141-145:

1. **Simple Questions**: Should route directly to Genie
   - "What was AAPL's revenue in 2015?"
   
2. **Complex Questions**: Should use ParallelExecutor
   - "Compare the profitability trends of AAPL, BAC, and AXP from 2010-2015"

3. **Edge Cases**: Test boundary conditions
   - Questions that could go either way
   - Follow-up clarification scenarios

## 5. Evaluation and Quality Measurement

### Evaluation Framework
Use Mosaic AI Agent Evaluation suite for systematic quality measurement.

**Evaluation Dataset:**
- Create 10-15 diverse, representative questions
- Cover both simple and complex query types
- Include edge cases and boundary conditions
- Match your actual data scope and use cases

**Automated Evaluation:**

-  Use MLflow for programmatic evaluation runs
- Built-in AI judges for response quality assessment
- Track improvement metrics over time

**Human Evaluation:**

- Deploy with `agents.deploy()` to get Review App access
- Collect SME feedback through structured review process
- Focus on financial accuracy and user experience

### Quality Metrics to Track

- **Routing Accuracy**: Correct agent selection rate
- **Response Quality**: Financial accuracy and completeness  
- **Latency**: Average response time by complexity
- **User Satisfaction**: Feedback from Review App
- **Cost Efficiency**: Token usage optimization

## 6. Best Practices Summary

### Prompt Engineering

- **Be Specific**: Include exact company names, date ranges, and metric types
- **Prioritize Simple Routing**: Default to Genie for single-step queries to reduce latency
- **Clear Boundaries**: Define precise criteria for when to use each agent
- **Handle Edge Cases**: Include guidance for follow-up questions and clarifications
- **Conservative Approach**: Err on the side of routing to Genie first
- **Data-Aware Examples**: Use examples that match your actual dataset

### System Configuration

- **Start Conservative**: Begin with lower max_iterations and simple routing bias
- **Monitor Performance**: Track latency, accuracy, and cost metrics
- **Iterate Based on Data**: Use MLflow traces to inform optimization decisions
- **Synchronize Components**: Keep Genie space and multi-agent prompts aligned

### Development Workflow

- **Test Before Deploy**: Use notebook environment for development iterations
- **Evaluate Systematically**: Run evaluation suite after each significant change
- **Document Changes**: Track prompt versions and performance impacts
- **Collaborate with SMEs**: Involve domain experts in evaluation and feedback

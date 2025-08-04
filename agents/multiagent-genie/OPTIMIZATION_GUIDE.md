# Multi-Agent Prompt Optimization Guide

This guide covers how to optimize prompts for the multi-agent financial analysis system. The system uses three key prompt areas in `configs.yaml` that directly control agent behavior.

## Overview of Prompt Architecture

The multi-agent system has three main prompt components:

1. **Supervisor Agent Prompts** - Controls routing logic and decision-making
2. **Research Planning Prompt** - Determines when to use parallel query execution  
3. **Final Answer Prompt** - Formats the final response to users

## 1. Supervisor Agent System Prompt

**Location**: `configs.yaml` → `agent_configs.supervisor_agent.system_prompt`

**Purpose**: Controls the overall routing logic and agent coordination strategy.

### Key Sections to Customize:

#### Data Limitations (Lines 27-28)
Update to reflect your actual data scope:
```yaml
**Data Limitations**: All agents only have access to SEC financial data (Income Statement and Balance Sheet) from 2003-2017 for companies like Apple Inc. (AAPL), Bank of America Corp (BAC), and American Express (AXP).
```

#### Decision Logic (Lines 40-52)
Critical for routing performance. Customize the criteria for:

**Direct to Genie (Simple Questions):**
- Single metric queries
- Basic calculations from one data source
- Simple year-over-year comparisons
- Single financial statement items

**Route to ParallelExecutor (Complex Analysis):**
- Multi-company comparative analysis
- Multiple interconnected financial ratios
- Trend analysis across multiple dimensions
- Questions requiring 3+ separate data queries

#### Available Agents Description (Line 38)
The `{formatted_descriptions}` placeholder gets populated from individual agent descriptions. Update those descriptions in:
- `agent_configs.genie_agent.description`
- `agent_configs.parallel_executor_agent.description`

### Best Practices:
- **Be Specific**: Include exact company names, date ranges, and metric types
- **Prioritize Simple Routing**: Default to Genie for single-step queries to reduce latency
- **Clear Boundaries**: Define precise criteria for when to use each agent
- **Handle Edge Cases**: Include guidance for follow-up questions and clarifications

## 2. Research Planning Prompt

**Location**: `configs.yaml` → `agent_configs.supervisor_agent.research_prompt`

**Purpose**: Determines when complex questions need parallel research execution.

### Key Customization Areas:

#### Bias Setting (Lines 67, 87)
Controls the default routing preference:
```yaml
**DEFAULT TO GENIE FIRST** - Only respond with should_plan_research=True if...
**BIAS TOWARD GENIE**: When in doubt, route to Genie first for single-step analysis.
```

#### Example Categories (Lines 69-80)
Update examples to match your data and use cases:

**Genie Examples (should_plan_research=False):**
- Single metric queries
- Basic calculations
- Simple time series requests
- Single company analysis

**ParallelExecutor Examples (should_plan_research=True):**
- Multi-company comparisons
- Multiple disconnected metrics
- Complex analysis requiring 3+ queries
- Cross-metric trend analysis

#### Query Planning Guidelines (Lines 82-86)
Specify constraints for generated research queries:
- Available data types and time ranges
- Supported companies/entities
- Required specificity levels

### Best Practices:
- **Conservative Approach**: Err on the side of routing to Genie first
- **Clear Thresholds**: Define specific criteria (e.g., "3+ separate queries")
- **Data-Aware Examples**: Use examples that match your actual dataset
- **Performance Consideration**: Remember parallel queries have overhead

## 3. Final Answer Prompt

**Location**: `configs.yaml` → `agent_configs.supervisor_agent.final_answer_prompt`

**Purpose**: Formats the final response based on the execution path taken.

### Key Customization Areas:

#### Response Style for Simple Questions (Lines 92-95)
Controls format for direct Genie responses:
```yaml
**For simple questions that went directly to Genie:**
- Provide a direct, concise answer to the specific question asked
- Include only the relevant financial data/metrics requested
- Keep the response brief and to the point
```

#### Response Style for Complex Questions (Lines 97-100)
Controls format for ParallelExecutor responses:
```yaml
**For complex questions that used ParallelExecutor:**
- Start with a brief "Research Summary" noting the parallel research approach
- Provide a comprehensive analysis synthesizing multiple data points
- Present findings in a structured financial analysis format
```

### Best Practices:
- **Differentiated Formatting**: Use different styles for simple vs. complex queries
- **User-Friendly**: Focus on answering the specific question asked
- **Structured Output**: Use consistent formatting for complex analyses
- **Avoid Over-Explanation**: Don't elaborate unnecessarily on methodology

## Common Optimization Scenarios

### Improving Routing Accuracy

**Problem**: Too many simple questions going to ParallelExecutor
**Solution**: Strengthen the bias toward Genie in both system_prompt and research_prompt

**Problem**: Complex questions being routed to Genie and failing
**Solution**: Add more specific examples of complex scenarios in research_prompt

### Enhancing Response Quality

**Problem**: Responses are too verbose
**Solution**: Update final_answer_prompt to be more concise, add specific length guidelines

**Problem**: Missing context in complex analyses
**Solution**: Update final_answer_prompt to include more structured formatting requirements

### Performance Optimization

**Problem**: System is too slow
**Solution**: Increase bias toward Genie routing, raise threshold for ParallelExecutor usage

**Problem**: Parallel queries timing out
**Solution**: Update research_prompt to generate more focused, specific queries

## Testing Your Prompt Changes

After updating prompts, test with the sample questions in `driver.py` cells 141-145:

1. **Simple Questions**: Should route directly to Genie
   - "What was AAPL's revenue in 2015?"
   
2. **Complex Questions**: Should use ParallelExecutor
   - "Compare the profitability trends of AAPL, BAC, and AXP from 2010-2015"

3. **Edge Cases**: Test boundary conditions
   - Questions that could go either way
   - Follow-up clarification scenarios

## Monitoring and Iteration

- Use MLflow tracing to monitor routing decisions
- Track response quality and user satisfaction
- Monitor latency differences between routing paths
- Iterate based on real usage patterns

Remember: The goal is to balance accuracy, performance, and user experience. Start conservative (bias toward Genie) and gradually increase ParallelExecutor usage as you validate performance.
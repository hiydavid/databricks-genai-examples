# Multi-Agent System Optimization Guide

## Config File Structure (`configs.yaml`)

### Key Optimization Areas

#### 1. Agent LLM Models
- **validator_agent**: Uses `databricks-claude-3-7-sonnet` (lighter model for simple validation)
- **planning_agent**: Uses `databricks-claude-sonnet-4` (advanced reasoning for planning)
- **retrieval_agent**: Uses `databricks-claude-sonnet-4` (complex synthesis capabilities)

#### 2. Model Parameters
```yaml
llm_parameters:
  max_tokens: 4000-8000  # Increase for longer responses
  temperature: 0.0       # Keep at 0 for consistent outputs
```

## Agent Prompt Optimization

### Validator Agent (`agent_configs.validator_agent.prompt`)
**Purpose**: Company existence validation
**Key modifications**:
- Adjust company lookup criteria
- Modify handoff conditions
- Change error messaging for invalid companies

### Planning Agent (`agent_configs.planning_agent.prompt`) 
**Purpose**: Research strategy and task decomposition
**Key modifications**:
- Add new data source mappings (lines 32-34)
- Modify research plan structure (lines 48-57)
- Adjust tool selection logic
- Change output format requirements

### Retrieval Agent (`agent_configs.retrieval_agent.prompt`)
**Purpose**: Data retrieval and final response synthesis
**Key modifications**:
- Tool selection criteria (lines 85-99)
- Parallel execution strategy (lines 127-139)
- Response synthesis instructions (lines 114-125)
- Citation and evidence requirements

## Tool Configuration

### Vector Search (`tool_configs.retrievers`)
```yaml
parameters:
  k: 5                    # Number of results per search
  query_type: hybrid     # Search type (hybrid, semantic, keyword)
```

### Search Tools (`tool_configs.indexes`)
- **sec_10k_business**: Business strategy, operations, competitive landscape
- **sec_10k_others**: Risks, legal proceedings, financial policies  
- **earnings_call**: Recent performance, management guidance

## Common Optimizations

### 1. Improve Search Quality
- Increase `k` parameter for more comprehensive results
- Modify tool descriptions to better target specific content types
- Adjust embedding model if needed

### 2. Enhance Planning Logic
- Update data source mappings in planning agent prompt
- Add new research categories or modify existing ones
- Adjust parallel execution strategy

### 3. Refine Response Quality
- Modify synthesis instructions in retrieval agent
- Adjust citation requirements
- Change response structure templates

### 4. Performance Tuning
- Increase `max_tokens` for longer, more detailed responses
- Adjust temperature (keep at 0.0 for consistency)
- Optimize parallel tool execution patterns

## Quick Reference: What to Modify

| To Change | Modify This Section |
|-----------|-------------------|
| Company validation logic | `agent_configs.validator_agent.prompt` |
| Research planning strategy | `agent_configs.planning_agent.prompt` lines 30-60 |
| Tool selection criteria | `agent_configs.retrieval_agent.prompt` lines 84-99 |
| Response synthesis | `agent_configs.retrieval_agent.prompt` lines 114-140 |
| Search parameters | `tool_configs.retrievers.parameters` |
| Model capabilities | `agent_configs.*.llm` sections |
# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Workflow

### Core Commands

- **Test agent locally**: Run `src/driver.ipynb` in Jupyter/Databricks notebooks
- **Install dependencies**: `pip install -r src/requirements.txt`
- **Deploy with Databricks bundles**: `databricks bundle deploy --target dev`

### Configuration Setup

Before development, update `src/config.yaml` with your Databricks settings:

- Set your catalog/schema for Unity Catalog access
- Configure Genie space_id for natural language queries  
- Update workspace_url and warehouse_id for your environment
- Ensure databricks_pat secret scope is configured
- Configure vector search index and schema mapping for document retrieval
- Customize parsing prompts for LLM-based filter extraction

## Architecture Overview

### Core Components

The system implements an **OpenAI Responses Agent** using MLflow's ResponsesAgent base class with three key integration layers:

1. **Tool System**: Extensible framework using `ToolInfo` wrapper pattern
   - Unity Catalog functions via `UCFunctionToolkit`
   - Databricks Genie via `databricks-ai-bridge`
   - Vector Search via `VectorSearchRetrieverTool`

2. **Agent Engine**: `MultiAgent` class handles:
   - Streaming responses with proper event marshalling
   - Message format conversion between Responses API and OpenAI ChatCompletion
   - Tool execution with automatic MLflow tracing
   - Conversation state management

3. **MLflow Integration**: Automatic logging and tracing for agent workflows

### Key Design Patterns

**Tool Creation Pattern**: All tools follow the same `ToolInfo` wrapper:

```python
def create_custom_tool(params):
    tool_spec = {"type": "function", "function": {...}}
    def exec_fn(**kwargs): # Tool implementation
    return ToolInfo(name, spec, exec_fn)
```

**Configuration-Driven**: All agent behavior controlled via `config.yaml` using `mlflow.models.ModelConfig`

**Streaming Architecture**: The agent implements proper streaming with `ResponsesAgentStreamEvent` handling for real-time interactions

### Critical Integration Points

- **Genie Integration**: Uses `databricks_ai_bridge.Genie.ask_question()` for simplified natural language data queries
- **Unity Catalog Tools**: Converts UC function names with `__` to `.` notation for proper UDF execution  
- **Vector Search Integration**: Self-querying retriever with automatic company/year filter extraction using LLM parsing
- **Message Format Conversion**: `_responses_to_cc()` method handles critical format translation between API specifications
- **Tool Registry**: `TOOL_INFOS` global list populated at module load time based on configuration

### Vector Search Architecture

The system implements intelligent document retrieval with automatic filter extraction:

- **LLM-Powered Parsing**: Extracts company names and years from natural language queries
- **Filter Construction**: Converts parsed filters to Databricks Vector Search format with proper data types
- **Smart Query Refinement**: Removes filter terms from search query to improve semantic matching
- **Configurable Prompts**: Parsing logic controlled via `config.yaml` for easy tuning

**Filter Format**:

- Company names as strings: `{"company": "APPLE"}`
- Years as integers: `{"year": [2022, 2023]}` (creates OR condition)
- Schema validation ensures only valid fields are used

### Databricks Bundle Structure

- `databricks.yml` defines the bundle with development target
- Agent deployment managed through Databricks Asset Bundles
- MLflow model registration handled automatically via `mlflow.models.set_model()`

### Development Notes

- The agent automatically initializes with configured tools on import
- MLflow experiment tracking is set up in `driver.ipynb` using experiment ID lookup with `mlflow.get_experiment()` and `mlflow.set_experiment()`
- All tool executions are automatically traced with MLflow spans
- Vector search requires proper schema configuration in `config.yaml` with field mappings
- The system supports both DataFrame and string responses from Genie
- LLM parsing handles markdown code block extraction automatically

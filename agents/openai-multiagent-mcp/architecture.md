# Architecture Diagram

## MCP Tool-Calling Agent Architecture

```
┌────────────────────────────────────────────────────────────────────┐
│                          User Request                              │
│                     (Natural Language Query)                       │
└────────────────────────────────┬───────────────────────────────────┘
                                 │
                                 ▼
┌────────────────────────────────────────────────────────────────────┐
│                    MCPToolCallingAgent                             │
│                   (MLflow ResponsesAgent)                          │
│                                                                    │
│  ┌────────────────────────────────────────────────────────────┐    │
│  │  System Prompt + User Message                              │    │
│  └────────────────────────────────────────────────────────────┘    │
│                                 │                                  │
│                                 ▼                                  │
│  ┌────────────────────────────────────────────────────────────┐    │
│  │  LLM (Databricks Foundation Model Serving)                 │    │
│  │  - Endpoint: databricks-claude-sonnet-4-5                  │    │
│  │  - Decides which tool(s) to call                           │    │
│  └────────────────────────────────────────────────────────────┘    │
│                                 │                                  │
│                                 ▼                                  │
│  ┌────────────────────────────────────────────────────────────┐    │
│  │  Tool Execution Router                                     │    │
│  │  - Parses tool calls                                       │    │
│  │  - Routes to appropriate MCP server                        │    │
│  │  - Handles retries with backoff                            │    │
│  └────────────────────────────────────────────────────────────┘    │
└────────────────────┬───────────────┬──────────────┬────────────────┘
                     │               │              │
         ┌───────────┘               │              └───────────┐
         │                           │                          │
         ▼                           ▼                          ▼
┌────────────────────┐  ┌────────────────────┐  ┌────────────────────┐
│   MCP Server #1    │  │   MCP Server #2    │  │   MCP Server #3    │
│                    │  │                    │  │                    │
│  Databricks Genie  │  │  Vector Search     │  │  UC Functions      │
│                    │  │                    │  │                    │
│  /api/2.0/mcp/     │  │  /api/2.0/mcp/     │  │  /api/2.0/mcp/     │
│  genie/{space_id}  │  │  vector-search/    │  │  functions/        │
│                    │  │  users/{user}      │  │  {schema_name}     │
└─────────┬──────────┘  └─────────┬──────────┘  └─────────┬──────────┘
          │                       │                        │
          ▼                       ▼                        ▼
┌────────────────────┐  ┌────────────────────┐  ┌────────────────────┐
│ Natural Language   │  │ Financial Docs     │  │ Python Execution   │
│ to SQL             │  │ Vector Index       │  │ Sandbox            │
│                    │  │                    │  │                    │
│ • Query generation │  │ • Semantic search  │  │ • system.ai.       │
│ • SQL execution    │  │ • Company filter   │  │   python_exec()    │
│ • Result retrieval │  │ • Year filter      │  │ • Calculations     │
└────────────────────┘  └────────────────────┘  └────────────────────┘
          │                       │                        │
          └───────────────────────┴────────────────────────┘
                                  │
                                  ▼
                    ┌──────────────────────────┐
                    │   MLflow Tracing         │
                    │   - Tool calls logged    │
                    │   - LLM calls traced     │
                    │   - Full conversation    │
                    └──────────────────────────┘
                                  │
                                  ▼
                    ┌──────────────────────────┐
                    │  Streaming Response      │
                    │  - Text chunks           │
                    │  - Tool outputs          │
                    │  - Final answer          │
                    └──────────────────────────┘
```

## Flow Description

1. **User Request**: User submits a natural language query about data or documents

2. **Agent Processing**: MCPToolCallingAgent receives the request and:
   - Combines system prompt with user message
   - Calls the LLM endpoint to determine appropriate actions

3. **Tool Selection**: LLM analyzes the query and decides which tool(s) to call:
   - **Genie**: For structured data queries (financial metrics, ratios)
   - **Vector Search**: For document-based research (company filings, qualitative analysis)
   - **UC Functions**: For calculations or data processing on tool results

4. **MCP Server Execution**: Selected tool(s) execute via managed MCP servers:
   - Each server is authenticated via Databricks workspace credentials
   - Tools return results in standardized format

5. **Iteration**: Agent can make multiple tool calls in sequence, using results from one tool as input to another

6. **Response**: Final answer is streamed back to user with full MLflow tracing

## Key Components

- **MCPToolCallingAgent**: Extends MLflow's ResponsesAgent with MCP tool integration
- **ToolInfo**: Wraps each tool with OpenAI-compatible spec and execution function
- **DatabricksMCPClient**: Handles authentication and communication with managed MCP servers
- **MLflow Integration**: Automatic tracing of all LLM calls and tool executions

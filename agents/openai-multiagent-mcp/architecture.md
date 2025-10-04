# Architecture Diagram

## MCP Tool-Calling Agent Architecture

```txt
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
         ┌───────────┴───────┬───────┴───────┬──────┴────────┐
         │                   │               │               │
         ▼                   ▼               ▼               ▼
┌────────────────┐  ┌────────────────┐  ┌────────────┐  ┌────────────┐
│ MCP Server #1  │  │ MCP Server #2  │  │MCP Srv #3  │  │MCP Srv #4  │
│                │  │                │  │(system/ai) │  │(users/     │
│Databricks      │  │Vector Search   │  │            │  │first_last) │
│Genie           │  │                │  │/api/2.0/   │  │            │
│/api/2.0/mcp/   │  │/api/2.0/mcp/   │  │mcp/        │  │/api/2.0/   │
│genie/          │  │vector-search/  │  │functions/  │  │mcp/        │
│{space_id}      │  │users/{user}    │  │system/ai   │  │functions/  │
│                │  │                │  │            │  │users/      │
│                │  │                │  │            │  │first_last  │
└────────┬───────┘  └────────┬───────┘  └─────┬──────┘  └─────┬──────┘
         │                   │                │               │
         ▼                   ▼                ▼               ▼
┌────────────────┐  ┌────────────────┐  ┌────────────┐  ┌────────────┐
│Natural Language│  │Financial Docs  │  │Python      │  │Web Search  │
│to SQL          │  │Vector Index    │  │Execution   │  │            │
│                │  │                │  │Sandbox     │  │• users.    │
│• Query gen     │  │• Semantic      │  │            │  │  first_    │
│• SQL exec      │  │  search        │  │• system.ai.│  │  last.     │
│• Result        │  │• Company       │  │  python_   │  │  search_   │
│  retrieval     │  │  filter        │  │  exec()    │  │  web()     │
│                │  │• Year filter   │  │• Calcs     │  │• UC Conn:  │
│                │  │                │  │            │  │  you-com-  │
│                │  │                │  │            │  │  api       │
│                │  │                │  │            │  │• Real-time │
│                │  │                │  │            │  │  info      │
└────────────────┘  └────────────────┘  └────────────┘  └────────────┘
         │                   │                │               │
         └───────────────────┴────────────────┴───────────────┘
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
   - **UC Functions (system/ai)**: For calculations or data processing on tool results
   - **UC Functions (users/first_last)**: For web search when information isn't in existing knowledge bases

4. **MCP Server Execution**: Selected tool(s) execute via managed MCP servers:
   - Each server is authenticated via Databricks workspace credentials
   - Web search function uses UC Connection (you-com-api) for external API access
   - Tools return results in standardized format

5. **Iteration**: Agent can make multiple tool calls in sequence, using results from one tool as input to another

6. **Response**: Final answer is streamed back to user with full MLflow tracing

## Key Components

- **MCPToolCallingAgent**: Extends MLflow's ResponsesAgent with MCP tool integration
- **ToolInfo**: Wraps each tool with OpenAI-compatible spec and execution function
- **DatabricksMCPClient**: Handles authentication and communication with managed MCP servers
- **MLflow Integration**: Automatic tracing of all LLM calls and tool executions

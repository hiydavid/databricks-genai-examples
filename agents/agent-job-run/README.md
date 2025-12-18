# (WIP) Async Agentic Workflow with Databricks Lakeflow Jobs

> **Note**: This demo is designed to run on **Databricks notebooks**. Clone or import this repo directly into your Databricks workspace.

This demo shows how to use **Databricks Lakeflow Jobs** to execute async agentic workflows that can run longer than 5 minutes without blocking the main agent.

## Key Concepts

- **Planner Agent** (interactive): Converses with users to develop research plans
- **Lakeflow Job submission**: Plan handed off to async job execution
- **Non-blocking polling**: Main agent can check status without waiting
- **UC Volume output**: Research reports saved to Unity Catalog Volume

## Architecture

```text
┌─────────────────┐     ┌──────────────────────┐     ┌─────────────────┐
│  User           │     │  Planner Agent       │     │  Lakeflow Job   │
│  (Notebook)     │────▶│  (Interactive)       │────▶│  (Async)        │
└─────────────────┘     └──────────────────────┘     └────────┬────────┘
                               │                              │
                               │ poll status                  │
                               │◀─────────────────────────────│
                               │                              │
                               │                     ┌────────▼────────┐
                               │                     │  UC Volume      │
                               │                     │  (Report)       │
                               └────────────────────▶└─────────────────┘
```

## Files

```text
src/
├── config.template.yaml     # Configuration template (copy to config.yaml)
├── job_tools.py             # Job submission/polling utilities
├── prompts.py               # Templates for reflection, synthesis, summary
├── models/
│   ├── research_plan.py     # Input plan with budget parameters
│   └── research_state.py    # Tracks findings, gaps, confidence
├── sdk/
│   ├── config.py            # SDK configuration, MLflow autolog, MCP init
│   ├── context.py           # ResearchContext, PlannerContext dataclasses
│   ├── planner_agent.py     # Planner with job submission tools
│   ├── report.py            # Report generation
│   └── researcher_agent.py  # Researcher with MCP search + state tools
└── notebooks/
    ├── 01_demo.ipynb        # Main interactive demo notebook
    └── 02_researcher_job.py # Notebook executed by Lakeflow Job
```

## Setup

### 1. Deploy to Databricks Workspace

Upload the `src/` directory to your Databricks workspace:

```text
/Workspace/Users/{your_email}/agent-job-run/src/
```

### 2. Configure External MCP Server

Register an external MCP server with web search capability in the MCP Catalog. The URL format is:

```text
https://<workspace>/api/2.0/mcp/external/{connection_name}
```

See [External MCP documentation](https://docs.databricks.com/aws/en/generative-ai/mcp/external-mcp) for setup instructions.

### 3. Update Configuration

Copy `src/config.template.yaml` to `src/config.yaml` and update:

- `llm.endpoint_name`: Your Databricks Foundation Model endpoint
- `mcp.connection_name`: Your external MCP server connection name from MCP Catalog
- `paths.researcher_notebook`: Workspace path to `02_researcher_job.py`
- `paths.output_volume`: UC Volume path (auto-created if it doesn't exist)

## Usage

1. Open `src/notebooks/01_demo.ipynb` in Databricks
2. Run the setup cells
3. Start a conversation with the Planner Agent:

   ```python
   response = agent.chat("I want to research trends in generative AI")
   ```

4. Refine the research plan through conversation
5. Approve the plan to trigger async job:

   ```python
   response = agent.chat("Looks good, let's run it")
   ```

6. Ask for status (agent uses `list_active_jobs` tool automatically):

   ```python
   response = agent.chat("How's my research job going?")
   ```

7. Retrieve the report when complete:

   ```python
   response = agent.chat("Show me the research report")
   ```

## How It Works

### Plan Serialization

Research plans are serialized as base64-encoded JSON and passed via job parameters:

```python
plan_b64 = research_plan.to_base64()
w.jobs.submit(..., base_parameters={"research_plan_b64": plan_b64})
```

### Non-Blocking Job Submission

`w.jobs.submit()` returns immediately with a `run_id`:

```python
waiter = w.jobs.submit(run_name="research", tasks=[...])
run_id = waiter.run_id  # Returns immediately
```

### Serverless Compute

The async job uses Databricks serverless compute for fast startup.

### Auto UC Volume Creation

The UC Volume is automatically created if it doesn't exist when submitting a job:

```python
# In job_tools.py - ensure_volume_exists()
workspace_client.volumes.create(
    catalog_name=catalog_name,
    schema_name=schema_name,
    name=volume_name,
    volume_type=catalog_service.VolumeType.MANAGED,
)
```

### MCP Tool Integration

The Researcher Agent uses Databricks external MCP for web search via `databricks_openai`:

```python
from databricks_openai.agents import McpServer

async def init_mcp_server(host: str, connection_name: str) -> McpServer:
    return McpServer(
        url=f"{host}/api/2.0/mcp/external/{connection_name}",
        name=f"{connection_name}-mcp",
    )

# Usage with async context manager
async with await init_mcp_server(host, connection_name) as mcp_server:
    agent = Agent(..., mcp_servers=[mcp_server])
```

## Requirements

- Databricks workspace with:
  - Foundation Model API access (e.g., `databricks-claude-sonnet-4`)
  - Unity Catalog with Volume support
  - MCP server with web search function
  - DBR 14.3+ or MLR 14.3+ (for Python 3.11+ support)

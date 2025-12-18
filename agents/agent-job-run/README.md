# (WIP) Async Agentic Workflow with Databricks Lakeflow Jobs

`**THIS IS EXPERIMENAL AND UNDER ACTIVE DEVELOPMENT**`

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
├── models/
│   └── research_plan.py     # Pydantic model for plan serialization
├── planner_agent.py         # Interactive agent for research planning
├── researcher_agent.py      # Agent that executes research (runs in job)
├── job_tools.py             # Job submission/polling utilities
├── config.template.yaml     # Configuration template
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

### 2. Configure MCP Server

Set up a managed MCP server with web search capability. The MCP URL format is:

```text
https://<workspace>/api/2.0/mcp/functions/{catalog}/{schema}
```

### 3. Update Configuration

Copy `src/config.template.yaml` to `src/config.yaml` and update:

- `llm.endpoint_name`: Your Databricks Foundation Model endpoint
- `mcp.catalog/schema`: Your MCP server location
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

The Researcher Agent uses Databricks MCP for web search:

```python
from databricks_mcp import DatabricksMCPClient

mcp_client = DatabricksMCPClient(
    server_url=f"{ws.config.host}/api/2.0/mcp/functions/{catalog}/{schema}",
    workspace_client=ws,
)
```

## Requirements

- Databricks workspace with:
  - Foundation Model API access (e.g., `databricks-claude-sonnet-4`)
  - Unity Catalog with Volume support
  - MCP server with web search function
- Python 3.11+

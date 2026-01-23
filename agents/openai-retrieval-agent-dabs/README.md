# Deploying a Simple Retrieval Agent with DABS

A retrieval-augmented generation (RAG) agent that parses PDF documents, creates a Vector Search index, and deploys an AI agent using the OpenAI Chat Completions API with MCP (Model Context Protocol) tool calling.

## Overview

This example demonstrates an end-to-end retrieval agent pipeline:

1. **Document Ingestion**: Parse PDFs using `ai_parse_document` and store chunks in Delta
2. **Vector Search Index**: Create a Delta Sync index with automatic embeddings
3. **Agent Implementation**: Build a retrieval agent using OpenAI API with MCP tools
4. **Deployment**: Deploy the agent to Model Serving via `agents.deploy()`
5. **Evaluation**: Evaluate agent quality using MLflow's evaluation framework

## Architecture

```text
┌───────────────────────────────────────────────────────────────────────┐
│                          ORCHESTRATION LAYER                          │
│                     (DABs + Lakeflow Jobs Pipeline)                   │
└───────────────────────────────────────────────────────────────────────┘
                                    │
        ┌───────────────────────────┼───────────────────────────┐
        ▼                           ▼                           ▼
┌───────────────┐         ┌─────────────────┐         ┌────────────────┐
│  Task 1:      │         │  Task 2:        │         │  Task 3:       │
│  Document     │────────▶│  Vector Search  │────────▶│  Agent         │
│  Ingestion    │         │  Index Creation │         │  Deployment    │
└───────────────┘         └─────────────────┘         └────────────────┘
        │                          │                          │
        ▼                          ▼                          ▼
┌───────────────┐         ┌─────────────────┐         ┌────────────────┐
│ai_parse_doc() │         │ Delta Sync      │         │ OpenAI API +   │
│ PDF → Text    │         │ VS Index        │         │ MCP Tools      │
└───────────────┘         └─────────────────┘         └────────────────┘
                                                              │
                                                              ▼
                                                      ┌────────────────┐
                                                      │ Model Serving  │
                                                      │ Endpoint       │
                                                      └────────────────┘
```

## Directory Structure

```text
agents/openai-retrieval-agent-dabs/
├── README.md                           # This file
├── databricks.template.yml             # DABs config template (copy to databricks.yml)
├── requirements.txt                    # Python dependencies
├── src/
│   ├── configs.template.yaml           # Agent runtime config template (LLM settings)
│   ├── 01_ingest_documents.py          # PDF parsing with ai_parse_document
│   ├── 02_create_vector_index.py       # Vector Search index creation
│   ├── agent.py                        # Retrieval agent with MCP
│   ├── 03_deployment.py                # MLflow logging and agents.deploy()
│   └── 04_evaluation.py                # Agent evaluation with MLflow
└── resources/
    ├── 01_full_pipeline.job.yml        # Full end-to-end pipeline
    ├── 02_index_update.job.yml         # Update index only
    ├── 03_agent_deploy.job.yml         # Deploy agent only
    └── 04_evaluation.job.yml           # Run evaluation only
```

## Prerequisites

1. **Databricks Workspace** with:
   - Unity Catalog enabled
   - Model Serving enabled
   - Vector Search enabled
   - `ai_parse_document` function available

2. **Resources to create**:
   - A Unity Catalog volume containing your PDF documents
   - A Vector Search endpoint (or let the pipeline create one)

3. **Databricks CLI** installed and configured:

   ```bash
   databricks auth login --host https://YOUR_WORKSPACE.cloud.databricks.com
   ```

## Configuration

Infrastructure settings are defined as DAB variables in `databricks.yml`:

| Variable | Description | Default |
| ---------- | ------------- | --------- |
| `catalog` | Unity Catalog name | `main` |
| `schema` | Schema name | `default` |
| `vs_endpoint` | Vector Search endpoint name | `retrieval-agent-vs-endpoint` |
| `run_as_service_principal` | SP application ID for `run_as` | `""` (required) |

The MLflow experiment is created at `/Shared/experiments/retrieval-agent-mcp`. In development mode, DABs adds a `[dev ...]` prefix automatically.

Agent runtime settings (LLM config) are in `src/configs.yaml`.

## Setup

### Step 1: Clone and Navigate

```bash
cd agents/openai-retrieval-agent-dabs
```

### Step 2: Create Config Files

Copy the templates:

```bash
cp databricks.template.yml databricks.yml
cp src/configs.template.yaml src/configs.yaml
```

### Step 3: Configure Agent Runtime

Edit `src/configs.yaml` to configure the LLM endpoint:

```yaml
agent_configs:
  agent_name: user-guide-retrieval-agent
  llm:
    endpoint_name: databricks-claude-haiku-4-5  # Or your preferred model
    temperature: 0.1
    max_tokens: 4096
```

### Step 4: Configure DABs

Edit `databricks.yml` to set your workspace URL and optionally override variables:

```yaml
variables:
  catalog:
    default: your_catalog      # Change default catalog
  schema:
    default: your_schema       # Change default schema

targets:
  dev:
    workspace:
      host: https://YOUR_WORKSPACE.cloud.databricks.com
    # Optional: target-specific overrides
    # variables:
    #   catalog: dev_catalog
```

### Step 5: Upload Your Documents

Upload PDF documents to `/Volumes/{catalog}/{schema}/user_guides`.

## Deployment

### Option 1: Full Pipeline (Recommended for Initial Setup)

Deploy everything from scratch:

```bash
databricks bundle deploy
databricks bundle run full_pipeline
```

This will:

1. Parse all PDFs in the source volume
2. Create the Vector Search index
3. Deploy the agent to Model Serving

### Option 2: Individual Jobs

Run specific jobs as needed:

```bash
# Update index when documents change
databricks bundle run index_update

# Redeploy agent when code/config changes
databricks bundle run agent_deploy

# Run evaluation
databricks bundle run agent_evaluation
```

### Override Variables via CLI

Override any variable at deploy/run time:

```bash
# Deploy to a different catalog
databricks bundle deploy --var catalog=test_catalog --var schema=test_schema

# Run with overrides
databricks bundle run full_pipeline --var catalog=prod_catalog
```

## Available Jobs

| Job | Purpose | When to Use |
| ----- | --------- | ------------- |
| `full_pipeline` | Complete setup | Initial deployment or full refresh |
| `index_update` | Refresh documents | When documents are added/updated |
| `agent_deploy` | Redeploy agent | When agent code/config changes |
| `agent_evaluation` | Quality check | After deployment to measure quality |

## Configuration Details

### Script Variables

Each script has hardcoded variable names that derive paths from the DAB `catalog` and `schema` parameters. If you need to customize these paths, modify the following variables in the respective scripts:

**`src/01_ingest_documents.py`**

| Variable | Default Value | Description |
| ---------- | --------------- | ------------- |
| `SOURCE_VOLUME` | `/Volumes/{catalog}/{schema}/user_guide/guide` | Volume path containing PDF documents |
| `CHUNKS_TABLE` | `{catalog}.{schema}.user_guide_chunks` | Delta table for parsed chunks |
| `IMAGES_VOLUME` | `/Volumes/{catalog}/{schema}/user_guide/parsed_images` | Volume for extracted images |

**`src/02_create_vector_index.py`**

| Variable | Default Value | Description |
| ---------- | --------------- | ------------- |
| `CHUNKS_TABLE` | `{catalog}.{schema}.user_guide_chunks` | Source table for VS index |
| `VS_INDEX` | `{catalog}.{schema}.user_guide_chunks_index` | Vector Search index name |
| `EMBEDDING_MODEL` | `databricks-gte-large-en` | Embedding model endpoint |

**`src/03_deployment.py`**

| Variable | Default Value | Description |
| ---------- | --------------- | ------------- |
| `VS_INDEX` | `{catalog}.{schema}.user_guide_chunks_index` | VS index for MCP tool |
| `UC_MODEL_NAME` | `{catalog}.{schema}.retrieval_agent` | Unity Catalog model name |

**`src/04_evaluation.py`**

| Variable | Default Value | Description |
| ---------- | --------------- | ------------- |
| `UC_MODEL_NAME` | `{catalog}.{schema}.retrieval_agent` | Model to evaluate |
| `AGENT_ENDPOINT_NAME` | `{catalog}_{schema}_retrieval_agent` | Serving endpoint name |
| `EVAL_TABLE` | `{catalog}.{schema}.eval_dataset` | Evaluation dataset table |

**`src/configs.yaml`** (agent runtime config)

| Variable | Default Value | Description |
| ---------- | --------------- | ------------- |
| `agent_name` | `user-guide-retrieval-agent` | Agent identifier |
| `llm.endpoint_name` | `databricks-claude-haiku-4-5` | LLM model serving endpoint |
| `llm.temperature` | `0.1` | LLM temperature |
| `system_prompt` | *(see template)* | Agent system prompt |

### Vector Search

The pipeline uses Delta Sync with automatic embeddings:

- Embedding model: `databricks-gte-large-en`
- Index type: `TRIGGERED` (manual sync)

## Evaluation

Create an evaluation dataset table with columns:

- `question`: The user question
- `expected_answer`: The expected/reference answer

```sql
CREATE TABLE your_catalog.your_schema.eval_dataset (
  question STRING,
  expected_answer STRING
);

INSERT INTO your_catalog.your_schema.eval_dataset VALUES
  ('How do I configure the system?', 'Go to Settings > Configuration...'),
  ('What authentication methods are supported?', 'SSO, LDAP, and local accounts...');
```

Then run evaluation:

```bash
databricks bundle run agent_evaluation
```

View results in the MLflow UI under the experiment.

## Service Principal Configuration

Jobs are configured to run as a Service Principal via the `run_as` setting. This requires passing the SP application ID when deploying.

### SP Prerequisites

1. Create a Service Principal in Databricks Account Console
2. Grant deploying users **Use** role on the SP (Account Console → Service Principals → Permissions)
3. Grant SP required permissions (see below)
4. For CI/CD: Generate an OAuth secret for the SP

### Required Permissions

- **Unity Catalog**:
  - `USE CATALOG`, `USE SCHEMA` on target catalog/schema
  - `CREATE TABLE`, `MODIFY`, `SELECT` on tables
  - `READ VOLUME` on source volume (PDFs)
  - `WRITE VOLUME` on images volume (parsed output)
  - `CREATE MODEL` and `CREATE MODEL VERSION` on schema (for registering the agent)
- **Vector Search**: `CAN MANAGE` on the VS endpoint
- **Workspace Folder**: `CAN_MANAGE` on `/Shared/experiments` folder

### Local Development (IDE Terminal)

When deploying from your local machine, you authenticate with your personal OAuth. The `run_as` variable controls which SP the jobs execute as:

```bash
# Deploy (you authenticate as yourself, jobs run as SP)
databricks bundle deploy -t dev \
  --var run_as_service_principal=<sp_application_id>

# Run the pipeline
databricks bundle run full_pipeline -t dev \
  --var run_as_service_principal=<sp_application_id>
```

### CI/CD Pipelines

In CI/CD, the pipeline itself authenticates as the SP. Configure these as **secrets** in your CI/CD platform:

**GitHub Actions:**

```yaml
env:
  DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
  DATABRICKS_CLIENT_ID: ${{ secrets.DATABRICKS_CLIENT_ID }}
  DATABRICKS_CLIENT_SECRET: ${{ secrets.DATABRICKS_CLIENT_SECRET }}

steps:
  - run: |
      databricks bundle deploy -t prod \
        --var run_as_service_principal=${{ secrets.DATABRICKS_CLIENT_ID }}
```

**Azure DevOps:**

```yaml
variables:
  DATABRICKS_HOST: $(DATABRICKS_HOST)
  DATABRICKS_CLIENT_ID: $(DATABRICKS_CLIENT_ID)
  DATABRICKS_CLIENT_SECRET: $(DATABRICKS_CLIENT_SECRET)

steps:
  - script: |
      databricks bundle deploy -t prod \
        --var run_as_service_principal=$(DATABRICKS_CLIENT_ID)
```

## References

- [Databricks MCP Documentation](https://docs.databricks.com/en/generative-ai/agent-framework/mcp.html)
- [Vector Search Documentation](https://docs.databricks.com/en/generative-ai/vector-search/index.html)
- [agents.deploy() Documentation](https://docs.databricks.com/en/generative-ai/agent-framework/deploy-agent.html)
- [MLflow Evaluation](https://mlflow.org/docs/latest/llms/llm-evaluate/index.html)

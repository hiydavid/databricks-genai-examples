# Retrieval Agent with MCP

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
┌─────────────────────────────────────────────────────────────────────────────┐
│                          ORCHESTRATION LAYER                                │
│                     (DABs + Lakeflow Jobs Pipeline)                         │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
        ┌───────────────────────────┼───────────────────────────┐
        ▼                           ▼                           ▼
┌───────────────┐         ┌─────────────────┐         ┌─────────────────┐
│  Task 1:      │         │  Task 2:        │         │  Task 3:        │
│  Document     │────────▶│  Vector Search  │────────▶│  Agent          │
│  Ingestion    │         │  Index Creation │         │  Deployment     │
└───────────────┘         └─────────────────┘         └─────────────────┘
        │                           │                           │
        ▼                           ▼                           ▼
┌───────────────┐         ┌─────────────────┐         ┌─────────────────┐
│ai_parse_doc() │         │ Delta Sync      │         │ OpenAI API +    │
│ PDF → Text    │         │ VS Index        │         │ MCP Tools       │
└───────────────┘         └─────────────────┘         └─────────────────┘
                                                                │
                                                                ▼
                                                      ┌─────────────────┐
                                                      │ Model Serving   │
                                                      │ Endpoint        │
                                                      └─────────────────┘
```

## Directory Structure

```text
agents/openai-retrieval-agent-dabs/
├── README.md                           # This file
├── databricks.yml                      # DABs config + infrastructure variables
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
|----------|-------------|---------|
| `catalog` | Unity Catalog name | `main` |
| `schema` | Schema name | `default` |
| `experiment_name` | MLflow experiment path | `/Users/{user}/retrieval-agent-mcp` |
| `vs_endpoint` | Vector Search endpoint name | `retrieval-agent-vs-endpoint` |

Agent runtime settings (LLM config) are in `src/configs.yaml`.

## Setup

### Step 1: Clone and Navigate

```bash
cd agents/openai-retrieval-agent-dabs
```

### Step 2: Create Agent Config File

Copy the template:

```bash
cp src/configs.template.yaml src/configs.yaml
```

Edit `src/configs.yaml` to configure the LLM endpoint:

```yaml
agent_configs:
  agent_name: user-guide-retrieval-agent
  llm:
    endpoint_name: databricks-claude-haiku-4-5  # Or your preferred model
    temperature: 0.1
    max_tokens: 4096
```

### Step 3: Update DABs Configuration

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

### Step 4: Upload Your Documents

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

### Derived Paths

The following paths are automatically derived from the `catalog` and `schema` variables:

| Resource | Path |
|----------|------|
| Source Volume | `/Volumes/{catalog}/{schema}/user_guides` |
| Chunks Table | `{catalog}.{schema}.user_guide_chunks` |
| Images Volume | `/Volumes/{catalog}/{schema}/parsed_images` |
| Vector Index | `{catalog}.{schema}.user_guide_chunks_index` |
| UC Model | `{catalog}.{schema}.retrieval_agent` |
| Eval Table | `{catalog}.{schema}.eval_dataset` |

### LLM Endpoints

Configure in `src/configs.yaml`. Default: `databricks-claude-haiku-4-5`

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

## References

- [Databricks MCP Documentation](https://docs.databricks.com/en/generative-ai/agent-framework/mcp.html)
- [Vector Search Documentation](https://docs.databricks.com/en/generative-ai/vector-search/index.html)
- [agents.deploy() Documentation](https://docs.databricks.com/en/generative-ai/agent-framework/deploy-agent.html)
- [MLflow Evaluation](https://mlflow.org/docs/latest/llms/llm-evaluate/index.html)

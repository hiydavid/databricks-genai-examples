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

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          ORCHESTRATION LAYER                                 │
│                     (DABs + Lakeflow Jobs Pipeline)                          │
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

```
agents/openai-retrieval-agent-dabs/
├── README.md                           # This file
├── databricks.yml                      # DABs configuration
├── requirements.txt                    # Python dependencies
├── src/
│   ├── configs.template.yaml           # Configuration template (copy to configs.yaml)
│   ├── 01_ingest_documents.py          # PDF parsing with ai_parse_document
│   ├── 02_create_vector_index.py       # Vector Search index creation
│   ├── 03_agent.py                     # Retrieval agent with MCP
│   ├── 04_deployment.py                # MLflow logging and agents.deploy()
│   └── 05_evaluation.py                # Agent evaluation with MLflow
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

## Setup

### Step 1: Clone and Configure

```bash
cd agents/openai-retrieval-agent-dabs
```

### Step 2: Create Configuration File

Copy the template and fill in your values:

```bash
cp src/configs.template.yaml src/configs.yaml
```

Edit `src/configs.yaml`:

```yaml
databricks_configs:
  catalog: your_catalog          # Your Unity Catalog name
  schema: your_schema            # Your schema name
  workspace_url: https://YOUR_WORKSPACE.cloud.databricks.com/

agent_configs:
  agent_name: user-guide-retrieval-agent
  llm:
    endpoint_name: databricks-claude-sonnet-4    # Or your preferred model
    temperature: 0.1
    max_tokens: 4096
  vector_search:
    endpoint_name: your-vs-endpoint     # Your VS endpoint name
    index_name: your_catalog.your_schema.user_guide_chunks_index
    user_name: your_username            # Your Databricks username (for MCP URL)

document_configs:
  source_volume: /Volumes/your_catalog/your_schema/user_guides
  chunks_table: your_catalog.your_schema.user_guide_chunks
```

### Step 3: Update DABs Configuration

Edit `databricks.yml` to set your workspace URL:

```yaml
targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://YOUR_WORKSPACE.cloud.databricks.com
```

### Step 4: Upload Your Documents

Upload your PDF documents to the source volume:

```bash
databricks fs cp ./my-documents/*.pdf /Volumes/your_catalog/your_schema/user_guides/
```

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

## Usage

Once deployed, interact with your agent via the Model Serving endpoint:

### Using the Databricks SDK

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
client = w.serving_endpoints.get_open_ai_client()

response = client.chat.completions.create(
    model="your_catalog_your_schema_retrieval_agent",
    messages=[
        {"role": "user", "content": "How do I configure the system?"}
    ]
)

print(response.choices[0].message.content)
```

### Using the REST API

```bash
curl -X POST "https://YOUR_WORKSPACE.cloud.databricks.com/serving-endpoints/your_catalog_your_schema_retrieval_agent/invocations" \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "messages": [
      {"role": "user", "content": "How do I configure the system?"}
    ]
  }'
```

## Available Jobs

| Job | Purpose | When to Use |
|-----|---------|-------------|
| `full_pipeline` | Complete setup | Initial deployment or full refresh |
| `index_update` | Refresh documents | When documents are added/updated |
| `agent_deploy` | Redeploy agent | When agent code/config changes |
| `agent_evaluation` | Quality check | After deployment to measure quality |

## Configuration Options

### LLM Endpoints

Supported Databricks model serving endpoints:
- `databricks-claude-sonnet-4` (default)
- `databricks-claude-3-5-sonnet`
- `databricks-meta-llama-3-3-70b-instruct`
- Custom fine-tuned models

### Vector Search

The pipeline uses Delta Sync with automatic embeddings:
- Embedding model: `databricks-gte-large-en`
- Index type: `TRIGGERED` (manual sync)

## Troubleshooting

### Common Issues

1. **Vector Search endpoint not found**
   - Ensure Vector Search is enabled in your workspace
   - Check that the endpoint name in config matches your endpoint

2. **ai_parse_document errors**
   - Verify your workspace supports this function
   - Check that PDFs are valid and not corrupted

3. **MCP connection errors**
   - Verify the `user_name` in config matches your Databricks username
   - Check that Vector Search MCP is enabled

4. **Deployment fails**
   - Check Model Serving logs in the Databricks UI
   - Verify all resources (LLM endpoint, VS index) exist and are accessible

### Logs

View job logs:
```bash
databricks runs get-output --run-id <RUN_ID>
```

View Model Serving logs in the Databricks UI under Machine Learning > Model Serving.

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

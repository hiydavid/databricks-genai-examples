# Retrieval Agent with MCP - Implementation Plan

## Overview

Build an end-to-end retrieval agent that:
1. Parses user guide PDFs using `ai_parse_document`
2. Creates a Vector Search index from parsed documents
3. Builds an AI agent using OpenAI API with MCP tool calls to query the VS index
4. Deploys the agent via `agents.deploy()`
5. Uses DABs and Lakeflow Jobs for orchestration and deployment

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
agents/retrieval-agent-mcp/
├── README.md                           # Documentation and setup guide
├── databricks.template.yml             # DABs configuration
├── requirements.txt                    # Python dependencies
├── src/
│   ├── configs.template.yaml           # Agent configuration (user customizable)
│   ├── 01_ingest_documents.py          # PDF parsing with ai_parse_document
│   ├── 02_create_vector_index.py       # Vector Search index creation
│   ├── 03_agent.py                     # Retrieval agent with MCP
│   ├── 04_deployment.py                # MLflow logging and agents.deploy()
│   └── utils/
│       └── helpers.py                  # Shared utilities
└── resources/
    └── retrieval_pipeline.job.yml      # Lakeflow Jobs definition
```

## Implementation Tasks

### Task 1: Document Ingestion (`01_ingest_documents.py`)

**Purpose**: Parse user guide PDFs and store extracted content in Delta table

**Key Components**:
- Read PDFs from Unity Catalog Volume using Spark streaming
- Use `ai_parse_document()` SQL function for parsing
- Extract and clean text content
- Store in Delta table with metadata

**Reference Pattern**: `/home/user/databricks-genai-examples/batch-inference/e2e-entity-extraction/src/pipeline/01_ingest.py`

**Implementation Details**:
```python
# Key imports
from pyspark.sql.functions import expr, col, explode, current_timestamp

# Read PDFs from volume
files_df = spark.readStream.format("binaryFile").load(SOURCE_VOLUME)

# Parse with ai_parse_document
parsed_df = files_df.withColumn(
    "parsed_result",
    expr("ai_parse_document(content)")
)

# Extract text elements and chunk for embedding
chunked_df = parsed_df.select(
    "path",
    explode("parsed_result.document.elements").alias("element")
).filter(col("element.type").isin(["text", "section_header"]))

# Write to Delta table
chunked_df.writeStream.format("delta").toTable(OUTPUT_TABLE)
```

**Output**: Delta table with columns: `doc_id`, `chunk_id`, `text_content`, `source_path`, `page_number`, `element_type`, `parsed_at`

---

### Task 2: Vector Search Index Creation (`02_create_vector_index.py`)

**Purpose**: Create Delta Sync Vector Search index from parsed documents

**Key Components**:
- Create Vector Search endpoint (if not exists)
- Create Delta Sync index with embedding model
- Wait for index to sync

**Reference Pattern**: `/home/user/databricks-genai-examples/vector-search/multi-index-search.py`

**Implementation Details**:
```python
from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient()

# Create endpoint
vsc.create_endpoint(
    name=VS_ENDPOINT_NAME,
    endpoint_type="STANDARD"
)

# Create Delta Sync index
vsc.create_delta_sync_index(
    endpoint_name=VS_ENDPOINT_NAME,
    index_name=VS_INDEX_NAME,
    source_table_name=SOURCE_TABLE,
    primary_key="chunk_id",
    pipeline_type="TRIGGERED",
    embedding_source_column="text_content",
    embedding_model_endpoint_name="databricks-gte-large-en"
)

# Sync index
index = vsc.get_index(endpoint_name=VS_ENDPOINT_NAME, index_name=VS_INDEX_NAME)
index.sync()
```

**Output**: Synced Vector Search index with embedded document chunks

---

### Task 3: Agent Implementation (`03_agent.py`)

**Purpose**: Build retrieval agent using OpenAI API with MCP tool for Vector Search

**Key Components**:
- Use `DatabricksMCPClient` for Vector Search MCP integration
- OpenAI ResponsesAgent for orchestration
- MLflow ChatAgent wrapper for deployment

**Reference Pattern**: `/home/user/databricks-genai-examples/agents/openai-multiagent-mcp/src/agent.py`

**Implementation Details**:
```python
import mlflow
from openai import OpenAI
from databricks_mcp import DatabricksMCPClient
from mlflow.pyfunc import ChatAgent

# MCP Setup for Vector Search
host = config["databricks_configs"]["workspace_url"]
user_name = config["agent_configs"]["vector_search"]["user_name"]
VS_MCP_URL = f"{host}/api/2.0/mcp/vector-search/users/{user_name}"

# Tool wrapper class
class ToolInfo(BaseModel):
    name: str
    spec: dict
    exec_fn: Callable

# Agent implementation
class RetrievalAgent(ChatAgent):
    def __init__(self, config):
        self.client = OpenAI(
            api_key=os.environ["DATABRICKS_TOKEN"],
            base_url=f"{config['workspace_url']}/serving-endpoints"
        )
        self.mcp_client = DatabricksMCPClient()
        self.tools = self._load_mcp_tools()

    def _load_mcp_tools(self):
        # Load Vector Search MCP tools
        tools = []
        async def load():
            async with self.mcp_client.connect(VS_MCP_URL) as client:
                for tool in await client.list_tools():
                    tools.append(ToolInfo(
                        name=tool.name,
                        spec=self._to_openai_schema(tool),
                        exec_fn=lambda args, t=tool: client.call_tool(t.name, args)
                    ))
        asyncio.run(load())
        return tools

    @mlflow.trace(span_type="AGENT")
    def predict(self, messages, context=None, custom_inputs=None):
        # OpenAI Response API loop with tool execution
        response = self.client.responses.create(
            model=self.model_endpoint,
            input=messages,
            tools=[t.spec for t in self.tools]
        )

        while response.output and has_tool_calls(response):
            tool_results = self._execute_tools(response.output)
            response = self.client.responses.create(
                model=self.model_endpoint,
                input=messages + tool_results,
                tools=[t.spec for t in self.tools]
            )

        return ChatAgentResponse(messages=[...])

# Register with MLflow
AGENT = RetrievalAgent(config)
mlflow.models.set_model(AGENT)
```

**Key Features**:
- MCP integration for Vector Search tool discovery
- OpenAI Response API for agent orchestration
- MLflow tracing for observability
- Streaming support for real-time responses

---

### Task 4: Deployment (`04_deployment.py`)

**Purpose**: Log agent to MLflow, register in Unity Catalog, deploy to Model Serving

**Key Components**:
- MLflow model logging with resources
- Unity Catalog registration
- `agents.deploy()` for endpoint creation

**Reference Pattern**: `/home/user/databricks-genai-examples/agents/agent-dabs/src/deployment.py`

**Implementation Details**:
```python
import mlflow
from databricks import agents
from mlflow.models.resources import (
    DatabricksServingEndpoint,
    DatabricksVectorSearchIndex
)

# Set MLflow tracking
mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(EXPERIMENT_PATH)

# Log model
with mlflow.start_run():
    logged_agent_info = mlflow.pyfunc.log_model(
        artifact_path="agent",
        python_model="03_agent.py",
        model_config="configs.yaml",
        input_example={
            "messages": [{"role": "user", "content": "How do I configure the system?"}]
        },
        resources=[
            DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT_NAME),
            DatabricksVectorSearchIndex(index_name=VS_INDEX_NAME),
        ],
        pip_requirements=["mlflow>=2.20", "openai", "databricks-mcp", "pydantic"],
    )

# Register to Unity Catalog
UC_MODEL_NAME = f"{CATALOG}.{SCHEMA}.retrieval_agent"
uc_registered_model_info = mlflow.register_model(
    model_uri=logged_agent_info.model_uri,
    name=UC_MODEL_NAME
)

# Deploy agent
agents.deploy(
    UC_MODEL_NAME,
    uc_registered_model_info.version,
    tags={"endpointSource": "retrieval-agent-mcp"},
)

print(f"Agent deployed: {UC_MODEL_NAME} v{uc_registered_model_info.version}")
```

---

### Task 5: Configuration Files

#### `configs.template.yaml`
```yaml
databricks_configs:
  catalog: YOUR_CATALOG_NAME
  schema: YOUR_SCHEMA_NAME
  workspace_url: https://YOUR_WORKSPACE.cloud.databricks.com/

agent_configs:
  agent_name: user-guide-retrieval-agent
  llm:
    endpoint_name: "databricks-claude-3-7-sonnet"  # or databricks-meta-llama-3-3-70b-instruct
    temperature: 0.1
    max_tokens: 4096
  vector_search:
    endpoint_name: YOUR_VS_ENDPOINT_NAME
    index_name: YOUR_CATALOG.YOUR_SCHEMA.user_guide_vs_index
    user_name: YOUR_USERNAME  # For MCP URL construction

document_configs:
  source_volume: /Volumes/YOUR_CATALOG/YOUR_SCHEMA/user_guides
  parsed_table: YOUR_CATALOG.YOUR_SCHEMA.parsed_user_guides
  chunked_table: YOUR_CATALOG.YOUR_SCHEMA.chunked_user_guides
```

#### `databricks.template.yml` (DABs)
```yaml
bundle:
  name: retrieval-agent-mcp

variables:
  catalog:
    description: Unity Catalog name
    default: main
  schema:
    description: Schema name
    default: default
  experiment_name:
    default: "/Users/${workspace.current_user.userName}/retrieval-agent-mcp"

resources:
  experiments:
    retrieval_agent_experiment:
      name: ${var.experiment_name}

  jobs:
    retrieval_agent_pipeline:
      name: "retrieval-agent-pipeline-${bundle.target}"
      tasks:
        - task_key: ingest_documents
          description: "Parse PDFs using ai_parse_document"
          notebook_task:
            notebook_path: "src/01_ingest_documents.py"
            base_parameters:
              catalog: ${var.catalog}
              schema: ${var.schema}
          job_cluster_key: shared_cluster

        - task_key: create_vector_index
          description: "Create and sync Vector Search index"
          depends_on:
            - task_key: ingest_documents
          notebook_task:
            notebook_path: "src/02_create_vector_index.py"
            base_parameters:
              catalog: ${var.catalog}
              schema: ${var.schema}
          job_cluster_key: shared_cluster

        - task_key: deploy_agent
          description: "Log and deploy agent"
          depends_on:
            - task_key: create_vector_index
          notebook_task:
            notebook_path: "src/04_deployment.py"
            base_parameters:
              catalog: ${var.catalog}
              schema: ${var.schema}
              mlflow_experiment: ${var.experiment_name}
          job_cluster_key: shared_cluster

      job_clusters:
        - job_cluster_key: shared_cluster
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            node_type_id: "Standard_DS3_v2"
            num_workers: 0
            spark_conf:
              spark.master: "local[*, 4]"

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://YOUR_WORKSPACE.cloud.databricks.com

  prod:
    mode: production
    workspace:
      host: https://YOUR_WORKSPACE.cloud.databricks.com
```

#### `resources/retrieval_pipeline.job.yml` (Lakeflow Jobs)
```yaml
resources:
  jobs:
    retrieval_pipeline:
      name: retrieval-agent-full-pipeline
      max_concurrent_runs: 1
      queue:
        enabled: true

      parameters:
        - name: catalog
          default: main
        - name: schema
          default: default

      tasks:
        - task_key: ingest_documents
          description: "Parse user guide PDFs with ai_parse_document"
          notebook_task:
            notebook_path: ../src/01_ingest_documents.py
            base_parameters:
              catalog: "{{job.parameters.catalog}}"
              schema: "{{job.parameters.schema}}"
          timeout_seconds: 3600
          job_cluster_key: processing_cluster

        - task_key: create_vector_index
          description: "Create Vector Search index from parsed documents"
          depends_on:
            - task_key: ingest_documents
          notebook_task:
            notebook_path: ../src/02_create_vector_index.py
            base_parameters:
              catalog: "{{job.parameters.catalog}}"
              schema: "{{job.parameters.schema}}"
          timeout_seconds: 1800
          job_cluster_key: processing_cluster

        - task_key: deploy_agent
          description: "Deploy retrieval agent to Model Serving"
          depends_on:
            - task_key: create_vector_index
          notebook_task:
            notebook_path: ../src/04_deployment.py
            base_parameters:
              catalog: "{{job.parameters.catalog}}"
              schema: "{{job.parameters.schema}}"
          timeout_seconds: 1200
          job_cluster_key: processing_cluster

      job_clusters:
        - job_cluster_key: processing_cluster
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            node_type_id: "Standard_DS4_v2"
            autoscale:
              min_workers: 1
              max_workers: 4
```

---

### Task 6: Requirements (`requirements.txt`)
```
mlflow>=2.20.0
databricks-sdk>=0.40.0
databricks-mcp>=0.1.0
databricks-langchain>=0.2.0
databricks-vectorsearch>=0.40.0
openai>=1.50.0
pydantic>=2.0.0
nest-asyncio>=1.5.0
```

---

## Implementation Order

| Step | Task | File | Dependencies |
|------|------|------|--------------|
| 1 | Create directory structure | - | None |
| 2 | Write configuration templates | `configs.template.yaml` | None |
| 3 | Document ingestion script | `01_ingest_documents.py` | Configs |
| 4 | Vector index creation script | `02_create_vector_index.py` | Task 1 output |
| 5 | Agent implementation | `03_agent.py` | Configs, Vector Index |
| 6 | Deployment script | `04_deployment.py` | Agent code |
| 7 | DABs configuration | `databricks.template.yml` | All scripts |
| 8 | Lakeflow Jobs definition | `retrieval_pipeline.job.yml` | All scripts |
| 9 | Helper utilities | `utils/helpers.py` | None |
| 10 | README documentation | `README.md` | All |

---

## Key Technical Decisions

### 1. MCP vs Direct Vector Search API
**Decision**: Use MCP for Vector Search integration
**Rationale**:
- Consistent with other examples in the repo (`openai-multiagent-mcp`)
- Automatic tool schema generation
- Better alignment with OpenAI Response API patterns
- Future-proof for additional MCP tools

### 2. OpenAI Response API vs LangGraph
**Decision**: Use OpenAI Response API with MCP
**Rationale**:
- Simpler implementation for single-agent retrieval
- Native tool calling support
- Better streaming experience
- User specified OpenAI API compatibility

### 3. Delta Sync vs Direct Index
**Decision**: Use Delta Sync index
**Rationale**:
- Automatic embedding generation
- Incremental updates when documents change
- No manual embedding pipeline needed
- Supported by Databricks MCP

### 4. Streaming vs Batch Document Processing
**Decision**: Use Spark Streaming with checkpoints
**Rationale**:
- Handles large document volumes
- Incremental processing for new documents
- Built-in fault tolerance
- Consistent with existing patterns

### 5. Chunking Strategy
**Decision**: Element-level chunking from ai_parse_document
**Rationale**:
- Preserves document structure (headers, paragraphs)
- Better retrieval granularity
- Maintains context boundaries

---

## Testing Strategy

### Unit Tests
- Agent tool execution
- MCP client connection
- Response parsing

### Integration Tests
- End-to-end document ingestion
- Vector Search queries
- Agent responses

### Validation Approach
```python
# Local testing before deployment
from src.agent import RetrievalAgent

agent = RetrievalAgent(config)
response = agent.predict([
    {"role": "user", "content": "How do I configure authentication?"}
])
print(response)
```

---

## Success Criteria

1. **Document Ingestion**: PDFs parsed and stored in Delta table with correct structure
2. **Vector Index**: Index created and synced with >95% documents embedded
3. **Agent Response**: Agent correctly retrieves relevant context for user queries
4. **Deployment**: Agent accessible via Model Serving endpoint
5. **Pipeline**: Full pipeline runs successfully via DABs `databricks bundle deploy && databricks bundle run`

---

## References

| Component | Reference File |
|-----------|----------------|
| MCP Integration | `/home/user/databricks-genai-examples/agents/openai-multiagent-mcp/src/agent.py` |
| Agent Deployment | `/home/user/databricks-genai-examples/agents/agent-dabs/src/deployment.py` |
| DABs Config | `/home/user/databricks-genai-examples/agents/agent-dabs/databricks.template.yml` |
| Vector Search | `/home/user/databricks-genai-examples/vector-search/multi-index-search.py` |
| ai_parse_document | `/home/user/databricks-genai-examples/batch-inference/e2e-entity-extraction/src/pipeline/01_ingest.py` |
| Lakeflow Jobs | `/home/user/databricks-genai-examples/batch-inference/e2e-entity-extraction/resources/claims_pipeline.job.yml` |

---

## Next Steps

Once this plan is approved:
1. Create the directory structure
2. Implement each script in order
3. Test locally with sample PDFs
4. Deploy using DABs
5. Write README with setup instructions

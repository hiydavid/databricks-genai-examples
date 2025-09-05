# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Databricks entity resolution tutorial that demonstrates using Vector Search and `ai_query()` to match merchant name variations at scale. The solution processes credit card transaction data to resolve abbreviated/misspelled merchant names to canonical entities.

## Core Architecture

### Data Pipeline Flow

1. **Master Entities** → Stored in Unity Catalog table with merchant metadata
2. **Name Variations** → Generated using `ai_query()` with LLMs to create realistic credit card statement variations
3. **Vector Search Index** → Delta Sync index with auto-embedding of merchant names
4. **Entity Resolution** → Combines Vector Search (top-N candidates) + LLM selection via `ai_query()`

### Key Databricks Components Used

- **Unity Catalog**: All tables stored in `{CATALOG}.{SCHEMA}.ner_demo_*` format
- **Vector Search**: Delta Sync index with triggered pipeline type
- **LLM Endpoints**:
  - `databricks-gpt-oss-120b` for variation generation
  - `databricks-llama-4-maverick` for entity resolution
- **Embedding Model**: `databricks-gte-large-en` for vector embeddings
- **Serverless Compute**: Using `DatabricksSession.builder.remote(serverless=True)`

## Development Setup

### Required Variables (Set in each notebook)

```python
CATALOG = "users"  # Unity Catalog name
SCHEMA = "david_huang"  # Schema name  
VS_ENDPOINT = "one-env-shared-endpoint-0"  # Vector Search endpoint
VS_INDEX = "ner_demo_merchant_index"  # Vector Search index name
LLM_ENDPOINT = "databricks-llama-4-maverick"  # LLM for resolution
```

### Running Notebooks Locally

Each notebook includes connection setup for local IDE:

```python
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.remote(serverless=True).getOrCreate()
```

## Data Schema

### Tables Created

- `ner_demo_merchant_entities`: Master entities with metadata
- `ner_demo_generated_entities`: AI-generated name variations
- `ner_demo_generated_transactions`: Synthetic transaction data
- `ner_demo_top_results`: Vector search results per transaction
- `ner_demo_resolution_output`: Final resolved entities
- `ner_demo_evaluation`: Evaluation table comparing predictions to ground truth

### Key SQL Patterns

**Vector Search Query:**

```sql
SELECT * FROM VECTOR_SEARCH(
    index => "{CATALOG}.{SCHEMA}.{INDEX}",
    query_text => merchant_name,
    num_results => 10
)
```

**AI Query for Resolution:**

```sql
ai_query(
    '{LLM_ENDPOINT}',
    prompt_with_context,
    failOnError => false
)
```

## Important Notes

- **Change Data Feed**: Must be enabled on source tables for Vector Search delta sync
- **Index Sync**: Use `idx.sync()` to trigger manual sync for TRIGGERED pipeline type
- **Merchant Data**: Base entities defined in `data/merchant_attributes.json`
- **Databricks Bundle**: Project configured as "mdm" bundle in `databricks.yml`

## Dependencies

From `requirements.txt`:

- `databricks-vectorsearch==0.57`
- `Faker==37.6.0` (for synthetic data generation)
- `openai==1.102.0` (optional, not used in current notebooks)

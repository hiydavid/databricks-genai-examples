# End-to-End Entity Extraction Pipeline

Medical claims entity extraction pipeline on Databricks. Classifies and extracts structured data from accident benefits forms using Claude AI via Databricks Model Serving.

## Architecture

```text
Unity Catalog Volume (PDFs)
         ↓
   ai_parse_document() [Streaming]
         ↓
   UC Table: parsed_documents
         ↓
   ai_query() → Model Serving Endpoint (PyFunc agent)
         ↓
   UC Table: extracted_claims → postprocessed_claims
```

## Quick Start

1. **Configure** - Copy `config.yaml.example` to `config.yaml` and set your Unity Catalog paths
2. **Deploy agent** - Run `src/setup/deploy_agent.py` to log and deploy the model
3. **Run pipeline** - Execute notebooks in order: `01_ingest.py` → `02_extract.py` → `03_postprocess.py`

## Key Features

- **Two-call inference**: Classify document type first, then extract fields based on classification
- **Structured output**: JSON schema enforcement via `response_format.json_schema`
- **Streaming ETL**: Incremental processing with Delta Lake checkpoints
- **MLflow tracing**: Full observability of LLM calls

## Requirements

- Databricks Runtime 15.4 LTS or above
- Unity Catalog enabled workspace
- Model Serving endpoint access

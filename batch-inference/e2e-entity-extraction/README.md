# End-to-End Entity Extraction Pipeline with DABs

Medical claims entity extraction pipeline on Databricks. Parsing PDFs with `ai_parse_document`, and then run classification and structured extraction with Databricks-hosted Claude Sonnet 4 model via Databricks Model Serving. Enabled with MLflow Tracing and Evaluation (MLflow v3+). Orchestrated with Databricks Asset Bundles and Lakeflow Jobs.

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

1. **Configure** - Copy `databricks.yml.template` to `databricks.yml` and set your workspace URL
2. **Deploy bundle** - `databricks bundle deploy`
3. **Deploy agent** - `databricks bundle run deploy_agent`
4. **Run pipeline** - `databricks bundle run claims_extraction_pipeline`

## Running with DABs

```bash
# Validate the bundle
databricks bundle validate

# Deploy the bundle
databricks bundle deploy

# Deploy the agent to Model Serving
databricks bundle run deploy_agent

# Run the full pipeline (ingest → extract)
databricks bundle run claims_extraction_pipeline

# Run individual tasks
databricks bundle run claims_extraction_pipeline --only ingest_documents
databricks bundle run claims_extraction_pipeline --only classify_extract

# Run evaluation
databricks bundle run evaluation
```

## Key Features

- **Two-call inference**: Classify document type first, then extract fields based on classification
- **Structured output**: JSON schema enforcement via `response_format.json_schema`
- **Streaming ETL**: Incremental processing with Delta Lake checkpoints
- **MLflow tracing**: Full observability of LLM calls

## Debugging

For debugging `ai_parse_document()` output, use the official Databricks notebook which provides visual bounding box overlays and element inspection:

**[ai_parse_document Debug Interface](https://docs.databricks.com/aws/en/notebooks/source/machine-learning/large-language-models/ai-parse-document-debug.html)**

## Requirements

- Databricks Serverless compute, environment version 4
- Unity Catalog enabled workspace
- Model Serving endpoint access
- MLflow 3

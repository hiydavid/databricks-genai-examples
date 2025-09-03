# AI-Powered Entity Resolution on Databricks

This tutorial demonstrates how to perform entity resolution using Databricks' AI capabilities, specifically leveraging **Vector Search** and **`ai_query()`** to match and resolve entity variations at scale.

## Overview

Entity resolution is the process of identifying when different data records refer to the same real-world entity. This is particularly challenging with transaction data where merchant names can appear in many variations (abbreviations, typos, different formats).

This solution uses:

- **Databricks Vector Search** - For semantic similarity matching of entity names
- **`ai_query()`** - For LLM-powered resolution of ambiguous matches
- **Delta Lake** - For scalable data storage and processing

## Tutorial Structure

1. **Generate Datasets (`00_generate-datasets.ipynb`)**: Creates a master merchant entity table with ~50 merchants across various categories. Uses LLM to generate realistic variations of each merchant entity to mimic merchant names as they appear on credit card statements. Generates synthetic transaction data with 1000+ records using the name variations with Faker.
2. **Build Vector Search Index (`01_generate-vs-index.ipynb`)**: Creates a Databricks Vector Search Index on the master merchant entity table using Delta Sync with auto-embedding capabilities.
3. **Resolve Entities (`02_resolve-entities-aiquery.ipynb`)**: Demonstrates how to use the `VECTOR_SEARCH()` function with hybrid query type to retrieve top-N candidate merchant entities for each transaction, then uses `ai_query()` with an LLM to intelligently select the correct entity from the candidates.

## Prerequisites

- Databricks workspace with access to:
  - Unity Catalog
  - Vector Search endpoints
  - LLM endpoints (e.g., `databricks-llama-4-maverick`, `databricks-gpt-oss-120b`)
- Python environment with required libraries (i.e., `requirements.txt`)

## Getting Started

1. Update the variables in each notebook:
   - `CATALOG`: Your Unity Catalog name
   - `SCHEMA`: Your schema name
   - `VS_ENDPOINT`: Your Vector Search endpoint
   - `LLM_ENDPOINT`: Your preferred LLM endpoint

2. Run the notebooks in sequence:
   - `00_generate-datasets.ipynb` - Create sample data
   - `01_generate-vs-index.ipynb` - Build search index
   - `02_resolve-entities-aiquery.ipynb` - Perform entity resolution

## Example Results

**Input transaction:** `"STRBKS #1234"`  
**Vector Search (Hybrid):** Returns top 5 candidates with similarity scores  
**Candidates:** `["Starbucks", "Star Market", "Stars Restaurant", ...]`  
**LLM Resolution:** `"Starbucks"`

This approach achieves high accuracy by:
- Using hybrid search (combining semantic and keyword matching) for better recall
- Leveraging LLM reasoning to select the correct entity from candidates
- Processing thousands of transactions in parallel using Databricks serverless compute

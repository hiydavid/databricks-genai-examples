# AI-Powered Entity Resolution on Databricks

This tutorial demonstrates how to perform entity resolution using Databricks' AI capabilities, specifically leveraging **Vector Search** and **`ai_query()`** to match and resolve entity variations at scale.

## Overview

Entity resolution is the process of identifying when different data records refer to the same real-world entity. This is particularly challenging with transaction data where merchant names can appear in many variations (abbreviations, typos, different formats).

This solution uses:

- **Databricks Vector Search** - For semantic similarity matching of entity names
- **`ai_query()`** - For LLM-powered resolution of ambiguous matches
- **Delta Lake** - For scalable data storage and processing

## Tutorial Structure

1. **Generate Datasets (`00_generate-datasets.ipynb`)**: In this notebook, you will creates a master merchant entity table with ~50 merchants across various categories. Then, we will use a LLM to generate variations of each merchant entity to mimic merchant names that look like those on credit card statements. And finally, we will generates synthetic transaction data with 1000+ records using the name variations with Faker.
2. **Build Vector Search Index (`01_generate-vs-index.ipynb`)**: In this notebook, we will create a Databricks Vector Search Index on the master merchant entity table.
3. **Resolve Entities (`02_resolve-entities-aiquery.ipynb`)**: In this final notebook, we will demonstrate how to use the `vector_search()` Databricks AI Function to search possible merchant entities for each transaction, and then use `ai_query()` with an LLM to intelligently select the correct entity from candidates entities.

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

Input transaction: `"STRBKS #1234"`
Vector Search candidates: `["Starbucks", "Star Market", "Stars Restaurant", ...]`
LLM resolution: `"Starbucks"`

This approach achieves high accuracy by combining the recall of semantic search with the precision of LLM-based reasoning.

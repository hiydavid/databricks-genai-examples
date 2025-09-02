# AI-Powered Entity Resolution on Databricks

This tutorial demonstrates how to perform entity resolution using Databricks' AI capabilities, specifically leveraging **Vector Search** and **`ai_query()`** to match and resolve entity variations at scale.

## Overview

Entity resolution is the process of identifying when different data records refer to the same real-world entity. This is particularly challenging with transaction data where merchant names can appear in many variations (abbreviations, typos, different formats).

This solution uses:

- **Databricks Vector Search** - For semantic similarity matching of entity names
- **`ai_query()`** - For LLM-powered resolution of ambiguous matches
- **Delta Lake** - For scalable data storage and processing

## Tutorial Structure

### 1. Generate Datasets (`00_generate-datasets.ipynb`)

- Creates a master merchant entity table with ~50 merchants across various categories
- Uses `ai_query()` with Databricks' LLM to generate realistic name variations (like those on credit card statements)
- Generates synthetic transaction data with 1000+ records using the name variations

### 2. Build Vector Search Index (`01_generate-vs-index.ipynb`)

- Enables Change Data Feed on the merchant entity table
- Creates a Delta Sync Vector Search index that auto-embeds merchant names
- Uses Databricks' embedding model to create semantic representations of merchant names

### 3. Resolve Entities (`02_resolve-entities-aiquery.ipynb`)

- For each transaction, retrieves top 10 possible entity matches using Vector Search
- Uses `ai_query()` with an LLM to intelligently select the correct entity from candidates
- Combines semantic search with LLM reasoning for high-accuracy entity resolution

## Key Features

- **Scalable**: Processes thousands of transactions in parallel using Spark SQL
- **Accurate**: Combines vector similarity with LLM intelligence for robust matching
- **Flexible**: Easily adaptable to different entity types beyond merchants
- **Production-Ready**: Built on Databricks' serverless compute and managed services

## Prerequisites

- Databricks workspace with access to:
  - Unity Catalog
  - Vector Search endpoints
  - LLM endpoints (e.g., `databricks-llama-4-maverick`, `databricks-gpt-oss-120b`)
- Python environment with required libraries

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

## Data Flow

```text
Master Entities → Generate Variations → Create Transactions
                           ↓
                   Vector Search Index
                           ↓
                Transaction + Vector Search → LLM Resolution → Resolved Entities
```

## Example Results

Input transaction: `"STRBKS #1234"`
Vector Search candidates: `["Starbucks", "Star Market", "Stars Restaurant", ...]`
LLM resolution: `"Starbucks"`

This approach achieves high accuracy by combining the recall of semantic search with the precision of LLM-based reasoning.

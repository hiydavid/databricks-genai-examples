# Databricks MLflow LLM Evaluation Demo

This repository contains a comprehensive example demonstrating how to evaluate Large Language Model (LLM) performance for lease document entity extraction using Databricks MLflow.

## Project Overview

This project showcases a complete evaluation pipeline for an LLM-powered entity extraction system that processes lease agreements and extracts structured data. The system uses Claude 3.7 Sonnet via Databricks Model Serving to extract key information from unstructured lease documents.

## Repository Structure

```text
evals/
├── eval-entity-extraction/
│   ├── 00_setup.ipynb                      # Environment setup and configuration
│   ├── 01_create-eval-dataset.ipynb        # Evaluation dataset creation
│   ├── 02_eval-with-predefined-scorers.ipynb # Evaluation using built-in scorers
│   ├── 03_eval-with-custom-guidelines.ipynb  # Custom evaluation guidelines
│   └── data/
│       └── leases.csv                       # Sample lease documents dataset
├── databricks.yml                          # Databricks project configuration
├── requirements.txt                        # Python dependencies
└── README.md                               # This file
```

## Workflow Components

### 1. Setup and Configuration (`00_setup.ipynb`)

- **Environment Detection**: Automatically detects whether running on Databricks workspace or local development
- **MLflow Configuration**: Sets up MLflow tracking and experiment management
- **Model Configuration**: Configures Databricks Model Serving endpoint for Claude 3.7 Sonnet
- **Entity Extraction Function**: Defines the core LLM function with structured JSON schema output

**Key Features:**

- Environment-aware MLflow experiment naming
- Structured JSON schema for lease entity extraction
- OpenAI-compatible client setup for Databricks Model Serving

**Extracted Entities:**

- `start_date`: Lease commencement date
- `end_date`: Lease termination date  
- `leased_space`: Property description and location
- `lessee`: Tenant information
- `lessor`: Landlord information
- `signing_date`: Contract signing date
- `term_of_payment`: Payment schedule and terms
- `designated_use`: Permitted property usage
- `extension_period`: Renewal/extension options
- `expiration_date_of_lease`: Final lease expiration

### 2. Evaluation Dataset Creation (`01_create-eval-dataset.ipynb`)

- **Dataset Initialization**: Creates MLflow evaluation dataset in Unity Catalog
- **Ground Truth Loading**: Loads labeled lease documents from Unity Catalog table
- **Record Transformation**: Converts ground truth data into MLflow evaluation format
- **Dataset Population**: Adds 15 lease document records with expected outputs

**Process:**

1. Creates evaluation dataset table in Unity Catalog
2. Loads ground truth data from `lease_docs_short` table
3. Transforms records into `inputs`/`expectations` format
4. Populates the evaluation dataset for downstream evaluation

### 3. Predefined Scorer Evaluation (`02_eval-with-predefined-scorers.ipynb`)

- **Built-in Metrics**: Uses MLflow's predefined evaluation scorers
- **Model Testing**: Validates model inference with sample lease text
- **Automated Evaluation**: Runs evaluation across all dataset records

**Evaluation Metrics:**

- **Correctness**: Compares model output against ground truth labels
- **Relevance to Query**: Assesses output relevance to input lease document
- **Safety**: Evaluates content safety and appropriateness

### 4. Custom Guidelines Evaluation (`03_eval-with-custom-guidelines.ipynb`)

- **Field-Specific Guidelines**: Custom evaluation criteria for each extracted entity
- **Date Format Validation**: Specific requirements for date field formatting
- **Domain-Specific Scoring**: Tailored evaluation for lease document context

**Custom Guidelines Include:**

- Date fields require valid formats (e.g., "June 8th, 2024")
- Entity-specific extraction accuracy requirements
- Domain knowledge validation for lease terminology

## Key Features

### Environment Flexibility

- **Databricks Workspace**: Uses workspace-based experiment paths
- **Local Development**: Uses current working directory for experiments
- Automatic environment detection via `DATABRICKS_RUNTIME_VERSION`

### MLflow Integration

- Unity Catalog dataset management
- Experiment tracking and comparison
- Model evaluation metrics and traces
- Results visualization in Databricks workspace

### Structured Data Extraction

- JSON schema-enforced output format
- Comprehensive lease entity coverage
- Error handling and validation

## Getting Started

1. **Configure Environment Variables**:
   - Update `USER`, `CATALOG`, `SCHEMA` in `00_setup.ipynb`
   - Set model serving endpoint URL for your workspace

2. **Run Notebooks in Sequence**:

   ```text
   00_setup.ipynb → 01_create-eval-dataset.ipynb → 02_eval-with-predefined-scorers.ipynb → 03_eval-with-custom-guidelines.ipynb
   ```

3. **View Results**:
   - Navigate to MLflow experiment in Databricks workspace
   - Compare evaluation runs and metrics
   - Review individual prediction traces

## Dataset

The evaluation uses a dataset of 15 lease agreements with ground truth labels, covering various:

- Commercial and office lease agreements
- Different date formats and terminology
- Various property types and locations
- Multiple payment terms and structures

## Dependencies

- `databricks-connect`: For local development
- `mlflow`: Model tracking and evaluation
- `openai`: API client for model serving
- Standard Python data science libraries

## Results and Insights

The evaluation framework enables:

- **Model Performance Tracking**: Quantitative metrics across evaluation runs
- **Error Analysis**: Detailed traces for debugging model predictions
- **Custom Validation**: Domain-specific evaluation criteria
- **Comparative Analysis**: Performance comparison across different model versions or configurations

This comprehensive evaluation setup provides a robust foundation for assessing and improving LLM performance in document entity extraction tasks.

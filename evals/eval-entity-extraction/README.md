# MLflow 3 Evaluation: Entity Extraction Use Case

This folder contains examples demonstrating how to evaluate Large Language Model (LLM) performance for entity extraction using MLflow 3 on Databricks.

## Project Overview

This project showcases a complete evaluation pipeline for an LLM-powered entity extraction system that processes lease agreements and extracts structured data. The system uses Claude 3.7 Sonnet via Databricks Model Serving to extract key information from unstructured lease documents.

## Repository Structure

```text
evals/
├── eval-entity-extraction/
│   ├── 00_setup.ipynb                        # Environment setup and configuration
│   ├── 01_create-eval-dataset.ipynb          # Evaluation dataset creation
│   ├── 02_eval-with-predefined-scorers.ipynb # Evaluation using built-in scorers
│   ├── 03_eval-with-custom-guidelines.ipynb  # Custom evaluation guidelines
│   ├── 04_eval-with-code-scorers.ipynb       # Custom code-based fuzzy matching scorers
│   └── data/
│       └── leases.csv                        # Sample lease documents dataset
├── requirements.txt                          # Python dependencies
└── README.md                                 # This file
```

## Workflow Components

### 0. Setup and Configuration (`00_setup.ipynb`)

In this notebook, we will setup all the critical environment variables that will be needed in the subsequent notebooks. In addition, we also define the LLM-calling function that will be used to produce structured extraction results from the lease agreement data.

```text
TODO: Be sure to replace all the TODO's in the notebook!
```

### 1. Evaluation Dataset Creation (`01_create-eval-dataset.ipynb`)

In this notebook, we will take a sample of the datasets, along with their ground-truth labels, and create a Mlflow Evaluation Dataset. See [this documentation](https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/build-eval-dataset) for more details.

### 2. Predefined Scorer Evaluation (`02_eval-with-predefined-scorers.ipynb`)

In this notebook, we will run an evaluation using MLflow's built-in predefined scorers. See [this documentation](https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/predefined-judge-scorers) for more details.

### 3. Custom Guidelines Evaluation (`03_eval-with-custom-guidelines.ipynb`)

The predefined scorers are adequate for most use cases. However, for entity extraction usecase, we might want a separate scorer for each extraction field. This is where the `custom guidelines` come in handy. In this notebook, we will run an evaluation using customer guidelines, where you'll be able to define individual scorers for each extraction field using simple natural language.

### 4. Custom Code-Based Scorers (`04_eval-with-code-scorers.ipynb`)

For more advanced evaluation scenarios, custom code-based scorers provide maximum flexibility. In this notebook, we implement fuzzy matching scorers for each of the 10 entity extraction fields using Python's `difflib` library. Each scorer compares predicted and expected field values with a 70% similarity threshold, returning 1 for matches above the threshold and 0 otherwise. This approach is particularly useful for handling variations in formatting, abbreviations, and minor textual differences while maintaining precise evaluation criteria.

## Getting Started

1. **Configure Environment Variables**:
   - Update the TODO-marked variables in `00_setup.ipynb`
   - Set model serving endpoint URL for your workspace

2. **Run Notebooks in Sequence**:

   ```text
   00_setup.ipynb → 
   01_create-eval-dataset.ipynb → 
   02_eval-with-predefined-scorers.ipynb → 
   03_eval-with-custom-guidelines.ipynb →
   04_eval-with-code-scorers.ipynb
   ```

3. **View Results**:
   - Navigate to MLflow experiment in Databricks workspace
   - Compare evaluation runs and metrics
   - Review individual prediction traces

## Dataset

The evaluation uses a dataset of 15 lease agreements with ground truth labels. We've sourced the sample data from a public [lease agreement dataset](https://arxiv.org/abs/2010.10386).

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

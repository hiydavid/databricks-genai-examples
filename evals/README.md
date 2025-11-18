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
│   ├── 04_eval-with-make-judge.ipynb         # LLM judges with make_judge API
│   ├── 05_eval-with-code-scorers.ipynb       # Custom code-based fuzzy matching scorers
│   ├── 06_eval-with-experts.ipynb            # Expert review with labeling sessions
│   └── data/
│       └── leases.csv                        # Sample lease documents dataset
├── pyproject.toml                            # Python dependencies and project configuration
├── uv.lock                                   # Locked dependencies for reproducibility
└── README.md                                 # This file
```

## Workflow Components

### 0. Setup and Configuration (`00_setup.ipynb`)

In this notebook, we will setup all the critical environment variables that will be needed in the subsequent notebooks. In addition, we also define the LLM-calling function that will be used to produce structured extraction results from the lease agreement data.

```text
TODO: Be sure to replace all the TODO's in the notebook!
```

### 1. Evaluation Dataset Creation (`01_create-eval-dataset.ipynb`)

In this notebook, we will take a sample of the datasets, along with their ground-truth labels, and create a Mlflow Evaluation Dataset. See [this documentation to learn more on creating evaluation datasets](https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/build-eval-dataset).

### 2. Predefined Scorer Evaluation (`02_eval-with-predefined-scorers.ipynb`)

In this notebook, we will run an evaluation using MLflow's built-in predefined scorers. See [this documentation to learn more on the predefined LLM judges](https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/predefined-judge-scorers).

### 3. Custom Guidelines Evaluation (`03_eval-with-custom-guidelines.ipynb`)

The predefined scorers are adequate for most use cases. However, for entity extraction usecase, we might want a separate scorer for each extraction field. This is where the `custom guidelines` come in handy.

In this notebook, we will run an evaluation using customer guidelines, where you'll be able to define individual scorers for each extraction field using simple natural language. See [this documentation to learn more on evaluating using custom LLM judges](https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/custom-judge).

### 4. LLM Judges with make_judge API (`04_eval-with-make-judge.ipynb`)

The `make_judge` API (available in MLflow>=3.4.0) provides a programmatic way to create custom LLM judges for specific evaluation criteria. This notebook demonstrates creating individual judges for each of the 10 entity extraction fields using natural language instructions.

Each judge is configured with detailed evaluation criteria and uses Claude Sonnet 4 to assess whether extracted values match expected values. This approach combines the flexibility of custom evaluation logic with the power of LLM reasoning, making it ideal for complex domain-specific validation. See [this documentation to learn more on make_judge](https://mlflow.org/docs/latest/genai/eval-monitor/scorers/llm-judge/make-judge/).

### 5. Custom Code-Based Scorers (`05_eval-with-code-scorers.ipynb`)

For deterministic evaluation scenarios, custom code-based scorers provide maximum control and transparency. In this notebook, we implement fuzzy matching scorers for each of the 10 entity extraction fields using Python's `difflib` library. Each scorer compares predicted and expected field values with a 70% similarity threshold, returning 1 for matches above the threshold and 0 otherwise.

This approach is particularly useful for handling variations in formatting, abbreviations, and minor textual differences while maintaining precise, reproducible evaluation criteria. See [this documentation to learn more on evaluating using code-based scorers](https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/custom-scorers).

### 6. Expert Review with Labeling Sessions (`06_eval-with-experts.ipynb`)

For cases where automated evaluation isn't sufficient, subject matter expert (SME) review provides human validation of model outputs. This notebook demonstrates how to create labeling sessions for expert reviewers to manually assess the accuracy of each extracted field.

The notebook creates individual label schemas for all 10 extraction fields, allowing experts to provide structured feedback with "Correct", "Incorrect", or "Not Found" assessments plus detailed comments. This approach enables high-quality human evaluation for model validation and continuous improvement. See [this documentation to learn more on creating labeling sessions](https://docs.databricks.com/aws/en/mlflow3/genai/human-feedback/concepts/labeling-sessions).

## Getting Started

1. **Configure Environment Variables**:
   - Update the TODO-marked variables in `00_setup.ipynb`
   - Set model serving endpoint URL for your workspace

2. **Install Dependencies**:
   ```bash
   uv sync
   ```

3. **Run Notebooks in Sequence**:
   ```text
   00_setup.ipynb →
   01_create-eval-dataset.ipynb →
   02_eval-with-predefined-scorers.ipynb →
   03_eval-with-custom-guidelines.ipynb →
   04_eval-with-make-judge.ipynb →
   05_eval-with-code-scorers.ipynb →
   06_eval-with-experts.ipynb
   ```

4. **View Results**:
   - Navigate to MLflow experiment in Databricks workspace
   - Compare evaluation runs and metrics
   - Review individual prediction traces

## Dataset

The evaluation uses a dataset of 15 lease agreements with ground truth labels. We've sourced the sample data from a public [lease agreement dataset](https://arxiv.org/abs/2010.10386).

## Dependencies

This project uses [uv](https://docs.astral.sh/uv/) for fast, reliable Python dependency management. Dependencies are defined in `pyproject.toml`:

- `mlflow[databricks]>=3.6.0`: Model tracking and evaluation with Databricks integration
- `databricks-agents>=1.8.2`: Databricks agents SDK for LLM applications

The `uv.lock` file ensures reproducible installations across environments.

## Results and Insights

The evaluation framework enables:

- **Model Performance Tracking**: Quantitative metrics across evaluation runs
- **Error Analysis**: Detailed traces for debugging model predictions
- **Custom Validation**: Domain-specific evaluation criteria
- **Comparative Analysis**: Performance comparison across different model versions or configurations

This comprehensive evaluation setup provides a robust foundation for assessing and improving LLM performance in document entity extraction tasks.

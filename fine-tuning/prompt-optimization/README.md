# Prompt Optimization with GEPA

This example demonstrates how to use **GEPA (Gradient-free Evolutionary Prompt optimization Algorithm)**, implemented in MLflow, to automatically optimize prompts for structured data extraction tasks.

## Overview

GEPA is a gradient-free prompt optimization technique that iteratively improves prompts through evolutionary search. Unlike manual prompt engineering, GEPA systematically explores the prompt space to find formulations that maximize task performance.

This demo uses a lease agreement entity extraction task, where the goal is to extract structured fields (dates, parties, terms) from unstructured lease documents.

## Results

| Prompt | Average Accuracy |
|--------|-----------------|
| Baseline | 61.5% |
| GEPA-optimized | **77.5%** |

With 70 training samples, GEPA achieved a **16+ percentage point improvement** in extraction accuracy across all fields.

## Notebooks

| Notebook | Description |
|----------|-------------|
| [00_setup.ipynb](src/00_setup.ipynb) | Register initial prompt template to Unity Catalog |
| [01_baseline-eval.ipynb](src/01_baseline-eval.ipynb) | Establish baseline performance with fuzzy match scorers |
| [02_prompt-optimization.ipynb](src/02_prompt-optimization.ipynb) | Run GEPA optimization and evaluate the optimized prompt |

## How It Works

1. **Setup**: Register a base prompt template to MLflow's prompt registry in Unity Catalog

2. **Baseline Evaluation**:
   - Split data into training (70 samples) and eval (30 samples) sets
   - Run `mlflow.genai.evaluate()` with fuzzy match scorers on each extraction field
   - Baseline achieves ~61.5% average accuracy

3. **GEPA Optimization**:
   - Use `mlflow.genai.optimize_prompts()` with `GepaPromptOptimizer`
   - The optimizer uses a reflection model to generate prompt variations
   - Iteratively improves the prompt based on scorer feedback
   - Optimized prompt is registered as a new version in Unity Catalog

4. **Final Evaluation**:
   - Evaluate the optimized prompt on the held-out eval set
   - Optimized prompt achieves ~77.5% average accuracy

## Extraction Fields

The task extracts 10 fields from lease agreements:

- `start_date`, `end_date`, `expiration_date_of_lease`
- `lessee`, `lessor`
- `leased_space`, `designated_use`
- `signing_date`, `term_of_payment`, `extension_period`

## Requirements

- Databricks workspace with Unity Catalog
- MLflow 2.x with `mlflow.genai` support
- Model serving endpoint (e.g., `databricks-gpt-oss-20b`)
- Optimizer endpoint for GEPA reflection (e.g., Claude Sonnet 4.5)

## Configuration

Update `src/config.yaml` with your settings:

```yaml
user: "your-email@databricks.com"
catalog: "your_catalog"
schema: "your_schema"
table: "your_lease_docs_table"
model_serving_endpoint: "your-model-endpoint"
optimizer_endpoint: "your-optimizer-endpoint"
```

## Key APIs

```python
# Register a prompt
mlflow.genai.register_prompt(name="catalog.schema.prompt_name", template=template)

# Load a prompt
prompt = mlflow.genai.load_prompt("prompts:/catalog.schema.prompt_name/1")

# Evaluate with scorers
mlflow.genai.evaluate(data=records, predict_fn=predict_fn, scorers=scorers)

# Optimize prompts with GEPA
mlflow.genai.optimize_prompts(
    predict_fn=predict_fn,
    train_data=records,
    prompt_uris=[prompt_uri],
    optimizer=GepaPromptOptimizer(reflection_model="databricks:/model-endpoint"),
    scorers=scorers,
    aggregation=aggregation_fn,
)
```

## References

- [MLflow GenAI Documentation](https://mlflow.org/docs/latest/llms/index.html)
- [Databricks Model Serving](https://docs.databricks.com/en/machine-learning/model-serving/index.html)

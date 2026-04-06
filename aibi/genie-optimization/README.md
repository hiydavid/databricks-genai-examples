# Genie Space Optimizer

Automated evaluation and optimization of Databricks Genie Spaces using the Benchmark Evaluation API and MLflow.

## What It Does

1. **Evaluates** your Genie Space by running all benchmark questions through the Genie Benchmark Evaluation API
2. **Tracks results** in an MLflow experiment with deterministic scorers covering UC metadata, SQL patterns, and business logic
3. **Generates prescriptive fixes** based on a fix taxonomy that maps each failure reason to specific config changes
4. **Optimizes** the space configuration via an LLM call that applies the fixes, with a self-fix loop that detects and corrects benchmark overlap (up to 2 retries)
5. **Validates** the output against serialized_space schema rules and anti-overfitting guardrails (Jaccard similarity check between examples and benchmarks)
6. **Creates an optimized Genie Space** with a date-stamped title (e.g. `[Optimized 2026-04-04] My Space`)
7. **Re-evaluates** the optimized space and compares before/after accuracy

## Prerequisites

- Databricks workspace with Genie Spaces enabled
- CAN EDIT permission on the target Genie Space
- At least 10 benchmark questions defined in the space
- Access to a Databricks Foundation Model endpoint (default: `databricks-claude-sonnet-4-6`)
- MLflow tracking (Databricks-managed)

## Setup

1. Open `src/optimizer.py` as a notebook and set `SPACE_ID` and `EXPERIMENT_ID` in the **Configuration** cell:

```python
SPACE_ID = "your-genie-space-id-here"
EXPERIMENT_ID = "your-mlflow-experiment-id"
```

2. Run all cells.

## Configuration

All settings are inline variables at the top of `src/optimizer.py`:

| Variable | Default | Description |
|---|---|---|
| `SPACE_ID` | `""` (required) | Genie Space ID to optimize |
| `EXPERIMENT_ID` | `""` (required) | MLflow Experiment ID (create in MLflow UI first) |
| `EXISTING_EVAL_RUN_ID` | `""` | Optional: reuse a completed eval run instead of creating a new one |
| `LLM_ENDPOINT` | `"databricks-claude-sonnet-4-6"` | Foundation model for generating optimized config |
| `POLL_INTERVAL_SECONDS` | `30` | Seconds between eval run status polls |
| `POLL_TIMEOUT_SECONDS` | `1800` | Max seconds to wait for eval run completion |

## Architecture

```
optimizer.py (notebook)
    |
    +-- genie_client.py      Genie Space API + Eval REST API
    |
    +-- scorers.py            Deterministic scorer functions
    |   +-- fix_taxonomy.py   Assessment reason -> config fix mapping
    |
    +-- fix_taxonomy.py       Also used directly for fix report compilation
    |
    +-- llm_optimizer.py      LLM call + self-fix loop for optimized config
    |   +-- validation.py     Benchmark overlap check during self-fix
    |
    +-- validation.py         Normalize + validate serialized_space
```

### Flow

```
Step 1:  Get config --> Step 2: Check benchmarks >= 10
                               |
Step 3:  Setup MLflow <--------+
    |
Step 4-5: Create (or reuse) eval run, poll until DONE
    |
Step 6:  List results
    |
Step 7:  Score results + log baseline to MLflow
    |
Step 8:  Compile fix report (grouped by category + priority)
    |
Step 9:  LLM optimization (config + fixes -> improved config)
    |        +-- Self-fix loop: detect benchmark overlap, retry up to 2x
    |
Step 10: Validate optimized config (schema rules + anti-overfitting check)
    |
Step 11: Create optimized space
    |
Step 12: Re-evaluate optimized space + compare before/after
```

### Anti-Overfitting Guardrails

The optimizer prevents the LLM from "teaching to the test" by copying benchmark questions into example_question_sqls:

- **System prompt rules** (14–16): instruct the LLM to create generalizable SQL patterns, not benchmark-specific answers
- **Benchmark stripping**: the benchmarks section is removed from the config sent to the LLM
- **Overlap detection**: Jaccard similarity (threshold ≥ 0.9) between example questions/SQL and benchmark questions/SQL flags violations
- **Self-fix loop**: if overlap is detected after the initial LLM call, a correction prompt is sent (up to 2 retries) asking the LLM to generalize the offending examples
- **Validation warnings**: any remaining overlap surfaces as warnings in Step 10

### Observability

- LLM optimization is traced via `@mlflow.trace` on `optimize_config`
- OpenAI calls are auto-logged via `mlflow.openai.autolog()`
- All traces appear in the same MLflow experiment alongside eval metrics

### Assessment Reason Categories

The scorers map to three fix categories, applied in order:

| Category | Labels | What It Covers |
|---|---|---|
| **UC Metadata** | 7 labels | Wrong tables/columns, missing joins, column types |
| **SQL Examples** | 9 labels | Wrong aggregations, syntax errors, missing rows |
| **Instructions** | 8 labels | Wrong filters, business logic, metric calculations |

## Output

- **MLflow Experiment** with two runs ("baseline" and "optimized"), each containing pass rate metrics and a detailed results table
- **Fix Report** printed in notebook showing prioritized prescriptive changes
- **Optimized Genie Space** created with a date-stamped title prefix
- **Before/after comparison** table showing accuracy delta per scorer

## Project Structure

```
genie-optimization/
+-- databricks.yml        Databricks Asset Bundle
+-- README.md             This file
+-- src/
    +-- optimizer.py       Main notebook (12-step flow, config at top)
    +-- genie_client.py    Genie Space + Eval API client
    +-- scorers.py         Deterministic scorer functions
    +-- fix_taxonomy.py    Assessment reason -> fix mapping
    +-- llm_optimizer.py   LLM config optimization
    +-- validation.py      Config validation + normalization
```

# Genie Space Optimizer

Automated evaluation and optimization of Databricks Genie Spaces using the Benchmark Evaluation API and MLflow.

## What It Does

1. **Evaluates** your Genie Space by running all benchmark questions through the Genie Benchmark Evaluation API
2. **Tracks results** in an MLflow experiment with per-assessment-reason scorers (24 labels covering UC metadata, SQL patterns, and business logic)
3. **Generates prescriptive fixes** based on a fix taxonomy that maps each failure reason to specific config changes
4. **Optimizes** the space configuration via an LLM call that applies the fixes
5. **Validates** the output against serialized_space schema rules
6. Optionally **creates a new Genie Space** with the optimized configuration

## Prerequisites

- Databricks workspace with Genie Spaces enabled
- CAN EDIT permission on the target Genie Space
- At least 10 benchmark questions defined in the space
- Access to a Databricks Foundation Model endpoint (default: `databricks-claude-sonnet-4-6`)
- MLflow tracking (Databricks-managed)

## Setup

1. Deploy to your Databricks workspace:

```bash
databricks bundle deploy --target dev
```

2. Open `src/optimizer.py` as a notebook and set `SPACE_ID` in the **Configuration** cell:

```python
SPACE_ID = "your-genie-space-id-here"
```

3. Run all cells.

## Configuration

All settings are inline variables at the top of `src/optimizer.py`:

| Variable | Default | Description |
|---|---|---|
| `SPACE_ID` | `""` (required) | Genie Space ID to optimize |
| `EXPERIMENT_ID` | `""` (required) | MLflow Experiment ID (create in MLflow UI first) |
| `LLM_ENDPOINT` | `"databricks-claude-sonnet-4-6"` | Foundation model for generating optimized config |
| `POLL_INTERVAL_SECONDS` | `30` | Seconds between eval run status polls |
| `POLL_TIMEOUT_SECONDS` | `1800` | Max seconds to wait for eval run completion |
| `CREATE_NEW_SPACE` | `False` | Whether to create a new space with optimized config |
| `OPTIMIZED_TITLE_PREFIX` | `"[Optimized] "` | Prefix for new space title |

## Architecture

```
optimizer.py (notebook)
    |
    +-- genie_client.py      Genie Space API + Eval REST API
    |
    +-- scorers.py            24 MLflow @scorer functions (deterministic)
    |   +-- fix_taxonomy.py   Assessment reason -> config fix mapping
    |
    +-- llm_optimizer.py      LLM call to generate optimized config
    |
    +-- validation.py         Normalize + validate serialized_space
```

### Flow

```
Step 1: Get config --> Step 2: Check benchmarks >= 10
                              |
Step 3: Setup MLflow <--------+
    |
Step 4: Create eval run --> Step 5: Poll until DONE
                                    |
Step 6: List results <--------------+
    |
Step 7: MLflow evaluate (24 scorers per question)
    |
Step 8: Compile fix report (grouped by category + priority)
    |
Step 9: LLM optimization (config + fixes -> improved config)
    |
Step 10: Validate --> Optional: Create new space
```

### Assessment Reason Categories

The 24 scorers map to three fix categories, applied in order:

| Category | Labels | What It Covers |
|---|---|---|
| **UC Metadata** | 7 labels | Wrong tables/columns, missing joins, column types |
| **SQL Examples** | 9 labels | Wrong aggregations, syntax errors, missing rows |
| **Instructions** | 8 labels | Wrong filters, business logic, metric calculations |

## Output

- **MLflow Experiment** at `/genie-optimization/{space_id}` with per-label pass rates
- **Fix Report** printed in notebook showing prioritized prescriptive changes
- **Validated Config** (JSON) -- optionally applied to a new Genie Space

## Project Structure

```
genie-optimization/
+-- databricks.yml        Databricks Asset Bundle
+-- README.md             This file
+-- src/
    +-- optimizer.py       Main notebook (10-step flow, config at top)
    +-- genie_client.py    Genie Space + Eval API client
    +-- scorers.py         MLflow custom scorers
    +-- fix_taxonomy.py    Assessment reason -> fix mapping
    +-- llm_optimizer.py   LLM config optimization
    +-- validation.py      Config validation + normalization
```

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

A collection of standalone Databricks GenAI examples organized by use case. Each subdirectory is an independent project with its own dependencies and configuration. The repo is **not** a monorepo with a shared build system — each example is self-contained.

Categories: `agents/`, `aibi/`, `batch-inference/`, `context-engineering/`, `evals/`, `fine-tuning/`, `mlops/`, `model-serving/`, `traditional-ml/`, `vector-search/`

## Common Development Commands

Each project manages its own dependencies. Projects use either `uv` (preferred for new projects) or `pip`:

```bash
# Projects with pyproject.toml (uv)
cd <project-dir>
uv sync
uv run python <script.py>

# Projects with requirements.txt (pip)
pip install -r requirements.txt
```

For Databricks Asset Bundle (DAB) projects:

```bash
# Validate bundle config
databricks bundle validate --target dev

# Deploy (syncs files + creates job/resource definitions in workspace)
databricks bundle deploy --target dev

# Run a job defined in the bundle
databricks bundle run <job_name> --target dev

# Pass variables at deploy or run time
databricks bundle deploy --target prod --var key=value
databricks bundle run <job> --var key=value
```

## Architecture Patterns

### Notebook-style Python scripts

Scripts that run on Databricks clusters use a notebook format with:

- `# Databricks notebook source` header
- `# COMMAND ----------` as cell separators
- `# MAGIC %pip install ...` for cluster-side installs
- `dbutils.widgets.get("param")` to receive job parameters

### Agent implementation pattern

Agents implement `mlflow.pyfunc.ChatAgent` and return `ChatAgentResponse` or yield `ChatAgentChunk` for streaming. Agents are logged with `mlflow.pyfunc.log_model()` using `python_model=agent` and registered to Unity Catalog via `mlflow.register_model()`.

### Configuration pattern

Each project uses a YAML config file (`configs.yaml` or `config.yaml`) loaded via `mlflow.models.ModelConfig(development_config="./configs.yaml")`. These contain workspace-specific values (catalog, schema, endpoint names, etc.). Template versions (`*.template.yaml`) exist with placeholder values — copy and fill in before running.

### Multi-agent pattern (LangGraph)

Agents use `langgraph.graph.StateGraph` with `MessagesState`. Handoff between agents is done via custom `handoff_tool` functions. The graph is compiled with `.compile()` and wrapped in a `ChatAgent` subclass.

### Multi-agent pattern (OpenAI Agents SDK)

Uses `databricks-openai` + `openai-agents` SDK. Agents are defined with `Agent(name=..., tools=[...])` and orchestrated with handoffs. Tools can be UC functions (via `UCFunctionToolkit`) or MCP-connected tools.

### DAB resource definitions

`databricks.yml` defines experiments, jobs, and sometimes model serving endpoints. Notebook tasks pass parameters via `base_parameters` which are received as `dbutils.widgets` in the notebook. The `run_as` field in jobs specifies execution identity (user or service principal).

## Key Dependencies (by category)

| Area | Key Packages |
| --- | --- |
| Core | `mlflow[databricks]>=3.x`, `databricks-sdk`, `databricks-agents` |
| LangGraph agents | `databricks-langchain`, `langgraph`, `pydantic<2.12.0` |
| OpenAI-style agents | `databricks-openai`, `openai`, `openai-agents` |
| Vector Search | `databricks-vectorsearch` |
| Evals | `mlflow[databricks]>=3.6`, `databricks-agents` |
| Fine-tuning | `dspy>=3.0`, `databricks-agents` |

## Configuration Files to Watch

- `configs.yaml` / `config.yaml` — workspace-specific settings; never commit secrets
- `databricks.yml` — bundle targets with workspace hosts and profile names; workspace `host` values are environment-specific
- Template files (`*.template.yaml`, `databricks.template.yml`) contain placeholder values and are committed to git. Copy to the non-template filename (which is gitignored) and fill in your values before running.
- `typings/` directories contain type stubs for Databricks runtime globals (`dbutils`, `spark`, etc.) for IDE autocompletion — not source code

## MLflow 3 Patterns

Evaluation uses `mlflow.genai.evaluate()` (MLflow 3+ API, not the older `mlflow.evaluate()`). Tracing uses `@mlflow.trace` decorator or `mlflow.start_span()`. Experiments are set with `mlflow.set_experiment()` before logging.

## Existing CLAUDE.md

The `mlops/genie-migration/` subdirectory has its own detailed CLAUDE.md covering that project's specific DAB commands, authentication setup, and deployment behavior. Refer to it when working in that directory.

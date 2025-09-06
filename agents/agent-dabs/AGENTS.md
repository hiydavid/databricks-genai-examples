# Agents on Databricks — Engineering Memory (AGENTS.md)

This repo implements a production‑oriented, multi‑agent financial analysis system using LangGraph, Databricks Genie, and MLflow, packaged and deployed with Databricks Asset Bundles (DABs).

## Quick Facts
- Purpose: Multi‑agent system that routes simple vs complex finance questions to Genie or a parallel research executor.
- Entry points: `src/deployment.py` (deploy & serve), `src/evaluation.py` (evaluate), `src/multiagent_genie.py` (core agent).
- Config: `src/configs.yaml` (Databricks + prompts/agents). Bundle config in `databricks.yml`.
- Serving dependencies: `DB_MODEL_SERVING_HOST_URL`, `DATABRICKS_GENIE_PAT` (provided via Databricks secrets in deployment).
- Versions: `mlflow[databricks]==3.2.0`, `databricks-langchain==0.6.0`, `databricks-agents==1.4.0`, `langgraph==0.5.4`, `pydantic<2.12.0`.

## Repo Layout
- `src/multiagent_genie.py`: Core multi‑agent graph + MLflow ChatAgent wrapper.
- `src/deployment.py`: Databricks notebook‑style script to log, validate, register, and deploy model.
- `src/evaluation.py`: Databricks notebook‑style evaluation using `mlflow.genai` scorers.
- `src/configs.yaml`: Databricks resources and agent prompts/descriptions (update TODOs).
- `databricks.yml`: DAB bundle defining experiments and two jobs (deploy + evaluation).
- `requirements.txt`, `setup.py`: Dependencies (note: `setup.py` includes `lark` not listed in `requirements.txt`).
- `data/evals/eval-questions.json`: Example evaluation dataset.
- `example-inputs.json`: Example serialized messages/state shape for debugging.
- `CLAUDE.md`: Development guidance; overlaps with this memory but is more narrative.

## Agent Architecture
- Graph: `supervisor` → [Genie | ParallelExecutor] → `supervisor` → `final_answer`.
- Supervisor: Decides routing and optionally plans research via structured output.
  - Code: `src/multiagent_genie.py:142` (`supervisor_agent`).
- Genie Agent: Databricks `GenieAgent` for NL→SQL over SEC data.
  - Code: `src/multiagent_genie.py:56` (`genie_agent`).
- Parallel Executor: Executes 2–4 Genie queries concurrently, then synthesizes results.
  - Code: `src/multiagent_genie.py:219` (`research_planner_node`).
- Final Answer: Synthesizes a final response, biasing concise direct answers or structured summaries based on path.
  - Code: `src/multiagent_genie.py:407` (`final_answer`).
- Chat Wrapper: Exposes `predict` and `predict_stream` with status updates and message sanitization.
  - Code: `src/multiagent_genie.py:473` (`LangGraphChatAgent`), exported as `AGENT` at `src/multiagent_genie.py:707`.

### Important Behaviors
- Temporal context: Injects today’s ISO date, fiscal year (FY ends Aug), and quarter (Q1=Sep–Nov, …) into supervisor prompt (`get_temporal_context`, `src/multiagent_genie.py:80`).
- Routing bias: Defaults to Genie for simple, single‑metric queries; uses ParallelExecutor only for true multi‑query analysis.
- Streaming UX: Emits “Processing with <node>…” chunks during streaming; final output comes from `final_answer` only.
- Message cap: Keeps last N messages (default 7) and drops ephemeral “Processing with …” messages.
- Robustness: Extensive try/except around nodes; individual parallel failures don’t cancel others.
- MLflow tracing: `@mlflow.trace` around all key nodes and I/O paths with SpanType.AGENT.

## Configuration
File: `src/configs.yaml`
- `databricks_configs`:
  - `catalog`, `schema`, `model`: UC target for registration and serving name.
  - `workspace_url`: Workspace host (also exported to `DB_MODEL_SERVING_HOST_URL`).
  - `sql_warehouse_id`: Required for Genie SQL execution.
  - `username`: Used to default the experiment path in notebooks.
  - `databricks_pat.secret_scope_name` / `secret_key_name`: Secret providing `DATABRICKS_GENIE_PAT` at deploy time.
- `agent_configs`:
  - `agent_name`: MLflow model name when logging.
  - `conversation.max_messages`: History window (default 7).
  - `llm.endpoint_name` and `temperature`: ChatDatabricks serving endpoint.
  - `genie_agent.space_id` and description: Genie space to use.
  - `parallel_executor_agent.description`: Label shown to the supervisor for planning.
  - `supervisor_agent`: `max_iterations`, and 3 prompts (`system_prompt`, `research_prompt`, `final_answer_prompt`).

Note: Paths in code load `./configs.yaml` relative to the working dir of the notebooks/scripts, which in Databricks resolves to `src/configs.yaml`.

## Databricks Asset Bundle (DAB)
File: `databricks.yml`
- Experiment: `resources.experiments.agents_experiment` → name from `var.experiment_name`.
- Jobs:
  - `agent_deploy_job`: runs `src/deployment.py`.
  - `agent_evaluation_job`: runs `src/evaluation.py`.
- Target: `targets.dev.workspace.host` must be set to your workspace URL.
- Variable: `experiment_name` defaults to `/Users/${workspace.current_user.userName}` and is overridden under `targets.dev`.

Common commands:
- `databricks bundle validate --profile <profile>`
- `databricks bundle deploy --profile <profile>`
- `databricks bundle run agent_deploy_job --profile <profile>`
- `databricks bundle run agent_evaluation_job --profile <profile>`

## Deployment Flow (Job: agent_deploy_job)
File: `src/deployment.py`
- Installs requirements and restarts Python (Databricks notebook semantics).
- Loads `src/configs.yaml`; sets `DB_MODEL_SERVING_HOST_URL` and reads `DATABRICKS_GENIE_PAT` from secrets.
- Logs the agent as an MLflow model with resources:
  - Serving endpoint (LLM), Genie space, SQL warehouse.
- Validates locally via `mlflow.models.predict(..., env_manager="uv")`.
- Registers to Unity Catalog and deploys via `databricks.agents.deploy` with secret‑backed `DATABRICKS_GENIE_PAT`.

## Evaluation Flow (Job: agent_evaluation_job)
File: `src/evaluation.py`
- Installs requirements, restarts Python (Databricks environment).
- Loads config and secrets; constructs an experiment path.
- Sample interaction using `AGENT.predict` and `AGENT.predict_stream`.
- Uses `mlflow.genai.evaluate` with scorers: Correctness, RelevanceToQuery, Safety.
- Data source: `data/evals/eval-questions.json`.

## Running Locally
- Install: `pip install -r requirements.txt` and `pip install databricks-cli`.
- Auth: `databricks auth login` to set your profile.
- Quick test (Python REPL):
  - `from src.multiagent_genie import AGENT`
  - `AGENT.predict({"messages": [{"role": "user", "content": "What was AAPL's revenue in 2015?"}]})`
- Note: `src/deployment.py` and `src/evaluation.py` use Databricks‑specific `dbutils` and should run on Databricks (via the bundle jobs).

## Secrets & Environment
- `DB_MODEL_SERVING_HOST_URL`: Databricks workspace host (from `workspace_url`).
- `DATABRICKS_GENIE_PAT`: PAT with access to the Genie space; provided through Databricks secrets in deployment.
- In deployment, the PAT is injected with `{{secrets/<scope>/<key>}}`.

## Known TODOs / Placeholders to Update
- `src/configs.yaml`:
  - `databricks_configs.catalog`, `schema`, `model`, `workspace_url`, `sql_warehouse_id`, `username`, `databricks_pat.*`.
  - `agent_configs.llm.endpoint_name` (e.g., `databricks-claude-3-7-sonnet`).
  - `agent_configs.genie_agent.space_id`.
- `databricks.yml`: `targets.dev.workspace.host`.
- Optional: Align `requirements.txt` with `setup.py` (e.g., `lark==1.2.2`).

## Scratch / Local Helpers
- `scratch/test-deploy.yaml` (if present): local snapshot of key deployment variables (catalog, schema, model, workspace URL, SQL warehouse ID, username, PAT secret scope/key, space_id) used during testing.

## Gotchas & Tips
- Running notebooks locally: Files with `# Databricks notebook source` rely on Databricks context (e.g., `dbutils`). Use the DAB jobs.
- Config path: Code loads `./configs.yaml` relative to the running directory; in Databricks jobs this resolves to `src/configs.yaml`.
- Streaming: Intermediate “Processing with …” messages are intentionally filtered from the final output.
- Iteration safety: Supervisor enforces `max_iterations` (default 3) to avoid loops.
- Genie auth: If Genie calls fail, verify `DATABRICKS_GENIE_PAT` secret and the Genie space permissions.

## Glossary
- Genie: Databricks natural language to SQL agent over curated datasets.
- Supervisor: Routing/planning LLM deciding between Genie and Parallel execution.
- ParallelExecutor: Async executor for 2–4 planned Genie queries with synthesis.
- DAB: Databricks Asset Bundles for IaC style deployment of jobs and resources.

---
This file is the authoritative “memory” of this codebase. Update it as you evolve prompts, agent routing, data scope, or deployment targets.

# AGENTS.md — memory/ (Managed Memory demo)

Folder-local rules for working in this example. The repo root AGENTS.md also applies.

## What this is

A phased demo (`PLAN.md`) that adds working, episodic, semantic, and procedural
memory to a support copilot using Databricks Managed agent memory (Beta). Notebooks
are Databricks notebook source exported as `.py`.

## Hard rules

- **Preserve cell markers**: every notebook keeps `# Databricks notebook source` on
  line 1 and `# COMMAND ----------` between cells. Markdown cells use `# MAGIC %md`.
- **Idempotent setup only**: rerunning `00_setup_foundations` must never fail or
  duplicate data. Destructive cleanup is Phase 5 and stays disabled by default.
- **No secrets, no real identifiers**: workspace URLs, tokens, PATs, emails, and real
  customer names never go in code. Use widgets/env vars for configuration.
- **Synthetic data only**: the `support_orders` rows are fabricated.
- **Scoped changes**: nothing leaves this folder; no cross-example refactors.

## Configuration conventions

- Defaults: catalog `main`, schema `default`, memory store `support_agent_memory`,
  table `support_orders`, model endpoint `databricks-glm-5-3-flash`
  (pay-per-token, OpenAI-compatible chat completions).
- All names come from `dbutils.widgets` in `00_setup_foundations`; other notebooks
  get them via `%run ./00_setup_foundations`.
- LLM calls go through `mlflow.deployments` (preinstalled on DBR) so notebook auth
  resolves automatically — no token handling anywhere.

## Run order

`00_setup_foundations` → `01_stateless_baseline` → `02_client_managed_working_memory`

## Phase discipline

Follow `PLAN.md`: each phase must run after the previous one with no manual code
changes beyond configuration, and leave the folder in a usable state.

# databricks-genai-examples — agent instructions

## What this repo is

This repository is a **collection of isolated Databricks examples** (not a single cohesive application/library).
Treat each example folder as standalone “customer-ready reference code”.

Top-level folders are thematic buckets (examples inside them are still independent), e.g.: `agents/`, `aibi/`, `batch-inference/`, `context-engineering/`, `evals/`, `fine-tuning/`, `model-serving/`, `traditional-ml/`, `vector-search/`

## Scope rules (most important)

- **Keep changes scoped to the requested example folder.**
  Avoid cross-folder refactors, shared utilities, or “repo-wide cleanup” unless explicitly asked.
- If the user request is ambiguous, prefer the smallest change that satisfies it **within one folder**.
- Do not rename or reorganize directories unless the user explicitly requests it.

## “Customer example” quality bar

When adding or modifying an example:

- Optimize for clarity and reuse: short narrative, explicit prerequisites, and runnable steps.
- Prefer small, readable code over clever abstractions.
- Add comments/markdown where it helps a customer understand *why* a pattern is used.

## No secrets / no customer data

- Never commit tokens, PATs, workspace URLs, emails, hostnames, or customer identifiers.
- Use placeholders + documented configuration.
  Examples: environment variables, Databricks secrets (`dbutils.secrets.get(...)`), or widgets.

## Databricks notebook hygiene

This repo contains notebooks and notebook-exported source.

- If editing Databricks-exported `.py` notebooks, **preserve cell markers** like
  `# Databricks notebook source` and `# COMMAND ----------`.
- If editing `.ipynb`, keep diffs minimal and avoid committing large cell outputs.

## Running / testing

There is **no single repo-wide build/test command**.

- Look for folder-local instructions (README / local AGENTS.md / `databricks.yml` / `requirements.txt`).
- If no instructions exist, add a short “How to run” section in the example folder’s README
  (or add a small folder-local AGENTS.md).

## Prefer local, folder-scoped agent guidance

If an example has special prerequisites (runtime, UC, Vector Search, Serving endpoints, DABs, etc.),
put them in a **folder-local `AGENTS.md`** so agents only load that context when working there.
Keep those local files short and concrete.

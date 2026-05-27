# genie-mlflow-eval agent notes

This folder is a standalone Databricks example for evaluating AI/BI Genie
multi-turn conversations with MLflow GenAI evaluation.

- Keep changes scoped to this folder.
- Preserve Databricks source notebook markers:
  `# Databricks notebook source` and `# COMMAND ----------`.
- Do not hard-code tokens, workspace URLs, user emails, customer identifiers, or
  private data. Use editable notebook globals for configuration and Databricks
  default authentication for credentials.
- Use the Databricks Python SDK for Genie conversation calls.
- Use MLflow GenAI APIs for datasets, tracing, expectations, and evaluation.
- Run `python -m py_compile evals/genie-mlflow-eval/*.py` after notebook edits.

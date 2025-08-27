# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Databricks MLflow 3 evaluation demo focused on LLM-powered entity extraction from lease agreements. The project demonstrates evaluation pipelines using Claude 3.7 Sonnet via Databricks Model Serving.

## Architecture

The codebase follows a sequential notebook workflow pattern:

1. **Setup (`00_setup.ipynb`)**: Core configuration and entity extraction function definition
2. **Dataset Creation (`01_create-eval-dataset.ipynb`)**: MLflow evaluation dataset initialization from ground truth data
3. **Predefined Evaluation (`02_eval-with-predefined-scorers.ipynb`)**: Built-in MLflow scorers (Correctness, Relevance, Safety)
4. **Custom Evaluation (`03_eval-with-custom-guidelines.ipynb`)**: Field-specific evaluation guidelines
5. **Code-Based Scorers (`04_eval-with-code-scorers.ipynb`)**: Custom fuzzy matching scorers using Python difflib

### Key Components

- **Entity Extraction Schema**: 10 structured fields (dates, parties, terms, locations) with JSON schema enforcement
- **MLflow Integration**: Unity Catalog dataset management with experiment tracking
- **Evaluation Framework**: Both predefined and custom evaluation criteria

## Configuration Requirements

Before running notebooks, update TODO-marked variables in `00_setup.ipynb`:
- `USER`: Databricks user email
- `CATALOG`, `SCHEMA`: Unity Catalog location  
- `GT_TABLE`, `EVAL_TABLE`: Ground truth and evaluation table names
- `MODEL_SERVING_BASE_URL`: Workspace-specific serving endpoint URL

## Development Workflow

### Running Evaluations
Execute notebooks sequentially: 00 → 01 → 02 → 03 → 04

### Environment Detection
The setup automatically detects execution context:
- Databricks workspace: Uses workspace-based experiment paths
- Local development: Uses current working directory paths

### Databricks Asset Bundle
The project includes `databricks.yml` for asset bundle deployment with job definitions for dataset creation.

## Dependencies

- `mlflow[databricks]==3.3.1`: MLflow with Databricks extensions
- `databricks-agents==1.4.0`: Databricks agent framework

## Data Sources

- Ground truth data: Unity Catalog table with lease documents and labels
- Sample data: 15 lease agreements from academic dataset (arxiv.org/abs/2010.10386)
- Schema: `inputs` (lease text) and `expectations` (structured JSON labels)

## Evaluation Patterns

### Custom Guidelines Structure
Use `Guidelines(name="field_name", guidelines="description")` for field-specific evaluation. Date fields require format specifications (e.g., "June 8th, 2024").

### Code-Based Scorers
Custom scorers using `@scorer` decorator for advanced evaluation logic. The fuzzy matching implementation:
- Uses `difflib.SequenceMatcher` for similarity scoring (0.0-1.0)
- 70% similarity threshold for pass/fail evaluation
- Returns 1 (pass) or 0 (fail) with detailed percentage rationale
- Handles ChatCompletion objects, JSON strings, and nested response structures

### MLflow Experiment Management
Experiments automatically handle existing experiment detection and creation. Results viewable in Databricks workspace MLflow UI.
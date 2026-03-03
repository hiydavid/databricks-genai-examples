# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Databricks Genie Space migration tool using Databricks Asset Bundles (DABs). Migrates Genie Spaces between Azure Databricks workspaces via the Genie Management APIs (CreateSpace, UpdateSpace, GetSpace). Designed for regulatory-compliant environments requiring Service Principal authentication (no PAT tokens) and git-based deployment.

This is a workaround for [databricks/cli#3008](https://github.com/databricks/cli/issues/3008) - DABs don't natively support Genie Spaces.

## Commands

```bash
# Install dependencies (uses uv)
uv sync

# Validate bundle configuration
databricks bundle validate --target dev

# Deploy bundle (syncs notebooks + creates job definitions)
databricks bundle deploy --target dev

# Export a Genie Space to JSON (writes to /Workspace/Shared/genie_exports/)
databricks bundle run export_genie_space \
    --var source_space_id=<space-id>

# Deploy a Genie Space (creates new)
# NOTE: --var must be on deploy, not run (base_parameters are set at deploy time)
databricks bundle deploy --target prod \
    --var deploy_config_path="/Workspace/Shared/.bundle/genie-migration/prod/files/genie_spaces/<filename>.json"
databricks bundle run deploy_genie_space --target prod

# Deploy a Genie Space (updates existing)
databricks bundle deploy --target prod \
    --var deploy_config_path="/Workspace/Shared/.bundle/genie-migration/prod/files/genie_spaces/<filename>.json" \
    --var target_space_id=<existing-space-id>
databricks bundle run deploy_genie_space --target prod
```

## Architecture

The project uses a notebook-based deployment pattern:

1. **databricks.yml** defines jobs with `notebook_task` that run Python scripts as notebooks
2. **scripts/*.py** are Python files with `# Databricks notebook source` header and `# COMMAND ----------` cell markers
3. Parameters are passed via `dbutils.widgets` (set by job's `base_parameters`)
4. Scripts use `databricks-sdk` WorkspaceClient with unified authentication

### Key API Concept: `serialized_space`

The Genie API's `get_space(include_serialized_space=True)` returns a JSON string containing:
- `version` - Schema version
- `config` - Sample questions, settings
- `data_sources` - Tables available to the space
- `instructions` - Context and rules for Genie

This field is portable and can be passed directly to `create_space()` or `update_space()` to recreate the space in another workspace.

### Authentication

**CLI Authentication** (for `bundle deploy`/`run`):

- Uses your local Databricks CLI profile (OAuth or PAT)

**Job Execution** (via `run_as` in databricks.yml):

- Jobs run as a Service Principal configured via per-target `run_as_service_principal` variable
- Each target has its own SP: `targets.dev.variables.run_as_service_principal` (source) and `targets.prod.variables.run_as_service_principal` (target)
- Use the SP's **application ID** (not display name) for Entra ID SPs
- SP must be added to the workspace with "Workspace access" entitlement

**CI/CD Authentication** (for pipelines):

Both SP types require `DATABRICKS_HOST`:
- `DATABRICKS_HOST` - Workspace URL (e.g., `https://adb-123.11.azuredatabricks.net`)

*Option A: Databricks Managed Service Principal*
- `DATABRICKS_CLIENT_ID` - Service principal client ID (from Account Console)
- `DATABRICKS_CLIENT_SECRET` - Service principal secret

*Option B: Microsoft Entra ID Service Principal*
- `ARM_TENANT_ID` - Azure tenant (directory) ID
- `ARM_CLIENT_ID` - Service principal application (client) ID
- `ARM_CLIENT_SECRET` - Service principal secret

**Required Permissions:**

- Export: CAN EDIT on source Genie Space
- Deploy: CAN USE on SQL Warehouse
- Deploy: CAN MANAGE on `target_parent_path` folder (to create Genie Spaces)

**Bundle Path:** Files deploy to `/Shared/.bundle/...` (not user folder) so SP can access them.

## Deployment Behavior

| Scenario | Behavior |
|----------|----------|
| No `--target-space-id` provided | Creates new space, outputs new ID |
| `--target-space-id` provided | Updates existing space |
| No `--target-space-id` but matching title | Falls back to title matching |

Destination workspace always gets a **different space_id** than source - track the new ID for subsequent deployments.

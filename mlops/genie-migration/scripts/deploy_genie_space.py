# Databricks notebook source
# MAGIC %pip install databricks-sdk>=0.76.0 --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
"""
Deploy a Databricks Genie Space from a JSON configuration file.

This notebook creates or updates a Genie Space in the target workspace using
the serialized_space configuration exported from another workspace.

Idempotent Behavior:
    - If space_id is provided, updates that specific space (recommended)
    - If no space_id, falls back to title-matching in parent_path
    - If no matching space exists, a new one will be created

Parameters (via job or widget):
    - config_path: Path to the JSON config file (workspace path or relative to bundle)
    - warehouse_id: SQL Warehouse ID in the target workspace
    - parent_path: Workspace folder path for creating new spaces
    - space_id: (optional) Existing Genie Space ID to update
"""

import json
from typing import Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import GenieSpace

# COMMAND ----------
# Parameters

dbutils.widgets.text("config_path", "", "Path to Genie Space JSON config")
dbutils.widgets.text("warehouse_id", "", "SQL Warehouse ID")
dbutils.widgets.text("parent_path", "", "Workspace path for new spaces")
dbutils.widgets.text("space_id", "", "Existing Space ID to update (optional)")

config_path = dbutils.widgets.get("config_path")
warehouse_id = dbutils.widgets.get("warehouse_id")
parent_path = dbutils.widgets.get("parent_path")
space_id = dbutils.widgets.get("space_id") or None  # Convert empty string to None

if not config_path:
    raise ValueError("config_path parameter is required")
if not warehouse_id:
    raise ValueError("warehouse_id parameter is required")
if not space_id and not parent_path:
    raise ValueError("Either space_id or parent_path must be provided")

print(f"Config path: {config_path}")
print(f"Warehouse ID: {warehouse_id}")
print(f"Parent path: {parent_path}")
print(f"Space ID: {space_id or '(not provided - will create new or match by title)'}")

# COMMAND ----------
# Helper functions


def find_existing_space(
    client: WorkspaceClient,
    parent_path: str,
    title: str
) -> Optional[str]:
    """
    Find an existing Genie Space by title in the given workspace path.

    Args:
        client: Databricks WorkspaceClient
        parent_path: Workspace folder path to search
        title: Title of the Genie Space to find

    Returns:
        The space_id if found, None otherwise
    """
    try:
        items = client.workspace.list(parent_path)

        for item in items:
            if item.object_type and "DASHBOARD" in str(item.object_type):
                try:
                    potential_id = item.path.split("/")[-1] if item.path else None
                    if potential_id:
                        space = client.genie.get_space(space_id=potential_id)
                        if space.title == title:
                            return potential_id
                except Exception:
                    continue
    except Exception as e:
        print(f"Note: Could not search {parent_path}: {e}")

    return None


def load_config(client: WorkspaceClient, config_path: str) -> dict:
    """
    Load configuration from workspace or bundle-relative path.

    Args:
        client: Databricks WorkspaceClient
        config_path: Path to the JSON config file

    Returns:
        The parsed configuration dictionary
    """
    import os

    # Try local filesystem first (works for /Workspace paths in notebooks)
    if os.path.exists(config_path):
        print(f"  Loading config from local path: {config_path}")
        with open(config_path, "r", encoding="utf-8") as f:
            return json.load(f)

    # Try workspace download via SDK
    if config_path.startswith("/Workspace") or config_path.startswith("/"):
        try:
            print(f"  Loading config via workspace API: {config_path}")
            content = client.workspace.download(config_path)
            return json.loads(content.read().decode("utf-8"))
        except Exception as e:
            print(f"  Workspace download failed: {e}")

    raise FileNotFoundError(f"Config file not found: {config_path}")


# COMMAND ----------
# Deploy logic


def deploy_genie_space(
    config_path: str,
    warehouse_id: str,
    parent_path: Optional[str] = None,
    space_id: Optional[str] = None
) -> str:
    """
    Deploy a Genie Space from a JSON configuration file.

    Args:
        config_path: Path to the JSON config file
        warehouse_id: SQL Warehouse ID for the Genie Space
        parent_path: Workspace folder path where space will be created (for new spaces)
        space_id: Existing space ID to update (recommended over title-matching)

    Returns:
        The space_id of the created/updated space
    """
    w = WorkspaceClient()

    print(f"Connecting to workspace: {w.config.host}")

    # Load configuration
    config = load_config(w, config_path)

    # Extract configuration
    title = config.get("title")
    description = config.get("description")
    serialized_space = config.get("serialized_space")

    if not serialized_space:
        raise ValueError("Config file missing 'serialized_space' field")
    if not title:
        raise ValueError("Config file missing 'title' field")

    print(f"Deploying Genie Space: {title}")
    print(f"  Warehouse ID: {warehouse_id}")
    if space_id:
        print(f"  Target Space ID: {space_id}")
    else:
        print(f"  Parent Path: {parent_path}")
    if "_metadata" in config:
        print(f"  Source: {config['_metadata'].get('source_workspace', 'unknown')}")

    # Determine if updating existing space or creating new
    existing_space_id = space_id

    # Fall back to title-matching if no space_id provided
    if not existing_space_id and parent_path:
        existing_space_id = find_existing_space(w, parent_path, title)

    if existing_space_id:
        # Update existing space
        print(f"  Found existing space: {existing_space_id}")
        print("  Updating...")

        result: GenieSpace = w.genie.update_space(
            space_id=existing_space_id,
            title=title,
            description=description,
            serialized_space=serialized_space,
            warehouse_id=warehouse_id
        )
        final_space_id = existing_space_id
        action = "Updated"
    else:
        # Create new space
        print("  No existing space found, creating new...")

        result: GenieSpace = w.genie.create_space(
            warehouse_id=warehouse_id,
            serialized_space=serialized_space,
            title=title,
            description=description,
            parent_path=parent_path
        )
        final_space_id = result.space_id
        action = "Created"

    print(f"\n{action} Genie Space: {final_space_id}")
    print(f"  Title: {title}")
    print(f"  Workspace: {w.config.host}")
    if parent_path:
        print(f"  Path: {parent_path}/{final_space_id}")

    return final_space_id


# COMMAND ----------
# Run deploy

result_space_id = deploy_genie_space(
    config_path=config_path,
    warehouse_id=warehouse_id,
    parent_path=parent_path,
    space_id=space_id
)

# Output for CI/CD piping
print(f"\nSPACE_ID={result_space_id}")

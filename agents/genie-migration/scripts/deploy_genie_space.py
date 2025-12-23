#!/usr/bin/env python3
"""
Deploy a Databricks Genie Space from a JSON configuration file.

This script creates or updates a Genie Space in the target workspace using
the serialized_space configuration exported from another workspace.

Authentication:
    Uses Databricks SDK unified authentication. Set environment variables:
    - DATABRICKS_HOST: Target workspace URL
    - For Service Principal (no PAT required):
        - ARM_TENANT_ID: Azure tenant ID
        - ARM_CLIENT_ID: Service principal client ID
        - ARM_CLIENT_SECRET: Service principal secret
    - Or use Azure Managed Identity with ARM_USE_MSI=true

Idempotent Behavior:
    - If --space-id is provided, updates that specific space (recommended)
    - If no --space-id, falls back to title-matching in parent_path
    - If no matching space exists, a new one will be created

Usage:
    # Update existing space by ID (recommended)
    python deploy_genie_space.py \
        --config <CONFIG_PATH> \
        --warehouse-id <WAREHOUSE_ID> \
        --space-id <EXISTING_SPACE_ID>

    # Create new space (or update by title-matching)
    python deploy_genie_space.py \
        --config <CONFIG_PATH> \
        --warehouse-id <WAREHOUSE_ID> \
        --parent-path <WORKSPACE_PATH>

Example:
    # Update existing space
    python deploy_genie_space.py \
        --config genie_spaces/my_space.json \
        --warehouse-id abc123def456 \
        --space-id 01ef2abc-3456-7890-abcd-ef1234567890

    # Create new space
    python deploy_genie_space.py \
        --config genie_spaces/my_space.json \
        --warehouse-id abc123def456 \
        --parent-path /Shared/genie_spaces
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import GenieSpace


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
        # List workspace contents to find existing spaces
        # Note: The Genie API doesn't have a direct list method,
        # so we use the workspace API to find spaces by path
        items = client.workspace.list(parent_path)

        for item in items:
            # Genie spaces appear as DASHBOARD type in workspace
            if item.object_type and "DASHBOARD" in str(item.object_type):
                # Try to get the space to check its title
                try:
                    # Extract potential space ID from path
                    # Genie spaces have paths like /path/to/space_id
                    potential_id = item.path.split("/")[-1] if item.path else None
                    if potential_id:
                        space = client.genie.get_space(space_id=potential_id)
                        if space.title == title:
                            return potential_id
                except Exception:
                    # Not a genie space or no access
                    continue
    except Exception as e:
        # Parent path may not exist yet, which is fine
        print(f"Note: Could not search {parent_path}: {e}")

    return None


def deploy_genie_space(
    config_path: str,
    warehouse_id: str,
    parent_path: Optional[str] = None,
    space_id: Optional[str] = None,
    title_override: Optional[str] = None,
    description_override: Optional[str] = None,
    verbose: bool = False
) -> str:
    """
    Deploy a Genie Space from a JSON configuration file.

    Args:
        config_path: Path to the JSON config file
        warehouse_id: SQL Warehouse ID for the Genie Space
        parent_path: Workspace folder path where space will be created (for new spaces)
        space_id: Existing space ID to update (recommended over title-matching)
        title_override: Optional title to use instead of config title
        description_override: Optional description override
        verbose: Print detailed information

    Returns:
        The space_id of the created/updated space
    """
    w = WorkspaceClient()

    if verbose:
        print(f"Connecting to workspace: {w.config.host}")

    # Validate arguments
    if not space_id and not parent_path:
        print("ERROR: Either --space-id or --parent-path must be provided", file=sys.stderr)
        sys.exit(1)

    # Load configuration
    config_file = Path(config_path)
    if not config_file.exists():
        print(f"ERROR: Config file not found: {config_path}", file=sys.stderr)
        sys.exit(1)

    with open(config_file, 'r', encoding='utf-8') as f:
        config = json.load(f)

    # Extract configuration
    title = title_override or config.get("title")
    description = description_override or config.get("description")
    serialized_space = config.get("serialized_space")

    if not serialized_space:
        print("ERROR: Config file missing 'serialized_space' field", file=sys.stderr)
        sys.exit(1)

    if not title:
        print("ERROR: No title specified in config or --title argument", file=sys.stderr)
        sys.exit(1)

    if verbose:
        print(f"Deploying Genie Space: {title}")
        print(f"  Warehouse ID: {warehouse_id}")
        if space_id:
            print(f"  Target Space ID: {space_id}")
        else:
            print(f"  Parent Path: {parent_path}")
        if "_metadata" in config:
            print(f"  Source: {config['_metadata'].get('source_workspace', 'unknown')}")

    # Determine if updating existing space or creating new
    existing_space_id = space_id  # Use provided space_id if available

    # Fall back to title-matching if no space_id provided
    if not existing_space_id and parent_path:
        existing_space_id = find_existing_space(w, parent_path, title)

    if existing_space_id:
        # Update existing space
        if verbose:
            print(f"  Found existing space: {existing_space_id}")
            print("  Updating...")

        result: GenieSpace = w.genie.update_space(
            space_id=existing_space_id,
            title=title,
            description=description,
            serialized_space=serialized_space,
            warehouse_id=warehouse_id
        )
        space_id = existing_space_id
        action = "Updated"
    else:
        # Create new space
        if verbose:
            print("  No existing space found, creating new...")

        result: GenieSpace = w.genie.create_space(
            warehouse_id=warehouse_id,
            serialized_space=serialized_space,
            title=title,
            description=description,
            parent_path=parent_path
        )
        space_id = result.space_id
        action = "Created"

    print(f"{action} Genie Space: {space_id}")
    print(f"  Title: {title}")
    print(f"  Workspace: {w.config.host}")
    print(f"  Path: {parent_path}/{space_id}")

    return space_id


def main():
    parser = argparse.ArgumentParser(
        description="Deploy a Databricks Genie Space from JSON configuration",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument(
        "--config", "-c",
        required=True,
        help="Path to the JSON config file (from export_genie_space.py)"
    )
    parser.add_argument(
        "--warehouse-id", "-w",
        required=True,
        help="SQL Warehouse ID in the target workspace"
    )
    parser.add_argument(
        "--space-id", "-s",
        help="Existing Genie Space ID to update (recommended). If not provided, falls back to title-matching."
    )
    parser.add_argument(
        "--parent-path", "-p",
        help="Workspace folder path for creating new spaces. Required if --space-id not provided."
    )
    parser.add_argument(
        "--title",
        help="Override the title from config file"
    )
    parser.add_argument(
        "--description",
        help="Override the description from config file"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Print detailed information"
    )

    args = parser.parse_args()

    try:
        result_space_id = deploy_genie_space(
            config_path=args.config,
            warehouse_id=args.warehouse_id,
            parent_path=args.parent_path,
            space_id=args.space_id,
            title_override=args.title,
            description_override=args.description,
            verbose=args.verbose
        )
        # Output just the space_id for CI/CD piping
        print(f"\nSPACE_ID={result_space_id}")
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Export a Databricks Genie Space to a JSON file for version control.

This script extracts the serialized_space field from an existing Genie Space,
which can then be used to deploy the space to another workspace.

Authentication:
    Uses Databricks SDK unified authentication. Set environment variables:
    - DATABRICKS_HOST: Workspace URL
    - For Service Principal (no PAT required):
        - ARM_TENANT_ID: Azure tenant ID
        - ARM_CLIENT_ID: Service principal client ID
        - ARM_CLIENT_SECRET: Service principal secret
    - Or use Azure Managed Identity with ARM_USE_MSI=true

Requirements:
    - CAN EDIT permission on the Genie Space to export serialized_space

Usage:
    python export_genie_space.py --space-id <SPACE_ID> --output <OUTPUT_PATH>

Example:
    python export_genie_space.py \
        --space-id 01234567-89ab-cdef-0123-456789abcdef \
        --output genie_spaces/my_space.json
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import GenieSpace


def export_genie_space(space_id: str, output_path: str, verbose: bool = False) -> dict:
    """
    Export a Genie Space configuration to a JSON file.

    Args:
        space_id: The Genie Space ID to export
        output_path: Path to write the JSON config file
        verbose: Print detailed information

    Returns:
        The exported configuration dictionary
    """
    w = WorkspaceClient()

    if verbose:
        print(f"Connecting to workspace: {w.config.host}")
        print(f"Fetching Genie Space: {space_id}")

    # Fetch the space with serialized configuration
    # include_serialized_space=True requires CAN EDIT permission
    space: GenieSpace = w.genie.get_space(
        space_id=space_id,
        include_serialized_space=True
    )

    if not space.serialized_space:
        print("ERROR: Could not retrieve serialized_space.", file=sys.stderr)
        print("Ensure you have CAN EDIT permission on the Genie Space.", file=sys.stderr)
        sys.exit(1)

    # Build export config
    export_data = {
        "_metadata": {
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "source_space_id": space_id,
            "source_workspace": w.config.host,
            "export_version": "1.0"
        },
        "title": space.title,
        "description": space.description,
        "serialized_space": space.serialized_space
    }

    # Write to file
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(export_data, f, indent=2, ensure_ascii=False)

    if verbose:
        print(f"\nExported Genie Space:")
        print(f"  Title: {space.title}")
        print(f"  Description: {space.description or '(none)'}")
        print(f"  Output: {output_file.absolute()}")

    print(f"Successfully exported Genie Space to: {output_path}")
    return export_data


def main():
    parser = argparse.ArgumentParser(
        description="Export a Databricks Genie Space to JSON for version control",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument(
        "--space-id",
        required=True,
        help="The Genie Space ID to export"
    )
    parser.add_argument(
        "--output", "-o",
        required=True,
        help="Output path for the JSON config file"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Print detailed information"
    )

    args = parser.parse_args()

    try:
        export_genie_space(
            space_id=args.space_id,
            output_path=args.output,
            verbose=args.verbose
        )
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

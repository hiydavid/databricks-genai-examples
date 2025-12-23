# Databricks notebook source
"""
Export a Databricks Genie Space to a JSON file for version control.

This notebook extracts the serialized_space field from an existing Genie Space,
which can then be used to deploy the space to another workspace.

Requirements:
    - CAN EDIT permission on the Genie Space to export serialized_space

Parameters (via job or widget):
    - space_id: The Genie Space ID to export
    - output_path: Workspace path to write the JSON config file
"""

import json
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import GenieSpace

# COMMAND ----------
# Parameters

dbutils.widgets.text("space_id", "", "Genie Space ID to export")
dbutils.widgets.text("output_path", "", "Output path for JSON config")

space_id = dbutils.widgets.get("space_id")
output_path = dbutils.widgets.get("output_path")

if not space_id:
    raise ValueError("space_id parameter is required")
if not output_path:
    raise ValueError("output_path parameter is required")

print(f"Space ID: {space_id}")
print(f"Output path: {output_path}")

# COMMAND ----------
# Export logic


def export_genie_space(space_id: str, output_path: str) -> dict:
    """
    Export a Genie Space configuration to a JSON file.

    Args:
        space_id: The Genie Space ID to export
        output_path: Workspace path to write the JSON config file

    Returns:
        The exported configuration dictionary
    """
    w = WorkspaceClient()

    print(f"Connecting to workspace: {w.config.host}")
    print(f"Fetching Genie Space: {space_id}")

    # Fetch the space with serialized configuration
    # include_serialized_space=True requires CAN EDIT permission
    space: GenieSpace = w.genie.get_space(
        space_id=space_id,
        include_serialized_space=True
    )

    if not space.serialized_space:
        raise ValueError(
            "Could not retrieve serialized_space. "
            "Ensure you have CAN EDIT permission on the Genie Space."
        )

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

    # Ensure output path ends with .json
    if not output_path.endswith(".json"):
        final_path = f"{output_path}/{space.title.replace(' ', '_').lower()}.json"
    else:
        final_path = output_path

    # Write to workspace file
    json_content = json.dumps(export_data, indent=2, ensure_ascii=False)
    w.workspace.upload(
        path=final_path,
        content=json_content.encode("utf-8"),
        overwrite=True
    )

    print(f"\nExported Genie Space:")
    print(f"  Title: {space.title}")
    print(f"  Description: {space.description or '(none)'}")
    print(f"  Output: {final_path}")

    return export_data


# COMMAND ----------
# Run export

result = export_genie_space(space_id=space_id, output_path=output_path)
print(f"\nSuccessfully exported Genie Space to: {output_path}")

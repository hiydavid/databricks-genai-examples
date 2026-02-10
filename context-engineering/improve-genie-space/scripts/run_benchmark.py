#!/usr/bin/env python3
"""
Run a single benchmark question against a Databricks Genie Space.

Usage: python run_benchmark.py <space_id> <question>
Output: JSON to stdout
Exit codes: 0 = result available (success or Genie-level failure), 1 = script-level error (stderr)

Requires:
  - databricks-sdk >= 0.85 (pip install "databricks-sdk>=0.85")
  - Databricks CLI profile configured (databricks configure)
  - CAN EDIT permission on the target Genie Space
"""

import json
import sys
from datetime import timedelta


def run_benchmark(space_id: str, question: str) -> dict:
    """Send a question to Genie and return the structured result."""
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.dashboards import MessageStatus
    except ImportError:
        print(
            'Error: databricks-sdk is not installed. Run: pip install "databricks-sdk>=0.85"',
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        client = WorkspaceClient()
    except Exception as e:
        print(
            f"Error: Failed to initialize Databricks client. "
            f"Ensure your CLI profile is configured (databricks configure).\n{e}",
            file=sys.stderr,
        )
        sys.exit(1)

    result = {
        "space_id": space_id,
        "question": question,
        "status": None,
        "generated_sql": None,
        "query_description": None,
        "text_response": None,
        "error": None,
    }

    try:
        message = client.genie.start_conversation_and_wait(
            space_id=space_id,
            content=question,
            timeout=timedelta(minutes=5),
        )
    except TimeoutError:
        result["status"] = "TIMEOUT"
        result["error"] = "Genie did not respond within 5 minutes"
        return result
    except Exception as e:
        error_msg = str(e)
        if "PERMISSION_DENIED" in error_msg or "403" in error_msg:
            print(
                f"Error: Permission denied on space '{space_id}'.",
                file=sys.stderr,
            )
            sys.exit(1)
        if "NOT_FOUND" in error_msg or "404" in error_msg:
            print(
                f"Error: Genie Space '{space_id}' not found.",
                file=sys.stderr,
            )
            sys.exit(1)
        result["status"] = "ERROR"
        result["error"] = f"SDK error: {error_msg}"
        return result

    if message.status == MessageStatus.FAILED:
        result["status"] = "FAILED"
        result["error"] = (
            message.error.message if message.error else "Unknown Genie error"
        )
        return result

    result["status"] = "COMPLETED"

    if message.attachments:
        for attachment in message.attachments:
            if attachment.query and attachment.query.query:
                result["generated_sql"] = attachment.query.query
                if attachment.query.description:
                    result["query_description"] = attachment.query.description
                break
            if attachment.text and attachment.text.content:
                result["text_response"] = attachment.text.content

    return result


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(
            "Usage: python run_benchmark.py <space_id> <question>",
            file=sys.stderr,
        )
        sys.exit(1)

    output = run_benchmark(sys.argv[1], sys.argv[2])
    print(json.dumps(output, indent=2, ensure_ascii=False))

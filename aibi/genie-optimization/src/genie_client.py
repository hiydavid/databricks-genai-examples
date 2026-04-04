"""
Genie Space client: space config operations + Benchmark Evaluation API.

Uses the Databricks SDK for space operations and REST API for eval methods
(not yet available in the installed SDK version).
"""

import json
import time

from databricks.sdk import WorkspaceClient


class GenieClient:
    """Genie Space operations + Evaluation API."""

    def __init__(self):
        self.client = WorkspaceClient()

    # =========================================================================
    # Space operations (from existing GenieSpaceClient)
    # =========================================================================

    def get_space_config(self, space_id: str) -> dict:
        """Fetch the serialized space configuration.

        Requires databricks-sdk>=0.50.0 for include_serialized_space support.

        Returns dict with keys: title, description, space_id, warehouse_id, serialized_space.
        """
        space = self.client.genie.get_space(
            space_id=space_id,
            include_serialized_space=True,
        )

        if not space.serialized_space:
            raise ValueError(
                "Could not retrieve serialized_space. "
                "Ensure you have CAN EDIT permission on the Genie Space."
            )

        return {
            "title": space.title,
            "description": space.description,
            "space_id": space_id,
            "warehouse_id": space.warehouse_id,
            "serialized_space": json.loads(space.serialized_space),
        }

    def create_optimized_space(
        self,
        original_space_id: str,
        updated_config: dict,
        title_prefix: str = "[Optimized] ",
        space_info: dict = None,
    ) -> dict:
        """Create a new Genie Space with the optimized configuration.

        Args:
            original_space_id: The source space ID.
            updated_config: The optimized serialized_space dict.
            title_prefix: Prefix for the new space title.
            space_info: Optional pre-fetched space info from get_space_config()
                        to avoid a redundant API call.
        """
        if space_info:
            warehouse_id = space_info.get("warehouse_id")
            title = space_info.get("title", "")
            description = space_info.get("description")
        else:
            original_space = self.client.genie.get_space(space_id=original_space_id)
            warehouse_id = original_space.warehouse_id
            title = original_space.title
            description = original_space.description

        if not warehouse_id:
            raise ValueError(
                f"Original space '{original_space_id}' has no warehouse_id; cannot create new space"
            )

        new_title = f"{title_prefix}{title}"
        created = self.client.genie.create_space(
            warehouse_id=warehouse_id,
            serialized_space=json.dumps(updated_config, ensure_ascii=False),
            title=new_title,
            description=description,
        )

        host = self.client.config.host.rstrip("/")
        return {
            "new_space_id": created.space_id,
            "new_space_title": new_title,
            "original_space_id": original_space_id,
            "new_space_url": f"{host}/spaces/{created.space_id}",
        }

    # =========================================================================
    # Evaluation API (REST — not yet in SDK)
    # =========================================================================

    def _api_get(self, path: str, params: dict = None) -> dict:
        """Make a GET request via the SDK's API client."""
        response = self.client.api_client.do(
            method="GET",
            path=path,
            query=params,
        )
        return response

    def _api_post(self, path: str, body: dict = None) -> dict:
        """Make a POST request via the SDK's API client."""
        response = self.client.api_client.do(
            method="POST",
            path=path,
            body=body or {},
        )
        return response

    def create_eval_run(
        self,
        space_id: str,
        benchmark_question_ids: list[str] = None,
    ) -> dict:
        """Create a new benchmark evaluation run.

        Args:
            space_id: The Genie Space ID.
            benchmark_question_ids: Optional list of specific benchmark question IDs to evaluate.
                                   If None, evaluates all benchmark questions.

        Returns:
            Dict with eval_run_id and eval_run_status.
        """
        body = {}
        if benchmark_question_ids:
            body["benchmark_question_ids"] = benchmark_question_ids

        response = self._api_post(
            f"/api/2.0/genie/spaces/{space_id}/eval-runs",
            body=body,
        )
        return self._normalize_eval_run(response)

    @staticmethod
    def _normalize_eval_run(run: dict) -> dict:
        """Ensure eval run dicts always have 'eval_run_id' and 'eval_run_status'."""
        if "eval_run_id" not in run and "id" in run:
            run["eval_run_id"] = run["id"]
        if "eval_run_status" not in run and "status" in run:
            run["eval_run_status"] = run["status"]
        return run

    @staticmethod
    def _normalize_eval_result(result: dict) -> dict:
        """Ensure eval result dicts always have 'result_id'."""
        if "result_id" not in result and "id" in result:
            result["result_id"] = result["id"]
        return result

    def get_eval_run(self, space_id: str, eval_run_id: str) -> dict:
        """Get the status of an evaluation run.

        Returns:
            Dict with eval_run_id, eval_run_status, and other metadata.
        """
        response = self._api_get(
            f"/api/2.0/genie/spaces/{space_id}/eval-runs/{eval_run_id}",
        )
        return self._normalize_eval_run(response)

    def poll_eval_run(
        self,
        space_id: str,
        eval_run_id: str,
        poll_interval: int = 30,
        timeout: int = 1800,
    ) -> dict:
        """Poll an evaluation run until it completes.

        Args:
            space_id: The Genie Space ID.
            eval_run_id: The evaluation run ID.
            poll_interval: Seconds between polls.
            timeout: Maximum seconds to wait.

        Returns:
            The completed eval run response.

        Raises:
            TimeoutError: If the run doesn't complete within the timeout.
            RuntimeError: If the run fails.
        """
        start = time.time()
        while True:
            run = self.get_eval_run(space_id, eval_run_id)
            status = run.get("eval_run_status", "")

            if status == "DONE":
                return run
            if status in ("FAILED", "CANCELLED"):
                raise RuntimeError(
                    f"Eval run {eval_run_id} ended with status: {status}"
                )

            elapsed = time.time() - start
            print(f"  Eval run status: {status} ({int(elapsed)}s elapsed, polling every {poll_interval}s)")
            time.sleep(poll_interval)

            if time.time() - start >= timeout:
                raise TimeoutError(
                    f"Eval run {eval_run_id} did not complete within {timeout}s "
                    f"(last status: {status})"
                )

    def list_eval_results(self, space_id: str, eval_run_id: str) -> list[dict]:
        """List all evaluation results for a completed run.

        Handles pagination automatically.

        Returns:
            List of result summary dicts.
        """
        all_results = []
        page_token = None

        while True:
            params = {}
            if page_token:
                params["page_token"] = page_token

            response = self._api_get(
                f"/api/2.0/genie/spaces/{space_id}/eval-runs/{eval_run_id}/results",
                params=params,
            )

            results = response.get("eval_results", response.get("results", []))
            all_results.extend(self._normalize_eval_result(r) for r in results)

            page_token = response.get("next_page_token")
            if not page_token:
                break

        return all_results

    def get_eval_result_details(
        self,
        space_id: str,
        eval_run_id: str,
        result_id: str,
    ) -> dict:
        """Get detailed evaluation result for a specific benchmark question.

        Returns:
            Dict with question, ground_truth_sql, generated_sql, assessment,
            assessment_reasons, and other details.
        """
        return self._api_get(
            f"/api/2.0/genie/spaces/{space_id}/eval-runs/{eval_run_id}/results/{result_id}",
        )

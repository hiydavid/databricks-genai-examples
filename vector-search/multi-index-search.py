from databricks.vector_search.client import VectorSearchClient
import asyncio
import mlflow
import mlflow.pyfunc
import pandas as pd
import logging
import os
from typing import Any, Dict, List, Optional, Union


class VectorSearchPyfuncModel(mlflow.pyfunc.PythonModel):
    """
    Databricks Model Serving wrapper for Vector Search.
    ───────────────────────────────────────────────────
    • `predict()` (sync) ⇢ internal `_perform_vector_search()` (async)
    • Filters available: query_text, folder, to_emails, from_emails, date range.
    • Returned JSON schema: one column called `results`, each cell is a list of
      dicts (the search hits).
    """

    # config - replace with your actual endpoint and index names
    _endpoint_name = "your-vector-search-endpoint"
    _index_names = [
        "catalog.schema.index_shard_1",
        "catalog.schema.index_shard_2",
        "catalog.schema.index_shard_3",
        "catalog.schema.index_shard_4",
        "catalog.schema.index_shard_5",
    ]
    _result_columns = [
        "message_id",
        "folder",
        "to",
        "from",
        "email_timestamp_local",
        "email_timestamp_unix",
        "content",
    ]

    # log
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # internal coroutine
    async def _perform_vector_search(
        self,
        query_text: str,
        folder: Optional[Union[str, List[str]]] = None,
        to_emails: Optional[Union[str, List[str]]] = None,
        from_emails: Optional[Union[str, List[str]]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        num_results: int = 10,
        query_type: str = "hybrid",
        timeout_per_index: float = 30.0,
    ) -> pd.DataFrame:
        """
        Runs the vector search concurrently across all shards and returns a
        DataFrame of the top `num_results` hits, ordered by similarity score.
        """
        columns = self._result_columns
        vsc = VectorSearchClient(
            workspace_url=os.environ.get("DATABRICKS_HOST"),
            personal_access_token=os.environ.get("DATABRICKS_TOKEN"),
        )
        filters: Dict[str, Any] = {}

        # build filters
        if folder:
            if isinstance(folder, str):
                filters["folder"] = [folder]
            else:
                filters[" OR ".join(["folder"] * len(folder))] = folder

        if to_emails:
            if isinstance(to_emails, str):
                filters["to"] = [to_emails]
            else:
                filters[" OR ".join(["to"] * len(to_emails))] = to_emails

        if from_emails:
            if isinstance(from_emails, str):
                filters["from"] = [from_emails]
            else:
                filters[" OR ".join(["from"] * len(from_emails))] = from_emails

        if start_date:
            filters["email_timestamp_unix >="] = int(
                pd.to_datetime(start_date).timestamp()
            )
        if end_date:
            filters["email_timestamp_unix <="] = int(
                pd.to_datetime(end_date).timestamp()
            )

        # per-index helper
        async def _search(ix: str) -> List:
            self.logger.info(f"Searching {ix}")
            try:
                loop = asyncio.get_event_loop()

                def _sync():
                    return (
                        vsc.get_index(self._endpoint_name, ix)
                        .similarity_search(
                            query_text=query_text,
                            columns=columns,
                            num_results=num_results,
                            query_type=query_type,
                            filters=filters,
                        )
                        .get("result", {})
                        .get("data_array", [])
                    )

                return await loop.run_in_executor(None, _sync)
            except Exception as exc:
                self.logger.error(f"{ix} failed: {exc}")
                return []

        # gather / flatten
        rows = await asyncio.gather(*(_search(ix) for ix in self._index_names))
        hits = [r for sub in rows if sub for r in sub]

        if not hits:
            empty_columns = columns + ["score"]
            return pd.DataFrame(columns=pd.Index(empty_columns))

        df = (
            pd.DataFrame(hits, columns=pd.Index(columns + ["score"]))
            .astype(
                {
                    "message_id": str,
                    "folder": str,
                    "to": str,
                    "from": str,
                    "email_timestamp_local": str,
                    "email_timestamp_unix": float,
                    "content": str,
                    "score": float,
                }
            )
            .sort_values("score", ascending=False)
            .head(num_results)
        )
        return df

    # helpers
    @staticmethod
    def _row_to_kwargs(row: pd.Series) -> Dict[str, Any]:
        """Translate one request row to kwargs for `_perform_vector_search`."""
        num_results_value = row.get("num_results", 10)
        if num_results_value is None:
            num_results_value = 10

        return {
            "query_text": row["query_text"],
            "folder": row.get("folder"),
            "to_emails": row.get("to_emails"),
            "from_emails": row.get("from_emails"),
            "start_date": row.get("start_date"),
            "end_date": row.get("end_date"),
            "num_results": int(num_results_value),
        }

    def _run_single_row(self, row: pd.Series) -> List[Dict[str, Any]]:
        """Execute search for one row synchronously."""
        df = asyncio.run(self._perform_vector_search(**self._row_to_kwargs(row)))
        return df.to_dict(orient="records")

    # MLflow entrypoint
    def predict(self, context, model_input: pd.DataFrame) -> pd.DataFrame:
        """
        Databricks Model Serving passes a pandas DataFrame → we respond with a
        one-column DataFrame (`results`) whose cells are lists of dicts.
        """
        if not isinstance(model_input, pd.DataFrame):
            model_input = pd.DataFrame(model_input)

        results = model_input.apply(self._run_single_row, axis=1)
        return pd.DataFrame({"results": results})


mlflow.models.set_model(model=VectorSearchPyfuncModel())

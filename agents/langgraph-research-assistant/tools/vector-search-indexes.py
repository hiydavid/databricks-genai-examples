# Databricks notebook source
# MAGIC %md
# MAGIC # Create & Update Vector Search Indexes
# MAGIC
# MAGIC User `Serverless` compute

# COMMAND ----------

# MAGIC %pip install databricks-vectorsearch --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

catalog = "catalog"
schema = "schema"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enable CDC

# COMMAND ----------

table_list = [
    f"{catalog}.{schema}.sec_10k_business",
    f"{catalog}.{schema}.sec_10k_others",
    f"{catalog}.{schema}.earnings_call_transcripts",
]

for table in table_list:
    # Get current table properties
    properties = (
        spark.sql(f"DESCRIBE DETAIL {table}")
        .select("properties")
        .collect()[0]["properties"]
    )

    # Check if CDC is already enabled
    if not (properties and properties.get("delta.enableChangeDataFeed") == "true"):
        print(f"Enabling CDC for table: {table}")
        spark.sql(
            f"ALTER TABLE {table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
        )
    else:
        print(f"CDC already enabled for table: {table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Vector Search Endpoint

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
from databricks.sdk import WorkspaceClient
import time

# COMMAND ----------

vs_endpoint_name = "your-vector-search-endpoint"
vs_endpoint_type = "STANDARD"
embedding_endpoint_name = "databricks-gte-large-en"

# COMMAND ----------

# Initialize clients
vsc = VectorSearchClient()
wc = WorkspaceClient()

try:
    endpoints = vsc.list_endpoints()
    endpoint_exists = any(
        endpoint.get("name") == vs_endpoint_name
        for endpoint in endpoints.get("endpoints", [])
    )
    if not endpoint_exists:
        print(f"Creating vector search endpoint: {vs_endpoint_name}")
        vsc.create_endpoint(name=vs_endpoint_name, endpoint_type=vs_endpoint_type)
        # Wait for endpoint to be ready
        while True:
            endpoint_status = (
                vsc.get_endpoint(vs_endpoint_name)
                .get("endpoint_status", {})
                .get("state")
            )
            if endpoint_status == "ONLINE":
                print(f"Endpoint {vs_endpoint_name} is online")
                break
            elif endpoint_status in ["FAILED", "DELETED"]:
                raise Exception(
                    f"Endpoint {vs_endpoint_name} creation failed with state: {endpoint_status}"
                )
            print(
                f"Waiting for endpoint {vs_endpoint_name} to be ready... "
                f"Current state: {endpoint_status}"
            )
            time.sleep(30)
    else:
        print(f"Endpoint {vs_endpoint_name} already exists")
except Exception as e:
    raise Exception(f"Failed to create or verify endpoint {vs_endpoint_name}: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Delta Sync VS Indexes

# COMMAND ----------

# Initialize clients
vsc = VectorSearchClient()
wc = WorkspaceClient()

# COMMAND ----------


def create_vs_index(vs_endpoint_name, base_table_name):
    try:
        indexes = vsc.list_indexes(vs_endpoint_name).get("vector_indexes", [])
        index_exists = any(
            index.get("name") == base_table_name + "_vsindex" for index in indexes
        )

        if not index_exists:
            vsc.create_delta_sync_index(
                endpoint_name=vs_endpoint_name,
                index_name=base_table_name + "_vsindex",
                source_table_name=base_table_name,
                primary_key="doc_id",
                pipeline_type="TRIGGERED",
                embedding_source_column="doc_chunk",
                embedding_model_endpoint_name=embedding_endpoint_name,
            )
            print(f"Creating vector search index: {base_table_name}_vsindex")
        else:
            vsc.get_index(vs_endpoint_name, base_table_name + "_vsindex").sync()
            print(f"Triggered sync for index: {base_table_name}_vsindex")
    except Exception as e:
        raise Exception(f"Failed to create index {base_table_name}_vsindex: {str(e)}")


# COMMAND ----------

for table in table_list:
    create_vs_index(vs_endpoint_name, table)

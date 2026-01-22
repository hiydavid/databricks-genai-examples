# Databricks notebook source
# MAGIC %md
# MAGIC # Vector Search Index Creation
# MAGIC
# MAGIC This notebook creates a Delta Sync Vector Search index from the parsed document chunks.
# MAGIC The index uses auto-embedding with `databricks-gte-large-en` model.

# COMMAND ----------

# MAGIC %pip install -r ../requirements.txt --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import time

from databricks.vector_search.client import VectorSearchClient

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Configuration

# COMMAND ----------

# Job parameters (DAB populates these via base_parameters)
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("vs_endpoint", "")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VS_ENDPOINT = dbutils.widgets.get("vs_endpoint")

if not CATALOG or not SCHEMA or not VS_ENDPOINT:
    raise ValueError("Required parameters: catalog, schema, vs_endpoint")

# Derive paths from parameters
CHUNKS_TABLE = f"{CATALOG}.{SCHEMA}.user_guide_chunks"
VS_INDEX = f"{CATALOG}.{SCHEMA}.user_guide_chunks_index"

# Embedding model to use
EMBEDDING_MODEL = "databricks-gte-large-en"

print(f"Catalog: {CATALOG}")
print(f"Schema: {SCHEMA}")
print(f"Vector Search Endpoint: {VS_ENDPOINT}")
print(f"Vector Search Index: {VS_INDEX}")
print(f"Source Table: {CHUNKS_TABLE}")
print(f"Embedding Model: {EMBEDDING_MODEL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Vector Search Client

# COMMAND ----------

vsc = VectorSearchClient(disable_notice=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create or Get Vector Search Endpoint

# COMMAND ----------


def wait_for_endpoint_ready(endpoint_name: str, timeout_minutes: int = 30):
    """Wait for Vector Search endpoint to be ready."""
    start_time = time.time()
    timeout_seconds = timeout_minutes * 60

    while True:
        try:
            endpoint = vsc.get_endpoint(endpoint_name)
            status = endpoint.get("endpoint_status", {}).get("state", "UNKNOWN")

            if status == "ONLINE":
                print(f"Endpoint {endpoint_name} is ONLINE")
                return True
            elif status in ["FAILED", "DELETED"]:
                raise Exception(f"Endpoint {endpoint_name} is in {status} state")

            elapsed = time.time() - start_time
            if elapsed > timeout_seconds:
                raise TimeoutError(
                    f"Timeout waiting for endpoint {endpoint_name} to be ready"
                )

            print(f"Endpoint status: {status}. Waiting...")
            time.sleep(30)
        except Exception as e:
            if "NOT_FOUND" in str(e):
                raise Exception(f"Endpoint {endpoint_name} not found")
            raise


# COMMAND ----------

# Create endpoint if it doesn't exist
try:
    endpoint = vsc.get_endpoint(VS_ENDPOINT)
    print(f"Endpoint {VS_ENDPOINT} already exists")
except Exception as e:
    if "NOT_FOUND" in str(e) or "does not exist" in str(e).lower():
        print(f"Creating endpoint {VS_ENDPOINT}...")
        vsc.create_endpoint(name=VS_ENDPOINT, endpoint_type="STANDARD")
        print(f"Endpoint {VS_ENDPOINT} creation initiated")
    else:
        raise

# Wait for endpoint to be ready
wait_for_endpoint_ready(VS_ENDPOINT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enable Change Data Feed on Source Table

# COMMAND ----------

# Delta Sync indexes require Change Data Feed (CDF) to be enabled on the source table
spark.sql(f"ALTER TABLE {CHUNKS_TABLE} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
print(f"Change Data Feed enabled on {CHUNKS_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create or Sync Delta Sync Index

# COMMAND ----------


def wait_for_index_ready(
    endpoint_name: str, index_name: str, timeout_minutes: int = 60
):
    """Wait for Vector Search index to be ready."""
    start_time = time.time()
    timeout_seconds = timeout_minutes * 60

    while True:
        try:
            index = vsc.get_index(endpoint_name=endpoint_name, index_name=index_name)
            status = index.describe().get("status", {}).get("ready", False)
            detailed_state = (
                index.describe().get("status", {}).get("detailed_state", "UNKNOWN")
            )

            if status:
                print(f"Index {index_name} is READY")
                return True
            elif detailed_state in ["FAILED"]:
                raise Exception(f"Index {index_name} is in {detailed_state} state")

            elapsed = time.time() - start_time
            if elapsed > timeout_seconds:
                raise TimeoutError(
                    f"Timeout waiting for index {index_name} to be ready"
                )

            print(f"Index status: ready={status}, state={detailed_state}. Waiting...")
            time.sleep(30)
        except Exception as e:
            if "NOT_FOUND" in str(e):
                raise Exception(f"Index {index_name} not found")
            raise


# COMMAND ----------

# Create new index or sync if exists
try:
    # Check if index already exists
    index = vsc.get_index(endpoint_name=VS_ENDPOINT, index_name=VS_INDEX)
    print(f"Index {VS_INDEX} already exists. Triggering sync...")
    index.sync()
except Exception as e:
    if "NOT_FOUND" in str(e) or "does not exist" in str(e).lower():
        print(f"Creating Delta Sync index {VS_INDEX}...")
        vsc.create_delta_sync_index(
            endpoint_name=VS_ENDPOINT,
            index_name=VS_INDEX,
            source_table_name=CHUNKS_TABLE,
            primary_key="chunk_id",
            pipeline_type="TRIGGERED",
            embedding_source_column="text_content",
            embedding_model_endpoint_name=EMBEDDING_MODEL,
        )
        print(f"Index {VS_INDEX} creation initiated")
    else:
        raise

# COMMAND ----------

# Wait for index to be ready
wait_for_index_ready(VS_ENDPOINT, VS_INDEX)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Index

# COMMAND ----------

# Get index details
index = vsc.get_index(endpoint_name=VS_ENDPOINT, index_name=VS_INDEX)
index_info = index.describe()

print("\n=== Index Information ===")
print(f"Name: {index_info.get('name')}")
print(f"Status: {index_info.get('status')}")
print(f"Primary Key: {index_info.get('primary_key')}")
print(f"Index Type: {index_info.get('index_type')}")

# COMMAND ----------

# Test a sample query
test_query = "How do I configure the system?"
print(f"\nTest query: '{test_query}'")

results = index.similarity_search(
    query_text=test_query,
    columns=["chunk_id", "text_content", "source_path"],
    num_results=3,
)

print("\nTop 3 results:")
for i, row in enumerate(results.get("result", {}).get("data_array", [])):
    print(f"\n{i+1}. Chunk ID: {row[0]}")
    print(f"   Source: {row[2]}")
    print(f"   Content: {row[1][:200]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n=== Vector Index Creation Summary ===")
print(f"Endpoint: {VS_ENDPOINT}")
print(f"Index: {VS_INDEX}")
print(f"Source Table: {CHUNKS_TABLE}")
print(f"Embedding Model: {EMBEDDING_MODEL}")
print(f"Status: READY")

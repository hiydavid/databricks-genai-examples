# Databricks notebook source

# MAGIC %md
# MAGIC # 00 — Setup: memory store, orders data, and tools
# MAGIC
# MAGIC This notebook creates the shared foundation for the Managed Memory demo:
# MAGIC
# MAGIC 1. A **Unity Catalog memory store** (Beta) that will back all later memory types.
# MAGIC 2. A synthetic **`support_orders` Delta table** — fake orders for a support copilot.
# MAGIC 3. A read-only **`lookup_order`** tool the copilot uses to answer questions.
# MAGIC 4. A minimal **`chat`** helper that talks to a pay-per-token foundation model endpoint.
# MAGIC
# MAGIC Everything is **idempotent**: rerun this notebook as many times as you like.
# MAGIC
# MAGIC **Prerequisites**
# MAGIC - A workspace where the *Managed agent memory* preview (Beta) is enabled.
# MAGIC - `CREATE MEMORY STORE` privilege on the target schema.
# MAGIC - Access to the `databricks-glm-5-3-flash` pay-per-token endpoint (or change the widget below).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC All resource names are configurable. Defaults use `main.default`, which exists on
# MAGIC every Unity Catalog workspace. Override the values here or via the widgets when the
# MAGIC notebook runs in the workspace.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "default")
dbutils.widgets.text("memory_store", "support_agent_memory")
dbutils.widgets.text("model_endpoint", "databricks-glm-5-3-flash")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
MEMORY_STORE_NAME = f"{CATALOG}.{SCHEMA}.{dbutils.widgets.get('memory_store')}"
ORDERS_TABLE = f"{CATALOG}.{SCHEMA}.support_orders"
# Pay-per-token Foundation Model APIs endpoint (OpenAI-compatible chat completions).
MODEL_ENDPOINT = dbutils.widgets.get("model_endpoint")

print(f"memory store: {MEMORY_STORE_NAME}")
print(f"orders table:  {ORDERS_TABLE}")
print(f"model endpoint: {MODEL_ENDPOINT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create the memory store (idempotent)
# MAGIC
# MAGIC A memory store is a Unity Catalog securable — a governed container for memory
# MAGIC entries and conversations. Databricks runs the storage and isolation, so there is
# MAGIC no infrastructure to deploy. This demo only needs one store; later phases split
# MAGIC memory *types* (episodic, semantic, procedural) by scope and naming convention,
# MAGIC not by store.
# MAGIC
# MAGIC The creation logic is get-first: look the store up, create it only when missing,
# MAGIC and tolerate a concurrent-create race.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError

w = WorkspaceClient()  # auth resolves automatically inside a notebook


def _get_memory_store(full_name: str):
    """Return the memory store object, or None if it does not exist yet."""
    try:
        return w.api_client.do(
            "GET", f"/api/2.1/unity-catalog/memory-stores/{full_name}"
        )
    except DatabricksError as e:
        if getattr(e, "error_code", "") in ("RESOURCE_DOES_NOT_EXIST", "NOT_FOUND"):
            return None
        raise


def ensure_memory_store(full_name: str) -> dict:
    """Idempotently create a memory store and return its securable object."""
    existing = _get_memory_store(full_name)
    if existing is not None:
        print(f"memory store already exists: {full_name}")
        return existing

    catalog, schema, name = full_name.split(".")
    try:
        store = w.api_client.do(
            "POST",
            "/api/2.1/unity-catalog/memory-stores",
            body={
                "name": name,
                "catalog_name": catalog,
                "schema_name": schema,
                "comment": "Support agent memory for the Managed Memory demo.",
            },
        )
        print(f"created memory store: {store.get('full_name', full_name)}")
        return store
    except DatabricksError as e:
        # Someone else created it between our GET and POST — treat as success.
        if getattr(e, "error_code", "") == "RESOURCE_ALREADY_EXISTS":
            print(f"memory store already exists (created concurrently): {full_name}")
            return _get_memory_store(full_name)
        raise


memory_store = ensure_memory_store(MEMORY_STORE_NAME)
memory_store

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create and seed the synthetic `support_orders` table
# MAGIC
# MAGIC The support copilot answers questions about these orders. All rows are fake;
# MAGIC statuses include a few `delayed` orders, which later phases use for a
# MAGIC delayed-replacement playbook. Seeding only happens when the table is empty, so
# MAGIC reruns never duplicate or churn the data.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {ORDERS_TABLE} (
  order_id      STRING,
  customer_id   STRING,
  customer_name STRING,
  status        STRING,
  order_date    DATE,
  items         STRING,   -- JSON array of {sku, name, qty}
  total_usd     DOUBLE,
  note          STRING
)
""")

# COMMAND ----------

import datetime
import json

from pyspark.sql import types as T

SEED_ORDERS = [
    # order_id, customer_id, customer_name, status, order_date, items, total_usd, note
    ("SO-1001", "CUST-001", "Avery Chen", "shipped", datetime.date(2025, 8, 12),
     json.dumps([{"sku": "SKU-4410", "name": "Mechanical keyboard", "qty": 1}]), 129.99,
     "Tracking uploaded by carrier."),
    ("SO-1002", "CUST-001", "Avery Chen", "delivered", datetime.date(2025, 7, 3),
     json.dumps([{"sku": "SKU-2201", "name": "27-inch monitor", "qty": 2}]), 559.98,
     "Signed for at front desk."),
    ("SO-1003", "CUST-002", "Priya Nair", "delayed", datetime.date(2025, 8, 25),
     json.dumps([{"sku": "SKU-8873", "name": "USB-C dock", "qty": 1}]), 89.50,
     "Warehouse backorder; new ETA 2025-09-20."),
    ("SO-1004", "CUST-002", "Priya Nair", "processing", datetime.date(2025, 9, 1),
     json.dumps([{"sku": "SKU-1190", "name": "Laptop stand", "qty": 1}]), 42.00,
     "Awaiting pick at fulfillment center."),
    ("SO-1005", "CUST-003", "Marco Silva", "delivered", datetime.date(2025, 6, 18),
     json.dumps([{"sku": "SKU-3345", "name": "Noise-canceling headset", "qty": 1}]), 199.00,
     "Customer left a five-star review."),
    ("SO-1006", "CUST-003", "Marco Silva", "cancelled", datetime.date(2025, 8, 2),
     json.dumps([{"sku": "SKU-7102", "name": "Webcam 4K", "qty": 1}]), 119.00,
     "Cancelled before shipment; refund issued."),
    ("SO-1007", "CUST-004", "Dana Okafor", "delayed", datetime.date(2025, 8, 30),
     json.dumps([{"sku": "SKU-9901", "name": "Standing desk", "qty": 1}]), 549.00,
     "Supplier shortage; replacement options discussed."),
    ("SO-1008", "CUST-004", "Dana Okafor", "shipped", datetime.date(2025, 9, 3),
     json.dumps([{"sku": "SKU-5520", "name": "Desk mat", "qty": 1}]), 35.00,
     "Shipped in the same box as a prior order."),
]

seed_schema = T.StructType([
    T.StructField("order_id", T.StringType()),
    T.StructField("customer_id", T.StringType()),
    T.StructField("customer_name", T.StringType()),
    T.StructField("status", T.StringType()),
    T.StructField("order_date", T.DateType()),
    T.StructField("items", T.StringType()),
    T.StructField("total_usd", T.DoubleType()),
    T.StructField("note", T.StringType()),
])

if spark.table(ORDERS_TABLE).limit(1).count() == 0:
    spark.createDataFrame(SEED_ORDERS, seed_schema).write.mode("append").saveAsTable(ORDERS_TABLE)
    print(f"seeded {len(SEED_ORDERS)} synthetic orders into {ORDERS_TABLE}")
else:
    print(f"{ORDERS_TABLE} already has data; skipped seeding")

display(spark.table(ORDERS_TABLE))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The `lookup_order` tool
# MAGIC
# MAGIC A deliberately tiny, **read-only** tool: one order ID in, one dict out. The copilot
# MAGIC (and later notebooks) calls it directly and hands the JSON to the model. Real
# MAGIC deployments would register it as an actual tool for the agent framework; the
# MAGIC manual pattern here keeps the memory mechanics in the foreground.

# COMMAND ----------

from pyspark.sql import functions as F


def lookup_order(order_id: str) -> dict:
    """Return the support record for one order, or an error payload.

    Read-only: this function never writes to the orders table.
    """
    rows = (
        spark.table(ORDERS_TABLE)
        .where(F.col("order_id") == order_id)
        .collect()
    )
    if not rows:
        return {"error": f"order {order_id} not found"}
    record = rows[0].asDict()
    record["order_date"] = str(record["order_date"])  # JSON-serializable
    return record


# Smoke test the tool.
print(json.dumps(lookup_order("SO-1001"), indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. The `chat` helper
# MAGIC
# MAGIC One call to the configured pay-per-token endpoint using the OpenAI-compatible
# MAGIC Chat Completions API. `mlflow.deployments` is preinstalled on Databricks Runtime
# MAGIC and picks up notebook authentication automatically, so there is no token handling
# MAGIC anywhere in this demo.

# COMMAND ----------

import mlflow.deployments

_deploy_client = mlflow.deployments.get_deploy_client("databricks")


def chat(messages: list, temperature: float = 0.1) -> str:
    """Send one OpenAI-style messages list to the model endpoint, return the reply text."""
    response = _deploy_client.predict(
        endpoint=MODEL_ENDPOINT,
        inputs={"messages": messages, "temperature": temperature},
    )
    return response["choices"][0]["message"]["content"]


# Verify the endpoint responds before moving on.
print(chat([{"role": "user", "content": "Reply with exactly: ok"}]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
# MAGIC
# MAGIC The foundation is in place:
# MAGIC - the memory store `support_agent_memory` (used from Phase 2 onward)
# MAGIC - the synthetic `support_orders` Delta table
# MAGIC - read-only `lookup_order(order_id)` tool
# MAGIC - `chat(messages)` helper
# MAGIC
# MAGIC **Next:** run `01_stateless_baseline` to see why the copilot needs memory at all.

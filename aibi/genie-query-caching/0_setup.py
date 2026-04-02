# Databricks notebook source
# MAGIC %md
# MAGIC # Genie Query Caching — Setup
# MAGIC
# MAGIC Creates all required infrastructure for the 3 caching scenarios:
# MAGIC
# MAGIC | Resource | Used by |
# MAGIC |----------|---------|
# MAGIC | Unity Catalog schema | All |
# MAGIC | Lakebase table `genie_cache` (pgvector) | Scenarios 1 & 3 |
# MAGIC | Delta table `cache_store` | Scenario 2 |
# MAGIC | Delta table `cache_knowledge_base` | Scenario 3 |
# MAGIC | Vector Search endpoint + indexes | Scenarios 2 & 3 |
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Copy `configs.template.yaml` → `configs.yaml` and fill in your values
# MAGIC - A Lakebase instance with the pgvector extension available
# MAGIC - A Databricks secret scope with Lakebase credentials
# MAGIC - Run on **Serverless** or a cluster with network access to Lakebase

# COMMAND ----------

# MAGIC %pip install "databricks-sdk>=0.85" "databricks-vectorsearch" "psycopg[binary]>=3.1" "pgvector>=0.3" pyyaml --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Configuration

# COMMAND ----------

from utils import (
    generate_id,
    get_lakebase_connection,
    lakebase_cache_write,
    load_config,
    normalize_question,
    sync_vs_index_and_wait,
    utcnow,
)

config = load_config("./configs.yaml")

CATALOG = config["catalog"]
SCHEMA = config["schema"]
VS_ENDPOINT = config["vs_endpoint"]
EMBEDDING_MODEL = config.get("embedding_model", "databricks-qwen3-embedding-0-6b")
EMBEDDING_DIM = config.get("embedding_dimension", 1024)

print(f"Catalog:          {CATALOG}")
print(f"Schema:           {SCHEMA}")
print(f"VS Endpoint:      {VS_ENDPOINT}")
print(f"Embedding Model:  {EMBEDDING_MODEL}")
print(f"Embedding Dim:    {EMBEDDING_DIM}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Catalog & Schema

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
print(f"Catalog and schema ready: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Delta Table — `cache_store` (Scenario 2)
# MAGIC
# MAGIC Stores cached Genie responses for the Vector Search caching scenario.
# MAGIC Change Data Feed (CDF) is enabled so the Delta Sync VS index can track changes.

# COMMAND ----------

CACHE_STORE_TABLE = f"{CATALOG}.{SCHEMA}.cache_store"

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CACHE_STORE_TABLE} (
        id STRING NOT NULL,
        question_text STRING,
        question_normalized STRING,
        cached_sql STRING,
        cached_response STRING,
        created_at TIMESTAMP,
        hit_count INT DEFAULT 0
    )
    USING DELTA
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")
print(f"Delta table ready: {CACHE_STORE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Delta Table — `cache_knowledge_base` (Scenario 3)
# MAGIC
# MAGIC Long-lived knowledge base of validated cache entries.  Entries are promoted
# MAGIC here from the L1 session cache after user validation or repeated use.

# COMMAND ----------

CACHE_KB_TABLE = f"{CATALOG}.{SCHEMA}.cache_knowledge_base"

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CACHE_KB_TABLE} (
        id STRING NOT NULL,
        question_text STRING,
        question_normalized STRING,
        cached_sql STRING,
        cached_response STRING,
        validated BOOLEAN DEFAULT false,
        validation_source STRING,
        created_at TIMESTAMP,
        hit_count INT DEFAULT 0
    )
    USING DELTA
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")
print(f"Delta table ready: {CACHE_KB_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Lakebase Table with pgvector (Scenarios 1 & 3)
# MAGIC
# MAGIC Creates the `genie_cache` table in Lakebase (Databricks-managed PostgreSQL)
# MAGIC with the pgvector extension for vector similarity search.
# MAGIC
# MAGIC - **HNSW index** on the embedding column for fast approximate nearest-neighbor search
# MAGIC - **B-tree index** on `question_normalized` for exact-match lookups
# MAGIC - **UNIQUE constraint** on `question_normalized` to support `ON CONFLICT` upserts

# COMMAND ----------

conn = get_lakebase_connection(config)
try:
    with conn.cursor() as cur:
        # Enable pgvector extension
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector")

        # Create cache table
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS genie_cache (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                question_normalized TEXT NOT NULL UNIQUE,
                embedding vector({EMBEDDING_DIM}),
                cached_sql TEXT,
                cached_response JSONB,
                session_id TEXT,
                created_at TIMESTAMPTZ DEFAULT now(),
                hit_count INTEGER DEFAULT 0
            )
        """)

        # HNSW index for vector similarity search
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_genie_cache_embedding
            ON genie_cache USING hnsw (embedding vector_cosine_ops)
            WITH (m = 16, ef_construction = 64)
        """)

        # B-tree index on session_id for session-scoped queries (Scenario 3)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_genie_cache_session
            ON genie_cache (session_id)
        """)

    conn.commit()
    print("Lakebase table 'genie_cache' ready with pgvector HNSW index")

    # Connectivity check: verify table exists
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM genie_cache")
        count = cur.fetchone()[0]
        print(f"  Current row count: {count}")

finally:
    conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Vector Search Endpoint

# COMMAND ----------

import time

from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient(disable_notice=True)


def wait_for_endpoint_ready(endpoint_name: str, timeout_minutes: int = 30):
    """Wait for a Vector Search endpoint to reach ONLINE state."""
    start_time = time.time()
    timeout_seconds = timeout_minutes * 60

    while True:
        endpoint = vsc.get_endpoint(endpoint_name)
        status = endpoint.get("endpoint_status", {}).get("state", "UNKNOWN")

        if status == "ONLINE":
            print(f"Endpoint {endpoint_name} is ONLINE")
            return
        if status in ("FAILED", "DELETED"):
            raise RuntimeError(f"Endpoint {endpoint_name} is in {status} state")
        if time.time() - start_time > timeout_seconds:
            raise TimeoutError(f"Timeout waiting for endpoint {endpoint_name}")

        print(f"  Endpoint status: {status}. Waiting...")
        time.sleep(30)


# COMMAND ----------

try:
    endpoint = vsc.get_endpoint(VS_ENDPOINT)
    print(f"Endpoint {VS_ENDPOINT} already exists")
except Exception as e:
    if "NOT_FOUND" in str(e) or "does not exist" in str(e).lower():
        print(f"Creating endpoint {VS_ENDPOINT}...")
        vsc.create_endpoint(name=VS_ENDPOINT, endpoint_type="STANDARD")
    else:
        raise

wait_for_endpoint_ready(VS_ENDPOINT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Vector Search Indexes
# MAGIC
# MAGIC Two Delta Sync indexes with managed embeddings on `question_text`:
# MAGIC 1. `cache_store_index` — for Scenario 2
# MAGIC 2. `cache_knowledge_base_index` — for Scenario 3 (L2)

# COMMAND ----------


def wait_for_index_ready(endpoint_name: str, index_name: str, timeout_minutes: int = 60):
    """Wait for a Vector Search index to be ready."""
    start_time = time.time()
    timeout_seconds = timeout_minutes * 60

    while True:
        index = vsc.get_index(endpoint_name=endpoint_name, index_name=index_name)
        status = index.describe().get("status", {})
        ready = status.get("ready", False)
        detailed = status.get("detailed_state", "UNKNOWN")

        if ready:
            print(f"Index {index_name} is READY")
            return
        if detailed == "FAILED":
            raise RuntimeError(f"Index {index_name} is in FAILED state")
        if time.time() - start_time > timeout_seconds:
            raise TimeoutError(f"Timeout waiting for index {index_name}")

        print(f"  Index status: ready={ready}, state={detailed}. Waiting...")
        time.sleep(30)


def create_or_sync_index(source_table: str, index_name: str):
    """Create a Delta Sync VS index or trigger sync if it already exists."""
    try:
        index = vsc.get_index(endpoint_name=VS_ENDPOINT, index_name=index_name)
        print(f"Index {index_name} already exists — triggering sync...")
        index.sync()
    except Exception as e:
        if "NOT_FOUND" in str(e) or "does not exist" in str(e).lower():
            print(f"Creating Delta Sync index {index_name}...")
            vsc.create_delta_sync_index(
                endpoint_name=VS_ENDPOINT,
                index_name=index_name,
                source_table_name=source_table,
                primary_key="id",
                pipeline_type="TRIGGERED",
                embedding_source_column="question_text",
                embedding_model_endpoint_name=EMBEDDING_MODEL,
            )
        else:
            raise


# COMMAND ----------

# Scenario 2 index
CACHE_STORE_INDEX = f"{CATALOG}.{SCHEMA}.cache_store_index"
create_or_sync_index(CACHE_STORE_TABLE, CACHE_STORE_INDEX)

# COMMAND ----------

# Scenario 3 index
CACHE_KB_INDEX = f"{CATALOG}.{SCHEMA}.cache_knowledge_base_index"
create_or_sync_index(CACHE_KB_TABLE, CACHE_KB_INDEX)

# COMMAND ----------

# Wait for both indexes
wait_for_index_ready(VS_ENDPOINT, CACHE_STORE_INDEX)
wait_for_index_ready(VS_ENDPOINT, CACHE_KB_INDEX)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Seed Demo Data
# MAGIC
# MAGIC Pre-populates all 3 cache stores with 5 seed entries so the scenario
# MAGIC notebooks show cache **HITs** immediately on the first pass.
# MAGIC
# MAGIC | Store | Scenario | Seeding method |
# MAGIC |-------|----------|----------------|
# MAGIC | Lakebase `genie_cache` (pgvector) | 1 & 3 | `lakebase_cache_write()` — upsert with real embeddings |
# MAGIC | Delta `cache_store` | 2 | Spark append (with duplicate check) |
# MAGIC | Delta `cache_knowledge_base` | 3 | Spark append (with duplicate check) + `validated=True` |

# COMMAND ----------

# The seed SQL references the Horizon Bank data schema (from genie-demo-data),
# which is separate from the cache schema. Adjust DATA_SCHEMA if your Horizon
# Bank tables live in a different schema.
DATA_SCHEMA = "horizon_bank"
ds = f"{CATALOG}.{DATA_SCHEMA}"

SEED_ENTRIES = [
    {
        "question_text": "What was total deposit volume in 2024?",
        "cached_sql": (
            f"SELECT SUM(amount_usd) AS total_deposit_volume\n"
            f"FROM {ds}.transactions\n"
            f"WHERE transaction_type = 'Deposit'\n"
            f"  AND transaction_year = 2024"
        ),
        "response_text": (
            "The total deposit volume in 2024 was approximately $15.2 million "
            "across all accounts and branches."
        ),
    },
    {
        "question_text": "Show monthly deposit trend from Jan 2023 to Dec 2025",
        "cached_sql": (
            f"SELECT transaction_year, transaction_month,\n"
            f"       SUM(amount_usd) AS monthly_deposit_volume\n"
            f"FROM {ds}.transactions\n"
            f"WHERE transaction_type = 'Deposit'\n"
            f"GROUP BY transaction_year, transaction_month\n"
            f"ORDER BY transaction_year, transaction_month"
        ),
        "response_text": (
            "Here is the monthly deposit trend from January 2023 through "
            "December 2025. Notable patterns include seasonal spikes in "
            "November/December and a Q2 2024 dip of approximately 15%."
        ),
    },
    {
        "question_text": "What is the average account balance for Private Client customers by state?",
        "cached_sql": (
            f"SELECT c.state,\n"
            f"       ROUND(AVG(a.current_balance_usd), 2) AS avg_balance\n"
            f"FROM {ds}.customers c\n"
            f"JOIN {ds}.accounts a ON c.customer_id = a.customer_id\n"
            f"WHERE c.relationship_tier = 'Private Client'\n"
            f"GROUP BY c.state\n"
            f"ORDER BY avg_balance DESC"
        ),
        "response_text": (
            "Private Client customers have average account balances roughly 3x "
            "higher than Standard tier. The highest averages are concentrated in "
            "New York, California, and Florida."
        ),
    },
    {
        "question_text": "Which 10 branches had the highest deposit volume this year?",
        "cached_sql": (
            f"SELECT b.branch_name, b.region,\n"
            f"       SUM(t.amount_usd) AS deposit_volume\n"
            f"FROM {ds}.transactions t\n"
            f"JOIN {ds}.branches b ON t.branch_id = b.branch_id\n"
            f"WHERE t.transaction_type = 'Deposit'\n"
            f"  AND t.transaction_year = 2025\n"
            f"GROUP BY b.branch_name, b.region\n"
            f"ORDER BY deposit_volume DESC\n"
            f"LIMIT 10"
        ),
        "response_text": (
            "The top 10 branches by deposit volume are led by the Manhattan "
            "Financial District and Miami South branches. Southeast branches "
            "show approximately 20% higher average transaction values."
        ),
    },
    {
        "question_text": "What is fee revenue per customer by relationship tier?",
        "cached_sql": (
            f"SELECT c.relationship_tier,\n"
            f"       COUNT(DISTINCT c.customer_id) AS customer_count,\n"
            f"       SUM(t.fee_usd) AS total_fee_revenue,\n"
            f"       ROUND(SUM(t.fee_usd) / COUNT(DISTINCT c.customer_id), 2) AS fee_per_customer\n"
            f"FROM {ds}.transactions t\n"
            f"JOIN {ds}.accounts a ON t.account_id = a.account_id\n"
            f"JOIN {ds}.customers c ON a.customer_id = c.customer_id\n"
            f"WHERE t.fee_usd > 0\n"
            f"GROUP BY c.relationship_tier\n"
            f"ORDER BY fee_per_customer DESC"
        ),
        "response_text": (
            "Fee revenue per customer varies significantly by tier. Private "
            "Client customers generate the highest fee revenue per customer, "
            "followed by Preferred and Standard tiers."
        ),
    },
]

print(f"Defined {len(SEED_ENTRIES)} seed entries")
for i, entry in enumerate(SEED_ENTRIES, 1):
    print(f"  {i}. {entry['question_text']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Seed Lakebase pgvector (Scenarios 1 & 3)
# MAGIC
# MAGIC Uses `lakebase_cache_write()` which handles normalization, embedding
# MAGIC generation via the Foundation Model API, and `ON CONFLICT` upsert.

# COMMAND ----------

for entry in SEED_ENTRIES:
    print(f"  Seeding: {entry['question_text'][:60]}...")
    lakebase_cache_write(
        config,
        question=entry["question_text"],
        sql=entry["cached_sql"],
        response_text=entry["response_text"],
    )

# Verify
conn = get_lakebase_connection(config)
with conn.cursor() as cur:
    cur.execute("SELECT count(*) FROM genie_cache")
    lb_count = cur.fetchone()[0]
print(f"\nLakebase genie_cache: {lb_count} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Seed Delta `cache_store` (Scenario 2)
# MAGIC
# MAGIC Appends rows to the Delta table. Skips if seed data already exists
# MAGIC (checked via row count) to avoid duplicates on re-run.

# COMMAND ----------

from pyspark.sql import Row

existing_count = spark.sql(f"SELECT count(*) FROM {CACHE_STORE_TABLE}").collect()[0][0]
if existing_count > 0:
    print(f"Delta table {CACHE_STORE_TABLE} already has {existing_count} rows — skipping seed")
else:
    rows = []
    for entry in SEED_ENTRIES:
        rows.append(
            Row(
                id=generate_id(),
                question_text=entry["question_text"],
                question_normalized=normalize_question(entry["question_text"]),
                cached_sql=entry["cached_sql"],
                cached_response=entry["response_text"],
                created_at=utcnow(),
                hit_count=0,
            )
        )
    df = spark.createDataFrame(rows)
    df.write.mode("append").saveAsTable(CACHE_STORE_TABLE)
    print(f"Seeded {len(rows)} rows into {CACHE_STORE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Seed Delta `cache_knowledge_base` (Scenario 3)
# MAGIC
# MAGIC Same entries but marked as `validated=True` with `validation_source="seed"`.

# COMMAND ----------

existing_count = spark.sql(f"SELECT count(*) FROM {CACHE_KB_TABLE}").collect()[0][0]
if existing_count > 0:
    print(f"Delta table {CACHE_KB_TABLE} already has {existing_count} rows — skipping seed")
else:
    rows = []
    for entry in SEED_ENTRIES:
        rows.append(
            Row(
                id=generate_id(),
                question_text=entry["question_text"],
                question_normalized=normalize_question(entry["question_text"]),
                cached_sql=entry["cached_sql"],
                cached_response=entry["response_text"],
                validated=True,
                validation_source="seed",
                created_at=utcnow(),
                hit_count=0,
            )
        )
    df = spark.createDataFrame(rows)
    df.write.mode("append").saveAsTable(CACHE_KB_TABLE)
    print(f"Seeded {len(rows)} rows into {CACHE_KB_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sync Vector Search Indexes

# COMMAND ----------

print("Syncing cache_store index...")
sync_vs_index_and_wait(vsc, VS_ENDPOINT, CACHE_STORE_INDEX)

print("Syncing cache_knowledge_base index...")
sync_vs_index_and_wait(vsc, VS_ENDPOINT, CACHE_KB_INDEX)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Setup

# COMMAND ----------

# Seed row counts
conn = get_lakebase_connection(config)
with conn.cursor() as cur:
    cur.execute("SELECT count(*) FROM genie_cache")
    lb_count = cur.fetchone()[0]

cs_count = spark.sql(f"SELECT count(*) FROM {CACHE_STORE_TABLE}").collect()[0][0]
kb_count = spark.sql(f"SELECT count(*) FROM {CACHE_KB_TABLE}").collect()[0][0]

print("=" * 60)
print("SETUP COMPLETE")
print("=" * 60)
print(f"  Catalog/Schema:        {CATALOG}.{SCHEMA}")
print(f"  Delta — cache_store:   {CACHE_STORE_TABLE} ({cs_count} rows)")
print(f"  Delta — knowledge_base:{CACHE_KB_TABLE} ({kb_count} rows)")
print(f"  Lakebase — genie_cache: ✓ pgvector + HNSW ({lb_count} rows)")
print(f"  VS Endpoint:           {VS_ENDPOINT}")
print(f"  VS Index (Scenario 2): {CACHE_STORE_INDEX}")
print(f"  VS Index (Scenario 3): {CACHE_KB_INDEX}")
print()
print("Next steps — run any scenario notebook:")
print("  1. 1_lakebase_pgvector_cache.py  — Scenario 1: Lakebase + pgvector")
print("  2. 2_vector_search_cache.py      — Scenario 2: Vector Search")
print("  3. 3_hybrid_cache.py             — Scenario 3: Hybrid (Recommended)")

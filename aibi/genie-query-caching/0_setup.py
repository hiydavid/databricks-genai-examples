# Databricks notebook source
# MAGIC %md
# MAGIC # Genie Query Caching — Setup
# MAGIC
# MAGIC Creates the infrastructure for the 3 caching scenarios.
# MAGIC
# MAGIC | Resource | Used by |
# MAGIC |----------|---------|
# MAGIC | Unity Catalog schema | All |
# MAGIC | Lakebase table `genie_cache` (pgvector) | Scenarios 1 & 3 |
# MAGIC | Delta table `cache_store` | Scenario 2 |
# MAGIC | Delta table `cache_knowledge_base` | Scenario 3 |
# MAGIC | Vector Search endpoint + indexes | Scenarios 2 & 3 |
# MAGIC
# MAGIC By default this notebook creates empty cache stores. Set `seed_demo_data: true`
# MAGIC in `configs.yaml` only when you want the scenario notebooks to start with hits.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Copy `configs.template.yaml` to `configs.yaml` and fill in your values
# MAGIC - A Lakebase instance with pgvector available
# MAGIC - A Databricks secret scope with Lakebase credentials
# MAGIC - Run on Serverless or a cluster with network access to Lakebase

# COMMAND ----------

# MAGIC %pip install "databricks-sdk>=0.85" "databricks-vectorsearch" "psycopg[binary]>=3.1" "pgvector>=0.3" pyyaml --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Configuration

# COMMAND ----------

from utils import (
    GLOBAL_SCOPE,
    build_cache_context,
    data_schema_name,
    delta_cache_upsert,
    get_lakebase_connection,
    lakebase_cache_write,
    load_config,
    sync_vs_index_and_wait,
    table_name,
)

config = load_config("./configs.yaml")
context = build_cache_context(config)

CATALOG = config["catalog"]
SCHEMA = config["schema"]
VS_ENDPOINT = config["vs_endpoint"]
EMBEDDING_MODEL = config.get("embedding_model", "databricks-qwen3-embedding-0-6b")
EMBEDDING_DIM = config.get("embedding_dimension", 1024)
SEED_DEMO_DATA = bool(config.get("seed_demo_data", False))

CACHE_STORE_TABLE = table_name(config, "cache_store")
CACHE_KB_TABLE = table_name(config, "cache_knowledge_base")
CACHE_STORE_INDEX = f"{CATALOG}.{SCHEMA}.cache_store_index"
CACHE_KB_INDEX = f"{CATALOG}.{SCHEMA}.cache_knowledge_base_index"

print(f"Catalog:           {CATALOG}")
print(f"Schema:            {SCHEMA}")
print(f"Data schema:       {data_schema_name(config)}")
print(f"Cache version:     {context.cache_version}")
print(f"Cache context key: {context.cache_context_key}")
print(f"VS Endpoint:       {VS_ENDPOINT}")
print(f"Embedding Model:   {EMBEDDING_MODEL}")
print(f"Embedding Dim:     {EMBEDDING_DIM}")
print(f"Seed demo data:    {SEED_DEMO_DATA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Catalog and Schema

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
print(f"Catalog and schema ready: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Cache Tables
# MAGIC
# MAGIC These v2 tables store cache metadata and generated SQL. They do not store an
# MAGIC authoritative result. Scenario notebooks re-execute cached SQL on every hit.

# COMMAND ----------

DELTA_CACHE_COLUMNS = {
    "id",
    "space_id",
    "cache_context_key",
    "cache_scope",
    "session_id",
    "question_text",
    "question_normalized",
    "generated_sql",
    "genie_response_text",
    "validated",
    "validation_source",
    "created_at",
    "updated_at",
    "last_hit_at",
    "hit_count",
}


def assert_delta_table_compatible(full_table_name: str):
    try:
        existing_columns = {field.name for field in spark.table(full_table_name).schema.fields}
    except Exception as e:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e) or "not found" in str(e).lower():
            return
        raise

    missing = sorted(DELTA_CACHE_COLUMNS - existing_columns)
    if missing:
        raise RuntimeError(
            f"{full_table_name} already exists with a v1/incompatible schema. "
            f"Missing columns: {missing}. Drop or migrate this cache table, then rerun setup."
        )


def create_delta_cache_table(full_table_name: str):
    assert_delta_table_compatible(full_table_name)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
            id STRING NOT NULL,
            space_id STRING NOT NULL,
            cache_context_key STRING NOT NULL,
            cache_scope STRING NOT NULL,
            session_id STRING NOT NULL,
            question_text STRING NOT NULL,
            question_normalized STRING NOT NULL,
            generated_sql STRING NOT NULL,
            genie_response_text STRING,
            validated BOOLEAN DEFAULT false,
            validation_source STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            last_hit_at TIMESTAMP,
            hit_count INT DEFAULT 0
        )
        USING DELTA
        TBLPROPERTIES (delta.enableChangeDataFeed = true)
    """)
    print(f"Delta table ready: {full_table_name}")


create_delta_cache_table(CACHE_STORE_TABLE)
create_delta_cache_table(CACHE_KB_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lakebase Table with pgvector
# MAGIC
# MAGIC The v2 unique key includes Genie space, cache context, scope, session, and
# MAGIC normalized question. This keeps Scenario 3's session cache isolated.

# COMMAND ----------

LAKEBASE_CACHE_COLUMNS = {
    "id",
    "space_id",
    "cache_context_key",
    "cache_scope",
    "session_id",
    "question_text",
    "question_normalized",
    "embedding",
    "generated_sql",
    "genie_response_text",
    "validated",
    "validation_source",
    "created_at",
    "updated_at",
    "last_hit_at",
    "hit_count",
}


conn = get_lakebase_connection(config)
try:
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector")

        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = 'genie_cache'
        """)
        existing_columns = {row[0] for row in cur.fetchall()}
        missing = sorted(LAKEBASE_CACHE_COLUMNS - existing_columns) if existing_columns else []
        if missing:
            raise RuntimeError(
                "Lakebase table genie_cache already exists with a v1/incompatible schema. "
                f"Missing columns: {missing}. Drop or migrate the Lakebase table, then rerun setup."
            )

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS genie_cache (
                id UUID PRIMARY KEY,
                space_id TEXT NOT NULL,
                cache_context_key TEXT NOT NULL,
                cache_scope TEXT NOT NULL,
                session_id TEXT NOT NULL DEFAULT '',
                question_text TEXT NOT NULL,
                question_normalized TEXT NOT NULL,
                embedding vector({EMBEDDING_DIM}),
                generated_sql TEXT NOT NULL,
                genie_response_text TEXT,
                validated BOOLEAN DEFAULT false,
                validation_source TEXT,
                created_at TIMESTAMPTZ DEFAULT now(),
                updated_at TIMESTAMPTZ DEFAULT now(),
                last_hit_at TIMESTAMPTZ,
                hit_count INTEGER DEFAULT 0,
                UNIQUE (
                    space_id,
                    cache_context_key,
                    cache_scope,
                    session_id,
                    question_normalized
                )
            )
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_genie_cache_embedding
            ON genie_cache USING hnsw (embedding vector_cosine_ops)
            WITH (m = 16, ef_construction = 64)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_genie_cache_context
            ON genie_cache (space_id, cache_context_key, cache_scope, session_id)
        """)

    conn.commit()
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM genie_cache")
        count = cur.fetchone()[0]
        print(f"Lakebase genie_cache ready with pgvector ({count} rows)")
finally:
    conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vector Search Endpoint and Indexes

# COMMAND ----------

import time

from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient(disable_notice=True)


def wait_for_endpoint_ready(endpoint_name: str, timeout_minutes: int = 30):
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


try:
    vsc.get_endpoint(VS_ENDPOINT)
    print(f"Endpoint {VS_ENDPOINT} already exists")
except Exception as e:
    if "NOT_FOUND" in str(e) or "does not exist" in str(e).lower():
        print(f"Creating endpoint {VS_ENDPOINT}...")
        vsc.create_endpoint(name=VS_ENDPOINT, endpoint_type="STANDARD")
    else:
        raise

wait_for_endpoint_ready(VS_ENDPOINT)

# COMMAND ----------

VS_COLUMNS_TO_SYNC = [
    "id",
    "space_id",
    "cache_context_key",
    "cache_scope",
    "session_id",
    "question_text",
    "question_normalized",
    "generated_sql",
    "genie_response_text",
    "validated",
    "validation_source",
    "created_at",
    "updated_at",
    "last_hit_at",
    "hit_count",
]


def wait_for_index_ready(endpoint_name: str, index_name: str, timeout_minutes: int = 60):
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
    try:
        index = vsc.get_index(endpoint_name=VS_ENDPOINT, index_name=index_name)
        print(f"Index {index_name} already exists; triggering sync...")
        print("  If this index was created before the v2 schema, delete it and rerun setup.")
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
                columns_to_sync=VS_COLUMNS_TO_SYNC,
            )
        else:
            raise


create_or_sync_index(CACHE_STORE_TABLE, CACHE_STORE_INDEX)
create_or_sync_index(CACHE_KB_TABLE, CACHE_KB_INDEX)
wait_for_index_ready(VS_ENDPOINT, CACHE_STORE_INDEX)
wait_for_index_ready(VS_ENDPOINT, CACHE_KB_INDEX)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional Seed Demo Data
# MAGIC
# MAGIC Seed rows use the Horizon Bank semantic layer where possible. Seeded mode is
# MAGIC useful for showing immediate hits; leave `seed_demo_data: false` to exercise
# MAGIC the cold path and Genie API calls.

# COMMAND ----------

ds = data_schema_name(config)

SEED_ENTRIES = [
    {
        "question_text": "What was total deposit volume in 2024?",
        "generated_sql": (
            f"SELECT MEASURE(`Deposit Volume`) AS deposit_volume\n"
            f"FROM {ds}.mv_banking_transactions\n"
            f"WHERE `Transaction Year` = 2024"
        ),
        "response_text": "Seeded validated SQL for 2024 deposit volume.",
    },
    {
        "question_text": "Show monthly deposit trend from Jan 2023 to Dec 2025",
        "generated_sql": (
            f"SELECT `Transaction Year`, `Transaction Month`,\n"
            f"       MEASURE(`Deposit Volume`) AS deposit_volume\n"
            f"FROM {ds}.mv_banking_transactions\n"
            f"WHERE `Transaction Month` BETWEEN DATE '2023-01-01' AND DATE '2025-12-01'\n"
            f"GROUP BY `Transaction Year`, `Transaction Month`\n"
            f"ORDER BY `Transaction Year`, `Transaction Month`"
        ),
        "response_text": "Seeded validated SQL for monthly deposit trend.",
    },
    {
        "question_text": "What is the average account balance for Private Client customers by state?",
        "generated_sql": (
            f"SELECT `State`, MEASURE(`Average Balance`) AS average_balance\n"
            f"FROM {ds}.mv_customer_health\n"
            f"WHERE `Relationship Tier` = 'Private Client'\n"
            f"GROUP BY `State`\n"
            f"ORDER BY average_balance DESC"
        ),
        "response_text": "Seeded validated SQL for Private Client average balances.",
    },
    {
        "question_text": "Which 10 branches had the highest deposit volume this year?",
        "generated_sql": (
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
        "response_text": "Seeded validated SQL for top branch deposit volume.",
    },
    {
        "question_text": "What is fee revenue per customer by relationship tier?",
        "generated_sql": (
            f"SELECT `Relationship Tier`,\n"
            f"       MEASURE(`Fee Revenue per Customer`) AS fee_revenue_per_customer\n"
            f"FROM {ds}.mv_banking_transactions\n"
            f"GROUP BY `Relationship Tier`\n"
            f"ORDER BY fee_revenue_per_customer DESC"
        ),
        "response_text": "Seeded validated SQL for fee revenue per customer.",
    },
]

print(f"Defined {len(SEED_ENTRIES)} optional seed entries")
for i, entry in enumerate(SEED_ENTRIES, 1):
    print(f"  {i}. {entry['question_text']}")

# COMMAND ----------

if SEED_DEMO_DATA:
    for entry in SEED_ENTRIES:
        print(f"  Seeding: {entry['question_text'][:60]}...")
        lakebase_cache_write(
            config,
            question=entry["question_text"],
            sql=entry["generated_sql"],
            response_text=entry["response_text"],
            cache_scope=GLOBAL_SCOPE,
            validated=True,
            validation_source="seed",
        )
        delta_cache_upsert(
            spark,
            CACHE_STORE_TABLE,
            config,
            question=entry["question_text"],
            sql=entry["generated_sql"],
            response_text=entry["response_text"],
            cache_scope=GLOBAL_SCOPE,
            validated=True,
            validation_source="seed",
        )
        delta_cache_upsert(
            spark,
            CACHE_KB_TABLE,
            config,
            question=entry["question_text"],
            sql=entry["generated_sql"],
            response_text=entry["response_text"],
            cache_scope=GLOBAL_SCOPE,
            validated=True,
            validation_source="seed",
        )

    print("\nSyncing Vector Search indexes after seed writes...")
    sync_vs_index_and_wait(vsc, VS_ENDPOINT, CACHE_STORE_INDEX)
    sync_vs_index_and_wait(vsc, VS_ENDPOINT, CACHE_KB_INDEX)
else:
    print("Skipping demo seed data. Scenario notebooks will exercise cold misses first.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Setup

# COMMAND ----------

conn = get_lakebase_connection(config)
with conn.cursor() as cur:
    cur.execute("SELECT count(*) FROM genie_cache")
    lb_count = cur.fetchone()[0]

cs_count = spark.sql(f"SELECT count(*) FROM {CACHE_STORE_TABLE}").collect()[0][0]
kb_count = spark.sql(f"SELECT count(*) FROM {CACHE_KB_TABLE}").collect()[0][0]

print("=" * 60)
print("SETUP COMPLETE")
print("=" * 60)
print(f"  Catalog/Schema:         {CATALOG}.{SCHEMA}")
print(f"  Data schema:            {data_schema_name(config)}")
print(f"  Cache context key:      {context.cache_context_key}")
print(f"  Delta - cache_store:    {CACHE_STORE_TABLE} ({cs_count} rows)")
print(f"  Delta - knowledge_base: {CACHE_KB_TABLE} ({kb_count} rows)")
print(f"  Lakebase - genie_cache: pgvector + HNSW ({lb_count} rows)")
print(f"  VS Endpoint:            {VS_ENDPOINT}")
print(f"  VS Index (Scenario 2):  {CACHE_STORE_INDEX}")
print(f"  VS Index (Scenario 3):  {CACHE_KB_INDEX}")
print()
print("Next steps - run any scenario notebook:")
print("  1. 1_lakebase_pgvector_cache.py  - Scenario 1: Lakebase + pgvector")
print("  2. 2_vector_search_cache.py      - Scenario 2: Vector Search")
print("  3. 3_hybrid_cache.py             - Scenario 3: Hybrid (Recommended)")

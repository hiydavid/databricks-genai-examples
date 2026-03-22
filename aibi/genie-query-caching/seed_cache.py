# Databricks notebook source

# MAGIC %md
# MAGIC # Seed Cache Data
# MAGIC
# MAGIC Pre-populates all 3 cache stores with 5 seed entries so the demo can
# MAGIC showcase cache **HITs** without requiring Genie API calls first.
# MAGIC
# MAGIC | Store | Scenario | Seeding method |
# MAGIC |-------|----------|----------------|
# MAGIC | Lakebase `genie_cache` (pgvector) | 1 & 3 | `lakebase_cache_write()` — upsert with real embeddings |
# MAGIC | Delta `cache_store` | 2 | Spark append (with duplicate check) |
# MAGIC | Delta `cache_knowledge_base` | 3 | Spark append (with duplicate check) + `validated=True` |
# MAGIC
# MAGIC **Prerequisites:** Run `0_setup.py` first to create all infrastructure.

# COMMAND ----------

# MAGIC %pip install "databricks-sdk>=0.85" "databricks-vectorsearch" "psycopg[binary]>=3.1" "pgvector>=0.3" pyyaml --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

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
CACHE_STORE_TABLE = f"{CATALOG}.{SCHEMA}.cache_store"
CACHE_STORE_INDEX = f"{CATALOG}.{SCHEMA}.cache_store_index"
CACHE_KB_TABLE = f"{CATALOG}.{SCHEMA}.cache_knowledge_base"
CACHE_KB_INDEX = f"{CATALOG}.{SCHEMA}.cache_knowledge_base_index"

# The seed SQL references the Horizon Bank data schema (from genie-demo-data),
# which is separate from the cache schema. Adjust DATA_SCHEMA if your Horizon
# Bank tables live in a different schema.
DATA_SCHEMA = "horizon_bank"

print(f"Cache schema:  {CATALOG}.{SCHEMA}")
print(f"Data schema:   {CATALOG}.{DATA_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Seed Entries
# MAGIC
# MAGIC Five questions drawn from the Horizon Bank domain, with realistic SQL and
# MAGIC response text matching what the Genie API would produce.

# COMMAND ----------

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
# MAGIC ## Seed Lakebase pgvector (Scenarios 1 & 3)
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
    count = cur.fetchone()[0]
print(f"\nLakebase genie_cache: {count} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Seed Delta `cache_store` (Scenario 2)
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
# MAGIC ## Seed Delta `cache_knowledge_base` (Scenario 3)
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
# MAGIC ## Sync Vector Search Indexes

# COMMAND ----------

vsc = VectorSearchClient(disable_notice=True)

print("Syncing cache_store index...")
sync_vs_index_and_wait(vsc, VS_ENDPOINT, CACHE_STORE_INDEX)

print("Syncing cache_knowledge_base index...")
sync_vs_index_and_wait(vsc, VS_ENDPOINT, CACHE_KB_INDEX)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

print("=" * 60)
print("SEED COMPLETE")
print("=" * 60)

# Lakebase
conn = get_lakebase_connection(config)
with conn.cursor() as cur:
    cur.execute("SELECT count(*) FROM genie_cache")
    lb_count = cur.fetchone()[0]
print(f"  Lakebase genie_cache:          {lb_count} rows")

# Delta cache_store
cs_count = spark.sql(f"SELECT count(*) FROM {CACHE_STORE_TABLE}").collect()[0][0]
print(f"  Delta cache_store:             {cs_count} rows")

# Delta cache_knowledge_base
kb_count = spark.sql(f"SELECT count(*) FROM {CACHE_KB_TABLE}").collect()[0][0]
print(f"  Delta cache_knowledge_base:    {kb_count} rows")

print()
print("Next: Run any scenario notebook to see cache HITs on the seed data.")

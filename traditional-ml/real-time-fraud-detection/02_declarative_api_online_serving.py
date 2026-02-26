# Databricks notebook source
# MAGIC %md
# MAGIC # Declarative feature engineering for online serving
# MAGIC
# MAGIC Prerequisites:
# MAGIC * https://docs.databricks.com/aws/en/machine-learning/feature-store/declarative-apis
# MAGIC * https://docs.databricks.com/aws/en/machine-learning/feature-store/materialized-features
# MAGIC * A classic compute cluster running Databricks Runtime 17.0 ML or above.

# COMMAND ----------

# MAGIC %pip install "databricks-feature-engineering>=0.14.0" "databricks-sdk>=0.39.0" --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from datetime import timedelta

from databricks.feature_engineering import FeatureEngineeringClient
from databricks.feature_engineering.entities import (
    Avg,
    ContinuousWindow,
    DeltaTableSource,
    OfflineStoreConfig,
    OnlineStoreConfig,
    SlidingWindow,
    Sum,
    TumblingWindow,
)

CATALOG_NAME = "main"
SCHEMA_NAME = "davidhuang"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get predefined features

# COMMAND ----------

# define features from transactions data
fe = FeatureEngineeringClient()

feature_names = [
    "avg_trans_amount_7d",
    "avg_trans_amount_30d",
    "sum_trans_amount_7d",
    "sum_trans_amount_30d",
]

features = []

for f in feature_names:
    features.append(fe.get_feature(full_name=f"{CATALOG_NAME}.{SCHEMA_NAME}.{f}"))

features

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create online store

# COMMAND ----------

fe = FeatureEngineeringClient()

# Create an online store with specified capacity
online_store_name = f"cc-online-store"

fe.create_online_store(
    name=online_store_name,
    # Lakebase Serverless not yet supported for Online Feaure Store
    capacity="CU_2",  # Valid options: "CU_1", "CU_2", "CU_4", "CU_8"
)

# COMMAND ----------

# Wait until the state is AVAILABLE
online_store = fe.get_online_store(name=online_store_name)
online_store.state

# COMMAND ----------

# MAGIC %md
# MAGIC ## Materialize features

# COMMAND ----------

offline_config = OfflineStoreConfig(
    catalog_name=CATALOG_NAME,
    schema_name=SCHEMA_NAME,
    table_name_prefix="cc_features",
)

online_config = OnlineStoreConfig(
    catalog_name=CATALOG_NAME,
    schema_name=SCHEMA_NAME,
    table_name_prefix="cc_online_features",
    online_store_name=online_store_name,
)

materialized_features = fe.materialize_features(
    features=features,
    offline_config=offline_config,
    online_config=online_config,
    pipeline_state="ACTIVE",
    cron_schedule="0 0 * * * ?",
)

# COMMAND ----------

print("Materialized features:")
for mf in materialized_features:
    print("  feature:", mf.feature_name)
    print("  table_name:", mf.table_name)
    print("---")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy trained model to Model Serving

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

w = WorkspaceClient()

MODEL_NAME = "main.davidhuang.recommendation_model"
ENDPOINT_NAME = "cc-recommendation-model-endpoint"

# Use the latest version as example; you can pin a specific version instead
versions = list(w.model_versions.list(full_name=MODEL_NAME))
latest_version = max(versions, key=lambda mv: int(mv.version)).version

config = EndpointCoreConfigInput(
    name=ENDPOINT_NAME,
    served_entities=[
        ServedEntityInput(
            name=ENDPOINT_NAME,
            entity_name=MODEL_NAME,
            entity_version=str(latest_version),
            workload_size="Small",
            scale_to_zero_enabled=True,
        )
    ],
)

# Idempotent create/update
try:
    w.serving_endpoints.create(name=ENDPOINT_NAME, config=config)
    print(f"Created model serving endpoint: {ENDPOINT_NAME}")
except Exception as e:
    if "already exists" in str(e).lower():
        w.serving_endpoints.update_config(
            name=ENDPOINT_NAME, served_entities=config.served_entities
        )
        print(f"Updated existing model serving endpoint: {ENDPOINT_NAME}")
    else:
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Online Inference

# COMMAND ----------

# get labeled df
score_df = spark.sql(
    """
SELECT
    customer_id, transaction_id, merchant_id, transaction_date, transaction_amount
FROM
    main.davidhuang.cc_transactions
"""
)

display(score_df.limit(5))

# COMMAND ----------

from pprint import pprint

import mlflow.deployments

inputs = score_df.limit(100).collect()
records = [
    {
        "customer_id": row.customer_id,
        "transaction_id": row.transaction_id,
        "merchant_id": row.merchant_id,
        "transaction_date": str(row.transaction_date),
        "transaction_amount": row.transaction_amount,
    }
    for row in inputs
]

client = mlflow.deployments.get_deploy_client("databricks")
response = client.predict(
    endpoint=ENDPOINT_NAME,
    inputs={"dataframe_records": records},
)

pprint(response)

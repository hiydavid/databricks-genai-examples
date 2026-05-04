# Databricks notebook source
# MAGIC %md
# MAGIC # Declarative feature engineering with online feature serving
# MAGIC
# MAGIC Prerequisites:
# MAGIC * https://docs.databricks.com/aws/en/machine-learning/feature-store/declarative-apis
# MAGIC * https://docs.databricks.com/aws/en/machine-learning/feature-store/materialized-features
# MAGIC * A classic compute cluster running Databricks Runtime 17.0 ML or above.

# COMMAND ----------

# MAGIC %pip install "databricks-feature-engineering>=0.15.0" "databricks-sdk>=0.39.0" --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient

CATALOG = "users"
SCHEMA = "david_huang"

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
    features.append(fe.get_feature(full_name=f"{CATALOG}.{SCHEMA}.{f}"))

features

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Feature Spec for feature serving

# COMMAND ----------

fe.create_feature_spec(name=f"{CATALOG}.{SCHEMA}.cc_features_for_serving",features=features)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a feature serving endpoint

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

workspace = WorkspaceClient()

# Create endpoint
workspace.serving_endpoints.create(
  name="cc-serving-endpoint",
  config = EndpointCoreConfigInput(
    name="cc-serving-endpoint",
    served_entities=[
    ServedEntityInput(
        entity_name=f"{CATALOG}.{SCHEMA}.cc_features_for_serving",
        scale_to_zero_enabled=True,
        workload_size="Small"
      )
    ]
  )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query the feature serving endpoint

# COMMAND ----------


# Databricks notebook source
# MAGIC %md
# MAGIC # Declarative Feature Engineering API
# MAGIC
# MAGIC This notebook demonstrates the Databricks Declarative Feature Engineering API with credit card transaction data.
# MAGIC
# MAGIC Prerequisites:
# MAGIC * Run `00_dummy-data-creation.py` first.
# MAGIC * Use a Unity Catalog-enabled workspace.
# MAGIC * Use a classic compute cluster running Databricks Runtime 17.0 ML or above.
# MAGIC * Enable the Declarative Feature Engineering preview.
# MAGIC
# MAGIC References:
# MAGIC * https://docs.databricks.com/aws/en/machine-learning/feature-store/declarative-apis
# MAGIC * https://docs.databricks.com/aws/en/machine-learning/feature-store/materialized-features
# MAGIC * https://docs.databricks.com/aws/en/machine-learning/feature-store/serve-declarative-features

# COMMAND ----------

# MAGIC %pip install "databricks-feature-engineering>=0.15.0" "databricks-sdk>=0.39.0"  --force-reinstall --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from datetime import timedelta
from pprint import pprint

import mlflow
from databricks.feature_engineering import FeatureEngineeringClient
from databricks.feature_engineering.entities import (
    AggregationFunction,
    Avg,
    CronSchedule,
    DeltaTableSource,
    Feature,
    MaterializedFeaturePipelineScheduleState,
    OfflineStoreConfig,
    OnlineStoreConfig,
    SlidingWindow,
    Sum,
)
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput
from mlflow.tracking import MlflowClient
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split

CATALOG = "users"
SCHEMA = "david_huang"

TRANSACTIONS_TABLE = "cc_transactions"
MODEL_NAME = f"{CATALOG}.{SCHEMA}.recommendation_model"
ONLINE_STORE_NAME = "cc-online-store"
ENDPOINT_NAME = "cc-recommendation-model-endpoint"

mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define data source

# COMMAND ----------

transaction_source = DeltaTableSource(
    catalog_name=CATALOG,
    schema_name=SCHEMA,
    table_name=TRANSACTIONS_TABLE,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define features

# COMMAND ----------

features = [
    Feature(
        source=transaction_source,
        entity=["customer_id"],
        timeseries_column="transaction_date",
        function=AggregationFunction(
            Avg(input="transaction_amount"),
            SlidingWindow(
                window_duration=timedelta(days=7), slide_duration=timedelta(days=1)
            ),
        ),
        name="avg_trans_amount_7d",
    ),
    Feature(
        source=transaction_source,
        entity=["customer_id"],
        timeseries_column="transaction_date",
        function=AggregationFunction(
            Avg(input="transaction_amount"),
            SlidingWindow(
                window_duration=timedelta(days=30), slide_duration=timedelta(days=1)
            ),
        ),
        name="avg_trans_amount_30d",
    ),
    Feature(
        source=transaction_source,
        entity=["customer_id"],
        timeseries_column="transaction_date",
        function=AggregationFunction(
            Sum(input="transaction_amount"),
            SlidingWindow(
                window_duration=timedelta(days=7), slide_duration=timedelta(days=1)
            ),
        ),
        name="sum_trans_amount_7d",
    ),
    Feature(
        source=transaction_source,
        entity=["customer_id"],
        timeseries_column="transaction_date",
        function=AggregationFunction(
            Sum(input="transaction_amount"),
            SlidingWindow(
                window_duration=timedelta(days=30), slide_duration=timedelta(days=1)
            ),
        ),
        name="sum_trans_amount_30d",
    ),
]

features

# COMMAND ----------

fe = FeatureEngineeringClient()

feature_preview_df = fe.compute_features(features=features)
display(feature_preview_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register features

# COMMAND ----------

registered_features = []

for feature in features:
    try:
        registered_feature = fe.register_feature(
            feature=feature,
            catalog_name=CATALOG,
            schema_name=SCHEMA,
        )
    except Exception as e:
        if "already exists" in str(e).lower() or "RESOURCE_ALREADY_EXISTS" in str(e):
            registered_feature = fe.get_feature(
                full_name=f"{CATALOG}.{SCHEMA}.{feature.name}"
            )
        else:
            raise
    registered_features.append(registered_feature)

features = registered_features
features

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create training set

# COMMAND ----------

labeled_df = spark.sql(
    f"""
SELECT
    customer_id,
    transaction_id,
    merchant_id,
    transaction_date,
    transaction_amount,
    fraud_flag
FROM {CATALOG}.{SCHEMA}.{TRANSACTIONS_TABLE}
"""
)

display(labeled_df.limit(5))

# COMMAND ----------

training_set = fe.create_training_set(
    df=labeled_df,
    features=features,
    label="fraud_flag",
    exclude_columns=[
        "customer_id",
        "transaction_id",
        "merchant_id",
        "transaction_date",
        "transaction_amount",
    ],
)

training_set_df = training_set.load_df()
display(training_set_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train and log model

# COMMAND ----------

with mlflow.start_run(run_name="fraud_detection_rf") as run:
    training_df = training_set_df.toPandas()

    print(f"Training data shape: {training_df.shape}")
    print(f"\nColumns: {list(training_df.columns)}")
    print(f"\nFraud distribution:\n{training_df['fraud_flag'].value_counts()}")

    feature_cols = [feature.name for feature in features]
    training_df_clean = training_df.dropna(subset=feature_cols + ["fraud_flag"])

    X = training_df_clean[feature_cols]
    y = training_df_clean["fraud_flag"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    clf = RandomForestClassifier(
        n_estimators=100, max_depth=10, random_state=42, n_jobs=-1
    )
    clf.fit(X_train, y_train)

    y_pred = clf.predict(X_test)
    y_pred_proba = clf.predict_proba(X_test)[:, 1]

    metrics = {
        "accuracy": accuracy_score(y_test, y_pred),
        "precision": precision_score(y_test, y_pred),
        "recall": recall_score(y_test, y_pred),
        "f1_score": f1_score(y_test, y_pred),
        "roc_auc": roc_auc_score(y_test, y_pred_proba),
    }

    mlflow.log_params(
        {
            "n_estimators": 100,
            "max_depth": 10,
            "test_size": 0.2,
        }
    )
    mlflow.log_metrics(metrics)

    fe.log_model(
        model=clf,
        artifact_path="fraud_detection_model",
        flavor=mlflow.sklearn,
        training_set=training_set,
        registered_model_name=MODEL_NAME,
    )

    print(f"Model logged to MLflow run: {run.info.run_id}")
    print("Model metrics:")
    pprint(metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Score with offline batch inference

# COMMAND ----------

versions = MlflowClient().search_model_versions(f"name='{MODEL_NAME}'")
latest_version = max(versions, key=lambda mv: int(mv.version)).version
model_uri = f"models:/{MODEL_NAME}/{latest_version}"

print(f"Scoring with model version: {latest_version}")

# COMMAND ----------

batch_df = labeled_df.drop("fraud_flag").limit(10)
display(batch_df)

# COMMAND ----------

batch_predictions_df = fe.score_batch(
    model_uri=model_uri,
    df=batch_df,
    result_type="string",
)

display(batch_predictions_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create online store

# COMMAND ----------

online_store = fe.get_online_store(name=ONLINE_STORE_NAME)

if online_store is None:
    online_store = fe.create_online_store(name=ONLINE_STORE_NAME, capacity="CU_1")
    print(f"Created online store: {ONLINE_STORE_NAME}")
else:
    print(f"Online store already exists: {ONLINE_STORE_NAME}")

online_store.state

# COMMAND ----------

# MAGIC %md
# MAGIC ## Materialize features

# COMMAND ----------

feature_full_names = {
    feature.name: f"{CATALOG}.{SCHEMA}.{feature.name}" for feature in features
}
materialized_features_by_name = {
    feature.name: fe.list_materialized_features(
        feature_name=feature_full_names[feature.name]
    )
    for feature in features
}

missing_features = [
    feature
    for feature in features
    if not materialized_features_by_name[feature.name]
]
existing_materialized_features = [
    materialized_feature
    for feature_materializations in materialized_features_by_name.values()
    for materialized_feature in feature_materializations
]

if missing_features:
    print("Materializing features:")
    for feature in missing_features:
        print(f"  {feature_full_names[feature.name]}")

    new_materialized_features = fe.materialize_features(
        features=missing_features,
        offline_config=OfflineStoreConfig(
            catalog_name=CATALOG,
            schema_name=SCHEMA,
            table_name_prefix="cc_features",
        ),
        online_config=OnlineStoreConfig(
            catalog_name=CATALOG,
            schema_name=SCHEMA,
            table_name_prefix="cc_online_features",
            online_store_name=ONLINE_STORE_NAME,
        ),
        trigger=CronSchedule(
            quartz_cron_expression="0 0 * * * ?",
            timezone_id="UTC",
            pipeline_schedule_state=MaterializedFeaturePipelineScheduleState.ACTIVE,
        ),
    )
    materialized_features = existing_materialized_features + new_materialized_features
else:
    print("Materialized features already exist.")
    materialized_features = existing_materialized_features

for materialized_feature in materialized_features:
    print("feature:", materialized_feature.feature_name)
    print("table_name:", materialized_feature.table_name)
    print("---")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy model to Model Serving

# COMMAND ----------

w = WorkspaceClient()

versions = MlflowClient().search_model_versions(f"name='{MODEL_NAME}'")
latest_version = max(versions, key=lambda mv: int(mv.version)).version
print(f"Deploying model version: {latest_version}")

endpoint_config = EndpointCoreConfigInput(
    name=ENDPOINT_NAME,
    served_entities=[
        ServedEntityInput(
            entity_name=MODEL_NAME,
            entity_version=str(latest_version),
            max_provisioned_concurrency=4,
            min_provisioned_concurrency=0,
        )
    ],
)

try:
    w.serving_endpoints.create(name=ENDPOINT_NAME, config=endpoint_config)
    print(f"Created model serving endpoint: {ENDPOINT_NAME}")
except Exception as e:
    if "already exists" in str(e).lower():
        w.serving_endpoints.update_config(
            name=ENDPOINT_NAME,
            served_entities=endpoint_config.served_entities,
        )
        print(f"Updated existing model serving endpoint: {ENDPOINT_NAME}")
    else:
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query the endpoint

# COMMAND ----------

score_df = spark.sql(
    f"""
SELECT
    customer_id,
    transaction_id,
    merchant_id,
    transaction_date,
    transaction_amount
FROM {CATALOG}.{SCHEMA}.{TRANSACTIONS_TABLE}
"""
)

display(score_df.limit(5))

# COMMAND ----------

records = [
    {
        "customer_id": row.customer_id,
        "transaction_id": row.transaction_id,
        "merchant_id": row.merchant_id,
        "transaction_date": str(row.transaction_date),
        "transaction_amount": row.transaction_amount,
    }
    for row in score_df.limit(100).collect()
]

response = w.serving_endpoints.query(
    name=ENDPOINT_NAME,
    dataframe_records=records,
)

pprint(response)

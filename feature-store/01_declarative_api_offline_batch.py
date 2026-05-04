# Databricks notebook source
# MAGIC %md
# MAGIC # Declarative feature engineering and offline batch inference
# MAGIC
# MAGIC Beta:
# MAGIC * https://docs.databricks.com/aws/en/machine-learning/feature-store/declarative-apis
# MAGIC * A classic compute cluster running Databricks Runtime 17.0 ML or above.

# COMMAND ----------

# MAGIC %pip install "databricks-feature-engineering>=0.15.0" --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from datetime import timedelta

from databricks.feature_engineering import FeatureEngineeringClient
from databricks.feature_engineering.entities import (
    AggregationFunction,
    Avg,
    DeltaTableSource,
    Feature,
    SlidingWindow,
    Sum,
)

CATALOG = "users"
SCHEMA = "david_huang"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define data sources (offline)

# COMMAND ----------

# transactions data
source_transactions_by_customer = DeltaTableSource(
    catalog_name=CATALOG,
    schema_name=SCHEMA,
    table_name="cc_transactions",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define features

# COMMAND ----------

# define features from transactions data
fe = FeatureEngineeringClient()

features = [
    Feature(
        source=source_transactions_by_customer,
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
        source=source_transactions_by_customer,
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
        source=source_transactions_by_customer,
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
        source=source_transactions_by_customer,
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

# COMMAND ----------

features

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

# COMMAND ----------

features

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create feature set

# COMMAND ----------

# get labeled df
labeled_df = spark.sql(
    f"""
SELECT
    customer_id, transaction_id, merchant_id, transaction_date, transaction_amount, fraud_flag
FROM
    {CATALOG}.{SCHEMA}.cc_transactions
"""
)

display(labeled_df.limit(5))

# COMMAND ----------

training_set = fe.create_training_set(
    df=labeled_df,
    features=features,
    label="fraud_flag",
)

# COMMAND ----------

training_set.load_df().limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train and log simple classifier

# COMMAND ----------

import mlflow
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split

# Log model with feature engineering
with mlflow.start_run(run_name="fraud_detection_rf") as run:

    # Load training dataframe
    training_df = training_set.load_df().toPandas()

    print(f"Training data shape: {training_df.shape}")
    print(f"\nColumns: {list(training_df.columns)}")
    print(f"\nFraud distribution:\n{training_df['fraud_flag'].value_counts()}")

    # Prepare features and labels
    feature_cols = [f.name for f in features]

    # Drop rows with missing values in feature columns
    training_df_clean = training_df.dropna(subset=feature_cols + ["fraud_flag"])

    X = training_df_clean[feature_cols]
    y = training_df_clean["fraud_flag"]

    print(f"\nClean data shape: {X.shape}")

    # Train-test split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    print(f"\nTrain set size: {X_train.shape[0]}")
    print(f"Test set size: {X_test.shape[0]}")

    # Train classifier
    clf = RandomForestClassifier(
        n_estimators=100, max_depth=10, random_state=42, n_jobs=-1
    )

    clf.fit(X_train, y_train)

    # Make predictions
    y_pred = clf.predict(X_test)
    y_pred_proba = clf.predict_proba(X_test)[:, 1]

    # Calculate metrics
    accuracy = accuracy_score(y_test, y_pred)
    precision = precision_score(y_test, y_pred)
    recall = recall_score(y_test, y_pred)
    f1 = f1_score(y_test, y_pred)
    roc_auc = roc_auc_score(y_test, y_pred_proba)

    print(f"\nModel Performance:")
    print(f"Accuracy: {accuracy:.4f}")
    print(f"Precision: {precision:.4f}")
    print(f"Recall: {recall:.4f}")
    print(f"F1 Score: {f1:.4f}")
    print(f"ROC AUC: {roc_auc:.4f}")

    # Log parameters
    mlflow.log_param("n_estimators", 100)
    mlflow.log_param("max_depth", 10)
    mlflow.log_param("test_size", 0.2)

    # Log metrics
    mlflow.log_metric("accuracy", accuracy)
    mlflow.log_metric("precision", precision)
    mlflow.log_metric("recall", recall)
    mlflow.log_metric("f1_score", f1)
    mlflow.log_metric("roc_auc", roc_auc)

    # Log model with feature engineering
    # Pass input_example so the signature correctly reflects the feature columns
    fe.log_model(
        model=clf,
        artifact_path="fraud_detection_model",
        flavor=mlflow.sklearn,
        training_set=training_set,
        registered_model_name=f"{CATALOG}.{SCHEMA}.recommendation_model",
        input_example=X_train.head(5),
    )

    print(f"\nModel logged to MLflow run: {run.info.run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Score with batch inference

# COMMAND ----------

# Load batch data without features (fe.score_batch will fetch features automatically)
# Use the original labeled_df, not training_set.load_df() to avoid duplicate columns
batch_df = labeled_df.drop("fraud_flag").limit(10)

display(batch_df)

# COMMAND ----------

score_df = fe.score_batch(
    model_uri=f"models:/{CATALOG}.{SCHEMA}.recommendation_model/1",
    df=batch_df,
    result_type="string",
)

# COMMAND ----------

display(score_df)

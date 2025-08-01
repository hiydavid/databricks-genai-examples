# Databricks notebook source
# MAGIC %md
# MAGIC # Financial Transaction Dataset (Synthetic)
# MAGIC Source: https://www.kaggle.com/datasets/computingvictor/transactions-fraud-datasets

# COMMAND ----------

import json

import pandas as pd
from pyspark.sql.functions import (
    col,
    concat_ws,
    lit,
    lower,
    regexp_replace,
    to_date,
    trim,
    when,
)
from pyspark.sql.types import DoubleType, IntegerType

# COMMAND ----------

catalog = "users"
schema = "david_huang"
volume = "financial_transactions_dataset"
volume_path = f"/Volumes/{catalog}/{schema}/{volume}"

display(dbutils.fs.ls(volume_path))

# COMMAND ----------

# merchant catagory data
mcc_raw = spark.read.text(f"dbfs:{volume_path}/mcc_codes.json")
raw_json_str = "\n".join(mcc_raw.toPandas()["value"].tolist())
mcc_dict = json.loads(raw_json_str)
mcc_pd = pd.DataFrame(list(mcc_dict.items()), columns=["mcc_code", "description"])
mcc_df = spark.createDataFrame(mcc_pd)

display(mcc_df.limit(5))

# COMMAND ----------

# cards data
cards_df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{volume_path}/cards_data.csv")
)

cards_df_cleaned = (
    cards_df.withColumn(
        "credit_limit",
        regexp_replace(col("credit_limit"), "[$,]", "").cast(IntegerType()),
    )
    .withColumn(
        "card_expiration_date",
        to_date(concat_ws("/", lit("01"), col("expires")), "dd/MM/yyyy"),
    )
    .withColumn(
        "card_open_date",
        to_date(concat_ws("/", lit("01"), col("acct_open_date")), "dd/MM/yyyy"),
    )
    .withColumn(
        "has_chip", when(lower(trim(col("has_chip"))) == "yes", True).otherwise(False)
    )
    .withColumn(
        "card_on_dark_web",
        when(lower(trim(col("card_on_dark_web"))) == "yes", True).otherwise(False),
    )
    .drop("acct_open_date", "expires")
)

cards_df_cleaned.write.mode("overwrite").saveAsTable(
    f"{catalog}.{schema}.ftd_cards_data"
)

display(spark.table(f"{catalog}.{schema}.ftd_cards_data").limit(5))

# COMMAND ----------

# transactions data
transactions_df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{volume_path}/transactions_data.csv")
)

transactions_df_cleaned = (
    transactions_df.withColumnRenamed("date", "transaction_date")
    .withColumnRenamed("errors", "transaction_errors")
    .withColumnRenamed("mcc", "merchant_category_code")
    .withColumn(
        "amount",
        regexp_replace(col("amount"), "[$,]", "").cast(DoubleType()),
    )
    .withColumn("zip", col("zip").cast(IntegerType()))
    .withColumn(
        "merchant_city",
        when(lower(trim(col("use_chip"))) == "online transaction", None).otherwise(
            col("merchant_city")
        ),
    )
)

transactions_df_joined = transactions_df_cleaned.join(
    mcc_df.withColumnRenamed("description", "merchant_category"),
    transactions_df_cleaned["merchant_category_code"] == mcc_df["mcc_code"],
    "left",
).drop(mcc_df["mcc_code"])

transactions_df_joined.write.mode("overwrite").saveAsTable(
    f"{catalog}.{schema}.ftd_transactions_data"
)

display(spark.table(f"{catalog}.{schema}.ftd_transactions_data").limit(5))

# COMMAND ----------

# users data
users_df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{volume_path}/users_data.csv")
)

users_df_cleaned = (
    users_df.withColumn(
        "per_capita_income",
        regexp_replace(col("per_capita_income"), "[$,]", "").cast(DoubleType()),
    )
    .withColumn(
        "yearly_income",
        regexp_replace(col("yearly_income"), "[$,]", "").cast(DoubleType()),
    )
    .withColumn(
        "total_debt",
        regexp_replace(col("total_debt"), "[$,]", "").cast(DoubleType()),
    )
)
users_df_cleaned.write.mode("overwrite").saveAsTable(
    f"{catalog}.{schema}.ftd_users_data"
)

display(spark.table(f"{catalog}.{schema}.ftd_users_data").limit(5))

# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Data
# MAGIC
# MAGIC * First make sure the files in `./data` are in a Unity Catalog Volume
# MAGIC * You can run this notebook using `Serverless` notebook compute.

# COMMAND ----------

catalog = "users"
schema = "david_huang"
volume = "agent_bricks_mas_demo"

display(dbutils.fs.ls(f"/Volumes/{catalog}/{schema}/{volume}"))

# COMMAND ----------

balance_sheet_df = spark.read.format("parquet").load(
    f"/Volumes/{catalog}/{schema}/{volume}/balance-sheet.parquet"
)

income_statement_df = spark.read.format("parquet").load(
    f"/Volumes/{catalog}/{schema}/{volume}/income-statement.parquet"
)

sec_10k_chunked_df = spark.read.format("parquet").load(
    f"/Volumes/{catalog}/{schema}/{volume}/sec-10k-chunked.parquet"
)

# COMMAND ----------

balance_sheet_df.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog}.{schema}.balance_sheet"
)
income_statement_df.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog}.{schema}.income_statement"
)
sec_10k_chunked_df.write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog}.{schema}.sec_10k_chunked"
)

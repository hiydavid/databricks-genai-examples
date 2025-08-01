# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Data for Genie
# MAGIC
# MAGIC Use `Serverless` compute.

# COMMAND ----------

catalog = "users"
schema = "david_huang"

# COMMAND ----------

import os
balance_sheet = spark.read.parquet(f"file://{os.getcwd()}/balance_sheet.parquet")
income_statement = spark.read.parquet(f"file://{os.getcwd()}/income_statement.parquet")

# COMMAND ----------

display(balance_sheet.limit(10))

# COMMAND ----------

display(income_statement.limit(10))

# COMMAND ----------

balance_sheet.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.genie_balance_sheet")
income_statement.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.genie_income_statement")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct ticker
# MAGIC from users.david_huang.genie_balance_sheet
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from users.david_huang.genie_balance_sheet
# MAGIC limit 5
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct YEAR
# MAGIC from users.david_huang.genie_balance_sheet
# MAGIC ;

# COMMAND ----------


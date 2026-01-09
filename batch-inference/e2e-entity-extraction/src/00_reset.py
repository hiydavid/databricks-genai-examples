# Databricks notebook source
# MAGIC %md
# MAGIC # Reset Pipeline State
# MAGIC
# MAGIC Utility notebook to clear checkpoints and drop tables for testing.
# MAGIC **Use with caution - this deletes data!**

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

cfg = Config.from_yaml()

CATALOG = cfg.catalog
SCHEMA = cfg.schema
CHECKPOINT_VOLUME = cfg.checkpoint_volume

# Table names
PARSED_TABLE_FULL = cfg.parsed_table_full
EXTRACTED_TABLE_FULL = cfg.extracted_table_full
FINAL_TABLE_FULL = cfg.final_table_full
EVAL_TABLE_FULL = cfg.eval_table_full

# Checkpoint paths
CHECKPOINTS = [
    f"{CHECKPOINT_VOLUME}/parse_documents",
    f"{CHECKPOINT_VOLUME}/classify_extract",
    f"{CHECKPOINT_VOLUME}/postprocessing",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select What to Reset

# COMMAND ----------

# Widgets for selecting what to reset
dbutils.widgets.dropdown("reset_checkpoints", "Yes", ["Yes", "No"], "Clear Checkpoints")
dbutils.widgets.dropdown("reset_parsed", "Yes", ["Yes", "No"], "Drop Parsed Table")
dbutils.widgets.dropdown("reset_extracted", "No", ["Yes", "No"], "Drop Extracted Table")
dbutils.widgets.dropdown("reset_final", "No", ["Yes", "No"], "Drop Final Table")
dbutils.widgets.dropdown("reset_eval", "No", ["Yes", "No"], "Drop Eval Table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clear Checkpoints

# COMMAND ----------

if dbutils.widgets.get("reset_checkpoints") == "Yes":
    print("Clearing checkpoints...")
    for checkpoint_path in CHECKPOINTS:
        try:
            dbutils.fs.rm(checkpoint_path, recurse=True)
            print(f"  Deleted: {checkpoint_path}")
        except Exception as e:
            print(f"  Skipped (not found): {checkpoint_path}")
    print("Checkpoints cleared.")
else:
    print("Skipping checkpoint cleanup.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop Tables

# COMMAND ----------

tables_to_drop = []

if dbutils.widgets.get("reset_parsed") == "Yes":
    tables_to_drop.append(PARSED_TABLE_FULL)

if dbutils.widgets.get("reset_extracted") == "Yes":
    tables_to_drop.append(EXTRACTED_TABLE_FULL)

if dbutils.widgets.get("reset_final") == "Yes":
    tables_to_drop.append(FINAL_TABLE_FULL)

if dbutils.widgets.get("reset_eval") == "Yes":
    tables_to_drop.append(EVAL_TABLE_FULL)

if tables_to_drop:
    print("Dropping tables...")
    for table in tables_to_drop:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {table}")
            print(f"  Dropped: {table}")
        except Exception as e:
            print(f"  Error dropping {table}: {e}")
    print("Tables dropped.")
else:
    print("No tables selected for dropping.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("RESET COMPLETE")
print("=" * 60)
print(f"Checkpoints cleared: {dbutils.widgets.get('reset_checkpoints')}")
print(f"Parsed table dropped: {dbutils.widgets.get('reset_parsed')}")
print(f"Extracted table dropped: {dbutils.widgets.get('reset_extracted')}")
print(f"Final table dropped: {dbutils.widgets.get('reset_final')}")
print(f"Eval table dropped: {dbutils.widgets.get('reset_eval')}")
print("=" * 60)

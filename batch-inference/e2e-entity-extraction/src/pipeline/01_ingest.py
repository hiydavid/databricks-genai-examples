# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest and Parse Claims Documents
# MAGIC
# MAGIC This notebook ingests PDF documents from a Unity Catalog Volume and parses them
# MAGIC using `ai_parse_document()`. Uses streaming with checkpoints for incremental processing.

# COMMAND ----------

# MAGIC %pip install -r ../../requirements.txt --quiet
# MAGIC %restart_python

# COMMAND ----------

from pyspark.sql.functions import col, concat_ws, current_timestamp, expr, lit, when
from pyspark.sql.types import (
    BinaryType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC **Prerequisites:** Ensure the following exist before running:
# MAGIC - Unity Catalog: `{catalog}.{schema}`
# MAGIC - Volume: `/Volumes/{catalog}/{schema}/e2e_claims_example` (with PDF files)
# MAGIC - Volume: `/Volumes/{catalog}/{schema}/e2e_claims_example/checkpoints`

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

cfg = Config.from_yaml()

CATALOG = cfg.catalog
SCHEMA = cfg.schema
SOURCE_VOLUME = cfg.source_volume
CHECKPOINT_VOLUME = cfg.checkpoint_volume
PARSED_TABLE_FULL = cfg.parsed_table_full
CHECKPOINT_PATH = f"{CHECKPOINT_VOLUME}/parse_documents"

# Set catalog and schema
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read PDFs from Volume (Streaming)

# COMMAND ----------

# Define schema for binary files (must match exact schema expected by binaryFile format)
binary_file_schema = StructType(
    [
        StructField("path", StringType(), False),
        StructField("modificationTime", TimestampType(), False),
        StructField("length", LongType(), False),
        StructField("content", BinaryType(), True),
    ]
)

# Read files using Structured Streaming
files_df = (
    spark.readStream.format("binaryFile")
    .schema(binary_file_schema)
    .option("pathGlobFilter", "*.pdf")
    .option("recursiveFileLookup", "true")
    .load(SOURCE_VOLUME)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Parse Documents with ai_parse_document()

# COMMAND ----------

# Parse documents and extract text
parsed_df = (
    files_df.repartition(8, expr("crc32(path) % 8"))
    .withColumn(
        "parsed_result",
        expr("ai_parse_document(content)"),
    )
    .withColumn("parsed_at", current_timestamp())
    .withColumn(
        "extracted_text",
        when(
            expr("try_cast(parsed_result:error_status AS STRING)").isNotNull(),
            lit(None),
        ).otherwise(
            concat_ws(
                "\n\n",
                expr(
                    """
                    transform(
                        try_cast(parsed_result:document:elements AS ARRAY<VARIANT>),
                        element -> try_cast(element:content AS STRING)
                    )
                    """
                ),
            )
        ),
    )
    .withColumn(
        "error_status",
        expr("try_cast(parsed_result:error_status AS STRING)"),
    )
    .withColumn(
        "document_id",
        expr("concat('doc_', substring(md5(path), 1, 12))"),
    )
    .select(
        "document_id",
        "path",
        col("length").alias("file_size_bytes"),
        col("modificationTime").alias("file_modified_at"),
        "parsed_result",
        "extracted_text",
        "error_status",
        "parsed_at",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Write Parsed Documents to Delta Table

# COMMAND ----------

# Write to Delta table with streaming
(
    parsed_df.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .option("delta.feature.variantType-preview", "supported")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(PARSED_TABLE_FULL)
).awaitTermination()

print(f"Parsed documents written to {PARSED_TABLE_FULL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Results

# COMMAND ----------

# Show sample of parsed documents
display(
    spark.table(PARSED_TABLE_FULL)
    .select(
        "document_id",
        "path",
        "file_size_bytes",
        "parsed_at",
        expr("length(extracted_text)").alias("text_length"),
        "error_status",
    )
    .limit(10)
)

# COMMAND ----------

# Show document counts
print(f"Total parsed documents: {spark.table(PARSED_TABLE_FULL).count()}")

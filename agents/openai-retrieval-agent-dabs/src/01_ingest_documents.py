# Databricks notebook source
# MAGIC %md
# MAGIC # Document Ingestion Pipeline
# MAGIC
# MAGIC This notebook parses user guide PDFs using `ai_parse_document` and stores
# MAGIC extracted content in a Delta table for Vector Search indexing.

# COMMAND ----------

# MAGIC %pip install PyYAML
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import yaml
from pyspark.sql.functions import (
    col,
    current_timestamp,
    explode,
    expr,
    lit,
    monotonically_increasing_id,
    regexp_replace,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Configuration

# COMMAND ----------

# Load configuration from widgets (set by DABs job) or defaults
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "default")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

# Load additional config from YAML if available
try:
    with open("configs.yaml", "r") as f:
        config = yaml.safe_load(f)
    document_configs = config.get("document_configs", {})
    SOURCE_VOLUME = document_configs.get(
        "source_volume", f"/Volumes/{CATALOG}/{SCHEMA}/user_guides"
    )
    CHUNKS_TABLE = document_configs.get(
        "chunks_table", f"{CATALOG}.{SCHEMA}.user_guide_chunks"
    )
except FileNotFoundError:
    SOURCE_VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/user_guides"
    CHUNKS_TABLE = f"{CATALOG}.{SCHEMA}.user_guide_chunks"

print(f"Source volume: {SOURCE_VOLUME}")
print(f"Output table: {CHUNKS_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read PDF Files from Volume

# COMMAND ----------

# Read all PDF files from the source volume (batch mode)
files_df = (
    spark.read.format("binaryFile")
    .option("pathGlobFilter", "*.pdf")
    .option("recursiveFileLookup", "true")
    .load(SOURCE_VOLUME)
)

print(f"Found {files_df.count()} PDF files")
files_df.select("path", "length", "modificationTime").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse Documents with ai_parse_document

# COMMAND ----------

# Parse PDFs using ai_parse_document SQL function
# This extracts structured content from PDF documents
parsed_df = files_df.withColumn("parsed_result", expr("ai_parse_document(content)"))

# Show parsing results
parsed_df.select("path", "parsed_result.document.title").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract and Chunk Text Content

# COMMAND ----------

# Extract text elements from parsed documents
# Filter to text and section_header types for meaningful content
elements_df = (
    parsed_df.select(
        col("path").alias("source_path"),
        col("parsed_result.document.title").alias("doc_title"),
        explode("parsed_result.document.elements").alias("element"),
    )
    .filter(col("element.type").isin(["text", "section_header", "paragraph"]))
    .select(
        "source_path",
        "doc_title",
        col("element.text_representation").alias("text_content"),
        col("element.type").alias("element_type"),
        col("element.page_number").alias("page_number"),
    )
)

# Filter out empty or very short chunks
elements_df = elements_df.filter(
    (col("text_content").isNotNull()) & (col("text_content") != "")
)

# Clean text content - remove excessive whitespace
elements_df = elements_df.withColumn(
    "text_content", regexp_replace(col("text_content"), r"\s+", " ")
)

print(f"Extracted {elements_df.count()} text elements")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Final Chunks Table

# COMMAND ----------

# Add unique chunk IDs and metadata
chunks_df = elements_df.select(
    monotonically_increasing_id().alias("chunk_id"),
    "source_path",
    "doc_title",
    "text_content",
    "element_type",
    "page_number",
    current_timestamp().alias("ingested_at"),
)

# Display sample chunks
chunks_df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta Table

# COMMAND ----------

# Write chunks to Delta table (overwrite for batch mode)
# For incremental updates, consider using merge or append mode
chunks_df.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(CHUNKS_TABLE)

print(f"Successfully wrote {chunks_df.count()} chunks to {CHUNKS_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Output

# COMMAND ----------

# Verify the table was created correctly
result_df = spark.read.table(CHUNKS_TABLE)
print(f"Total chunks in table: {result_df.count()}")
print(f"Columns: {result_df.columns}")

# Show sample data
result_df.limit(5).display()

# COMMAND ----------

# Summary statistics
print("\n=== Ingestion Summary ===")
print(f"Source: {SOURCE_VOLUME}")
print(f"Output table: {CHUNKS_TABLE}")
print(f"Total documents processed: {files_df.count()}")
print(f"Total chunks created: {result_df.count()}")

# Chunks per document
result_df.groupBy("source_path").count().orderBy("count", ascending=False).display()

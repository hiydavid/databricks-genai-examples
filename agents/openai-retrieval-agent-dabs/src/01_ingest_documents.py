# Databricks notebook source
# MAGIC %md
# MAGIC # Document Ingestion Pipeline
# MAGIC
# MAGIC This notebook parses user guide PDFs using `ai_parse_document` and stores
# MAGIC extracted content in a Delta table for Vector Search indexing.

# COMMAND ----------

# MAGIC %pip install -r ../requirements.txt --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    collect_list,
    concat_ws,
    current_timestamp,
    explode,
    expr,
    monotonically_increasing_id,
    regexp_replace,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Configuration

# COMMAND ----------

# Job parameters (DAB populates these via base_parameters)
dbutils.widgets.text("catalog", "")  # Non-DAB: set default, e.g. "users"
dbutils.widgets.text("schema", "")  # Non-DAB: set default, e.g. "your_schema"

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

if not CATALOG or not SCHEMA:
    raise ValueError("Required parameters: catalog, schema")

# Derive paths from parameters
SOURCE_VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/user_guide/guide"
CHUNKS_TABLE = f"{CATALOG}.{SCHEMA}.user_guide_chunks"
IMAGES_VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/user_guide/parsed_images"

print(f"Catalog: {CATALOG}")
print(f"Schema: {SCHEMA}")
print(f"Source volume: {SOURCE_VOLUME}")
print(f"Output table: {CHUNKS_TABLE}")
print(f"Images volume: {IMAGES_VOLUME}")

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
# Save page images to UC volume and generate descriptions for figures
parsed_df = files_df.withColumn(
    "parsed_result",
    expr(
        f"""ai_parse_document(
            content,
            map(
                'imageOutputPath', '{IMAGES_VOLUME}/',
                'descriptionElementTypes', 'figure',
                'version', '2.0'
            )
        )"""
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract and Chunk Text Content

# COMMAND ----------

# Extract text elements from parsed documents
# Schema ref: https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_parse_document
elements_df = (
    parsed_df.select(
        col("path").alias("source_path"),
        explode(
            expr("try_cast(parsed_result:document:elements AS ARRAY<VARIANT>)")
        ).alias("element"),
    )
    .filter(
        expr("element:type::STRING").isin(
            ["text", "section_header", "title", "caption"]
        )
    )
    .select(
        "source_path",
        expr("element:content::STRING").alias("text_content"),
        expr("element:bbox[0]:page_id::INT").alias("page_number"),
    )
)

# Filter out empty content
elements_df = elements_df.filter(
    (col("text_content").isNotNull()) & (col("text_content") != "")
)

# Clean text content - remove excessive whitespace
elements_df = elements_df.withColumn(
    "text_content", regexp_replace(col("text_content"), r"\s+", " ")
)

# Aggregate elements by page - concatenate all text on same page
pages_df = elements_df.groupBy("source_path", "page_number").agg(
    concat_ws("\n\n", collect_list("text_content")).alias("text_content")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Final Chunks Table

# COMMAND ----------

# Add unique chunk IDs and metadata (order by source and page to ensure sequential chunk IDs)
chunks_df = pages_df.orderBy("source_path", "page_number").select(
    monotonically_increasing_id().alias("chunk_id"),
    "source_path",
    "text_content",
    "page_number",
    current_timestamp().alias("ingested_at"),
)

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

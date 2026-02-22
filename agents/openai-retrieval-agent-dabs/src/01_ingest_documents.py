# Databricks notebook source
# MAGIC %md
# MAGIC # Document Ingestion Pipeline
# MAGIC
# MAGIC This notebook parses user guide PDFs using `ai_parse_document`, extracts
# MAGIC figure descriptions with crop references, and stores page-level chunks
# MAGIC in a Delta table for Vector Search indexing.

# COMMAND ----------

# MAGIC %pip install -r ../requirements.txt --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import uuid

from pyspark.sql import Window
from pyspark.sql.functions import (
    col,
    concat,
    current_timestamp,
    explode,
    expr,
    lit,
    monotonically_increasing_id,
    posexplode,
    regexp_replace,
    row_number,
    trim,
    when,
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
RUN_ID = uuid.uuid4().hex
IMAGES_RUN_VOLUME = f"{IMAGES_VOLUME}/{RUN_ID}"
PARSED_INTERMEDIATE_VOLUME = (
    f"/Volumes/{CATALOG}/{SCHEMA}/user_guide/parsed_intermediate/{RUN_ID}"
)

print(f"Catalog: {CATALOG}")
print(f"Schema: {SCHEMA}")
print(f"Source volume: {SOURCE_VOLUME}")
print(f"Output table: {CHUNKS_TABLE}")
print(f"Images volume: {IMAGES_VOLUME}")
print(f"Images run volume: {IMAGES_RUN_VOLUME}")
print(f"Parsed intermediate volume: {PARSED_INTERMEDIATE_VOLUME}")

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
display(files_df.select("path", "length", "modificationTime"))

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
                'imageOutputPath', '{IMAGES_RUN_VOLUME}/',
                'descriptionElementTypes', 'figure',
                'version', '2.0'
            )
        )"""
    ),
)

# Materialize parse step in Delta (serverless-compatible) so image files are fully
# written before readback and ai_parse_document is not recomputed downstream.
parsed_df.select("path", "parsed_result").write.format("delta").mode("overwrite").save(
    PARSED_INTERMEDIATE_VOLUME
)
parsed_df = spark.read.format("delta").load(PARSED_INTERMEDIATE_VOLUME)
parsed_doc_count = parsed_df.count()
print(f"Parsed {parsed_doc_count} documents with ai_parse_document")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Parsed Elements and Page Lookup

# COMMAND ----------

# Schema refs:
# - ai_parse_document: https://docs.databricks.com/sql/language-manual/functions/ai_parse_document.html
pages_lookup_df = parsed_df.select(
    col("path").alias("source_path"),
    explode(expr("try_cast(parsed_result:document:pages AS ARRAY<VARIANT>)")).alias("page"),
).select(
    "source_path",
    expr("page:id::INT").alias("page_number"),
    expr("page:image_uri::STRING").alias("page_image_uri"),
)

parsed_elements_df = parsed_df.select(
    col("path").alias("source_path"),
    posexplode(expr("try_cast(parsed_result:document:elements AS ARRAY<VARIANT>)")).alias(
        "element_index", "element"
    ),
).select(
    "source_path",
    "element_index",
    expr("element:type::STRING").alias("element_type"),
    expr("element:content::STRING").alias("element_content"),
    expr("element:description::STRING").alias("element_description"),
    expr("element:bbox[0]:page_id::INT").alias("page_number"),
    expr("element:bbox[0]:coord[0]::DOUBLE").alias("x1"),
    expr("element:bbox[0]:coord[1]::DOUBLE").alias("y1"),
    expr("element:bbox[0]:coord[2]::DOUBLE").alias("x2"),
    expr("element:bbox[0]:coord[3]::DOUBLE").alias("y2"),
).withColumn(
    "element_content",
    regexp_replace(col("element_content"), r"\s+", " "),
).withColumn(
    "element_description",
    regexp_replace(col("element_description"), r"\s+", " "),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Page-Level Text Chunks

# COMMAND ----------

text_elements_df = (
    parsed_elements_df.filter(
        col("element_type").isin(["text", "section_header", "title", "caption"])
    )
    .filter(col("page_number").isNotNull())
    .filter(col("element_content").isNotNull())
    .filter(trim(col("element_content")) != "")
    .withColumn(
        "text_content",
        when(
            col("element_type") == "caption",
            concat(lit("FIGURE CAPTION: "), col("element_content")),
        ).otherwise(col("element_content")),
    )
)

pages_text_df = text_elements_df.groupBy("source_path", "page_number").agg(
    expr(
        """
        concat_ws(
            '\n\n',
            transform(
                sort_array(
                    collect_list(named_struct('idx', element_index, 'txt', text_content))
                ),
                x -> x.txt
            )
        )
        """
    ).alias("text_content")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Figure Candidates

# COMMAND ----------

caption_elements_df = (
    parsed_elements_df.filter(col("element_type") == "caption")
    .filter(col("page_number").isNotNull())
    .filter(col("element_content").isNotNull())
    .filter(trim(col("element_content")) != "")
    .select(
        "source_path",
        "page_number",
        col("element_index").alias("caption_index"),
        col("element_content").alias("caption_text"),
    )
)

figure_elements_df = (
    parsed_elements_df.filter(col("element_type") == "figure")
    .filter(col("page_number").isNotNull())
    .select(
        "source_path",
        "page_number",
        col("element_index").alias("figure_index"),
        col("element_description").alias("figure_description"),
        "x1",
        "y1",
        "x2",
        "y2",
    )
)

figure_caption_candidates_df = (
    figure_elements_df.alias("f")
    .join(
        caption_elements_df.alias("c"),
        on=["source_path", "page_number"],
        how="left",
    )
    .withColumn(
        "caption_distance",
        expr("abs(figure_index - caption_index)"),
    )
)

caption_rank_window = Window.partitionBy(
    "source_path",
    "page_number",
    "figure_index",
).orderBy(
    col("caption_distance").asc_nulls_last(),
    col("caption_index").asc_nulls_last(),
)

# Keep all figure elements; attach nearest caption when available.
figure_candidates_df = (
    figure_caption_candidates_df.withColumn(
        "caption_rank",
        row_number().over(caption_rank_window),
    )
    .filter(col("caption_rank") == 1)
    .drop("caption_rank")
    .join(
        pages_lookup_df.select(
            "source_path",
            "page_number",
            "page_image_uri",
        ),
        on=["source_path", "page_number"],
        how="left",
    )
)

print(f"Figures identified: {figure_candidates_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Figure Description Entries (No Extra ai_query Step)

# COMMAND ----------

figures_described_df = (
    figure_candidates_df.filter(col("page_image_uri").isNotNull())
    .withColumn(
        "figure_description_text",
        when(
            col("figure_description").isNotNull() & (trim(col("figure_description")) != ""),
            col("figure_description"),
        )
        .when(
            col("caption_text").isNotNull() & (trim(col("caption_text")) != ""),
            col("caption_text"),
        )
        .otherwise(lit("No figure description provided by parser.")),
    )
    .withColumn(
        "bounding_box_location",
        when(
            col("x1").isNotNull()
            & col("y1").isNotNull()
            & col("x2").isNotNull()
            & col("y2").isNotNull(),
            expr(
                """
                concat(
                    'x1=',
                    cast(round(greatest(0D, least(1000D, x1))) AS STRING),
                    ', y1=',
                    cast(round(greatest(0D, least(1000D, y1))) AS STRING),
                    ', x2=',
                    cast(round(greatest(0D, least(1000D, x2))) AS STRING),
                    ', y2=',
                    cast(round(greatest(0D, least(1000D, y2))) AS STRING)
                )
                """
            ),
        ).otherwise(lit("x1=NA, y1=NA, x2=NA, y2=NA")),
    )
    .withColumn(
        "visual_entry",
        concat(
            lit("<Image Tag>: Figure\n"),
            lit("<Image Description>: "),
            col("figure_description_text"),
            lit("\n<Image Location>: "),
            col("page_image_uri"),
            lit("\n<Bounding Box Location>: "),
            col("bounding_box_location"),
        ),
    )
)

print(
    "Figure descriptions with crop references:",
    figures_described_df.filter(col("figure_description_text").isNotNull()).count(),
)

pages_visual_df = figures_described_df.filter(col("visual_entry").isNotNull()).groupBy(
    "source_path", "page_number"
).agg(
    expr(
        """
        concat_ws(
            '\n\n',
            transform(
                sort_array(
                    collect_list(named_struct('idx', figure_index, 'txt', visual_entry))
                ),
                x -> x.txt
            )
        )
        """
    ).alias("visual_insight")
)

ordered_text_entries_df = text_elements_df.select(
    "source_path",
    "page_number",
    col("element_index").alias("entry_index"),
    col("text_content").alias("entry_text"),
)

ordered_visual_entries_df = figures_described_df.select(
    "source_path",
    "page_number",
    col("figure_index").alias("entry_index"),
    col("visual_entry").alias("entry_text"),
)

# Interleave textual and visual entries in original parse order.
ordered_entries_df = ordered_text_entries_df.unionByName(ordered_visual_entries_df).filter(
    col("entry_text").isNotNull()
).filter(
    trim(col("entry_text")) != ""
)

pages_retrieval_df = ordered_entries_df.groupBy("source_path", "page_number").agg(
    expr(
        """
        concat_ws(
            '\n\n',
            transform(
                sort_array(
                    collect_list(named_struct('idx', entry_index, 'txt', entry_text))
                ),
                x -> x.txt
            )
        )
        """
    ).alias("retrieval_text")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Final Page Chunks

# COMMAND ----------

pages_df = (
    pages_retrieval_df.join(
        pages_text_df, on=["source_path", "page_number"], how="left"
    ).join(
        pages_visual_df, on=["source_path", "page_number"], how="left"
    )
)

# Add unique chunk IDs and metadata (order by source and page to keep stable ordering)
chunks_df = pages_df.orderBy("source_path", "page_number").select(
    monotonically_increasing_id().alias("chunk_id"),
    "source_path",
    "text_content",
    "visual_insight",
    "retrieval_text",
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
display(result_df.limit(5))

# COMMAND ----------

# Summary statistics
print("\n=== Ingestion Summary ===")
print(f"Source: {SOURCE_VOLUME}")
print(f"Output table: {CHUNKS_TABLE}")
print(f"Total documents processed: {files_df.count()}")
print(f"Total chunks created: {result_df.count()}")
print(
    "Pages with visual insights:",
    result_df.filter(col("visual_insight").isNotNull()).count(),
)

# Chunks per document
display(result_df.groupBy("source_path").count().orderBy("count", ascending=False))

# Best-effort cleanup of intermediate parsed results.
try:
    dbutils.fs.rm(PARSED_INTERMEDIATE_VOLUME, True)
    print(f"Cleaned up intermediate path: {PARSED_INTERMEDIATE_VOLUME}")
except Exception as e:
    print(
        "Warning: Could not clean intermediate parsed path "
        f"{PARSED_INTERMEDIATE_VOLUME}: {e}"
    )

# Databricks notebook source
# MAGIC %md
# MAGIC # Batch Inference with Autoloader & `ai_query`

# COMMAND ----------
from pyspark.sql.functions import expr

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set autoloader configurations

# COMMAND ----------

# replace with your model endpoint name
model_endpoint = "your-model-endpoint-name"

# replace with your input azure blob storage source
input_dir = "dbfs:/Volumes/catalog/schema/lease_docs_txts"

# make sure you created the checkpoints folder, do not delete the checkpoint unless re-doing the batch
checkpoint_dir = "/Volumes/catalog/schema/checkpoints/1"

# output table
output_table = "catalog.schema.ai_query_test"

# replace with your prompt -- make sure it's the same as what you fine-tuned the model with
prompt = """Extract the following information from the document in a JSON format: 
Name, Address, City, State, Zip, and Phone Number. 
DOC: 
"""

# the ai_query expression
ai_query_expr = f"""
    ai_query(
        '{model_endpoint}', 
        prompt, 
        modelParameters => named_struct('max_tokens', 4000, 'temperature', 0.0), 
        failOnError => false
    )
    """

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute streaming job

# COMMAND ----------

# Start streaming
stream_job = (
    # Read stream from cloudFiles
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.maxFilesPerTrigger", 1000)  # default value
    .option("cloudFiles.format", "binaryFile")
    .option("pathGlobfilter", "*.txt")
    .load(input_dir)
    # Create columns
    .withColumn("content", expr("CAST(content AS STRING)"))
    .withColumn("prompt", expr(f"CONCAT('{prompt}', content)"))
    .withColumn("response", expr(f"{ai_query_expr}"))
    # Write to output
    .writeStream.format("delta")
    .trigger(availableNow=True)
    .outputMode("append")
    .option("checkpointLocation", checkpoint_dir)
    .toTable(output_table)
)

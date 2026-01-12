# Databricks notebook source
# MAGIC %md
# MAGIC # Classify and Extract Claims Data
# MAGIC
# MAGIC This notebook reads parsed documents and calls the deployed classification-extraction
# MAGIC agent via `ai_query()` for batch processing with automatic parallelism.

# COMMAND ----------

# MAGIC %pip install -r ../../requirements.txt --quiet
# MAGIC %restart_python

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    FloatType,
    StringType,
    StructField,
    StructType,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

cfg = Config.from_yaml()

ENDPOINT_NAME = cfg.endpoint_name
CHECKPOINT_VOLUME = cfg.checkpoint_volume
PARSED_TABLE_FULL = cfg.parsed_table_full
EXTRACTED_TABLE_FULL = cfg.extracted_table_full
CHECKPOINT_PATH = f"{CHECKPOINT_VOLUME}/extract"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Output Schema for ai_query()
# MAGIC
# MAGIC The schema must match the agent's output structure for proper parsing.

# COMMAND ----------

# SQL return type for ai_query - matches AgentOutput from the agent
RETURN_TYPE = """
STRUCT<
    classification: STRUCT<
        claim_type: STRING,
        priority: STRING,
        confidence: FLOAT
    >,
    extraction: STRUCT<
        insured: STRUCT<
            full_name: STRING,
            employer_company: STRING,
            group_policy_no: STRING,
            ssn: STRING,
            date_of_birth: STRING,
            phone: STRING,
            address: STRING,
            city: STRING,
            state: STRING,
            zip_code: STRING,
            sex: STRING
        >,
        patient: STRUCT<
            full_name: STRING,
            relationship_to_insured: STRING,
            ssn: STRING,
            date_of_birth: STRING,
            sex: STRING
        >,
        accident: STRUCT<
            date_of_accident: STRING,
            location: STRING,
            description: STRING,
            motor_vehicle_accident: BOOLEAN,
            work_related: BOOLEAN,
            hospitalized: BOOLEAN,
            admission_date: STRING,
            discharge_date: STRING,
            hospital_name: STRING
        >,
        treatment: STRUCT<
            date_of_service: STRING,
            diagnosis_description: STRING,
            diagnosis_code_icd: STRING,
            procedure_code_cpt: STRING,
            procedure_description: STRING,
            emergency_room: BOOLEAN,
            emergency_room_date: STRING,
            urgent_care: BOOLEAN,
            urgent_care_date: STRING,
            surgery_performed: BOOLEAN,
            surgery_details: STRING,
            facility_name: STRING,
            other_conditions: STRING
        >,
        physician: STRUCT<
            name: STRING,
            specialty: STRING,
            address: STRING,
            city: STRING,
            state: STRING,
            zip_code: STRING,
            phone: STRING,
            fax: STRING,
            signature_date: STRING
        >,
        dismemberment: STRUCT<
            hearing_loss: BOOLEAN,
            hearing_loss_details: STRING,
            sight_loss: BOOLEAN,
            sight_loss_details: STRING,
            limb_loss: BOOLEAN,
            limb_loss_details: STRING,
            paralysis: BOOLEAN,
            paralysis_details: STRING
        >,
        benefits_claimed: STRUCT<
            lodging: BOOLEAN,
            transportation: BOOLEAN,
            youth_sport: BOOLEAN,
            accidental_death: BOOLEAN,
            death_date: STRING
        >,
        signature_date: STRING
    >,
    is_failed: BOOLEAN,
    error_message: STRING
>
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read Parsed Documents (Streaming)

# COMMAND ----------

parsed_stream = spark.readStream.table(PARSED_TABLE_FULL).select(
    "document_id",
    "path",
    "extracted_text",
    "parsed_at",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Prepare Input for Agent
# MAGIC
# MAGIC The agent expects JSON input with document_id, text, and optional metadata.

# COMMAND ----------

# Create input struct for the agent
# Note: Do NOT use to_json() - ai_query() expects a struct, not a JSON string
agent_input = parsed_stream.withColumn(
    "agent_input",
    F.struct(
        F.col("document_id"),
        F.col("extracted_text").alias("text"),
        F.struct(F.col("path")).alias("metadata"),
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Call Agent via ai_query()
# MAGIC
# MAGIC Uses the deployed Model Serving endpoint for classification and extraction.

# COMMAND ----------

# Apply ai_query to call the agent endpoint
# Note: ai_query expects the endpoint name, input, and return type
extracted = agent_input.selectExpr(
    "document_id",
    "path",
    "parsed_at",
    f"""
    ai_query(
        '{ENDPOINT_NAME}',
        agent_input,
        returnType => '{RETURN_TYPE}'
    ) as result
    """,
    "current_timestamp() as extracted_at",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Flatten the Result Structure

# COMMAND ----------

# Flatten the nested result structure
flattened = extracted.select(
    "document_id",
    "path",
    "parsed_at",
    "extracted_at",
    # Classification fields
    F.col("result.classification.claim_type").alias("claim_type"),
    F.col("result.classification.priority").alias("priority"),
    F.col("result.classification.confidence").alias("classification_confidence"),
    # Insured info
    F.col("result.extraction.insured.full_name").alias("insured_name"),
    F.col("result.extraction.insured.employer_company").alias("employer"),
    F.col("result.extraction.insured.group_policy_no").alias("policy_number"),
    F.col("result.extraction.insured.ssn").alias("insured_ssn"),
    F.col("result.extraction.insured.date_of_birth").alias("insured_dob"),
    F.col("result.extraction.insured.phone").alias("insured_phone"),
    F.col("result.extraction.insured.address").alias("insured_address"),
    F.col("result.extraction.insured.city").alias("insured_city"),
    F.col("result.extraction.insured.state").alias("insured_state"),
    F.col("result.extraction.insured.zip_code").alias("insured_zip"),
    F.col("result.extraction.insured.sex").alias("insured_sex"),
    # Patient info
    F.col("result.extraction.patient.full_name").alias("patient_name"),
    F.col("result.extraction.patient.relationship_to_insured").alias(
        "patient_relationship"
    ),
    F.col("result.extraction.patient.ssn").alias("patient_ssn"),
    F.col("result.extraction.patient.date_of_birth").alias("patient_dob"),
    F.col("result.extraction.patient.sex").alias("patient_sex"),
    # Accident info
    F.col("result.extraction.accident.date_of_accident").alias("accident_date"),
    F.col("result.extraction.accident.location").alias("accident_location"),
    F.col("result.extraction.accident.description").alias("accident_description"),
    F.col("result.extraction.accident.motor_vehicle_accident").alias(
        "is_motor_vehicle"
    ),
    F.col("result.extraction.accident.work_related").alias("is_work_related"),
    F.col("result.extraction.accident.hospitalized").alias("was_hospitalized"),
    F.col("result.extraction.accident.admission_date").alias("admission_date"),
    F.col("result.extraction.accident.discharge_date").alias("discharge_date"),
    F.col("result.extraction.accident.hospital_name").alias("hospital_name"),
    # Treatment info (APS)
    F.col("result.extraction.treatment.date_of_service").alias("service_date"),
    F.col("result.extraction.treatment.diagnosis_description").alias("diagnosis"),
    F.col("result.extraction.treatment.diagnosis_code_icd").alias("diagnosis_code"),
    F.col("result.extraction.treatment.procedure_code_cpt").alias("procedure_code"),
    F.col("result.extraction.treatment.procedure_description").alias(
        "procedure_description"
    ),
    F.col("result.extraction.treatment.emergency_room").alias("emergency_room"),
    F.col("result.extraction.treatment.emergency_room_date").alias(
        "emergency_room_date"
    ),
    F.col("result.extraction.treatment.urgent_care").alias("urgent_care"),
    F.col("result.extraction.treatment.urgent_care_date").alias("urgent_care_date"),
    F.col("result.extraction.treatment.surgery_performed").alias("surgery_performed"),
    F.col("result.extraction.treatment.surgery_details").alias("surgery_details"),
    F.col("result.extraction.treatment.facility_name").alias("facility_name"),
    F.col("result.extraction.treatment.other_conditions").alias("other_conditions"),
    # Physician info (APS)
    F.col("result.extraction.physician.name").alias("physician_name"),
    F.col("result.extraction.physician.specialty").alias("physician_specialty"),
    F.col("result.extraction.physician.address").alias("physician_address"),
    F.col("result.extraction.physician.city").alias("physician_city"),
    F.col("result.extraction.physician.state").alias("physician_state"),
    F.col("result.extraction.physician.zip_code").alias("physician_zip"),
    F.col("result.extraction.physician.phone").alias("physician_phone"),
    F.col("result.extraction.physician.fax").alias("physician_fax"),
    F.col("result.extraction.physician.signature_date").alias(
        "physician_signature_date"
    ),
    # Dismemberment info (APS)
    F.col("result.extraction.dismemberment.hearing_loss").alias("hearing_loss"),
    F.col("result.extraction.dismemberment.hearing_loss_details").alias(
        "hearing_loss_details"
    ),
    F.col("result.extraction.dismemberment.sight_loss").alias("sight_loss"),
    F.col("result.extraction.dismemberment.sight_loss_details").alias(
        "sight_loss_details"
    ),
    F.col("result.extraction.dismemberment.limb_loss").alias("limb_loss"),
    F.col("result.extraction.dismemberment.limb_loss_details").alias(
        "limb_loss_details"
    ),
    F.col("result.extraction.dismemberment.paralysis").alias("paralysis"),
    F.col("result.extraction.dismemberment.paralysis_details").alias(
        "paralysis_details"
    ),
    # Benefits claimed (EPS)
    F.col("result.extraction.benefits_claimed.lodging").alias("benefit_lodging"),
    F.col("result.extraction.benefits_claimed.transportation").alias(
        "benefit_transportation"
    ),
    F.col("result.extraction.benefits_claimed.youth_sport").alias(
        "benefit_youth_sport"
    ),
    F.col("result.extraction.benefits_claimed.accidental_death").alias(
        "benefit_accidental_death"
    ),
    F.col("result.extraction.benefits_claimed.death_date").alias("benefit_death_date"),
    # Signature
    F.col("result.extraction.signature_date").alias("signature_date"),
    # Status fields
    F.col("result.is_failed").alias("is_failed"),
    F.col("result.error_message").alias("error_message"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Write Extracted Claims to Delta Table

# COMMAND ----------

extract_query = (
    flattened.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/extracted")
    .trigger(availableNow=True)
    .toTable(EXTRACTED_TABLE_FULL)
).awaitTermination()

print(f"Extracted claims written to {EXTRACTED_TABLE_FULL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Results

# COMMAND ----------

# Show sample of extracted claims
display(
    spark.table(EXTRACTED_TABLE_FULL)
    .select(
        "document_id",
        "claim_type",
        "priority",
        "insured_name",
        "accident_date",
        "accident_description",
        "physician_name",
        "is_failed",
    )
    .limit(10)
)

# COMMAND ----------

# Show extraction statistics
stats = spark.table(EXTRACTED_TABLE_FULL).agg(
    F.count("*").alias("total_documents"),
    F.sum(F.when(F.col("is_failed"), 1).otherwise(0)).alias("failed_documents"),
    F.avg("classification_confidence").alias("avg_classification_confidence"),
    F.sum(F.when(F.col("claim_type") == "APS", 1).otherwise(0)).alias("aps_forms"),
    F.sum(F.when(F.col("claim_type") == "EPS", 1).otherwise(0)).alias("eps_forms"),
    F.sum(F.when(F.col("claim_type") == "Other", 1).otherwise(0)).alias("other_forms"),
    F.sum(F.when(F.col("priority") == "Urgent", 1).otherwise(0)).alias("urgent_claims"),
)

display(stats)

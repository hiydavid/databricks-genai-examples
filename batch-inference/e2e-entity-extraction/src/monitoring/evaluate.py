# Databricks notebook source
# MAGIC %md
# MAGIC # MLflow 3 Evaluation and Production Monitoring
# MAGIC
# MAGIC This notebook runs MLflow GenAI Evaluation on the claims extraction agent
# MAGIC using the MLflow 3 scorer API.

# COMMAND ----------

# MAGIC %pip install -r ../../requirements.txt --quiet
# MAGIC %restart_python

# COMMAND ----------

import json

import mlflow
from mlflow.entities import Feedback
from mlflow.genai.scorers import scorer

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

cfg = Config.from_yaml()

MODEL_NAME = cfg.model_name
ENDPOINT_NAME = cfg.endpoint_name
EVAL_SAMPLE_RATE = cfg.eval_sample_rate
REGISTERED_MODEL_NAME = cfg.registered_model_name
EVAL_TABLE_FULL = cfg.eval_table_full
LLM_ENDPOINT = cfg.llm_endpoint
MLFLOW_EXPERIMENT_ID = cfg.mlflow_experiment_id

# Evaluation dataset creation config
EVAL_DATASET_SIZE = cfg.eval_dataset_size
EVAL_MIN_CONFIDENCE = cfg.eval_min_confidence
EVAL_SAMPLING_STRATEGY = cfg.eval_sampling_strategy
PARSED_TABLE_FULL = cfg.parsed_table_full
EXTRACTED_TABLE_FULL = cfg.extracted_table_full

print(f"Source tables:")
print(f"  Parsed: {PARSED_TABLE_FULL}")
print(f"  Extracted: {EXTRACTED_TABLE_FULL}")
print(f"  Eval output: {EVAL_TABLE_FULL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expected Output Structure
# MAGIC
# MAGIC Agent outputs should have this structure (validated loosely by key presence):
# MAGIC ```
# MAGIC {
# MAGIC   "classification": {"claim_type": "APS|EPS|Other", "priority": "Urgent|Standard", "confidence": 0.0-1.0},
# MAGIC   "extraction": {"insured": {...}, "patient": {...}, "accident": {...}, ...},
# MAGIC   "is_failed": bool,
# MAGIC   "error_message": str|null
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Custom Scorers (MLflow 3 API)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Schema Compliance Scorer

# COMMAND ----------


@scorer
def schema_compliance(outputs: str) -> Feedback:
    """Validate that outputs have the expected top-level structure."""
    try:
        pred = json.loads(outputs) if isinstance(outputs, str) else outputs

        # Check required top-level keys
        missing = []
        if "classification" not in pred:
            missing.append("classification")
        if "extraction" not in pred:
            missing.append("extraction")

        if missing:
            return Feedback(value=False, rationale=f"Missing required keys: {missing}")

        # Check classification has expected fields
        classification = pred.get("classification", {})
        if not all(
            k in classification for k in ["claim_type", "priority", "confidence"]
        ):
            return Feedback(
                value=False,
                rationale="Classification missing claim_type, priority, or confidence",
            )

        # Check claim_type is valid
        if classification.get("claim_type") not in ["APS", "EPS", "Other"]:
            return Feedback(
                value=False,
                rationale=f"Invalid claim_type: {classification.get('claim_type')}",
            )

        return Feedback(value=True, rationale="Output has valid structure")
    except json.JSONDecodeError:
        return Feedback(value=False, rationale="Invalid JSON output")
    except Exception as e:
        return Feedback(value=False, rationale=f"Validation error: {str(e)}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Field Completeness Scorer

# COMMAND ----------


@scorer
def field_completeness(outputs: str) -> Feedback:
    """Score based on how many core fields were extracted.

    Checks fields that should be present in most APS/EPS forms:
    - Insured: name, DOB
    - Accident: date, description
    - Classification confidence >= 0.5
    """
    try:
        pred = json.loads(outputs) if isinstance(outputs, str) else outputs
        extraction = pred.get("extraction", {}) or {}
        classification = pred.get("classification", {}) or {}

        filled_fields = []
        total_fields = 5

        # Insured info
        insured = extraction.get("insured", {}) or {}
        if insured.get("full_name"):
            filled_fields.append("insured.full_name")
        if insured.get("date_of_birth"):
            filled_fields.append("insured.date_of_birth")

        # Accident info
        accident = extraction.get("accident", {}) or {}
        if accident.get("date_of_accident"):
            filled_fields.append("accident.date_of_accident")
        if accident.get("description"):
            filled_fields.append("accident.description")

        # Classification confidence
        if classification.get("confidence") and classification["confidence"] >= 0.5:
            filled_fields.append("classification.confidence")

        score = len(filled_fields) / total_fields
        return Feedback(
            value=score,
            rationale=f"Extracted {len(filled_fields)}/{total_fields} core fields: {', '.join(filled_fields) if filled_fields else 'none'}",
        )
    except Exception as e:
        return Feedback(value=0.0, rationale=f"Error evaluating completeness: {str(e)}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Confidence Threshold Scorer

# COMMAND ----------


@scorer
def confidence_threshold(outputs: str) -> Feedback:
    """Score based on classification confidence meeting threshold (0.7)."""
    CONFIDENCE_THRESHOLD = 0.7

    try:
        pred = json.loads(outputs) if isinstance(outputs, str) else outputs
        classification = pred.get("classification", {}) or {}
        confidence = classification.get("confidence", 0.0)

        if confidence:
            passes = confidence >= CONFIDENCE_THRESHOLD
            return Feedback(
                value=passes,
                rationale=f"Classification confidence: {confidence:.2f} ({'meets' if passes else 'below'} threshold {CONFIDENCE_THRESHOLD})",
            )
        else:
            return Feedback(value=False, rationale="No classification confidence found")
    except Exception as e:
        return Feedback(value=False, rationale=f"Error evaluating confidence: {str(e)}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Correctness Scorer (Requires Ground Truth)
# MAGIC
# MAGIC **NOTE:** This scorer requires ground truth labels (`expected_facts`). The current
# MAGIC implementation uses placeholder expected_facts. For production use, replace the
# MAGIC placeholder values in `create_evaluation_dataset_from_pipeline()` with real ground truth.
# MAGIC
# MAGIC See: https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/concepts/judges/is_correct

# COMMAND ----------

from mlflow.genai.scorers import Correctness

# Initialize Correctness scorer
# Uses LLM-as-judge to verify if outputs contain the expected_facts
correctness_scorer = Correctness()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Evaluation Dataset from Pipeline Outputs

# COMMAND ----------


def create_evaluation_dataset_from_pipeline(
    parsed_table: str,
    extracted_table: str,
    sample_size: int = 100,
    sampling_strategy: str = "stratified",
    min_confidence: float = 0.7,
    exclude_failed: bool = True,
    seed: int = 42,
):
    """
    Create evaluation dataset by joining pipeline outputs.

    Builds evaluation dataset from production pipeline data by joining parsed documents
    with extracted claims. Includes both input text and agent outputs (reconstructed
    from flattened columns) so predictions don't need to be re-run.

    Reference: https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/build-eval-dataset

    Args:
        parsed_table: Full name of parsed_documents table
        extracted_table: Full name of extracted_claims table
        sample_size: Target number of samples (max 2000 per MLflow constraints)
        sampling_strategy: "stratified" or "random"
        min_confidence: Minimum classification confidence for filtering
        exclude_failed: Whether to exclude failed extractions
        seed: Random seed for reproducibility

    Returns:
        Spark DataFrame with evaluation schema including outputs, or None if no data
    """
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    # Load source tables
    parsed_df = spark.table(parsed_table).select(
        "document_id",
        "extracted_text",
    )

    # Select all columns needed to reconstruct agent output
    extracted_df = spark.table(extracted_table)

    # Join tables
    joined_df = parsed_df.join(extracted_df, on="document_id", how="inner")

    # Apply filters
    filtered_df = joined_df.filter(
        F.col("extracted_text").isNotNull()
        & (F.length(F.col("extracted_text")) > 100)  # Ensure meaningful content
    )

    if exclude_failed:
        filtered_df = filtered_df.filter(
            (F.col("is_failed") == False) | F.col("is_failed").isNull()
        )

    if min_confidence > 0:
        filtered_df = filtered_df.filter(
            F.col("classification_confidence") >= min_confidence
        )

    total_count = filtered_df.count()
    if total_count == 0:
        return None

    # Apply sampling strategy
    if sampling_strategy == "stratified" and total_count > sample_size:
        # Use window function for stratified sampling by claim_type and priority
        window = Window.partitionBy("claim_type", "priority").orderBy(F.rand(seed))

        # Get stratum counts to calculate proportions
        strata_counts = filtered_df.groupBy("claim_type", "priority").count().collect()
        strata_limits = []
        for row in strata_counts:
            stratum_count = row["count"]
            # Proportional allocation, minimum 1 per stratum
            target = max(1, int(sample_size * stratum_count / total_count))
            strata_limits.append((row["claim_type"], row["priority"], target))

        limits_df = spark.createDataFrame(
            strata_limits, ["_claim_type", "_priority", "_limit"]
        )

        sampled_df = (
            filtered_df.withColumn("_row_num", F.row_number().over(window))
            .join(
                limits_df,
                (F.col("claim_type") == F.col("_claim_type"))
                & (F.col("priority") == F.col("_priority")),
                "left",
            )
            .filter(
                F.col("_row_num") <= F.coalesce(F.col("_limit"), F.lit(sample_size))
            )
            .drop("_row_num", "_claim_type", "_priority", "_limit")
        )
    else:
        # Random sampling or dataset smaller than sample_size
        if total_count > sample_size:
            fraction = sample_size / total_count
            sampled_df = filtered_df.sample(
                fraction=min(1.0, fraction * 1.2), seed=seed
            )
        else:
            sampled_df = filtered_df

    # Limit to exact sample size
    sampled_df = sampled_df.limit(sample_size)

    # Reconstruct agent output JSON from flattened columns
    outputs_struct = F.to_json(
        F.struct(
            F.struct(
                F.col("claim_type"),
                F.col("priority"),
                F.col("classification_confidence").alias("confidence"),
            ).alias("classification"),
            F.struct(
                F.struct(
                    F.col("insured_name").alias("full_name"),
                    F.col("employer").alias("employer_company"),
                    F.col("policy_number").alias("group_policy_no"),
                    F.col("insured_ssn").alias("ssn"),
                    F.col("insured_dob").alias("date_of_birth"),
                    F.col("insured_phone").alias("phone"),
                    F.col("insured_address").alias("address"),
                    F.col("insured_city").alias("city"),
                    F.col("insured_state").alias("state"),
                    F.col("insured_zip").alias("zip_code"),
                    F.col("insured_sex").alias("sex"),
                ).alias("insured"),
                F.struct(
                    F.col("patient_name").alias("full_name"),
                    F.col("patient_relationship").alias("relationship_to_insured"),
                    F.col("patient_ssn").alias("ssn"),
                    F.col("patient_dob").alias("date_of_birth"),
                    F.col("patient_sex").alias("sex"),
                ).alias("patient"),
                F.struct(
                    F.col("accident_date").alias("date_of_accident"),
                    F.col("accident_location").alias("location"),
                    F.col("accident_description").alias("description"),
                    F.col("is_motor_vehicle").alias("motor_vehicle_accident"),
                    F.col("is_work_related").alias("work_related"),
                    F.col("was_hospitalized").alias("hospitalized"),
                    F.col("admission_date"),
                    F.col("discharge_date"),
                    F.col("hospital_name"),
                ).alias("accident"),
                F.col("signature_date"),
            ).alias("extraction"),
            F.col("is_failed"),
            F.col("error_message"),
        )
    )

    # Format to evaluation schema with outputs
    # Note: expected_facts are placeholders for the Correctness scorer.
    # Replace with real ground truth labels for production evaluation.
    # See: https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/concepts/judges/is_correct
    eval_df = sampled_df.select(
        F.col("document_id"),
        F.col("extracted_text").alias("inputs"),
        outputs_struct.alias("outputs"),
        # Placeholder expected_facts for Correctness scorer
        # TODO: Replace with real ground truth labels when available
        F.array(
            F.lit("This is a dummy ground-truth label. Replace with real ones."),
        ).alias("expected_facts"),
    )

    return eval_df


# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Evaluation Dataset
# MAGIC
# MAGIC Set `refresh_eval_data = true` in the widget above to force recreation from pipeline tables.

# COMMAND ----------

# Widget for forcing refresh - set to "true" to regenerate eval dataset from pipeline
dbutils.widgets.dropdown("refresh_eval_data", "false", ["true", "false"])
force_refresh = dbutils.widgets.get("refresh_eval_data") == "true"
print(f"Force refresh: {force_refresh}")

# COMMAND ----------


def load_or_create_evaluation_dataset():
    """Load evaluation dataset from table or create from pipeline outputs."""

    # Try loading existing evaluation table (unless refresh forced)
    if not force_refresh:
        try:
            eval_spark_df = spark.table(EVAL_TABLE_FULL)
            count = eval_spark_df.count()
            if count > 0:
                print(f"Loaded {count} existing examples from {EVAL_TABLE_FULL}")
                print("Set refresh_eval_data=true to regenerate from pipeline")
                return eval_spark_df.toPandas()
        except Exception as e:
            print(f"Evaluation table not found: {e}")

    # Create from pipeline outputs
    print("Creating evaluation dataset from pipeline outputs...")
    eval_spark_df = create_evaluation_dataset_from_pipeline(
        parsed_table=PARSED_TABLE_FULL,
        extracted_table=EXTRACTED_TABLE_FULL,
        sample_size=EVAL_DATASET_SIZE,
        sampling_strategy=EVAL_SAMPLING_STRATEGY,
        min_confidence=EVAL_MIN_CONFIDENCE,
        exclude_failed=True,
        seed=42,
    )

    if eval_spark_df is None or eval_spark_df.count() == 0:
        raise ValueError(
            f"No evaluation data available. Ensure pipeline has run:\n"
            f"  1. Run 01_ingest.py to populate {PARSED_TABLE_FULL}\n"
            f"  2. Run 02_extract.py to populate {EXTRACTED_TABLE_FULL}"
        )

    # Save to evaluation table for future runs
    eval_spark_df.write.mode("overwrite").saveAsTable(EVAL_TABLE_FULL)
    count = eval_spark_df.count()
    print(f"Created and saved {count} examples to {EVAL_TABLE_FULL}")
    return eval_spark_df.toPandas()


# COMMAND ----------

eval_df = load_or_create_evaluation_dataset()
print(f"Evaluation dataset ready with {len(eval_df)} examples")

# Outputs are already included from the pipeline - no need to re-run predictions
print(f"Columns: {list(eval_df.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run MLflow GenAI Evaluation

# COMMAND ----------

# Prepare evaluation data in the format expected by mlflow.genai.evaluate
# Each row needs 'inputs', 'outputs', and 'expectations' columns for Correctness scorer
eval_data = eval_df[["inputs", "outputs", "expected_facts"]].copy()
eval_data["inputs"] = eval_data["inputs"].apply(lambda x: {"text": x})
# Map expected_facts to expectations dict for Correctness scorer
eval_data["expectations"] = eval_data["expected_facts"].apply(
    lambda facts: {"expected_facts": list(facts) if facts is not None else []}
)
eval_data = eval_data.drop(columns=["expected_facts"])

# Use the same MLflow experiment as the agent (configured in config.yaml)
mlflow.set_experiment(experiment_id=MLFLOW_EXPERIMENT_ID)

# Run evaluation with MLflow 3 GenAI API
# Note: mlflow.genai.evaluate() creates its own run internally
eval_results = mlflow.genai.evaluate(
    data=eval_data,
    scorers=[
        schema_compliance,
        field_completeness,
        confidence_threshold,
        correctness_scorer,
    ],
)

print("Evaluation Results:")
print(eval_results.metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("EVALUATION SUMMARY")
print("=" * 60)
print(f"Model: {REGISTERED_MODEL_NAME}")
print(f"Endpoint: {ENDPOINT_NAME}")
print(f"Evaluation samples: {len(eval_df)}")
print(f"MLflow Experiment ID: {MLFLOW_EXPERIMENT_ID}")
print()
print("Metrics:")
for metric, value in eval_results.metrics.items():
    if isinstance(value, float):
        print(f"  {metric}: {value:.3f}")
    else:
        print(f"  {metric}: {value}")
print("=" * 60)

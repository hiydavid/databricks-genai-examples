# Databricks notebook source
# MAGIC %md
# MAGIC # Multi-Threading Approach
# MAGIC
# MAGIC How it works:
# MAGIC * Manually send parallel batches to the model endpoint.
# MAGIC * Use a notebook cluster with at least 4 worker nodes
# MAGIC * Currently the endpoint is set to max concurrency of 4

# COMMAND ----------

import requests
import json
import pandas as pd
import numpy as np
import time
from typing import Dict, List, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Configuration
WORKSPACE_URL = "YOUR_WORKSPACE_URL"
ENDPOINT_NAME = "YOUR_ENDPOINT_NAME"
API_URL = f"https://{WORKSPACE_URL}/serving-endpoints/{ENDPOINT_NAME}/invocations"


def get_databricks_token():
    """Get the Databricks token from the current context."""
    try:
        # Get token from Databricks context
        token = (
            dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .apiToken()
            .get()
        )
        return token
    except Exception as e:
        print(f"Error getting token: {e}")
        print("Please set your token manually:")
        print("token = 'your-databricks-token-here'")
        return None


TOKEN = get_databricks_token()  # TODO: Replace with your actual token


def create_sample_record() -> Dict[str, Any]:
    """Create a single sample record with all required fields."""

    # Set seed for reproducible results
    np.random.seed(42)

    record = {
        "feature_1": 13456789,
        "feature_2": 321654987,
        "feature_3": True,
        "feature_4": 15.0,
        "feature_5": 45.0,
        "feature_6": 10.0,
        "feature_7": True,
        "feature_8": 300000.0,
        "feature_9": 400.0,
        "feature_10": 75000.0,
        "feature_11": 150000.0,
        "feature_12": 5000.0,
        "feature_13": 5000.0,
        "feature_14": True,
        "feature_15": False,
        "feature_16": "abc123xyz",
        "feature_17": True,
        "feature_18": "908",
        "feature_19": True,
        "feature_20": False,
    }

    return record


def create_test_batch(num_records: int = 5) -> List[Dict[str, Any]]:
    """Create multiple test records."""

    records = []
    base_record = create_sample_record()

    for i in range(num_records):
        record = base_record.copy()

        # Vary some fields to create diversity
        record["feature_4"] = float(np.random.randint(5, 50))
        record["feature_5"] = float(np.random.randint(25, 75))
        record["feature_6"] = float(np.random.randint(5, 15))
        record["feature_8"] = float(np.random.choice([250000, 300000, 500000, 750000]))
        record["feature_9"] = float(np.random.choice([100, 400, 800, 1000]))
        record["feature_10"] = float(np.random.choice([50000, 75000, 90000, 120000]))
        record["feature_11"] = float(np.random.choice([100000, 150000, 200000, 250000]))
        record["feature_12"] = float(np.random.choice([1000, 2500, 5000, 10000]))
        record["feature_13"] = float(np.random.choice([1000, 2500, 5000, 10000]))
        records.append(record)

    return records


def to_native(x):
    """Convert numpy types to native types."""
    if isinstance(x, np.integer):
        return int(x)
    elif isinstance(x, np.floating):
        return float(x)
    elif isinstance(x, np.bool_):
        return bool(x)
    else:
        return x


def sanitize(records):
    """Sanitize records by converting numpy types to native types."""
    return [{k: to_native(v) for k, v in rec.items()} for rec in records]


def query_endpoint_batch(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Query the endpoint for a batch of records."""

    headers = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

    payload = {"dataframe_records": records}

    try:
        response = requests.post(API_URL, headers=headers, json=payload, timeout=60)
        response.raise_for_status()

        result = response.json()
        predictions = result.get("predictions", [])

        # Combine original records with predictions
        results = []
        for i, record in enumerate(records):
            if i < len(predictions):
                result_with_data = {**record, **predictions[i]}
            else:
                result_with_data = {**record, "error": "No prediction returned"}
            results.append(result_with_data)

        return results

    except requests.exceptions.RequestException as e:
        print(f"Error querying endpoint batch: {e}")
        return [{**record, "error": str(e)} for record in records]


def process_records_with_batching(
    records: List[Dict[str, Any]], batch_size: int = 16, max_workers: int = 4
) -> List[Dict[str, Any]]:
    """Process records using batching and parallel execution."""

    print(
        f"Processing {len(records)} records with batch size {batch_size} and {max_workers} workers"
    )

    # Split records into batches
    batches = [records[i : i + batch_size] for i in range(0, len(records), batch_size)]
    print(f"Created {len(batches)} batches")

    all_results = []
    start_time = time.time()

    # Process batches in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all batch jobs
        future_to_batch = {
            executor.submit(query_endpoint_batch, batch): i
            for i, batch in enumerate(batches)
        }

        # Collect results as they complete
        for future in as_completed(future_to_batch):
            batch_idx = future_to_batch[future]
            try:
                batch_results = future.result()
                all_results.extend(batch_results)
                print(
                    f"Completed batch {batch_idx + 1}/{len(batches)} ({len(batch_results)} records)"
                )
            except Exception as e:
                print(f"Batch {batch_idx + 1} failed: {e}")
                # Add error records for failed batch
                failed_batch = batches[batch_idx]
                error_results = [{**record, "error": str(e)} for record in failed_batch]
                all_results.extend(error_results)

    end_time = time.time()
    total_time = end_time - start_time
    print(f"Total processing time: {total_time:.2f} seconds")
    print(f"Average time per record: {total_time/len(records):.3f} seconds")
    print(f"Records per second: {len(records)/total_time:.2f}")

    return all_results


# Main execution
if __name__ == "__main__":
    # Create test data
    test_records = create_test_batch(30000)  # Start with 100 records
    clean_records = sanitize(test_records)

    print("\n=== Testing Parallel Batching ===")
    parallel_results = process_records_with_batching(
        clean_records, batch_size=2500, max_workers=4
    )

    # Convert to DataFrame for analysis
    df_parallel = pd.DataFrame(parallel_results)

    print(f"Parallel results shape: {df_parallel.shape}")

    # Check for errors
    parallel_errors = (
        df_parallel[df_parallel["error"].notna()]
        if "error" in df_parallel.columns
        else pd.DataFrame()
    )

    print(f"Parallel errors: {len(parallel_errors)}")

    # Display sample results
    print("\nSample results (first 3 records):")
    if len(df_parallel) > 0:
        print(df_parallel.head(3))

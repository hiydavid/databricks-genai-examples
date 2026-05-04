# Feature Store Declarative API Examples

This folder contains simple Databricks notebooks that demonstrate the Declarative Feature API.

## Notebooks

Run the setup notebook first:

1. `00_dummy-data-creation.py` creates dummy credit card customer, merchant, and transaction data.

Then run the example notebooks for each use case:

1. `01_declarative_api_offline_batch.py` demonstrates declarative features for offline training and batch inference.
2. `02_declarative_api_online_serving.py` demonstrates materialized declarative features for online inference with Model Serving.
3. Feature Serving example notebook is planned.
4. Managed streaming features example notebook is planned.

## Prerequisites

- Unity Catalog-enabled Databricks workspace.
- A classic compute cluster running Databricks Runtime 17.0 ML or above.
- Permissions to create tables, register features, and create serving resources in the configured catalog and schema.

Update the `CATALOG` and `SCHEMA` constants in each notebook before running.

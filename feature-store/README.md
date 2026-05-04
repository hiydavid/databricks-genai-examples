# Feature Store Declarative API Examples

This folder contains simple Databricks notebooks that demonstrate the Declarative Feature API.

## Notebooks

Run the setup notebook first:

1. `00_dummy-data-creation.py` creates dummy credit card customer, merchant, and transaction data.

Then run the example notebooks for each use case:

1. `01_declarative_features_api.py` demonstrates declarative features for offline training, batch inference, materialization, and online inference with Model Serving.
2. `03_declarative_feature_spec_serving.py` demonstrates declarative feature specs for feature serving.
3. Managed streaming features example notebook is planned.

## Prerequisites

- Unity Catalog-enabled Databricks workspace.
- A classic compute cluster running Databricks Runtime 17.0 ML or above.
- Permissions to create tables, register features, and create serving resources in the configured catalog and schema.

Update the `CATALOG` and `SCHEMA` constants in each notebook before running.

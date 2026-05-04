# Declarative Feature Engineering Example

This folder contains a simple Databricks example for the Declarative Feature Engineering API.

## Notebooks

Run the setup notebook first:

1. `00_dummy-data-creation.py` creates dummy credit card customer, merchant, and transaction data.

Then run the declarative feature example:

1. `01_declarative_features_api.py` demonstrates feature definition, feature registration, training set creation, model logging, offline batch inference, feature materialization, online inference with Model Serving, and feature spec serving.

Managed streaming features are not included yet.

## Prerequisites

- Unity Catalog-enabled Databricks workspace.
- A classic compute cluster running Databricks Runtime 17.0 ML or above.
- Declarative Feature Engineering preview enabled.
- Permissions to create tables, register features, materialize features, and create serving resources in the configured catalog and schema.

Update the `CATALOG` and `SCHEMA` constants in each notebook before running.

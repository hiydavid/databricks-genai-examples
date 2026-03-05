# Horizon Bank — Demo Dataset Setup

Synthetic retail banking dataset for demonstrating Databricks AI/BI Genie. Generates 6 Delta tables and 3 metric views in Unity Catalog.

## Files

| File | Purpose |
|---|---|
| `generate_data.py` | Databricks notebook — generates all 6 tables, views, and constraints end-to-end |
| `generate_genie.py` | Databricks notebook — creates or updates the Genie space via the Databricks SDK |

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- A catalog and schema you have `CREATE TABLE` privileges on
- A SQL warehouse or cluster with access to that catalog
- `faker` library (installed automatically by `generate_data.py`)
- `databricks-sdk` (installed automatically by `generate_genie.py`)

## Setup Steps

### Step 1 — Set your catalog and schema

At the top of both `generate_data.py` and `generate_genie.py`, set your catalog and schema:

```python
CATALOG = "my_catalog"   # your Unity Catalog name
SCHEMA  = "horizon_bank" # your target schema
```

### Step 2 — Import and run `generate_data.py`

1. In the Databricks workspace, go to **Workspace → Import**
2. Upload `generate_data.py`
3. Open it and attach to a cluster
4. Click **Run All**

The notebook will:
- Create the catalog/schema if they don't already exist
- Write all 6 tables as Delta tables
- Create the 3 metric views
- Register PK/FK constraints and column comments
- Print row counts and run pattern validation queries at the end

Expected output:
```
products:              20 rows
branches:              25 rows
customers:          1,000 rows
accounts:          ~2,500 rows
transactions:      10,000 rows
service_requests:   3,000 rows
```

### Step 3 — Create the Genie space

1. Import `generate_genie.py` into your Databricks workspace
2. Set `CATALOG`, `SCHEMA`, and optionally `WAREHOUSE_ID` at the top of the notebook (must match `generate_data.py`)
3. Click **Run All**

The notebook will create (or update) the **Horizon Bank Analytics** Genie space with all tables, metric views, space instructions, sample questions, and verified SQL examples.

## Schema Overview

```
products (20)          branches (25)
    │                       │
    └──── accounts (~2,500) ┘
              │
          customers (1,000)
              │
    ┌─────────┴──────────┐
transactions (10,000)  service_requests (3,000)
```

## Injected Patterns

The dataset contains several non-random patterns designed to surface in demo queries:

| Pattern | Where to see it |
|---|---|
| **Nov/Dec +45% volume spike** | Monthly transaction count chart |
| **Q2 2024 deposit dip (−15%)** | Deposit trend by quarter, 2024 |
| **Mobile channel growth 25% → 48%** | Channel share over time |
| **Fee revenue +20% YoY** | Annual fee revenue comparison |
| **Top 10% customers = 35% of deposits** | Customer deposit concentration |
| **Southeast branches +20% avg txn value** | Branch performance by region |
| **Jan 2024 complaint spike (+80%)** | Service request category by month |
| **Private Client: 3× avg balance, 60% mortgage** | Customer segment breakdown |

## Reproducing the Data

The generator uses `random.seed(42)` and `Faker.seed(42)`. Running `generate_data.py` with the same seed always produces the same dataset.

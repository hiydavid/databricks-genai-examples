# Genie Query Caching

Demonstrates three cache strategies for Databricks Genie responses. The cache
stores generated SQL and metadata; on every cache hit the notebook re-executes
the cached SQL under the current Databricks identity for freshness and access
control.

## Scenarios

| Scenario | Cache Layer | Key Benefit |
|----------|-------------|-------------|
| 1. Lakebase + pgvector | PostgreSQL with vector similarity | ACID writes and immediate read-after-write |
| 2. Vector Search Index | Delta table + managed embeddings | Unity Catalog governance and hybrid semantic + BM25 search |
| 3. Hybrid | L1 Lakebase session cache + L2 Vector Search knowledge base | Fast session cache plus validated cross-session reuse |

## Architecture

| Scenario 1 | Scenario 2 | Scenario 3 |
|:---:|:---:|:---:|
| ![Scenario 1](scenario1-lakebase-pgvector.png) | ![Scenario 2](scenario2-vector-search.png) | ![Scenario 3](scenario3-hybrid.png) |

## Prerequisites

- Databricks workspace with Genie Spaces enabled and at least one Genie Space configured
- Horizon Bank demo data, or another schema compatible with your Genie space
- Lakebase instance with the pgvector extension (Scenarios 1 and 3)
- Databricks secret scope with Lakebase credentials
- Vector Search endpoint permissions (Scenarios 2 and 3)
- Unity Catalog catalog/schema for cache tables
- Serverless compute or a cluster with network access to Lakebase

## Quick Start

1. Copy `configs.template.yaml` to `configs.yaml` and fill in your values.
2. Set `data_schema` and `cache_version`; set `data_catalog` if the data lives outside the cache catalog.
3. Keep `seed_demo_data: false` if you want to exercise the cold miss path.
4. Run `0_setup.py` to create the cache schema, Lakebase table, Vector Search endpoint, and indexes.
5. Run a scenario notebook:
   - `1_lakebase_pgvector_cache.py`
   - `2_vector_search_cache.py`
   - `3_hybrid_cache.py`

Set `seed_demo_data: true` only when you want setup to pre-populate demo entries
so the notebooks start with cache hits.

## Cache Safety Model

- Cache keys include the Genie space ID, configured data catalog/schema, and `cache_version`.
- Bump `cache_version` whenever Genie instructions, table schemas, metric views, or other semantic definitions change.
- Cache hits return cached SQL only after re-executing it with Spark SQL under the current Databricks identity.
- Text-only Genie responses are not cached because they cannot be refreshed or permission-checked.
- Scenario 3 uses `cache_scope=session` for L1 and `cache_scope=global` for validated L2 entries, so sessions do not overwrite each other.

## Notebooks

| File | Purpose |
|------|---------|
| `0_setup.py` | Create v2 cache tables, Lakebase pgvector table, Vector Search endpoint/indexes, and optional seed data |
| `1_lakebase_pgvector_cache.py` | Scenario 1: exact match + pgvector similarity |
| `2_vector_search_cache.py` | Scenario 2: Vector Search with confidence tiering |
| `3_hybrid_cache.py` | Scenario 3: session L1 + validated durable L2 |
| `utils.py` | Shared helpers for config validation, cache IDs, Genie calls, SQL execution, tracing, and cache writes |

## Schema Changes

This example uses a v2 cache schema. If setup finds old v1 tables, it fails with
a reset/migration message instead of silently using incompatible tables. For a
demo reset, drop the old Delta cache tables, delete the old Vector Search
indexes, and drop the Lakebase `genie_cache` table, then rerun `0_setup.py`.

## Optional MLflow Tracing

Set `mlflow.enable_tracing: true` and configure `MLFLOW_TRACKING_URI` plus
`MLFLOW_EXPERIMENT_ID` to trace cache lookup, Genie calls, Vector Search lookup,
SQL execution, and scenario orchestration. Without those environment variables,
the tracing hooks are no-ops.

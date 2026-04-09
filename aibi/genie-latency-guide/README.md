# Genie Latency Performance Tuning Guide

A practical guide for diagnosing and reducing latency in [Databricks AI/BI Genie](https://docs.databricks.com/aws/en/genie/) spaces, covering both query generation and query execution bottlenecks.

---

## Decision Tree

```
                        ┌──────────────────────────┐
                        │   Genie Response Slow?   │
                        └────────────┬─────────────┘
                                     │
                        ┌────────────▼──────────────┐
                        │   Measure with API        │
                        │   State Polling or        │
                        │   System Tables           │
                        └────────────┬──────────────┘
                                     │
                    ┌────────────────┼─────────────────┐
                    │                                  │
         ┌──────────▼──────────┐            ┌──────────▼──────────┐
         │  ASKING_AI phase    │            │  EXECUTING_QUERY    │
         │  is the bottleneck  │            │  phase is the       │
         │  (Query Generation) │            │  bottleneck         │
         └──────────┬──────────┘            │  (Query Execution)  │
                    │                       └──────────┬──────────┘
                    │                                  │
    ┌───────────────▼───────────────┐  ┌───────────────▼───────────────┐
    │  1. DATA MODEL (Highest)      │  │  1. WAREHOUSE CONFIG          │
    │  ┌──────────────────────────┐ │  │  ┌──────────────────────────┐ │
    │  │ • Limit to 5-7 tables    │ │  │  │ • Use Serverless SQL WH  │ │
    │  │ • Denormalize tables     │ │  │  │ • Set min clusters > 0   │ │
    │  │ • Clean, curated data    │ │  │  │ • Right-size for volume  │ │
    │  └──────────────────────────┘ │  │  └──────────────────────────┘ │
    │              │                │  │              │                │
    │              ▼                │  │              ▼                │
    │  2. KNOWLEDGE STORE           │  │  2. DATA LAYER                │
    │  ┌──────────────────────────┐ │  │  ┌──────────────────────────┐ │
    │  │ • SQL expressions for    │ │  │  │ • Liquid Clustering on   │ │
    │  │   business terms         │ │  │  │   filter/dimension cols  │ │
    │  │ • Explicit join defs     │ │  │  │ • Enable Predictive Opt  │ │
    │  │ • Table/col descriptions │ │  │  │ • Compute column stats   │ │
    │  └──────────────────────────┘ │  │  │ • Enable query caching   │ │
    │              │                │  │  └──────────────────────────┘ │
    │              ▼                │  │              │                │
    │  3. SQL EXAMPLES              │  │              ▼                │
    │  ┌──────────────────────────┐ │  │  3. QUERY PROFILE ANALYSIS    │
    │  │ • Parameterized SQL for  │ │  │  ┌──────────────────────────┐ │
    │  │   common patterns        │ │  │  │ • Open DBSQL Query       │ │
    │  │ • "Save as Instruction"  │ │  │  │   Profile for slow query │ │
    │  │ • UDFs for complex logic │ │  │  │ • Check task_total_time  │ │
    │  └──────────────────────────┘ │  │  │   / execution_time ratio │ │
    │              │                │  │  └──────────────────────────┘ │
    │              ▼                │  │              │                │
    │  4. TEXT INSTRUCTIONS (Low)   │  │              ▼                │
    │  ┌──────────────────────────┐ │  │  4. FIX BAD GENERATED SQL     │
    │  │ • Keep focused, concise  │ │  │  ┌──────────────────────────┐ │
    │  │ • No contradictions      │ │  │  │ • Add SQL examples that  │ │
    │  └──────────────────────────┘ │  │  │   show optimal patterns  │ │
    │              │                │  │  │ • Auto-cancel at 15 min  │ │
    │              ▼                │  │  └──────────────────────────┘ │
    │  5. CACHING (Bypass Gen)      │  └───────────────────────────────┘
    │  ┌──────────────────────────┐ │                 │
    │  │ Pillar 1: Static FAQ     │ │                 │
    │  │   (pre-computed answers) │ │                 │
    │  │           │              │ │                 │
    │  │           ▼              │ │                 │
    │  │ Pillar 2: Parameterized  │ │                 │
    │  │   SQL tools / UDFs       │ │                 │
    │  │           │              │ │                 │
    │  │           ▼              │ │                 │
    │  │ Pillar 3: Semantic cache │ │                 │
    │  │   (with Vector Search    │ │                 │
    │  │    and/or Lakebase)      │ │                 │
    │  │                          │ │                 │
    │  │  ~80% latency reduction  │ │                 │
    │  └──────────────────────────┘ │                 │
    └───────────────────────────────┘                 │
                    │─────────────────────────────────┘
                    ▼
    ┌───────────────────────────────┐
    │  Still slow? Quick wins:      │
    │  • Start a new chat session   │
    │  • Open a support ticket with │
    │    your Databricks account    │
    │    team                       │
    └───────────────────────────────┘
```

---

## Step 0: Isolate the Bottleneck — Generation vs Execution

Genie response time breaks into two phases:

- **Query Generation** (`ASKING_AI`) — the LLM interprets the question and generates SQL
- **Query Execution** (`EXECUTING_QUERY`) — the SQL warehouse runs the generated query

### How to Measure Each Phase

Use the **Genie Conversation API** to poll message state and track timestamps across each phase:

`FETCHING_METADATA` → `FILTERING_CONTEXT` → `ASKING_AI` → `EXECUTING_QUERY` → `COMPLETED`

The accompanying [genie-tracing](./genie-tracing.py) notebook can help here.

See: [Genie Conversation API](https://docs.databricks.com/api/workspace/genie) for additional detail.

### Expected Baselines

| Metric | Typical Value |
|--------|---------------|
| Generation time (complex queries) | ~20s |
| P90 total (generation + execution) | ~30s |
| P99 total | < 60s |
| Agent mode (multi-step reasoning) | > 60s |

> **Note:** There is no published SLA for Genie response times. Agent mode involves multiple LLM calls and tool invocations, so latency above 60s is expected.

---

## Slow Query Generation (NL-to-SQL)

Priority order from field experience: **Data Model > Knowledge Store > SQL Examples > Instructions**

### 1. Simplify the Data Model (Highest Impact)

- **Limit to 5–7 tables** per Genie space — the hard limit is 30, but fewer tables means faster and more accurate generation
- **Denormalize aggressively** — fewer wide tables beat many normalized tables requiring complex joins
- Use **curated, clean, well-structured data** — less ambiguity means less LLM reasoning time

> See: [Curate a Genie space](https://docs.databricks.com/aws/en/genie/best-practices)

### 2. Optimize the Knowledge Store (Second Highest)

- **Define SQL expressions** for business terms, metrics, filters, and dimensions — these are reusable and reduce LLM reasoning
- **Explicitly define joins**, even if PK/FK relationships exist in Unity Catalog
- **Add table and column descriptions** to reduce ambiguity

> See: [Build a Knowledge Store](https://docs.databricks.com/aws/en/genie/knowledge-store)

### 3. Add Parameterized SQL Examples (Third)

- Provide parameterized SQL for common question patterns — Genie can invoke these directly as "Trusted" answers, bypassing most LLM generation
- Use **"Save as Instruction"** on verified SQL to teach Genie for future questions
- Encode complex business logic in **UDFs** that Genie can call

### 4. Keep Text Instructions Focused (Lowest Impact)

- Avoid overloaded or contradictory instructions — they degrade performance
- Keep instructions concise and specific to the domain

### 5. Caching — Bypass Generation Entirely

A **3-pillar caching pattern** can reduce latency by ~80%:

1. **Static FAQ cache** — pre-compute answers for top questions, serve without calling Genie
2. **Parameterized SQL tools/UDFs** — pattern-match questions and invoke SQL directly
3. **Semantic cache** ([Databricks Vector Search](https://www.databricks.com/blog/building-cost-optimized-chatbot-semantic-caching) or [Lakebase](https://community.databricks.com/t5/lakebase-blogs/how-to-perform-semantic-search-in-databricks-lakebase/ba-p/139846) with pgvector) — use embedding similarity to match incoming questions to prior answers

### 6. Other Tips

- **Start a new chat session** if responses degrade — accumulated conversation history can slow generation

---

## Slow Query Execution (SQL Warehouse)

### 1. Warehouse Selection and Configuration

- **Use Serverless SQL Warehouses** — predictable latency with no cold-start delays. Pro warehouses introduce variable response times
- **Avoid cold starts** — keep `min_clusters > 0` for latency-sensitive spaces (serverless cold start is 2–6s)
- **Right-size the warehouse** for your query complexity and data volume

### 2. Data Layer Optimization

- **[Liquid Clustering](https://docs.databricks.com/aws/en/delta/clustering)** — apply on dimension and filter columns for large tables
- **[Predictive Optimization](https://docs.databricks.com/aws/en/optimizations/predictive-optimization)** — enable on Unity Catalog managed tables for automatic `OPTIMIZE` and `VACUUM` scheduling
- **Ensure column statistics** are computed for filter and join columns
- **[DBSQL query caching](https://docs.databricks.com/aws/en/sql/admin/query-caching)** — significantly speeds up repeated queries

### 3. Query Profile Analysis

- Use the **[DBSQL Query Profile](https://docs.databricks.com/aws/en/sql/admin/query-profile)** to diagnose slow execution
- Check the `task_total_time / execution_time` ratio for slot utilization

### 4. Fix Genie-Generated Inefficient SQL

- When Genie generates suboptimal SQL, add **SQL examples** in the knowledge store showing the optimal query pattern
- Genie auto-cancels queries exceeding **15 minutes**

---

## References

### Databricks Documentation

| Resource | Link |
|----------|------|
| AI/BI Genie overview | [Docs](https://docs.databricks.com/aws/en/genie/) |
| Curate a Genie space | [Docs](https://docs.databricks.com/aws/en/genie/best-practices) |
| Build a Knowledge Store | [Docs](https://docs.databricks.com/aws/en/genie/knowledge-store) |
| Troubleshoot Genie spaces | [Docs](https://docs.databricks.com/aws/en/genie/troubleshooting) |
| Genie Conversation API | [API Reference](https://docs.databricks.com/api/workspace/genie) |
| Liquid Clustering | [Docs](https://docs.databricks.com/aws/en/delta/clustering) |
| Predictive Optimization | [Docs](https://docs.databricks.com/aws/en/optimizations/predictive-optimization) |
| DBSQL Query Caching | [Docs](https://docs.databricks.com/aws/en/sql/admin/query-caching) |
| DBSQL Query Profile | [Docs](https://docs.databricks.com/aws/en/sql/admin/query-profile) |
| Serverless SQL Warehouses | [Docs](https://docs.databricks.com/aws/en/compute/sql-warehouse/serverless) |

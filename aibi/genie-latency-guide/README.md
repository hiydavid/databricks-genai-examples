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
                        │                           │
                        │   First rule out:         │
                        │   • Hitting QPM limits?   │
                        │   • Agent mode enabled?   │
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
    │  │ • Denormalize / views    │ │  │  │ • Set min clusters > 0   │ │
    │  │ • Use Metric Views       │ │  │  │ • Right-size for volume  │ │
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
    │  │ • Watch token limits     │ │  │  │   show optimal patterns  │ │
    │  └──────────────────────────┘ │  │  │ • Auto-cancel at 15 min  │ │
    │              │                │  │  └──────────────────────────┘ │
    │              ▼                │  └───────────────────────────────┘
    │  5. CACHING (Bypass Gen)      │                 │
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
    │              │                │                 │
    │              ▼                │                 │
    │  6. INSPECT (Accuracy/Speed)  │                 │
    │  ┌──────────────────────────┐ │                 │
    │  │ • Auto-verifies gen SQL  │ │                 │
    │  │ • Adds time per query    │ │                 │
    │  │ • Reduces retries        │ │                 │
    │  └──────────────────────────┘ │                 │
    └───────────────────────────────┘                 │
                    │─────────────────────────────────┘
                    ▼
    ┌───────────────────────────────┐
    │  Still slow? Quick wins:      │
    │  • Start a new chat session   │
    │  • Check regional hosting     │
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

Use the **Genie Conversation API** (now GA as of April 2026) to poll message state and track timestamps across each phase:

`FETCHING_METADATA` → `FILTERING_CONTEXT` → `ASKING_AI` → `EXECUTING_QUERY` → `COMPLETED`

The accompanying [genie-tracing](./genie-tracing.py) notebook can help here.

**Polling best practices:**

- Poll every **1–5 seconds**; increase the interval with exponential backoff up to 60s
- Set a **10-minute timeout** — stop polling and return an error after that
- **GET polls are exempt** from rate limits — only POST requests count toward throughput

See: [Genie Conversation API](https://docs.databricks.com/aws/en/genie/conversation-api) for additional detail.

### Throughput Limits

If responses seem queued or delayed under load, you may be hitting throughput limits:

| Access Method | Limit |
|---------------|-------|
| Databricks UI | 20 questions/min per workspace (across all spaces) |
| Genie API (free tier) | ~5 questions/min per workspace (best-effort) |

Only `POST` requests (new questions) count toward these limits. Contact your Databricks account team for higher throughput.

### Expected Baselines

| Metric | Typical Value |
|--------|---------------|
| Generation time (complex queries) | ~20s |
| P90 total (generation + execution) | ~30s |
| P99 total | < 60s |
| Agent mode (multi-step reasoning) | > 60s |

> **Note:** There is no published SLA for Genie response times. Agent mode involves multiple LLM calls and tool invocations, so latency above 60s is expected. Agent mode is now the **default** when enabled on a workspace — users may see higher latency without realizing they are in agent mode.

**Agent mode latency improvements (2026):**

- **Parallel query execution** — agent mode now runs multiple SQL queries simultaneously instead of sequentially
- **Prompt caching** — reduces generation latency for repeated prompt patterns
- **Single-agent architecture** — consolidated from multi-agent, reducing orchestration overhead
- **Faster "what" responses** — improved system prompting produces more concise answers for simple factual questions
- Agent mode is **UI-only** during Public Preview — latency cannot be measured via the Conversation API yet

See: [Agent mode in Genie spaces](https://docs.databricks.com/aws/en/genie/agent-mode)

---

## Slow Query Generation (Text-to-SQL)

Priority order from field experience: **Data Model > Knowledge Store > SQL Examples > Instructions**

### 1. Simplify the Data Model (Highest Impact)

- **Limit to 5–7 tables** per Genie space — the hard limit is 30, but fewer tables means faster and more accurate generation
- **Denormalize aggressively** — fewer wide tables beat many normalized tables requiring complex joins
- Use **curated, clean, well-structured data** — less ambiguity means less LLM reasoning time
- **Use [Metric Views](https://docs.databricks.com/aws/en/metric-views/)** to pre-define metrics, dimensions, and aggregations — this can replace multiple tables with a single governed definition, simplifying the data model and reducing generation time

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
- From field experience: **1 quality SQL example is roughly equivalent to 20 lines of text instructions** in effectiveness — prioritize SQL examples over prose

### 4. Keep Text Instructions Focused (Lowest Impact)

- Avoid overloaded or contradictory instructions — they degrade performance
- Keep instructions concise and specific to the domain
- **Watch for token limit warnings** — when the context window fills up, Genie silently drops instructions, leading to worse SQL and more retries. Mitigations:
  - Remove unnecessary columns (create views to exclude them)
  - Prune overlapping example SQL queries
  - Avoid redundant column descriptions
- **Hard limits:** 30 tables max per space, 100 instructions (SQL examples + functions + text), 200 SQL expressions (separate limit)

### 5. Caching — Bypass Generation Entirely

A **3-pillar caching pattern** can reduce latency by ~80%:

1. **Static FAQ cache** — pre-compute answers for top questions, serve without calling Genie
2. **Parameterized SQL tools/UDFs** — pattern-match questions and invoke SQL directly
3. **Semantic cache** ([Databricks Vector Search](https://www.databricks.com/blog/building-cost-optimized-chatbot-semantic-caching) or [Lakebase](https://community.databricks.com/t5/lakebase-blogs/how-to-perform-semantic-search-in-databricks-lakebase/ba-p/139846) with pgvector) — use embedding similarity to match incoming questions to prior answers

### 6. Inspect — Latency vs Accuracy Tradeoff

[Inspect](https://docs.databricks.com/aws/en/genie/set-up) (Public Preview) automatically reviews generated SQL, runs smaller verification queries to check filters/joins/aggregations, and re-generates if issues are found. This **adds time to individual queries** but can **reduce overall latency** by cutting down retries from incorrect SQL. Consider enabling it for spaces where users frequently iterate on bad results.

### 7. Other Tips

- **Start a new chat session** if responses degrade — accumulated conversation history can slow generation
- **Check regional model hosting** — APAC regions (Tokyo, Seoul) now use locally hosted models, eliminating cross-geo latency. Other regions may require a workspace admin to enable cross-geo processing
- Use [Genie Workbench](https://github.com/databricks-solutions/databricks-genie-workbench) to score and diagnose your space configuration before tuning

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
| Genie Conversation API | [Docs](https://docs.databricks.com/aws/en/genie/conversation-api) |
| Agent mode in Genie spaces | [Docs](https://docs.databricks.com/aws/en/genie/agent-mode) |
| Trusted assets | [Docs](https://docs.databricks.com/aws/en/genie/trusted-assets) |
| Genie benchmarks | [Docs](https://docs.databricks.com/aws/en/genie/benchmarks) |
| Unity Catalog metric views | [Docs](https://docs.databricks.com/aws/en/metric-views/) |
| AI/BI 2026 release notes | [Docs](https://docs.databricks.com/aws/en/ai-bi/release-notes/2026) |
| Liquid Clustering | [Docs](https://docs.databricks.com/aws/en/delta/clustering) |
| Predictive Optimization | [Docs](https://docs.databricks.com/aws/en/optimizations/predictive-optimization) |
| DBSQL Query Caching | [Docs](https://docs.databricks.com/aws/en/sql/admin/query-caching) |
| DBSQL Query Profile | [Docs](https://docs.databricks.com/aws/en/sql/admin/query-profile) |
| Serverless SQL Warehouses | [Docs](https://docs.databricks.com/aws/en/compute/sql-warehouse/serverless) |

---
name: improve-genie-space
description: 'Analyze, optimize, and improve Databricks Genie Space (AI/BI Dashboard) configurations. Use when users want to: (1) evaluate a Genie Space against best practices, (2) audit space configuration quality, (3) get recommendations for improving their Genie Space, or (4) optimize Genie Space performance. Triggers on: "improve genie space", "analyze genie space", "optimize genie", "audit genie", "review genie space", "genie best practices".'
---

# Improve Genie Space

Analyze and optimize Databricks Genie Space configurations by evaluating them against best practices and providing specific, actionable recommendations.

## Prerequisites

1. **Databricks SDK** (v0.85+): If not installed or needs updating, run:
   ```bash
   pip install "databricks-sdk>=0.85"
   ```
2. **Databricks CLI profile**: Must be configured (`databricks configure`) or have environment variables set (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`).
3. **CAN EDIT permission** on the target Genie Space (required to read the serialized configuration).

## Step 1: Identify the Space ID

Ask the user for the Genie Space ID. It's a 32-character hex string (e.g., `01ef8a1b2c3d4e5f6a7b8c9d0e1f2a3b`). Users can find it in the URL when viewing their Genie Space: `https://<workspace>.databricks.com/spaces/<space_id>`.

## Step 2: Fetch Space Configuration

Run the fetch script to retrieve the serialized space JSON:

```bash
python <skill_path>/scripts/fetch_space.py <space_id>
```

This outputs JSON to stdout with keys: `title`, `description`, `space_id`, `serialized_space` (parsed dict).

### Step 2b: Save Raw Config

1. Create a `reports/<space_id>/` directory in the user's project root if it doesn't already exist.
2. Save the full JSON output from Step 2 to `reports/<space_id>/space-config.json`.
3. Inform the user the raw config has been saved.

If the script fails:
- **Missing SDK**: Prompt user to `pip install "databricks-sdk>=0.85"`
- **Auth failure**: Prompt user to run `databricks configure` or check environment variables
- **Permission denied**: User needs CAN EDIT on the space
- **Not found**: Verify the space ID

## Step 3: Determine Sub-Workflow

Based on the user's request, select the appropriate workflow:

### Option A: Analyze with Best Practices (default)
Evaluate the space configuration against the best practices checklist. **This is the default if the user doesn't specify.**

Proceed to [Analyze with Best Practices](#analyze-with-best-practices).

### Option B: Analyze with Benchmarks
Run benchmark questions against Genie and compare generated SQL to expected answers.

Proceed to [Analyze with Benchmarks](#analyze-with-benchmarks).

### Option C: Optimize Genie Space (future)
Create an updated Genie configuration with learnings from the `reports/<space_id>/config-analysis.md` and `reports/<space_id>/benchmark-analysis.md` reports. Both reports should be present in order to run this option. Prompt the user to run the other sub-workflows first. **This workflow is not yet implemented.** Inform the user it will be available in a future update, and offer to run the best practices analysis instead.

---

## Analyze with Best Practices

### Step 3a: Load Checklist

Read `references/best-practices-checklist.md` for the full evaluation criteria.

### Step 3b: Load Schema Reference (if needed)

If you need to understand specific fields in the serialized space JSON, read `references/space-schema.md`.

### Step 3c: Evaluate Each Checklist Item

For each item in the checklist, examine the fetched space configuration and determine:

- **Status**: `pass`, `fail`, `warning`, or `na`
- **Explanation**: Why this assessment was made, referencing specific data from the space
- **Fix** (for fail/warning only): A specific, actionable recommendation

Be concrete — reference actual table names, column names, instruction text, and field values from the space. Don't give generic advice.

Examples of specific fixes:
- "Add a description to column `unit_price` in table `catalog.schema.orders` — e.g., `'Unit price in USD for a single item'`"
- "Add synonyms `['revenue', 'sales amount']` to column `total_sales` in table `catalog.schema.transactions`"
- "Enable `get_example_values: true` on column `region` in table `catalog.schema.stores` — this column appears filterable"
- "Add a join spec between `catalog.schema.orders` and `catalog.schema.customers` on `orders.customer_id = customers.id`"

### Step 3d: Generate Output

Present the analysis in this format:

#### Summary
- Total items evaluated: N
- Pass: X | Fail: Y | Warning: Z | N/A: W

#### Data Sources
| Item | Status | Explanation |
|------|--------|-------------|
| ... | ... | ... |

Fixes:
1. ...

#### Instructions
| Item | Status | Explanation |
|------|--------|-------------|
| ... | ... | ... |

Fixes:
1. ...

#### Benchmarks
| Item | Status | Explanation |
|------|--------|-------------|
| ... | ... | ... |

Fixes:
1. ...

#### Config
| Item | Status | Explanation |
|------|--------|-------------|
| ... | ... | ... |

Fixes:
1. ...

#### Priority Recommendations
List the top 3-5 most impactful fixes, ordered by expected improvement to Genie accuracy.

### Step 3e: Save Report

1. Create a `reports/<space_id>/` directory in the user's project root if it doesn't already exist.
2. Save the full analysis markdown (everything from Step 3d) to `reports/<space_id>/config-analysis.md` in the project root.
3. Inform the user of the saved file path.

---

## Analyze with Benchmarks

Run the space's benchmark questions against Genie via the SDK, compare generated SQL to expected SQL, and produce a detailed accuracy report.

### Step 3a: Extract Benchmark Questions

From the fetched space configuration, read `serialized_space.benchmarks.questions`.

Parse the benchmark data format:
- `question` is an **array of strings** — join them to get the full question text
- `answer` is a **list of objects** — find the one with `format: "SQL"`
- `answer[].content` is an **array of strings** — join them to get the full expected SQL

If the space has no benchmarks (empty or missing `benchmarks.questions`), inform the user:
> "This Genie Space has no benchmark questions configured. Benchmarks are question-answer pairs that let you test Genie's SQL generation accuracy. Would you like to run the best practices analysis instead?"

### Step 3b: Present Benchmarks for Selection

Display the benchmark questions as a numbered list:

```
Found N benchmark questions:
  1. What are the top 5 customers by total spend?
  2. What is the monthly revenue trend?
  3. ...

Which benchmarks would you like to run? Enter numbers (e.g., "1,3,5"), a range (e.g., "1-5"), or "all".
```

Wait for the user's selection before proceeding.

### Step 3c: Run Selected Benchmarks

Execute each selected benchmark question **sequentially** using the runner script:

```bash
python <skill_path>/scripts/run_benchmark.py <space_id> "<question_text>"
```

After each question completes, report progress:
```
[1/5] "What are the top 5 customers by total spend?" — SQL generated
[2/5] "What is the monthly revenue trend?" — failed: <error message>
[3/5] "Show cancelled orders from last quarter" — timed out
```

**Error handling:**
- **Exit code 1** (script-level error: auth failure, space not found) → halt all remaining benchmarks and report the error to the user
- **Exit code 0 with `status: "FAILED"`, `"TIMEOUT"`, or `"ERROR"`** → record the result and continue to the next question

### Step 3d: Analyze Each Result

For each benchmark that produced SQL (`status: "COMPLETED"` with `generated_sql` present), compare the generated SQL against the expected SQL across these dimensions:

| Dimension | What to compare |
|-----------|----------------|
| Tables referenced | Same tables used (ignoring alias differences)? |
| Join conditions | Same joins with equivalent conditions? |
| WHERE clauses | Same filters applied (accounting for equivalent expressions)? |
| Aggregations | Same aggregate functions on same columns? |
| GROUP BY | Same grouping columns? |
| ORDER BY | Same ordering columns and direction? |
| LIMIT | Same row limit? |
| Column selection | Same output columns (ignoring aliases)? |
| Expressions | Same calculations and transformations? |

Assign a verdict to each question:
- **correct**: Generated SQL is semantically equivalent to expected SQL (may differ in formatting, aliases, or expression order)
- **partial**: Right general approach but with meaningful differences (e.g., missing a filter, different aggregation)
- **incorrect**: Wrong logic (wrong tables, wrong joins, wrong calculations)
- **error**: Genie could not generate SQL (failed, timed out, or returned a text response instead)

### Step 3e: Generate Benchmark Report

Produce the report in this markdown format:

```markdown
# Benchmark Analysis: <space_title>

**Space ID:** `<space_id>`
**Date:** <YYYY-MM-DD>
**Questions tested:** X of Y total benchmarks

## Summary

| Verdict | Count |
|---------|-------|
| Correct | X |
| Partial | X |
| Incorrect | X |
| Error | X |
| **Score** | **X/Y (Z%)** |

Score counts correct as 1, partial as 0.5, incorrect and error as 0.

## Detailed Results

### 1. <question text>

**Verdict:** correct | partial | incorrect | error

**Expected SQL:**
​```sql
<expected SQL>
​```

**Generated SQL:**
​```sql
<generated SQL or "N/A — <reason>">
​```

**Analysis:**
<dimensional comparison — which aspects matched, which differed, and why it matters>

---

### 2. <next question>
...

## Patterns & Recommendations

<Identify recurring issues across the benchmark results. For example:>
- If multiple questions missed a specific join, recommend adding a join spec
- If aggregations are consistently wrong, recommend adding example SQLs
- If certain tables are never used correctly, recommend improving table/column descriptions
- Link recommendations to specific Genie Space configuration changes
```

### Step 3f: Save Report

1. Create a `reports/<space_id>/` directory in the user's project root if it doesn't already exist.
2. Save the full report markdown to `reports/<space_id>/benchmark-analysis.md` in the project root.
3. Inform the user of the saved file path.

---
name: improve-genie-space
description: >
  Analyze, optimize, and improve Databricks Genie Space (AI/BI Dashboard) configurations.
  Use when users want to: (1) evaluate a Genie Space against best practices,
  (2) audit space configuration quality, (3) get recommendations for improving
  their Genie Space, or (4) optimize Genie Space performance.
  Triggers on: "improve genie space", "analyze genie space", "optimize genie",
  "audit genie", "review genie space", "genie best practices".
---

# Improve Genie Space

Analyze and optimize Databricks Genie Space configurations by evaluating them against best practices and providing specific, actionable recommendations.

## Prerequisites

1. **Databricks SDK**: If not installed, run:
   ```bash
   pip install databricks-sdk
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

If the script fails:
- **Missing SDK**: Prompt user to `pip install databricks-sdk`
- **Auth failure**: Prompt user to run `databricks configure` or check environment variables
- **Permission denied**: User needs CAN EDIT on the space
- **Not found**: Verify the space ID

## Step 3: Determine Sub-Workflow

Based on the user's request, select the appropriate workflow:

### Option A: Analyze with Best Practices (default)
Evaluate the space configuration against the best practices checklist. **This is the default if the user doesn't specify.**

Proceed to [Analyze with Best Practices](#analyze-with-best-practices).

### Option B: Optimize with Benchmarks (future)
Run benchmark questions against Genie and compare generated SQL to expected answers. **This workflow is not yet implemented.** Inform the user it will be available in a future update, and offer to run the best practices analysis instead.

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

# Serialized Space JSON Schema Reference

The `serialized_space` field is a JSON string (parsed into a dict) returned by `client.genie.get_space(space_id, include_serialized_space=True)`. It contains the full Genie Space configuration.

## Top-Level Structure

```json
{
  "version": 2,
  "config": { ... },
  "data_sources": { ... },
  "instructions": { ... },
  "benchmarks": { ... }
}
```

---

## `config`

Space-level configuration.

```json
{
  "config": {
    "sample_questions": [
      {
        "id": "q1",
        "question": "What are the top 10 products by revenue?"
      }
    ]
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `sample_questions` | array | Questions shown in the Genie UI as starting points |
| `sample_questions[].id` | string | Unique identifier for the question |
| `sample_questions[].question` | string | The question text displayed to users |

---

## `data_sources`

Tables, columns, and metric views available to Genie.

### Tables

```json
{
  "data_sources": {
    "tables": [
      {
        "name": "catalog.schema.table_name",
        "description": "Human-readable table description",
        "columns": [
          {
            "name": "column_name",
            "description": "What this column contains",
            "synonyms": ["alias1", "alias2"],
            "get_example_values": true,
            "build_value_dictionary": true
          }
        ]
      }
    ],
    "metric_views": [
      {
        "name": "catalog.schema.metric_view_name",
        "description": "What this metric view computes"
      }
    ]
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `tables` | array | Unity Catalog tables exposed to Genie |
| `tables[].name` | string | Fully qualified table name (`catalog.schema.table`) |
| `tables[].description` | string | Human-readable description of the table |
| `tables[].columns` | array | Column definitions with metadata |
| `tables[].columns[].name` | string | Column name |
| `tables[].columns[].description` | string | Contextual description beyond the column name |
| `tables[].columns[].synonyms` | array of strings | Alternative names users might use |
| `tables[].columns[].get_example_values` | boolean | Whether Genie fetches sample values for this column |
| `tables[].columns[].build_value_dictionary` | boolean | Whether Genie builds a dictionary of discrete values |
| `metric_views` | array | Pre-computed metric views |
| `metric_views[].name` | string | Fully qualified metric view name |
| `metric_views[].description` | string | What the metric view computes |

---

## `instructions`

Guidance that shapes how Genie interprets questions and generates SQL.

### Text Instructions

```json
{
  "instructions": {
    "text_instructions": [
      {
        "content": "Revenue is calculated as quantity * unit_price. Always filter out cancelled orders unless explicitly requested."
      }
    ]
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `text_instructions` | array | Free-text instructions applied globally |
| `text_instructions[].content` | string | The instruction text |

### Example SQLs

```json
{
  "instructions": {
    "example_sqls": [
      {
        "question": "What is the monthly revenue trend for the last year?",
        "sql": "SELECT DATE_TRUNC('month', order_date) AS month, SUM(quantity * unit_price) AS revenue FROM catalog.schema.orders WHERE order_date >= DATE_ADD(CURRENT_DATE(), -365) GROUP BY 1 ORDER BY 1",
        "usage_guidance": "Use this pattern for any time-series trend question involving revenue or sales amounts",
        "parameters": [
          {
            "name": "time_period",
            "description": "The time granularity (day, week, month, quarter, year)"
          }
        ]
      }
    ]
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `example_sqls` | array | Question-SQL pairs that teach Genie query patterns |
| `example_sqls[].question` | string | Natural language question |
| `example_sqls[].sql` | string | The SQL query that answers the question |
| `example_sqls[].usage_guidance` | string | When Genie should apply this pattern |
| `example_sqls[].parameters` | array | Parameterized values in the query |
| `example_sqls[].parameters[].name` | string | Parameter name |
| `example_sqls[].parameters[].description` | string | What the parameter represents |

### Join Specs

```json
{
  "instructions": {
    "join_specs": [
      {
        "left_table": "catalog.schema.orders",
        "right_table": "catalog.schema.customers",
        "join_type": "LEFT JOIN",
        "condition": "orders.customer_id = customers.id",
        "comment": "Link orders to customer details; use LEFT JOIN to include orders with unknown customers"
      }
    ]
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `join_specs` | array | Explicit join definitions for multi-table queries |
| `join_specs[].left_table` | string | Left table in the join |
| `join_specs[].right_table` | string | Right table in the join |
| `join_specs[].join_type` | string | Join type (INNER JOIN, LEFT JOIN, etc.) |
| `join_specs[].condition` | string | Join condition expression |
| `join_specs[].comment` | string | Business context for the relationship |

### SQL Functions

```json
{
  "instructions": {
    "sql_functions": [
      {
        "name": "catalog.schema.calculate_margin",
        "description": "Calculates profit margin percentage from revenue and cost"
      }
    ]
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `sql_functions` | array | Unity Catalog functions available to Genie |
| `sql_functions[].name` | string | Fully qualified function name |
| `sql_functions[].description` | string | What the function does |

### SQL Snippets

```json
{
  "instructions": {
    "sql_snippets": {
      "filters": [
        {
          "name": "last_30_days",
          "content": "WHERE order_date >= DATE_ADD(CURRENT_DATE(), -30)",
          "synonyms": ["recent", "last month", "past 30 days"]
        }
      ],
      "expressions": [
        {
          "name": "customer_segment",
          "content": "CASE WHEN lifetime_value > 10000 THEN 'Enterprise' WHEN lifetime_value > 1000 THEN 'Mid-Market' ELSE 'SMB' END",
          "synonyms": ["segment", "customer tier", "customer type"]
        }
      ],
      "measures": [
        {
          "name": "total_revenue",
          "content": "SUM(quantity * unit_price)",
          "synonyms": ["revenue", "sales", "total sales"]
        }
      ]
    }
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `sql_snippets` | object | Reusable SQL fragments organized by type |
| `sql_snippets.filters` | array | Common WHERE clause patterns |
| `sql_snippets.expressions` | array | Reusable CASE/calculation expressions |
| `sql_snippets.measures` | array | Standard aggregation definitions |
| `sql_snippets.*[].name` | string | Snippet identifier |
| `sql_snippets.*[].content` | string | The SQL fragment |
| `sql_snippets.*[].synonyms` | array of strings | Terms that trigger this snippet |

---

## `benchmarks`

Q&A pairs for validating Genie's SQL generation accuracy.

```json
{
  "benchmarks": {
    "questions": [
      {
        "question": "What are the top 5 customers by total spend?",
        "answer": {
          "format": "SQL",
          "content": "SELECT c.name, SUM(o.amount) AS total_spend FROM catalog.schema.customers c JOIN catalog.schema.orders o ON c.id = o.customer_id GROUP BY 1 ORDER BY 2 DESC LIMIT 5"
        }
      }
    ]
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `benchmarks.questions` | array | Benchmark question-answer pairs |
| `benchmarks.questions[].question` | string | The test question |
| `benchmarks.questions[].answer` | object | Expected answer |
| `benchmarks.questions[].answer.format` | string | Answer format (typically `"SQL"`) |
| `benchmarks.questions[].answer.content` | string | The expected SQL query |

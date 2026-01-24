# Genie Space Checklist (Organized by Schema)

Based on the [official Databricks Genie Best Practice doc](https://docs.databricks.com/aws/en/genie/best-practices). Last updated 12/16/2025.

This checklist is organized according to the serialized Genie Space JSON schema structure. All items are evaluated by LLM analysis.

---

## `data_sources`

### `tables`

**Table Selection:**

- [ ] Between 1 and 25 tables are configured
- [ ] Tables are focused (only necessary tables for intended questions)
- [ ] Tables are well-annotated with descriptions
- [ ] Datasets are simplified (prejoined where appropriate, unnecessary columns removed)

**Column Descriptions:**

- [ ] Columns have descriptions defined
- [ ] Descriptions provide clear, contextual information beyond what column names convey

**Column Synonyms:**

- [ ] Key columns have synonyms defined
- [ ] Synonyms include business terminology, abbreviations, and alternative phrasings users would naturally use

**Example Values / Value Dictionary:**

- [ ] Filterable columns have `get_example_values` enabled
- [ ] Columns with discrete values have `build_value_dictionary` enabled

**Column Exclusions:**

- [ ] No duplicative columns exist within the same table
- [ ] Columns not relevant to the space's purpose are hidden

### `metric_views`

- [ ] Metric views have descriptions (if any exist)
- [ ] Pre-computed metrics have comments explaining valid aggregations

---

## `instructions`

### `text_instructions`

- [ ] At least 1 text instruction exists
- [ ] Instructions are focused and minimal (not excessive)
- [ ] Instructions provide globally-applied context
- [ ] Business jargon is mapped to standard terminology where needed
- [ ] SQL examples, metrics, join logic, and filters are moved to their respective sections (not embedded in text instructions)

### `example_question_sqls`

**Example Questions:**

- [ ] At least 1 example question-SQL pair exists
- [ ] Examples cover complex, multi-part questions with intricate SQL patterns
- [ ] Examples are diverse (not redundant)
- [ ] Queries are as short as possible while remaining complete

**Parameters:**

- [ ] Parameters have descriptions defined (if parameters exist)
- [ ] Parameters are used for commonly varied values (dates, names, limits)

**Usage Guidance:**

- [ ] Complex examples have usage guidance describing applicable scenarios and trigger keywords

### `sql_functions`

- [ ] SQL functions are registered and documented in Unity Catalog (if any defined)

### `join_specs`

- [ ] Join specs are defined for multi-table relationships and complex scenarios like self-joins (if applicable)
- [ ] Foreign key references are defined in Unity Catalog when possible
- [ ] Join specs have comments explaining the relationship

### `sql_snippets`

#### `filters`

- [ ] Common time period filters exist
- [ ] Business-specific filters are defined

#### `expressions`

- [ ] Reusable expressions are defined for common categorizations
- [ ] Expressions include synonyms for user terminology

#### `measures`

- [ ] More than 1 measure is defined (consider adding more if only 1 exists)
- [ ] Measures cover standard business concepts used across queries

---

## `benchmarks`

### `questions`

- [ ] At least 10 diverse benchmark Q&A pairs exist, covering different use cases and topics

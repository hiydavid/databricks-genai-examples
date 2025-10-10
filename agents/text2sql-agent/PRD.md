# (WIP) Databricks Text2SQL Agent Architecture Plan: MLflow and Unity Catalog Implementation

## Executive Summary

Building a Text2SQL agent with Databricks requires integrating multiple components: MLflow's Agent Framework for orchestration, Unity Catalog for unified governance across data, models, and AI assets, Managed MCPs for standardized tool integration, and UC functions as agent-callable tools. This architecture enables secure, governed, and scalable natural language to SQL conversion for complex multi-table queries.

## Architecture Components

### 1. MLflow Response Agent Framework

The Agent Framework in Databricks provides a unified way to author, register, and deploy agents. Key capabilities include:

- **Agent Authoring**: Support for multiple agent types including function-calling agents, ReAct agents, and chain-of-thought agents
- **Tool Integration**: Agents can use Python functions, Unity Catalog functions, or Langchain tools as callable tools
- **Model Support**: Compatible with foundation models from OpenAI, Anthropic, DBRX, and other providers
- **Logging & Tracking**: Full MLflow integration for tracking agent runs, parameters, and outputs

### 2. Unity Catalog Integration

Unity Catalog serves as the central governance layer for data, AI, and analytics assets. For Text2SQL agents, Unity Catalog provides:

**Metadata Access**:

- Schema information (tables, columns, data types)
- Table relationships and foreign keys
- Column descriptions and business metadata
- Data lineage tracking

**Security & Governance**:

- Fine-grained access control at catalog, schema, table, and column levels
- Attribute-based access control for dynamic permissions
- Audit logging for all data access
- Data masking and filtering capabilities

**Best Practice**: Query Unity Catalog's information schema to dynamically retrieve table metadata and inject it into the agent's context

### 3. Managed MCP Servers

Databricks Managed MCP (Model Context Protocol) servers provide pre-configured, enterprise-ready integrations for common agent patterns.

**Available Managed MCPs**:

- Unity Catalog Function MCP
- Vector Search MCP
- Genie MCP
- (Coming soon!) Databricks SQL MCP

**Key Benefits**:

- Managed MCPs are hosted and maintained by Databricks, eliminating infrastructure overhead
- Built-in authentication using personal access tokens or OAuth
- Automatic scaling and security updates

### 4. Unity Catalog Functions as Agent Tools

Unity Catalog functions can be registered and used as callable tools within the agent framework. This is powerful for Text2SQL as you can encapsulate SQL execution, validation, and optimization logic.

**UC Function Types for Text2SQL**:

Text2SQL agents benefit from a comprehensive set of UC functions that enable schema discovery, query validation, and intelligent SQL generation. Functions are categorized by implementation priority.

#### Core Functions (Required)

These functions are essential for basic Text2SQL functionality:

**1. SQL Execution Function**:

```sql
CREATE FUNCTION catalog.schema.execute_sql(
    query STRING COMMENT 'The SQL query to execute'
)
RETURNS TABLE
COMMENT 'Executes a SQL query and returns results'
LANGUAGE SQL
RETURN SELECT * FROM IDENTIFIER(query);
```

**2. Query Validation Function**:

```sql
CREATE FUNCTION catalog.schema.validate_query(
    query STRING COMMENT 'SQL query to validate'
)
RETURNS STRUCT<is_valid: BOOLEAN, error_message: STRING>
COMMENT 'Validates SQL syntax and permissions'
LANGUAGE SQL
```

**3. Schema Discovery Function**:

```sql
CREATE FUNCTION catalog.schema.get_table_info(
    table_name STRING COMMENT 'Fully qualified table name'
)
RETURNS TABLE
COMMENT 'Returns schema information for a table'
LANGUAGE SQL
RETURN SELECT * FROM information_schema.columns
WHERE concat(table_catalog, '.', table_schema, '.', table_name) = table_name;
```

**4. List Available Tables Function**:

```sql
CREATE OR REPLACE FUNCTION main.text2sql.list_available_tables()
RETURNS TABLE
COMMENT 'Returns all tables the agent is allowed to query with descriptions'
LANGUAGE SQL
RETURN
    SELECT
        concat(table_catalog, '.', table_schema, '.', table_name) as full_table_name,
        table_name,
        table_comment as description,
        table_type
    FROM information_schema.tables
    WHERE concat(table_catalog, '.', table_schema, '.', table_name) IN (
        'main.sales.orders',
        'main.sales.customers',
        'main.sales.products',
        'main.sales.order_items'
    );
```

**Why Essential**: Enables dynamic table discovery without hardcoding table lists in prompts. Agent can list available data sources on demand.

**5. Get Sample Data Function**:

```sql
CREATE OR REPLACE FUNCTION main.text2sql.get_sample_data(
    table_name STRING COMMENT 'Fully qualified table name',
    num_rows INT DEFAULT 3 COMMENT 'Number of sample rows'
)
RETURNS TABLE
COMMENT 'Returns sample rows from a table to help understand data patterns'
LANGUAGE SQL
RETURN
    SELECT *
    FROM IDENTIFIER(table_name)
    LIMIT num_rows;
```

**Why Essential**: LLMs need concrete examples to understand data formats, value patterns, and data types. Critical for generating accurate WHERE clauses and understanding date formats, enums, etc.

**6. Get Table Relationships Function**:

```sql
CREATE OR REPLACE FUNCTION main.text2sql.get_table_relationships(
    table_name STRING COMMENT 'Fully qualified table name'
)
RETURNS TABLE
COMMENT 'Returns foreign key relationships for a table'
LANGUAGE SQL
RETURN
    SELECT
        fk.table_catalog,
        fk.table_schema,
        fk.table_name,
        fk.column_name as foreign_key_column,
        fk.referenced_table_catalog,
        fk.referenced_table_schema,
        fk.referenced_table_name,
        fk.referenced_column_name as referenced_column
    FROM information_schema.constraint_column_usage fk
    WHERE concat(fk.table_catalog, '.', fk.table_schema, '.', fk.table_name) = table_name
    OR concat(fk.referenced_table_catalog, '.', fk.referenced_table_schema, '.', fk.referenced_table_name) = table_name;
```

**Why Essential**: Required for multi-table queries. Agent learns correct JOIN syntax and relationships between tables.

#### Enhanced Functions (High Value)

Implement these functions to significantly improve agent accuracy and user experience:

**7. Search Tables by Keyword Function**:

```sql
CREATE OR REPLACE FUNCTION main.text2sql.search_tables(
    keyword STRING COMMENT 'Keyword to search in table and column names/descriptions'
)
RETURNS TABLE
COMMENT 'Searches for tables and columns matching keyword'
LANGUAGE SQL
RETURN
    SELECT DISTINCT
        concat(table_catalog, '.', table_schema, '.', table_name) as full_table_name,
        table_comment,
        column_name,
        column_comment,
        data_type
    FROM information_schema.columns
    WHERE (
        LOWER(table_name) LIKE LOWER(concat('%', keyword, '%'))
        OR LOWER(column_name) LIKE LOWER(concat('%', keyword, '%'))
        OR LOWER(table_comment) LIKE LOWER(concat('%', keyword, '%'))
        OR LOWER(column_comment) LIKE LOWER(concat('%', keyword, '%'))
    )
    AND concat(table_catalog, '.', table_schema, '.', table_name) IN (
        'main.sales.orders',
        'main.sales.customers',
        'main.sales.products',
        'main.sales.order_items'
    );
```

**Why High Value**: Enables semantic schema discovery. Agent can find relevant tables when user asks about "revenue", "customers", or domain-specific terms.

**8. Check Query Complexity Function**:

```python
CREATE OR REPLACE FUNCTION main.text2sql.check_query_complexity(
    query STRING COMMENT 'SQL query to analyze'
)
RETURNS STRUCT<
    complexity_score: INT,
    num_joins: INT,
    num_aggregations: INT,
    num_subqueries: INT,
    estimated_cost: STRING
>
COMMENT 'Analyzes query complexity and provides optimization suggestions'
LANGUAGE PYTHON
AS $$
    import re

    query_upper = query.upper()

    # Count various complexity indicators
    num_joins = len(re.findall(r'\bJOIN\b', query_upper))
    num_aggregations = len(re.findall(r'\b(COUNT|SUM|AVG|MIN|MAX|GROUP BY)\b', query_upper))
    num_subqueries = query_upper.count('SELECT') - 1

    # Simple complexity score
    complexity_score = num_joins * 2 + num_aggregations + num_subqueries * 3

    if complexity_score < 5:
        estimated_cost = 'low'
    elif complexity_score < 15:
        estimated_cost = 'medium'
    else:
        estimated_cost = 'high'

    return {
        'complexity_score': complexity_score,
        'num_joins': num_joins,
        'num_aggregations': num_aggregations,
        'num_subqueries': num_subqueries,
        'estimated_cost': estimated_cost
    }
$$
```

**Why High Value**: Prevents expensive runaway queries. Agent can warn users or simplify complex queries before execution.

**9. Get Column Statistics Function**:

```python
CREATE OR REPLACE FUNCTION main.text2sql.get_column_stats(
    table_name STRING COMMENT 'Fully qualified table name',
    column_name STRING COMMENT 'Column name to analyze'
)
RETURNS STRUCT<
    min_value: STRING,
    max_value: STRING,
    distinct_count: BIGINT,
    null_count: BIGINT,
    common_values: ARRAY<STRING>
>
COMMENT 'Returns statistics for a column to help with query generation'
LANGUAGE PYTHON
AS $$
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, countDistinct, isnan, when, count, desc

    spark = SparkSession.getActiveSession()
    df = spark.table(table_name)

    # Get basic stats
    stats = df.select(
        col(column_name).cast("string").alias("val")
    ).agg(
        count(when(col("val").isNull(), 1)).alias("null_count"),
        countDistinct("val").alias("distinct_count"),
        min("val").alias("min_value"),
        max("val").alias("max_value")
    ).collect()[0]

    # Get top 5 most common values
    common = df.groupBy(column_name).count().orderBy(desc("count")).limit(5)
    common_values = [str(row[column_name]) for row in common.collect()]

    return {
        'min_value': stats['min_value'],
        'max_value': stats['max_value'],
        'distinct_count': stats['distinct_count'],
        'null_count': stats['null_count'],
        'common_values': common_values
    }
$$
```

**Why High Value**: Provides data distribution insights. Helps LLM generate accurate WHERE clauses with valid date ranges, understand categorical values, and detect potential data quality issues.

#### Advanced Functions (Future Enhancements)

Implement these functions for enterprise-grade deployments and specialized use cases:

**10. Get Similar Queries Function** (Few-Shot Learning):

```sql
CREATE OR REPLACE FUNCTION main.text2sql.get_similar_queries(
    user_query STRING COMMENT 'Natural language query'
)
RETURNS TABLE
COMMENT 'Returns similar successful queries for few-shot learning'
LANGUAGE SQL
RETURN
    SELECT
        natural_language,
        sql_query,
        tables_used,
        success_rate
    FROM main.agents.query_patterns
    WHERE success_rate > 0.8
    ORDER BY created_at DESC
    LIMIT 3;
```

**Why Advanced**: Enables continuous learning from successful patterns. Requires maintaining a query history table and embedding infrastructure.

**11. Get Business Glossary Function** (Domain Mapping):

```sql
CREATE OR REPLACE FUNCTION main.text2sql.get_business_terms()
RETURNS TABLE
COMMENT 'Returns business terminology mappings for domain-specific queries'
LANGUAGE SQL
RETURN
    SELECT
        business_term,
        technical_column,
        table_name,
        description,
        example_values
    FROM main.text2sql.business_glossary;

-- Supporting table structure
CREATE TABLE IF NOT EXISTS main.text2sql.business_glossary (
    business_term STRING COMMENT 'Business terminology used by users',
    technical_column STRING COMMENT 'Actual column name in database',
    table_name STRING COMMENT 'Fully qualified table name',
    description STRING COMMENT 'Explanation of mapping',
    example_values ARRAY<STRING> COMMENT 'Sample values',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Example data
INSERT INTO main.text2sql.business_glossary VALUES
    ('revenue', 'total_amount', 'main.sales.orders', 'Total sales revenue', ['1000.50', '2500.00']),
    ('customer name', 'customer_full_name', 'main.sales.customers', 'Full customer name', ['John Doe', 'Jane Smith']),
    ('product title', 'product_name', 'main.sales.products', 'Product display name', ['Widget A', 'Gadget B']);
```

**Why Advanced**: Maps business language to technical schema. Critical for enterprise deployments where business users don't know technical column names.

**12. Explain Query Function** (Performance Analysis):

```sql
CREATE OR REPLACE FUNCTION main.text2sql.explain_query(
    query STRING COMMENT 'SQL query to explain'
)
RETURNS STRING
COMMENT 'Returns query execution plan to estimate performance'
LANGUAGE SQL
RETURN
    (SELECT explain_string FROM (EXPLAIN EXTENDED IDENTIFIER(query)));
```

**Why Advanced**: Provides execution plan analysis. Most useful for query optimization but adds complexity to agent reasoning.

**13. Format Query Results Function** (Output Formatting):

```python
CREATE OR REPLACE FUNCTION main.text2sql.format_results(
    query STRING COMMENT 'SQL query to execute',
    format STRING DEFAULT 'table' COMMENT 'Output format: table, json, csv, markdown'
)
RETURNS STRING
COMMENT 'Executes query and formats results in requested format'
LANGUAGE PYTHON
AS $$
    import json
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    results = spark.sql(query)

    if format == 'json':
        return json.dumps([row.asDict() for row in results.collect()])
    elif format == 'csv':
        return results.toPandas().to_csv(index=False)
    elif format == 'markdown':
        return results.toPandas().to_markdown()
    else:
        return str(results.toPandas())
$$
```

**Why Advanced**: Flexible output formatting for different consumption patterns (APIs, reports, dashboards). Consider implementing at application layer instead.

#### Security Best Practices for UC Functions

To ensure READ-ONLY access and table restrictions, UC functions should be designed with security constraints. See the [Security & Governance](#security--governance) section for implementation details on:

- Service principal configuration with SELECT-only permissions
- Table allowlisting in schema discovery functions
- SQL validation to block write operations

### 5. Databricks SQL Integration

Databricks SQL Warehouses provide the compute layer for query execution. Key considerations:

**Warehouse Selection**:

- Use Serverless SQL for variable workloads and fast startup
- Use Pro or Classic warehouses for predictable, sustained workloads
- Consider separate warehouses for agent validation vs. user query execution

**Query Safety**:

- Implement query timeouts to prevent runaway queries
- Use statement execution APIs with result limits
- Enable query result caching for repeated queries

**Performance Optimization**:

- Leverage Photon acceleration for analytical queries
- Use query history to identify optimization opportunities
- Implement query result pagination for large datasets

## Text2SQL Agent Architecture Pattern

### Recommended Multi-Stage Architecture

Enterprise Text2SQL systems should use a multi-stage approach:

Stage 1: Query Understanding & Schema Selection

- Parse natural language query
- Extract entities and intent
- Use Unity Catalog metadata to identify relevant tables
- Apply user permissions to filter accessible tables

Stage 2: SQL Generation

- Generate candidate SQL queries using LLM
- Inject relevant table schemas and sample data into context
- Use few-shot examples from similar queries
- Generate multiple SQL candidates if needed

Stage 3: Validation & Refinement

- Validate SQL syntax using UC function
- Check query permissions and security
- Estimate query cost/complexity
- Apply query optimization hints

Stage 4: Execution & Results

- Execute query via Databricks SQL
- Handle errors and retry with refinements
- Format results for user consumption
- Log execution metrics to MLflow

Stage 5: Feedback Loop

- Collect user feedback on results
- Store successful query patterns
- Fine-tune prompt examples
- Update schema descriptions

## Best Practices & Patterns

### 1. Context Management for Multi-Table Queries

For complex schemas, implement intelligent context selection to avoid token limit issues:

```python
def select_relevant_tables(user_query, catalog_metadata):
    """
    Use semantic similarity to identify relevant tables
    Returns: List of table metadata to include in context
    """
    # Embed user query
    query_embedding = embed_text(user_query)
    
    # Compute similarity with table descriptions
    table_scores = []
    for table in catalog_metadata:
        table_desc = f"{table.name} {table.comment}"
        similarity = cosine_similarity(
            query_embedding, 
            embed_text(table_desc)
        )
        table_scores.append((table, similarity))
    
    # Return top-k relevant tables
    return sorted(table_scores, key=lambda x: x[1], reverse=True)[:5]
```

### 2. Query Safety & Validation

Implement multi-layer validation:

```python
def validate_query_safety(sql_query, user_context):
    """Multi-layer query validation"""
    checks = {
        "syntax": validate_syntax(sql_query),
        "permissions": check_permissions(sql_query, user_context),
        "cost_estimate": estimate_query_cost(sql_query),
        "timeout_risk": estimate_execution_time(sql_query)
    }
    
    if checks["cost_estimate"] > COST_THRESHOLD:
        return {"approved": False, "reason": "Query too expensive"}
    
    if checks["timeout_risk"] > TIME_THRESHOLD:
        return {"approved": False, "reason": "Query may timeout"}
        
    return {"approved": True, "checks": checks}
```

### 3. Error Handling & Self-Correction

Implement intelligent error handling with self-correction loops:

```python
def execute_with_retry(sql_query, max_retries=3):
    """Execute SQL with intelligent retry logic"""
    for attempt in range(max_retries):
        try:
            result = execute_sql(sql_query)
            return result
        except SQLSyntaxError as e:
            # Ask LLM to fix the syntax error
            fixed_query = agent.fix_sql_error(sql_query, str(e))
            sql_query = fixed_query
        except PermissionError as e:
            # Cannot retry - return error to user
            return {"error": "Insufficient permissions", "details": str(e)}
        except TimeoutError as e:
            # Suggest query optimization
            optimized = agent.optimize_query(sql_query)
            sql_query = optimized
    
    return {"error": "Max retries exceeded"}
```

### 4. Few-Shot Learning with Query History

Maintain a repository of successful query patterns:

```python
# Store successful patterns in Unity Catalog
CREATE TABLE catalog.agents.query_patterns (
    natural_language STRING,
    sql_query STRING,
    tables_used ARRAY<STRING>,
    success_rate DOUBLE,
    avg_execution_time DOUBLE,
    created_at TIMESTAMP
)

# Retrieve similar examples for few-shot prompting
def get_similar_examples(user_query, k=3):
    """Retrieve k most similar successful query examples"""
    query_embedding = embed_text(user_query)
    
    similar_queries = vector_search(
        index="query_patterns_index",
        query_vector=query_embedding,
        k=k
    )
    
    return similar_queries
```

### 5. Incremental Schema Context

For complex queries, build context incrementally:

```python
class IncrementalSchemaContext:
    def __init__(self, initial_tables):
        self.context = {table: get_schema(table) for table in initial_tables}
        self.tokens_used = count_tokens(str(self.context))
    
    def add_table(self, table_name):
        """Add table only if it fits in context window"""
        table_schema = get_schema(table_name)
        new_tokens = count_tokens(str(table_schema))
        
        if self.tokens_used + new_tokens < MAX_CONTEXT_TOKENS:
            self.context[table_name] = table_schema
            self.tokens_used += new_tokens
            return True
        return False
    
    def get_prompt(self):
        """Generate optimized prompt from current context"""
        return format_schema_context(self.context)
```

## Security & Governance

### Access Control Strategy

#### 1. Service Principal Configuration for READ-ONLY Access

**Recommended Architecture**: Deploy the agent as a service principal with restricted Unity Catalog permissions to enforce read-only access at the database level, providing defense-in-depth security.

**Service Principal Setup**:

```python
# Create service principal via Databricks API or UI
# Example: service-principal://text2sql-agent

# Grant SELECT-only permissions on specific tables
spark.sql("""
    GRANT SELECT ON TABLE main.sales.orders TO `text2sql-agent`
""")

spark.sql("""
    GRANT SELECT ON TABLE main.sales.customers TO `text2sql-agent`
""")

spark.sql("""
    GRANT SELECT ON TABLE main.sales.products TO `text2sql-agent`
""")

# Explicitly grant USAGE on catalog and schema
spark.sql("""
    GRANT USAGE ON CATALOG main TO `text2sql-agent`
""")

spark.sql("""
    GRANT USAGE ON SCHEMA main.sales TO `text2sql-agent`
""")

# DO NOT grant: INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, TRUNCATE, MERGE
```

**Benefits of Service Principal Approach**:

- **Database-level enforcement**: Even if the LLM generates write operations or is prompt-injected, Unity Catalog will reject unauthorized queries
- **Audit trail**: All queries are logged with service principal identity
- **Least privilege**: Agent can only access explicitly granted tables
- **Separation of concerns**: Agent permissions are isolated from user permissions

#### 2. Table Allowlisting in Schema Discovery

Restrict which tables the agent can discover and query by filtering in the schema discovery function:

```sql
CREATE OR REPLACE FUNCTION main.text2sql.get_table_info(
    table_name STRING COMMENT 'Fully qualified table name'
)
RETURNS TABLE
COMMENT 'Returns schema information only for allowed tables'
LANGUAGE SQL
RETURN
    SELECT *
    FROM information_schema.columns
    WHERE concat(table_catalog, '.', table_schema, '.', table_name) = table_name
    AND table_name IN (
        'main.sales.orders',
        'main.sales.customers',
        'main.sales.products',
        'main.sales.order_items'
    );
```

**Alternative: Dynamic Allowlist**:

```sql
-- Store allowed tables in a UC table
CREATE TABLE IF NOT EXISTS main.text2sql.allowed_tables (
    table_name STRING,
    description STRING,
    enabled BOOLEAN DEFAULT TRUE
);

-- Reference in discovery function
CREATE OR REPLACE FUNCTION main.text2sql.get_table_info(table_name STRING)
RETURNS TABLE
LANGUAGE SQL
RETURN
    SELECT c.*
    FROM information_schema.columns c
    INNER JOIN main.text2sql.allowed_tables a
        ON concat(c.table_catalog, '.', c.table_schema, '.', c.table_name) = a.table_name
    WHERE a.enabled = TRUE
    AND concat(c.table_catalog, '.', c.table_schema, '.', c.table_name) = table_name;
```

#### 3. SQL Validation to Block Write Operations

Implement multi-layer validation in the query validation and execution functions:

```python
# Update the validate_query UC function to check for write operations
CREATE OR REPLACE FUNCTION main.text2sql.validate_query(
    query STRING COMMENT 'SQL query to validate'
)
RETURNS STRUCT<is_valid: BOOLEAN, error_message: STRING>
COMMENT 'Validates SQL syntax and ensures read-only operations'
LANGUAGE PYTHON
AS $$
    import re

    # Normalize query for checking
    query_upper = query.upper()

    # Block write operations
    write_keywords = [
        'INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE',
        'ALTER', 'TRUNCATE', 'MERGE', 'GRANT', 'REVOKE',
        'REPLACE', 'RENAME', 'COPY', 'LOAD', 'UNLOAD'
    ]

    for keyword in write_keywords:
        # Use word boundary to avoid false positives
        if re.search(rf'\b{keyword}\b', query_upper):
            return {
                'is_valid': False,
                'error_message': f'Operation {keyword} is not allowed. Only SELECT queries are permitted.'
            }

    # Ensure query starts with SELECT
    query_stripped = query.strip()
    if not re.match(r'^SELECT\b', query_stripped, re.IGNORECASE):
        return {
            'is_valid': False,
            'error_message': 'Only SELECT queries are allowed.'
        }

    # Additional validation: check for SQL injection patterns
    injection_patterns = [
        r';.*?DROP',
        r';.*?DELETE',
        r'--.*?(DROP|DELETE|UPDATE)',
        r'/\*.*?(DROP|DELETE|UPDATE).*?\*/'
    ]

    for pattern in injection_patterns:
        if re.search(pattern, query_upper):
            return {
                'is_valid': False,
                'error_message': 'Potentially malicious SQL pattern detected.'
            }

    return {'is_valid': True, 'error_message': None}
$$
```

**Enhanced Execute Function with Validation**:

```python
CREATE OR REPLACE FUNCTION main.text2sql.execute_query(
    sql_query STRING COMMENT 'SQL query to execute'
)
RETURNS TABLE
COMMENT 'Executes SQL query with safety validation and limits'
LANGUAGE PYTHON
AS $$
    import re
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()

    # Validate query is read-only
    validation = spark.sql(f"""
        SELECT main.text2sql.validate_query('{sql_query.replace("'", "''")}') as result
    """).collect()[0]['result']

    if not validation['is_valid']:
        raise ValueError(f"Query validation failed: {validation['error_message']}")

    # Add row limit for safety
    safe_query = f"SELECT * FROM ({sql_query}) LIMIT 1000"

    try:
        result = spark.sql(safe_query)
        return result
    except Exception as e:
        raise ValueError(f"Query execution failed: {str(e)}")
$$
```

#### 4. System Prompt Constraints

Add explicit instructions in the agent's system prompt to reinforce read-only behavior:

```python
system_prompt="""
You are a SQL expert for Databricks. Convert natural language to SQL queries.

CRITICAL CONSTRAINTS:
- You can ONLY query these tables: main.sales.orders, main.sales.customers, main.sales.products, main.sales.order_items
- You can ONLY generate SELECT queries. Never use INSERT, UPDATE, DELETE, DROP, or any write operations
- All queries must be read-only

Always follow these steps:
1. Understand the user's question
2. Identify relevant tables using get_table_schema
3. Generate a SELECT query only
4. Validate using validate_sql
5. Execute using execute_query
6. Present results clearly

If a user asks you to modify, delete, or create data, politely explain that you can only query existing data.

Use Delta Lake syntax. All tables are in Unity Catalog format: catalog.schema.table
"""
```

#### 5. User Context Propagation

- Pass user identity through agent calls
- Unity Catalog enforces row and column-level security
- Agent respects existing data permissions

#### 6. Query Logging

```python
# Log all generated queries for audit
def log_query_generation(user, query, sql, result):
    mlflow.log_param("user", user)
    mlflow.log_param("natural_query", query)
    mlflow.log_param("generated_sql", sql)
    mlflow.log_metric("result_rows", len(result))
    mlflow.log_artifact("query_plan.json")
```

#### 7. Data Masking

Apply Unity Catalog masking functions automatically:

- PII masking for sensitive columns
- Dynamic views based on user attributes

### Defense-in-Depth Summary

The recommended security architecture implements multiple layers:

1. **Layer 1 (Strongest)**: Service principal with SELECT-only UC grants - database enforces read-only
2. **Layer 2**: Table allowlisting in schema discovery - agent only sees approved tables
3. **Layer 3**: SQL validation in UC functions - programmatic blocking of write operations
4. **Layer 4**: System prompt instructions - LLM guidance to avoid generating write queries
5. **Layer 5**: Query logging and monitoring - detect and respond to anomalies

This approach ensures that even if prompt injection or LLM misbehavior occurs, the database itself will reject unauthorized operations.

### Additional Security Considerations

#### Prompt Injection Protection

While the service principal and SQL validation layers provide strong protection, additional input sanitization can be implemented:

```python
def sanitize_user_input(query):
    """Detect and prevent prompt injection (defense-in-depth)"""
    dangerous_patterns = [
        r"ignore previous instructions",
        r"system prompt",
        r"DROP TABLE",
        r"DELETE FROM",
    ]

    for pattern in dangerous_patterns:
        if re.search(pattern, query, re.IGNORECASE):
            raise SecurityError("Potentially malicious input detected")

    return query
```

**Note**: With service principal enforcement, even successful prompt injection cannot execute write operations, as UC will reject unauthorized queries.

## Performance Optimization

### 1. Caching Strategy

Implement multi-layer caching:

- **Schema Cache**: Cache Unity Catalog metadata (refresh every 1-6 hours)
- **Query Results Cache**: Cache frequent query results
- **Embedding Cache**: Cache embeddings for table descriptions

```python
@cache(ttl=3600)  # 1 hour TTL
def get_catalog_metadata(catalog, schema):
    return spark.sql(f"""
        SELECT * FROM {catalog}.information_schema.tables
        WHERE table_schema = '{schema}'
    """).toPandas()
```

### 2. Query Optimization Hints

Inject optimization hints into generated SQL:

```python
def add_optimization_hints(sql_query, table_sizes):
    """Add Databricks optimization hints"""
    large_tables = [t for t, size in table_sizes.items() if size > LARGE_TABLE_THRESHOLD]
    
    # Add broadcast hint for small tables
    for table in large_tables:
        sql_query = sql_query.replace(
            f"JOIN {table}",
            f"JOIN /*+ BROADCAST */ {table}"
        )
    
    return sql_query
```

### 3. Async Query Execution

Use asynchronous patterns for better responsiveness:

```python
import asyncio

async def execute_query_async(sql):
    """Execute query asynchronously"""
    statement = await sql_warehouse.execute_async(sql)
    
    # Poll for results without blocking
    while not statement.is_complete():
        await asyncio.sleep(1)
    
    return statement.get_results()
```

## Monitoring & Observability

### MLflow Tracking Integration

Track comprehensive metrics:

```python
with mlflow.start_run():
    # Input metrics
    mlflow.log_param("user_query", user_input)
    mlflow.log_param("selected_tables", table_list)
    
    # Generation metrics
    mlflow.log_metric("sql_generation_time_ms", gen_time)
    mlflow.log_metric("validation_time_ms", val_time)
    
    # Execution metrics
    mlflow.log_metric("query_execution_time_ms", exec_time)
    mlflow.log_metric("rows_returned", row_count)
    mlflow.log_metric("bytes_scanned", bytes_scanned)
    
    # Quality metrics
    mlflow.log_metric("user_satisfaction", feedback_score)
    mlflow.log_metric("retry_count", retries)
```

### Dashboard Metrics

Monitor these KPIs:

- Query success rate
- Average response time
- Token usage per query
- Most queried tables
- Error patterns and frequencies
- User satisfaction scores

## Deployment Strategy

### 1. Development → Staging → Production

```python
# Development: Notebook-based testing
agent_dev = create_text2sql_agent(env="dev")

# Staging: MLflow Registry
mlflow.register_model(
    model_uri=f"runs:/{run_id}/agent",
    name="text2sql_agent",
    tags={"stage": "staging"}
)

# Production: Model Serving
client.create_endpoint(
    name="text2sql-prod",
    config={
        "served_models": [{
            "model_name": "text2sql_agent",
            "model_version": "3",
            "workload_size": "Small",
            "scale_to_zero_enabled": True
        }]
    }
)
```

### 2. A/B Testing

Deploy multiple agent versions for comparison:

```python
# Deploy two versions for A/B testing
config = {
    "served_models": [
        {
            "name": "version_gpt4",
            "model_name": "text2sql_agent_gpt4",
            "traffic_percentage": 50
        },
        {
            "name": "version_dbrx",
            "model_name": "text2sql_agent_dbrx",
            "traffic_percentage": 50
        }
    ],
    "traffic_config": {"routes": [...]}
}
```

## Example End-to-End Implementation

Here's a complete working example:

```python
from databricks import agents
from databricks.agents import UCFunctionTool
import mlflow

# Step 1: Create UC functions
spark.sql("""
CREATE OR REPLACE FUNCTION main.text2sql.execute_query(
    sql_query STRING COMMENT 'SQL query to execute'
)
RETURNS TABLE
COMMENT 'Executes SQL query with timeout and row limit'
LANGUAGE PYTHON
AS $$
    import time
    from pyspark.sql import SparkSession
    
    spark = SparkSession.getActiveSession()
    
    # Add safety limits
    safe_query = f"{sql_query} LIMIT 1000"
    
    try:
        result = spark.sql(safe_query)
        return result
    except Exception as e:
        raise ValueError(f"Query execution failed: {str(e)}")
$$
""")

# Step 2: Define the agent
text2sql_agent = agents.create_agent(
    model="databricks-dbrx-instruct",
    tools=[
        UCFunctionTool("main.text2sql.get_table_schema"),
        UCFunctionTool("main.text2sql.execute_query"),
        UCFunctionTool("main.text2sql.validate_sql"),
    ],
    system_prompt="""
    You are a SQL expert for Databricks. Convert natural language to SQL queries.

    CRITICAL CONSTRAINTS:
    - You can ONLY query these tables: main.sales.orders, main.sales.customers, main.sales.products
    - You can ONLY generate SELECT queries. Never use INSERT, UPDATE, DELETE, DROP, or any write operations
    - All queries must be read-only

    Always follow these steps:
    1. Understand the user's question
    2. Identify relevant tables using get_table_schema
    3. Generate a SELECT query only
    4. Validate using validate_sql
    5. Execute using execute_query
    6. Present results clearly

    If a user asks you to modify, delete, or create data, politely explain that you can only query existing data.

    Use Delta Lake syntax. All tables are in Unity Catalog format: catalog.schema.table
    """,
    agent_type="function_calling",
    max_iterations=8
)

# Step 3: Log to MLflow
with mlflow.start_run() as run:
    agent_info = mlflow.log_agent(
        text2sql_agent,
        "text2sql_agent",
        input_example={
            "messages": [{
                "role": "user",
                "content": "What were the top 5 products by revenue last quarter?"
            }]
        }
    )
    
# Step 4: Test locally
response = text2sql_agent.predict(
    "Show me customers who made purchases over $1000 in the last month"
)

print(response)

# Step 5: Deploy to Model Serving
deployment = agents.deploy(
    model_uri=agent_info.model_uri,
    endpoint_name="text2sql-production",
    workload_size="Small",
    scale_to_zero=True
)

print(f"Agent deployed at: {deployment.endpoint_url}")
```

## Testing Strategy

### Unit Tests for UC Functions

```python
def test_query_validation():
    """Test SQL validation function"""
    valid_sql = "SELECT * FROM main.sales.orders WHERE date > '2024-01-01'"
    result = validate_query(valid_sql)
    assert result["is_valid"] == True
    
    invalid_sql = "SELCT * FORM orders"
    result = validate_query(invalid_sql)
    assert result["is_valid"] == False
    assert "syntax error" in result["error_message"].lower()
```

### Integration Tests

```python
def test_end_to_end_query():
    """Test complete query flow"""
    query = "What's the average order value by region?"
    
    response = text2sql_agent.predict(query)
    
    assert response["status"] == "success"
    assert "sql_query" in response
    assert len(response["results"]) > 0
    
    # Verify SQL is valid
    generated_sql = response["sql_query"]
    assert "GROUP BY" in generated_sql
    assert "AVG" in generated_sql
```

## Conclusion

This architecture provides a production-ready foundation for building Text2SQL agents with Databricks. Key advantages include:

- **Unified Governance**: Unity Catalog ensures consistent security and access control
- **Scalability**: Leverages Databricks SQL and serverless compute
- **Flexibility**: UC functions enable custom business logic as tools
- **Observability**: MLflow provides comprehensive tracking and monitoring
- **Enterprise-Ready**: Managed MCPs reduce operational overhead

The modular design allows you to start simple and progressively add complexity as your use case evolves from basic queries to sophisticated multi-table analysis with domain-specific business logic.

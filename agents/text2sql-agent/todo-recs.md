# Security Recommendations: Text-to-SQL Agent

**Document Version**: 1.0
**Date**: 2025-10-17
**Status**: Recommendations Pending Implementation

---

## Executive Summary

The Databricks Text-to-SQL agent has **CRITICAL SQL INJECTION VULNERABILITIES** despite implementing a defense-in-depth model. The current implementation can be bypassed through multiple attack vectors that allow:
- Execution of write operations (INSERT, UPDATE, DELETE, DROP)
- Access to tables outside the allowlist
- Execution of arbitrary SQL through insufficient input sanitization

**The ONLY reliable security control is Layer 1 (service principal permissions)**. All other layers have exploitable weaknesses that render them ineffective.

---

## Critical Findings Summary

### 🔴 CRITICAL Vulnerabilities (Immediate Exploitation Possible)

1. **SQL Comment Injection (Vuln 3.1)** - Can bypass all keyword blocking
   - Attack: `SELECT * FROM table; DROP/**/TABLE data;`
   - Impact: Write operations execute despite validation
   - Root Cause: Regex `\bDROP\b` doesn't match `D/**/ROP`

2. **Table Allowlist Not Enforced in execute_query() (Vuln 2.1)** - Direct unauthorized table access
   - Attack: `SELECT * FROM prod.hr.salaries WHERE salary > 150000`
   - Impact: Any table accessible to service principal can be queried
   - Root Cause: `execute_query()` doesn't check table names against allowlist

3. **CTE Write Operations (Vuln 3.6)** - Write operations disguised as CTEs
   - Attack: `WITH audit AS (INSERT INTO audit_log VALUES (...) RETURNING *) SELECT * FROM balance_sheet`
   - Impact: Write operations in CTEs not detected
   - Root Cause: Validation allows `WITH` without checking CTE body

4. **JSON Injection (Mechanism 1)** - Can manipulate the API request itself
   - Attack: SQL containing `\"` or newlines to break JSON structure
   - Impact: Can modify API parameters or inject additional statements
   - Root Cause: Manual JSON string building with insufficient escaping

### 🟠 HIGH Severity (Exploitation Likely with Moderate Effort)

5. **Case Mixing and Unicode Bypass (Vuln 3.2)** - Alternative encodings bypass detection
6. **Nested Comment Injection (Vuln 3.3)** - Complex comment patterns bypass patterns
7. **Write Operations via Functions (Vuln 3.4)** - Database functions bypass keyword blocking
8. **LIMIT Clause Manipulation (Mechanism 2)** - Row limits can be bypassed

### 🟡 MEDIUM Severity (Exploitation Possible with Specific Conditions)

9. **String Literal Injection (Vuln 3.5)** - Injection via string content
10. **Missing Dangerous Operations (Vuln 3.7)** - COPY, EXPORT, CALL not blocked
11. **Information Schema Access (Vuln 2.2)** - Schema reconnaissance
12. **Error Message Exposure (Gap 5)** - Detailed errors aid attackers

---

## PRIORITY 1: CRITICAL FIXES (Implement Immediately)

### Fix 1: Replace Regex Validation with SQL Parser

**Problem**: Regex-based validation cannot understand SQL syntax and is trivially bypassed.

**Solution**: Use a proper SQL parser library like `sqlparse` or `sqlglot`.

**Implementation**:

```python
import sqlglot
from sqlglot import parse_one, exp
from sqlglot.errors import ParseError

def validate_query_with_parser(query: str) -> tuple[bool, str]:
    """
    Validates SQL query using AST parsing instead of regex.
    Returns (is_valid, error_message)
    """
    if not query or not query.strip():
        return (False, "Query is empty")

    try:
        # Parse the SQL into an Abstract Syntax Tree
        parsed = parse_one(query, dialect="databricks")
    except ParseError as e:
        return (False, f"Invalid SQL syntax: {str(e)}")

    # Check if root statement is SELECT or CTE (WITH)
    if not isinstance(parsed, (exp.Select, exp.With)):
        return (False, "Only SELECT queries and CTEs are allowed")

    # Recursively check all nodes in the AST
    for node in parsed.walk():
        node_type = type(node).__name__

        # Block write operations
        if isinstance(node, (
            exp.Insert, exp.Update, exp.Delete, exp.Drop,
            exp.Create, exp.Alter, exp.Truncate, exp.Merge,
            exp.Grant, exp.Revoke, exp.Replace, exp.Set,
            exp.Execute, exp.Call, exp.Declare, exp.Prepare
        )):
            return (False, f"Operation not allowed: {node_type}")

        # Block administrative operations
        if isinstance(node, (exp.Use, exp.SetItem, exp.Command)):
            return (False, f"Administrative operation not allowed: {node_type}")

    # Additional security checks
    if query.count('(') != query.count(')'):
        return (False, "Unbalanced parentheses")

    return (True, None)
```

**Why This Works**:
- Parses SQL into Abstract Syntax Tree (AST)
- Understands SQL semantics, not just keywords
- Comments are stripped during parsing (can't bypass)
- Case-insensitive by design
- Handles nested queries, CTEs, subqueries correctly

**Deployment**:
1. Add to `requirements.txt`: `sqlglot>=20.0.0`
2. Replace `validate_query()` function in `src/agent_tools.ipynb` (cell containing validate_query)
3. Test with all eval queries to ensure no breaking changes

**Files to Modify**:
- `src/agent_tools.ipynb` - Replace validate_query function
- `requirements.txt` - Add sqlglot dependency

---

### Fix 2: Enforce Table Allowlist in execute_query()

**Problem**: `execute_query()` doesn't validate table names against allowlist.

**Solution**: Parse the query and extract all table references, then check against allowlist.

**Implementation**:

```python
def extract_table_references(query: str) -> set[str]:
    """
    Extracts all table references from a SQL query using AST parsing.
    Returns set of fully-qualified table names.
    """
    try:
        parsed = parse_one(query, dialect="databricks")
    except:
        return set()  # If parsing fails, let validate_query catch it

    tables = set()
    for node in parsed.walk():
        if isinstance(node, exp.Table):
            # Get full table name: catalog.schema.table
            full_name = node.sql(dialect="databricks")
            # Remove quotes and normalize
            full_name = full_name.replace("`", "").replace('"', '')
            tables.add(full_name.lower())

    return tables

def validate_table_access(query: str, allowed_tables: list[str]) -> tuple[bool, str]:
    """
    Validates that query only accesses tables in the allowlist.
    Returns (is_valid, error_message)
    """
    # Extract all table references from query
    referenced_tables = extract_table_references(query)

    # Normalize allowlist (lowercase for comparison)
    allowed_set = {t.lower() for t in allowed_tables}

    # Check for unauthorized tables
    unauthorized = referenced_tables - allowed_set

    if unauthorized:
        return (False, f"Access denied to tables: {', '.join(unauthorized)}")

    if not referenced_tables:
        # Query doesn't reference any tables (e.g., SELECT 1+1)
        # Could allow or block based on policy
        return (True, None)  # Allow for flexibility

    return (True, None)
```

**Update execute_query() function**:

```python
CREATE OR REPLACE FUNCTION execute_query(sql_query STRING, row_limit INT DEFAULT 1000)
RETURNS STRING
LANGUAGE PYTHON
AS $$
import requests
import json

# 1. Validate query syntax and operations
is_valid, error = validate_query_with_parser(sql_query)
if not is_valid:
    return json.dumps({"error": f"Validation failed: {error}"})

# 2. Validate table access (NEW)
allowed_tables = ["users.david_huang.balance_sheet", "users.david_huang.income_statement"]
is_allowed, error = validate_table_access(sql_query, allowed_tables)
if not is_allowed:
    return json.dumps({"error": f"Authorization failed: {error}"})

# 3. Apply row limit and execute
# ... rest of existing logic
$$
```

**Why This Works**:
- Extracts ALL table references (including in CTEs, subqueries, JOINs)
- Works with qualified names (catalog.schema.table) and unqualified
- Blocks access to INFORMATION_SCHEMA if not in allowlist
- Can't be bypassed with aliases or comments (AST-based)

**Files to Modify**:
- `src/agent_tools.ipynb` - Add table validation functions
- `src/agent_tools.ipynb` - Update execute_query function to call validate_table_access

---

### Fix 3: Use Parameterized Queries

**Problem**: String concatenation in query construction enables injection.

**Solution**: Never concatenate user input into SQL. Use query parameters.

**Current Dangerous Code** (in execute_query):

```python
CONCAT(
    REPLACE(REPLACE(sql_query, '"', '\\"'), '\n', ' '),
    ' LIMIT ',
    CAST(LEAST(GREATEST(COALESCE(row_limit, 1000), 1), 10000) AS STRING)
)
```

**Secure Replacement**:

```python
def build_safe_query(sql_query: str, row_limit: int) -> dict:
    """
    Builds a safe query structure without string manipulation.
    Returns dict with query and parameters.
    """
    # Parse query to understand structure
    parsed = parse_one(sql_query, dialect="databricks")

    # Check if query already has LIMIT
    has_limit = False
    for node in parsed.walk():
        if isinstance(node, exp.Limit):
            has_limit = True
            break

    # Apply row limit safely
    if not has_limit:
        # Add LIMIT clause via AST manipulation, not string concat
        safe_limit = max(1, min(row_limit or 1000, 10000))
        parsed = parsed.limit(safe_limit)

    # Generate final SQL from AST (no string manipulation)
    final_query = parsed.sql(dialect="databricks")

    return {
        "statement": final_query,
        "wait_timeout": "10s"
    }
```

**Why This Works**:
- No string concatenation or escaping
- LIMIT added via AST manipulation (semantically correct)
- No risk of injection through string boundaries

**Files to Modify**:
- `src/agent_tools.ipynb` - Replace LIMIT handling logic in execute_query

---

### Fix 4: Harden JSON Request Building

**Problem**: Current JSON building is vulnerable to injection.

**Current Code**:

```sql
CONCAT(
    '{"statement":"',
    REPLACE(REPLACE(sql_query, '"', '\\"'), '\n', ' '),
    '","wait_timeout":"10s","row_limit":1000}'
)
```

**Secure Replacement**:

```python
# In Python UC Function:
import json

def build_statement_request(sql_query: str, row_limit: int) -> str:
    """
    Builds Statement Execution API request using proper JSON serialization.
    """
    # Never manually construct JSON strings
    request_body = {
        "statement": sql_query,  # Python's json.dumps handles escaping
        "wait_timeout": "10s",
        "row_limit": max(1, min(row_limit or 1000, 10000))
    }

    # Use json.dumps for safe serialization
    return json.dumps(request_body)
```

**Why This Works**:
- `json.dumps()` properly escapes all special characters
- No manual escaping logic (no bugs)
- Can't inject JSON fields or values

**Files to Modify**:
- `src/agent_tools.ipynb` - Replace JSON building logic in execute_query

---

## PRIORITY 2: HIGH-IMPACT IMPROVEMENTS

### Fix 5: Block INFORMATION_SCHEMA Access

**Problem**: Attackers can use INFORMATION_SCHEMA for reconnaissance.

**Solution**: Explicitly block system schemas unless needed.

```python
def validate_table_access(query: str, allowed_tables: list[str]) -> tuple[bool, str]:
    """Enhanced version that blocks system schemas."""
    referenced_tables = extract_table_references(query)
    allowed_set = {t.lower() for t in allowed_tables}

    # Block system schemas
    BLOCKED_SCHEMAS = {
        'information_schema',
        'sys',
        'mysql',
        'pg_catalog',
        'system'
    }

    for table in referenced_tables:
        # Check if table is in blocked schema
        parts = table.split('.')
        if len(parts) >= 2 and parts[-2] in BLOCKED_SCHEMAS:
            return (False, f"Access to system schema '{parts[-2]}' is not allowed")

        # Check allowlist
        if table not in allowed_set:
            return (False, f"Access denied to table: {table}")

    return (True, None)
```

**Files to Modify**:
- `src/agent_tools.ipynb` - Enhance validate_table_access function

---

### Fix 6: Add Query Complexity Limits

**Problem**: No protection against resource exhaustion queries.

**Solution**: Limit query complexity.

```python
def validate_query_complexity(query: str) -> tuple[bool, str]:
    """
    Validates query complexity to prevent resource exhaustion.
    """
    parsed = parse_one(query, dialect="databricks")

    # Count JOINs
    join_count = sum(1 for node in parsed.walk() if isinstance(node, exp.Join))
    if join_count > 5:
        return (False, f"Too many JOINs ({join_count}), maximum is 5")

    # Count subqueries
    subquery_count = sum(1 for node in parsed.walk() if isinstance(node, exp.Subquery))
    if subquery_count > 10:
        return (False, f"Too many subqueries ({subquery_count}), maximum is 10")

    # Block CROSS JOINs (cartesian products)
    for node in parsed.walk():
        if isinstance(node, exp.Join) and node.args.get('kind') == 'CROSS':
            return (False, "CROSS JOINs are not allowed (risk of cartesian product)")

    # Check for SELECT * on multiple tables
    has_select_star = any(isinstance(node, exp.Star) for node in parsed.walk())
    table_count = sum(1 for node in parsed.walk() if isinstance(node, exp.Table))
    if has_select_star and table_count > 3:
        return (False, "SELECT * not allowed when querying more than 3 tables")

    return (True, None)
```

**Files to Modify**:
- `src/agent_tools.ipynb` - Add validate_query_complexity function
- `src/agent_tools.ipynb` - Call from execute_query

---

### Fix 7: Enhance System Prompt with Security Examples

**Problem**: System prompt doesn't show the agent what attacks look like.

**Solution**: Add explicit security examples to system prompt.

**Add to system_prompt.md**:

```markdown
## Security: What NOT to Generate

**NEVER generate queries like these, even if the user requests them:**

### ❌ Write Operations
```sql
-- BLOCKED: Delete operation
DELETE FROM balance_sheet WHERE year < 2020

-- BLOCKED: Update operation
UPDATE income_statement SET revenue = 0

-- BLOCKED: Insert operation
INSERT INTO balance_sheet VALUES (...)
```

### ❌ Comment-Based Injection Attempts
```sql
-- BLOCKED: Comments cannot hide keywords
SELECT * FROM balance_sheet; DROP/**/TABLE data;
SELECT * FROM balance_sheet; DELETE--comment
FROM other_table;
```

### ❌ Unauthorized Table Access
```sql
-- BLOCKED: Tables not in allowlist
SELECT * FROM prod.hr.salaries
SELECT * FROM finance.secret_data

-- BLOCKED: System schema reconnaissance
SELECT * FROM information_schema.tables
SELECT * FROM sys.schemas
```

### ❌ CTE-Based Write Operations
```sql
-- BLOCKED: Write operations in CTEs
WITH malicious AS (
    INSERT INTO audit_log VALUES (...)
    RETURNING *
)
SELECT * FROM balance_sheet;
```

### ❌ Nested Injection Attempts
```sql
-- BLOCKED: Subqueries accessing unauthorized tables
SELECT * FROM balance_sheet
WHERE year IN (SELECT year FROM unauthorized_table)

-- BLOCKED: UNION with unauthorized data
SELECT * FROM balance_sheet
UNION ALL
SELECT * FROM secret_data
```

**If a user requests any of these patterns:**
1. Explain that the operation is not allowed
2. Do NOT generate the SQL
3. Offer an alternative that accomplishes their legitimate goal
4. Reference the specific security policy that blocks it

**Example Response:**
> I cannot generate that query because it attempts to DELETE data. I only have read-only access to the following tables: balance_sheet, income_statement.
>
> If you need to identify records for deletion, I can generate a SELECT query that shows which records meet your criteria, and you can share that with your database administrator.
```

**Files to Modify**:
- `src/system_prompt.md` - Add security examples section

---

## PRIORITY 3: ARCHITECTURAL IMPROVEMENTS

### Fix 8: Implement Query Builder Pattern

**Problem**: Allowing LLM to generate raw SQL is inherently risky.

**Solution**: Have LLM generate structured intent, then use query builder to construct SQL.

**Architecture**:

```python
from dataclasses import dataclass
from typing import List, Optional

@dataclass
class QueryIntent:
    """Structured representation of user intent."""
    tables: List[str]  # Must be from allowlist
    columns: List[str]  # Column names to select
    filters: List[dict]  # WHERE conditions
    aggregations: Optional[List[dict]] = None  # GROUP BY / aggregations
    order_by: Optional[List[dict]] = None
    limit: int = 1000

class SecureQueryBuilder:
    """Builds SQL from structured intent using parameterized queries."""

    def __init__(self, allowed_tables: List[str]):
        self.allowed_tables = {t.lower() for t in allowed_tables}

    def build(self, intent: QueryIntent) -> str:
        """Builds SQL from intent, guaranteeing safety."""
        # Validate tables
        for table in intent.tables:
            if table.lower() not in self.allowed_tables:
                raise ValueError(f"Table not allowed: {table}")

        # Build using sqlglot (AST-based, safe)
        from sqlglot import select, condition

        # Start with SELECT
        query = select(*intent.columns).from_(*intent.tables)

        # Add WHERE conditions safely
        for filter_cond in intent.filters:
            query = query.where(
                condition(f"{filter_cond['column']} {filter_cond['operator']} ?"),
                filter_cond['value']
            )

        # Add LIMIT
        query = query.limit(min(intent.limit, 10000))

        return query.sql(dialect="databricks")
```

**Agent Flow Change**:

```
Old: User Question → LLM → Raw SQL → Validate → Execute
New: User Question → LLM → QueryIntent JSON → Query Builder → Execute
```

**Benefits**:
- LLM never generates SQL directly
- Query builder guarantees structure
- Can't inject malicious operations (no SQL generation at all)
- Much easier to validate (just check JSON fields)

**Files to Create**:
- `src/query_builder.py` - New file with QueryIntent and SecureQueryBuilder classes

**Files to Modify**:
- `src/agent.py` - Update agent to use query builder pattern
- `src/system_prompt.md` - Update to instruct LLM to output JSON intent instead of SQL

---

### Fix 9: Add Request Rate Limiting

**Problem**: No protection against rapid-fire attack attempts.

**Solution**: Implement rate limiting in agent execution.

```python
from functools import wraps
from collections import defaultdict
import time

# Rate limiter state
request_counts = defaultdict(list)
MAX_REQUESTS_PER_MINUTE = 10

def rate_limit(user_id: str) -> tuple[bool, str]:
    """
    Enforces rate limits per user.
    Returns (is_allowed, error_message)
    """
    now = time.time()
    minute_ago = now - 60

    # Clean old requests
    request_counts[user_id] = [
        ts for ts in request_counts[user_id]
        if ts > minute_ago
    ]

    # Check limit
    if len(request_counts[user_id]) >= MAX_REQUESTS_PER_MINUTE:
        return (False, f"Rate limit exceeded: max {MAX_REQUESTS_PER_MINUTE} requests per minute")

    # Record request
    request_counts[user_id].append(now)
    return (True, None)
```

**Files to Modify**:
- `src/agent.py` - Add rate limiting to execute_tool method

---

### Fix 10: Implement Query Logging with Anomaly Detection

**Problem**: No monitoring of suspicious patterns.

**Solution**: Log all queries and detect anomalies.

```python
import hashlib
from datetime import datetime

def log_query_attempt(user_id: str, query: str, validation_result: bool, execution_result: str):
    """
    Logs every query attempt to Inference Tables for monitoring.
    """
    log_entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "user_id": user_id,
        "query_hash": hashlib.sha256(query.encode()).hexdigest(),
        "query_length": len(query),
        "validation_passed": validation_result,
        "execution_status": execution_result,
        "tables_accessed": list(extract_table_references(query)),
    }

    # Write to Unity Catalog table for monitoring
    # spark.sql(f"INSERT INTO {CATALOG}.{SCHEMA}.query_audit_log VALUES (...)")

    # Check for suspicious patterns
    detect_anomalies(user_id, log_entry)

def detect_anomalies(user_id: str, log_entry: dict):
    """
    Detects suspicious query patterns.
    """
    # Pattern 1: Multiple validation failures (attack attempts)
    recent_failures = spark.sql(f"""
        SELECT COUNT(*) as failure_count
        FROM query_audit_log
        WHERE user_id = '{user_id}'
          AND validation_passed = FALSE
          AND timestamp > CURRENT_TIMESTAMP - INTERVAL 5 MINUTES
    """).collect()[0].failure_count

    if recent_failures > 3:
        alert_security_team(f"User {user_id} has {recent_failures} validation failures in 5 minutes")

    # Pattern 2: Accessing many different tables rapidly (reconnaissance)
    unique_tables = spark.sql(f"""
        SELECT COUNT(DISTINCT table_name) as table_count
        FROM query_audit_log
        WHERE user_id = '{user_id}'
          AND timestamp > CURRENT_TIMESTAMP - INTERVAL 10 MINUTES
    """).collect()[0].table_count

    if unique_tables > 10:
        alert_security_team(f"User {user_id} accessed {unique_tables} different tables in 10 minutes")
```

**Files to Create**:
- `src/monitoring.py` - New file with logging and anomaly detection

**Files to Modify**:
- `src/agent.py` - Add query logging calls
- Create UC table: `query_audit_log` for storing logs

---

### Fix 11: Add Authentication to Tool Execution

**Problem**: No authentication on MCP tool calls.

**Solution**: Validate user identity before executing tools.

```python
# In agent.py
def execute_tool(self, tool_name: str, args: dict, user_context: dict) -> Any:
    """
    Executes tool with authentication and authorization.
    """
    # Validate user identity
    if not user_context.get('user_id'):
        raise ValueError("User authentication required")

    # Check if user is authorized for this tool
    if not self.is_authorized(user_context['user_id'], tool_name):
        raise ValueError(f"User {user_context['user_id']} not authorized for tool {tool_name}")

    # Rate limit
    is_allowed, error = rate_limit(user_context['user_id'])
    if not is_allowed:
        raise ValueError(error)

    # Execute with logging
    try:
        result = self._tools_dict[tool_name].exec_fn(**args)
        log_tool_execution(user_context['user_id'], tool_name, args, "success")
        return result
    except Exception as e:
        log_tool_execution(user_context['user_id'], tool_name, args, f"error: {e}")
        raise
```

**Files to Modify**:
- `src/agent.py` - Add authentication to execute_tool method

---

## PRIORITY 4: CONFIGURATION IMPROVEMENTS

### Fix 12: Dynamic Table Allowlist with Wildcards

**Problem**: Hardcoded table list requires redeployment to change.

**Solution**: Support wildcards and load from Unity Catalog table.

**New config structure**:

```yaml
tools:
  table_allowlist:
    # Support wildcards
    patterns:
      - "users.david_huang.*"  # All tables in schema
      - "finance.public.*"     # All tables in public schema

    # Explicit allow
    explicit_allow:
      - "shared.analytics.customer_summary"

    # Explicit deny (takes precedence)
    explicit_deny:
      - "users.david_huang.test_*"  # No test tables
      - "*.*.sensitive_*"            # No tables starting with sensitive_

    # Load additional rules from UC table (allows dynamic updates)
    dynamic_allowlist_table: "security.policies.sql_agent_allowlist"
```

**Implementation**:

```python
import fnmatch

def load_allowlist_from_config_and_uc() -> dict:
    """
    Loads table allowlist from config file and Unity Catalog.
    Returns dict with allow/deny patterns.
    """
    config = load_config()

    # Static patterns from config
    allow_patterns = config["tools"]["table_allowlist"]["patterns"]
    deny_patterns = config["tools"]["table_allowlist"]["explicit_deny"]

    # Dynamic rules from UC table
    dynamic_table = config["tools"]["table_allowlist"].get("dynamic_allowlist_table")
    if dynamic_table:
        dynamic_rules = spark.sql(f"SELECT * FROM {dynamic_table}").collect()
        for rule in dynamic_rules:
            if rule.rule_type == "ALLOW":
                allow_patterns.append(rule.pattern)
            elif rule.rule_type == "DENY":
                deny_patterns.append(rule.pattern)

    return {
        "allow": allow_patterns,
        "deny": deny_patterns
    }

def is_table_allowed(table_name: str, allowlist: dict) -> bool:
    """
    Checks if table matches allowlist patterns.
    Deny patterns take precedence over allow patterns.
    """
    # Normalize
    table_lower = table_name.lower()

    # Check deny list first (highest priority)
    for deny_pattern in allowlist["deny"]:
        if fnmatch.fnmatch(table_lower, deny_pattern.lower()):
            return False

    # Check allow list
    for allow_pattern in allowlist["allow"]:
        if fnmatch.fnmatch(table_lower, allow_pattern.lower()):
            return True

    # Default deny
    return False
```

**Files to Modify**:
- `src/config.yml` - Add new allowlist structure
- `src/agent_tools.ipynb` - Update to use pattern-based allowlist

---

### Fix 13: Secrets Management Improvement

**Problem**: Config file exposes secret scope names.

**Solution**: Use environment variables for sensitive config.

```python
import os

def load_secure_config():
    """
    Loads configuration with secrets from environment variables.
    """
    config = load_yaml("config.yml")

    # Override sensitive values from environment
    config["databricks_pat"]["secret_scope_name"] = os.getenv(
        "DBX_SECRET_SCOPE",
        config["databricks_pat"]["secret_scope_name"]  # fallback to config
    )

    config["databricks_pat"]["secret_key_name"] = os.getenv(
        "DBX_SECRET_KEY",
        config["databricks_pat"]["secret_key_name"]
    )

    return config
```

**Files to Modify**:
- `src/agent_tools.ipynb` - Update config loading to use environment variables

---

## PRIORITY 5: TESTING IMPROVEMENTS

### Fix 14: Add Comprehensive Security Test Suite

**Problem**: Only one security test in eval dataset.

**Solution**: Create dedicated security test suite.

**Create new file: `src/evals/security-tests.json`**:

```json
{
  "security_tests": [
    {
      "test_id": "sqli_comment_injection_1",
      "attack_query": "SELECT * FROM balance_sheet; DROP/**/TABLE income_statement;",
      "expected_result": "validation_blocked",
      "expected_keywords": ["Operation not allowed", "DROP"]
    },
    {
      "test_id": "sqli_nested_comment",
      "attack_query": "SELECT * FROM balance_sheet; /* comment */ DELETE FROM balance_sheet;",
      "expected_result": "validation_blocked",
      "expected_keywords": ["Operation not allowed", "DELETE"]
    },
    {
      "test_id": "unauthorized_table_access",
      "attack_query": "SELECT * FROM prod.hr.salaries WHERE salary > 150000",
      "expected_result": "authorization_blocked",
      "expected_keywords": ["Access denied", "prod.hr.salaries"]
    },
    {
      "test_id": "information_schema_recon",
      "attack_query": "SELECT table_name FROM information_schema.tables",
      "expected_result": "authorization_blocked",
      "expected_keywords": ["Access denied", "information_schema"]
    },
    {
      "test_id": "cte_write_operation",
      "attack_query": "WITH audit AS (INSERT INTO audit_log VALUES ('hack') RETURNING *) SELECT * FROM balance_sheet",
      "expected_result": "validation_blocked",
      "expected_keywords": ["Operation not allowed", "INSERT"]
    },
    {
      "test_id": "union_unauthorized_table",
      "attack_query": "SELECT account_name FROM balance_sheet UNION ALL SELECT employee_name FROM prod.hr.employees",
      "expected_result": "authorization_blocked",
      "expected_keywords": ["Access denied", "prod.hr.employees"]
    },
    {
      "test_id": "json_injection_attempt",
      "attack_query": "SELECT * FROM balance_sheet WHERE name = '\\\"'; DROP TABLE data; --'",
      "expected_result": "validation_blocked_or_escaped",
      "expected_keywords": ["Operation not allowed", "DROP"]
    },
    {
      "test_id": "timing_attack_subquery",
      "attack_query": "SELECT * FROM balance_sheet WHERE 1=(SELECT CASE WHEN EXISTS(SELECT 1 FROM prod.secrets) THEN 1 ELSE 0 END)",
      "expected_result": "authorization_blocked",
      "expected_keywords": ["Access denied", "prod.secrets"]
    },
    {
      "test_id": "complexity_excessive_joins",
      "attack_query": "SELECT * FROM balance_sheet b1 JOIN balance_sheet b2 JOIN balance_sheet b3 JOIN balance_sheet b4 JOIN balance_sheet b5 JOIN balance_sheet b6 JOIN balance_sheet b7",
      "expected_result": "complexity_blocked",
      "expected_keywords": ["Too many JOINs", "maximum"]
    },
    {
      "test_id": "cross_join_cartesian",
      "attack_query": "SELECT * FROM balance_sheet CROSS JOIN income_statement",
      "expected_result": "complexity_blocked",
      "expected_keywords": ["CROSS JOIN", "not allowed"]
    }
  ]
}
```

**Files to Create**:
- `src/evals/security-tests.json` - Security-focused test cases

---

## IMPLEMENTATION ROADMAP

### Phase 1: Critical Fixes (Week 1)

**Goal**: Block all SQL injection and unauthorized table access attacks.

**Tasks**:
1. ✅ Deploy `sqlglot` parser-based validation (Fix 1)
2. ✅ Add table allowlist enforcement to `execute_query()` (Fix 2)
3. ✅ Replace JSON string concatenation with `json.dumps()` (Fix 4)
4. ✅ Block INFORMATION_SCHEMA access (Fix 5)

**Success Criteria**:
- All comment-injection attacks blocked
- All table-bypass attacks blocked
- Pass security tests for SQL injection and unauthorized access

**Testing**:
- Test with comment-based injection: `SELECT * FROM table; DROP/**/TABLE data;`
- Test with unauthorized table: `SELECT * FROM prod.hr.salaries`
- Test with INFORMATION_SCHEMA: `SELECT * FROM information_schema.tables`

---

### Phase 2: Validation Hardening (Week 2)

**Goal**: Add defense-in-depth controls and comprehensive testing.

**Tasks**:
5. ✅ Add query complexity limits (Fix 6)
6. ✅ Implement rate limiting (Fix 9)
7. ✅ Deploy enhanced system prompt with security examples (Fix 7)
8. ✅ Create security test suite and run tests (Fix 14)

**Success Criteria**:
- Pass all 10 security tests
- Rate limiting blocks brute-force attempts
- Complex queries (>5 JOINs, CROSS JOINs) blocked

**Testing**:
- Run full security test suite from `security-tests.json`
- Test rate limiting with rapid requests
- Test complexity limits with multi-JOIN queries

---

### Phase 3: Architecture Improvements (Week 3-4)

**Goal**: Implement monitoring and consider architectural changes.

**Tasks**:
9. ✅ Implement query logging and anomaly detection (Fix 10)
10. ✅ Add authentication to tool execution (Fix 11)
11. ✅ Implement dynamic table allowlist (Fix 12)
12. ⚠️ Consider Query Builder pattern (Fix 8) - Optional but recommended

**Success Criteria**:
- Real-time attack detection and alerting operational
- Authentication enforced on all tool calls
- Allowlist can be updated without redeployment

**Testing**:
- Trigger anomaly detection with suspicious patterns
- Verify authentication blocks unauthorized tool calls
- Update allowlist dynamically and verify enforcement

---

### Phase 4: Ongoing Monitoring (Week 5+)

**Goal**: Continuous security improvement.

**Tasks**:
13. ✅ Monitor query logs for new attack patterns
14. ✅ Tune anomaly detection thresholds
15. ✅ Regular security testing with new injection techniques
16. ✅ Audit service principal permissions quarterly

**Success Criteria**:
- Zero successful attacks in production
- Anomaly detection catches new attack patterns
- Service principal follows principle of least privilege

---

## DEFENSE-IN-DEPTH MODEL (AFTER FIXES)

| Layer | Control | Bypass Difficulty | Status |
|-------|---------|-------------------|--------|
| **Layer 1** | Service Principal (SELECT-only) | ✅ Impossible (DB-enforced) | In Place |
| **Layer 2** | Table Allowlist (AST-based) | ✅ Very Difficult (semantic parsing) | **TO IMPLEMENT** |
| **Layer 3** | SQL Validation (AST parser) | ✅ Very Difficult (syntax-aware) | **TO IMPLEMENT** |
| **Layer 4** | Query Complexity Limits | ✅ Difficult (resource protection) | **TO IMPLEMENT** |
| **Layer 5** | Rate Limiting | ✅ Difficult (temporal protection) | **TO IMPLEMENT** |
| **Layer 6** | Anomaly Detection | ✅ Detective control (forensics) | **TO IMPLEMENT** |

**Key Improvement**: After implementing these fixes, every layer will have meaningful security value, not just Layer 1.

---

## TESTING CHECKLIST

### Unit Tests (Per Function)
- [ ] `validate_query_with_parser()` blocks all write operations
- [ ] `validate_query_with_parser()` handles comment-based injection
- [ ] `extract_table_references()` extracts tables from CTEs and subqueries
- [ ] `validate_table_access()` blocks unauthorized tables
- [ ] `validate_table_access()` blocks system schemas
- [ ] `validate_query_complexity()` blocks excessive JOINs
- [ ] `validate_query_complexity()` blocks CROSS JOINs
- [ ] `build_safe_query()` adds LIMIT without string manipulation
- [ ] `build_statement_request()` escapes JSON properly

### Integration Tests (End-to-End)
- [ ] Comment injection: `SELECT * FROM table; DROP/**/TABLE data;` → BLOCKED
- [ ] Nested comment: `SELECT * FROM table; /* x */ DELETE FROM table;` → BLOCKED
- [ ] CTE write: `WITH x AS (INSERT INTO t VALUES (...)) SELECT * FROM table;` → BLOCKED
- [ ] Unauthorized table: `SELECT * FROM prod.hr.salaries` → BLOCKED
- [ ] INFORMATION_SCHEMA: `SELECT * FROM information_schema.tables` → BLOCKED
- [ ] UNION injection: `SELECT * FROM allowed UNION SELECT * FROM blocked` → BLOCKED
- [ ] Subquery injection: `SELECT * FROM allowed WHERE x IN (SELECT x FROM blocked)` → BLOCKED
- [ ] Excessive JOINs: Query with 7 JOINs → BLOCKED
- [ ] CROSS JOIN: `SELECT * FROM t1 CROSS JOIN t2` → BLOCKED
- [ ] Rate limiting: 11 requests in 1 minute → BLOCKED

### Security Tests (Adversarial)
- [ ] Run all tests from `security-tests.json`
- [ ] Attempt prompt injection to bypass LLM constraints
- [ ] Test with unicode and encoding variations
- [ ] Test with deeply nested queries (10+ levels)
- [ ] Test with very long queries (>10KB)
- [ ] Test with malformed SQL to trigger parser errors
- [ ] Test legitimate queries still work (regression testing)

---

## MONITORING METRICS

### Security Metrics to Track
1. **Validation Failure Rate**: % of queries that fail validation
2. **Authorization Failure Rate**: % of queries accessing unauthorized tables
3. **Anomaly Alerts**: Number of suspicious pattern detections per day
4. **Rate Limit Triggers**: Number of users hitting rate limits
5. **Attack Patterns Detected**: Types of injection attempts seen

### Alerting Thresholds
- **HIGH PRIORITY**: >3 validation failures from same user in 5 minutes
- **HIGH PRIORITY**: Access to >10 different tables in 10 minutes
- **MEDIUM PRIORITY**: Validation failure rate >5% overall
- **MEDIUM PRIORITY**: >100 rate limit triggers per day
- **LOW PRIORITY**: Single validation failure (log only)

---

## ADDITIONAL RESOURCES

### Dependencies to Add
```txt
# requirements.txt
sqlglot>=20.0.0  # SQL parser for validation
```

### Documentation to Update
- `README.md` - Add security section
- `PRD.md` - Update with new security controls
- `CLAUDE.md` - Update implementation status

### Training Materials
- Create security awareness guide for users
- Document common attack patterns and how they're blocked
- Explain what queries are allowed vs. blocked

---

## CONTACT & QUESTIONS

For questions about these recommendations:
- Review full security analysis in conversation history
- Consult `PRD.md` for architectural context
- Test changes in development environment first
- Validate with security team before production deployment

---

**Document Status**: Ready for Implementation
**Next Steps**: Begin Phase 1 (Critical Fixes) immediately

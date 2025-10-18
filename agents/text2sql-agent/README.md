# Text-to-SQL Agent with Unity Catalog

> ⚠️ **EXPERIMENTAL CODE**: This is a proof-of-concept implementation and should NOT be used in production without thorough security review and hardening. See [Security Considerations](#security-considerations) below.

A Databricks-native Text-to-SQL agent that converts natural language queries into SQL using the MLflow Agent Framework and Unity Catalog. The agent uses Unity Catalog Functions as tools to enable secure, governed schema discovery and SQL execution.

---

## Table of Contents

- [Overview](#overview)
- [Security Considerations](#security-considerations)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Usage](#usage)
- [UC Functions Reference](#uc-functions-reference)
- [Known Limitations](#known-limitations)

---

## Overview

This agent demonstrates how to build a secure Text-to-SQL system using Databricks' native governance and security features:

- **Unity Catalog** for data governance and access control
- **MLflow Agent Framework** for LLM orchestration and tool calling
- **UC Functions** as secure, reusable tools for schema discovery and query execution
- **Defense-in-depth security** with multiple validation layers

**Key Features**:

- Natural language to SQL conversion
- Automatic schema discovery from Unity Catalog
- Multi-layered query validation
- Table-level access control via allowlisting
- Read-only query enforcement
- Query logging and monitoring (via Inference Tables)

---

## Security Considerations

### 🔴 CRITICAL: Service Principal Configuration

**The most important security control in this system is proper service principal configuration.**

#### Required Service Principal Setup

1. **Create a dedicated service principal** for the agent:

2. **Grant ONLY SELECT permissions** on specific tables:

   ```sql
   -- DO NOT grant broad permissions like:
   -- GRANT ALL PRIVILEGES ON SCHEMA finance TO `text2sql-agent-sp`;

   -- Instead, grant granular SELECT-only permissions:
   GRANT SELECT ON TABLE catalog.schema.table1 TO `text2sql-agent-sp`;
   GRANT SELECT ON TABLE catalog.schema.table2 TO `text2sql-agent-sp`;
   ```

3. **Verify permissions are minimal**:

   ```sql
   -- Check what the service principal can access
   SHOW GRANTS ON TABLE catalog.schema.table1;

   -- Ensure no write permissions exist
   REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON TABLE catalog.schema.table1 FROM `text2sql-agent-sp`;
   ```

#### Why This Matters

**Database-level permissions are your ONLY guaranteed protection against SQL injection and malicious queries.**

Even with multiple layers of validation in the application code (regex, SQL parsing, allowlists), vulnerabilities may exist that allow attackers to bypass these controls. Historical examples:

- Comment injection bypassing keyword detection: `DROP/**/TABLE`
- JSON injection through improper escaping
- CTE-based write operations
- Unicode and encoding-based bypasses

**If the service principal has write permissions or access to sensitive tables, ALL other security layers can potentially be bypassed.**

#### Service Principal Best Practices

✅ **DO**:

- Use a dedicated service principal per agent/application
- Grant only SELECT permissions on specific tables
- Use Unity Catalog's built-in access control (not workspace ACLs)
- Regularly audit service principal permissions (quarterly)
- Store service principal tokens in Databricks Secrets
- Use token expiration and rotation policies

❌ **DON'T**:

- Grant workspace admin or account admin privileges
- Use personal access tokens (PATs) from user accounts
- Grant permissions at schema or catalog level (too broad)
- Reuse service principals across multiple applications
- Store tokens in code or configuration files

### Experimental Code Warning

⚠️ **This is experimental code for demonstration purposes.** Known security issues include:

1. **SQL Injection Vulnerabilities**:

   - Regex-based validation can be bypassed with comment injection
   - JSON construction vulnerable to escape sequence attacks
   - CTE validation gaps allow write operations in WITH clauses

2. **Authorization Gaps**:

   - Table allowlist not enforced in `execute_query()` function
   - INFORMATION_SCHEMA accessible for reconnaissance
   - No rate limiting or anomaly detection

3. **Architecture Limitations**:

   - LLM generates raw SQL (not structured query objects)
   - No query complexity limits
   - No authentication on tool execution

**Before using in production**:

- Review and implement recommendations in [todo-recs.md](todo-recs.md)
- Conduct a security audit with your security team
- Test with adversarial queries from the security test suite
- Implement comprehensive monitoring and alerting

---

## Architecture

### System Components

```text
┌─────────────┐
│    User     │
└──────┬──────┘
       │ Natural Language Query
       ▼
┌─────────────────────────────────────────────┐
│          MLflow Agent Framework             │
│  ┌─────────────────────────────────────┐    │
│  │  LLM (Claude Sonnet 4)              │    │
│  │  - Understands user intent          │    │
│  │  - Generates SQL or tool calls      │    │
│  └─────────────────────────────────────┘    │
└──────┬──────────────────────────────────────┘
       │ Tool Calls
       ▼
┌─────────────────────────────────────────────┐
│         Unity Catalog Functions             │
│  ┌──────────────────────────────────────┐   │
│  │ Schema Discovery Tools:              │   │
│  │ - list_available_tables()            │   │
│  │ - get_table_schema(table_name)       │   │
│  │ - get_sample_data(table_name)        │   │
│  └──────────────────────────────────────┘   │
│  ┌──────────────────────────────────────┐   │
│  │ Query Execution Tools:               │   │
│  │ - validate_query(sql)                │   │
│  │ - execute_query(sql, limit)          │   │
│  └──────────────────────────────────────┘   │
└──────┬──────────────────────────────────────┘
       │ SQL Execution (via Statement API)
       ▼
┌─────────────────────────────────────────────┐
│          Unity Catalog                      │
│  ┌──────────────────────────────────────┐   │
│  │ Governed Tables:                     │   │
│  │ - catalog.schema.table1              │   │
│  │ - catalog.schema.table2              │   │
│  └──────────────────────────────────────┘   │
│  ┌──────────────────────────────────────┐   │
│  │ Access Control:                      │   │
│  │ - Service Principal (SELECT-only)    │   │
│  │ - Fine-grained permissions           │   │
│  └──────────────────────────────────────┘   │
└─────────────────────────────────────────────┘
```

### Agent Workflow

1. **User Input**: User asks a natural language question (e.g., "What was our total revenue in 2023?")

2. **Schema Discovery**: Agent calls UC Functions to discover available tables and their schemas:

   - `list_available_tables()` → Returns allowlisted tables
   - `get_table_schema('income_statement')` → Returns column metadata
   - `get_sample_data('income_statement', 3)` → Returns sample rows

3. **SQL Generation**: LLM generates SQL based on schema information:

   ```sql
   SELECT SUM(revenue) as total_revenue
   FROM catalog.schema.income_statement
   WHERE year = 2023
   ```

4. **Validation**: Agent calls `validate_query()` to check for:

   - Write operations (INSERT, UPDATE, DELETE, DROP)
   - SQL injection patterns
   - Query structure (must be SELECT or CTE)

5. **Execution**: Agent calls `execute_query()` which:

   - Re-validates the query
   - Applies row limits (max 10,000)
   - Executes via Databricks Statement Execution API
   - Returns results as JSON

6. **Response**: LLM formats results in natural language for the user

---

## Quick Start

### Prerequisites

- Databricks workspace (AWS, Azure, or GCP)
- Unity Catalog enabled
- Service principal with SELECT-only permissions (see [Security Considerations](#security-considerations))
- Databricks CLI installed and configured

### Installation

1. Clone the repository
2. Create and activate virtual environment
3. Install dependencies
4. Configure the agent
5. Set up Unity Catalog resources
6. Test the agent

---

## Configuration

### config.yml Structure

```yaml
databricks:
  catalog: your_catalog             # UC catalog name
  schema: your_schema               # UC schema name
  model: text2sql_agent             # Model name for registration
  workspace_url: https://...        # Databricks workspace URL
  sql_warehouse_id: <warehouse_id>  # SQL Warehouse ID
  mlflow_experiment_id: <exp_id>    # MLflow experiment ID
  databricks_pat:
    secret_scope_name: your_scope   # Databricks secret scope
    secret_key_name: your_key       # Secret key for PAT

tools:
  uc_connection:
    name: your_connection           # UC Connection name for REST API
  tables:                           # TABLE ALLOWLIST (critical!)
    - catalog.schema.table1
    - catalog.schema.table2

agent:
  llm:
    endpoint: databricks-claude-sonnet-4  # LLM endpoint name
    temperature: 0.1                      # LLM temperature (0-1)
  max_iterations: 10                      # Max tool calls per query
  system_prompt_path: ./system_prompt.md  # Path to system prompt
```

### Critical Configuration: Table Allowlist

**The `tools.tables` list controls which tables the agent can ACCESS via schema discovery functions.**

⚠️ **Important**: This allowlist is currently NOT enforced in `execute_query()`. This is a known vulnerability. See [todo-recs.md](todo-recs.md#fix-2-enforce-table-allowlist-in-execute_query) for the fix.

**Best Practices**:

- Only include tables that users should be able to query
- Use fully-qualified names: `catalog.schema.table`
- Keep the list minimal (principle of least privilege)
- Never include system schemas: `information_schema`, `sys`, etc.
- Document why each table is included

---

## UC Functions Reference

All UC Functions are defined in [src/agent_tools.ipynb](src/agent_tools.ipynb).

---

## Known Limitations

### Security Limitations

1. **SQL Injection Vulnerabilities** (CRITICAL):
   - Regex validation bypassable with comment injection: `DROP/**/TABLE`
   - CTE validation gaps allow write operations in WITH clauses
   - See [todo-recs.md](todo-recs.md) for full analysis

2. **Authorization Gaps** (HIGH):
   - Table allowlist not enforced in `execute_query()`
   - Any table accessible to service principal can be queried
   - INFORMATION_SCHEMA accessible for reconnaissance

3. **No Rate Limiting** (MEDIUM):
   - Users can send unlimited queries
   - Vulnerable to brute-force injection attempts
   - No protection against resource exhaustion

4. **Limited Monitoring** (MEDIUM):
   - Query logging not implemented
   - No anomaly detection for suspicious patterns
   - No alerting on validation failures

### Functional Limitations

1. **Foreign Key Discovery**: `get_table_relationships()` not implemented
2. **Complex Queries**: No support for window functions, recursive CTEs
3. **Query Optimization**: No cost estimation or optimization hints
4. **Error Handling**: Limited error messages (may expose internal details)

---

**⚠️ REMINDER: This is experimental code. Do not use in production without implementing security recommendations from [todo-recs.md](todo-recs.md).**

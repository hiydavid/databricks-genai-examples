# Genie Metadata Integration Plan

## Research Summary

This document outlines the research findings and implementation approaches for exposing Genie space metadata (table descriptions, column descriptions, general instructions, and trusted assets) to the supervisor agent as contextual information.

## Available APIs and Methods

### 1. Genie Space Metadata via REST API ✅

**Available Endpoints:**

- `GET /api/2.0/genie/spaces` - Lists all accessible Genie spaces with description, space ID, and title
- `GET /api/2.0/genie/spaces/{space_id}` - Retrieves specific space configuration (inferred from list endpoint)

**Space-Level Metadata Available:**

- Table descriptions (customized at Genie space level, separate from Unity Catalog)
- Column descriptions and synonyms  
- Knowledge store information including sampled values and value dictionaries
- Space-specific context that informs how Genie answers questions

**Key Insight:** Any metadata added to data assets in a Genie space is scoped to that space and does not overwrite Unity Catalog metadata.

### 2. Unity Catalog Table Metadata via REST API ✅

**Available Endpoints:**

- `GET /api/2.1/unity-catalog/tables/{catalog}.{schema}.{table}` - Retrieves detailed table metadata
- SQL-based access via `information_schema.columns` and `information_schema.tables`
- `DESCRIBE TABLE AS JSON` for programmatic schema access

**Metadata Available:**

- Column names, data types, comments/descriptions, nullable status
- Table-level descriptions and properties
- AI-generated comments (when enabled)
- Data lineage information

**JSON Schema Format:**

```json
{
  "columns": [
    {
      "name": "<column_name>", 
      "type": <type_json>,
      "comment": "<comment>",
      "nullable": <boolean>
    }
  ]
}
```

### 3. Trusted Assets Limitations ⚠️

**Current State:**

- Trusted assets (parameterized queries, UDFs) are managed primarily through Databricks UI
- No direct REST API endpoints for retrieving Genie-specific trusted assets programmatically
- UDFs can be listed through Unity Catalog functions API, but not parameterized example queries

**Types of Trusted Assets:**

- Parameterized example SQL queries (UI-managed)
- User-defined table functions (UDFs) registered with Unity Catalog

## Implementation Approaches

### Approach 1: Module-Level Metadata Loading (Recommended)

**Advantages:**

- Simple and reliable - no complex caching logic needed
- Zero runtime performance impact - metadata loaded once at module import
- Compatible with existing architecture and asyncio parallel execution
- Perfect for stable financial data schemas
- Integrates seamlessly with temporal context

**Key Insight:** For financial SEC data, table schemas are stable. Load once at module level, use throughout session alongside existing config loading pattern.

**Implementation Steps:**

1. **Add Synchronous Metadata Retrieval Functions**

   ```python
   def get_genie_space_metadata(client: WorkspaceClient, space_id: str) -> dict:
       """Retrieve Genie space metadata once at module load"""
       try:
           response = client.api_client.do("GET", f"/api/2.0/genie/spaces/{space_id}")
           return response.json() if response else {}
       except Exception as e:
           print(f"[WARNING] Failed to load Genie metadata: {e}")
           return {}  # Graceful fallback
   ```

2. **Add Table Schema Retrieval**

   ```python
   def get_table_schemas(client: WorkspaceClient, catalog: str, schema: str) -> dict:
       """Get schemas for all tables in the financial dataset"""
       tables = ["balance_sheet", "income_statement"]  # Known financial tables
       schemas = {}
       for table in tables:
           try:
               table_info = client.tables.get(f"{catalog}.{schema}.{table}")
               schemas[table] = {
                   "columns": [{"name": col.name, "type": str(col.type_name), 
                               "comment": col.comment} for col in table_info.columns],
                   "comment": table_info.comment
               }
           except Exception as e:
               print(f"[WARNING] Failed to get schema for {table}: {e}")
       return schemas
   ```

3. **Module-Level Metadata Loading (in multiagent-genie.py)**

   ```python
   # Load alongside existing config (after WorkspaceClient creation)
   def load_metadata_context() -> dict:
       """Load all metadata once at module import time"""
       try:
           workspace_client = WorkspaceClient(
               host=os.getenv("DB_MODEL_SERVING_HOST_URL"),
               token=os.getenv("DATABRICKS_GENIE_PAT"),
           )
           
           genie_metadata = get_genie_space_metadata(workspace_client, GENIE_SPACE_ID)
           table_schemas = get_table_schemas(workspace_client, 
                                           databricks_configs.get("catalog"),
                                           databricks_configs.get("schema"))
           
           return {"genie": genie_metadata, "schemas": table_schemas}
       except Exception as e:
           print(f"[WARNING] Failed to load metadata context: {e}")
           return {}  # Graceful degradation

   # Load at module level alongside other config
   METADATA_CONTEXT = load_metadata_context()
   ```

4. **Enhance Supervisor with Metadata Context**

   ```python
   def supervisor_agent(state):
       # Combine temporal context with metadata context
       temporal_ctx = get_temporal_context()
       
       # Build comprehensive context prefix
       context_prefix = build_enhanced_context_prefix(temporal_ctx, METADATA_CONTEXT)
       
       # Enhance system prompts with both temporal and metadata context
       enhanced_system_prompt = context_prefix + SYSTEM_PROMPT
       enhanced_research_prompt = context_prefix + SYSTEM_PROMPT + "\n\n" + RESEARCH_PROMPT
       
       # Continue with existing supervisor logic...
   ```

5. **Context Formatting Helper**

   ```python
   def build_enhanced_context_prefix(temporal_ctx: dict, metadata_ctx: dict) -> str:
       """Build context prefix combining temporal and metadata information"""
       prefix_parts = [
           "Below is information on the current date, fiscal context, and available data structures:",
           f"- The current date is: {temporal_ctx['today_iso']}",
           f"- The current fiscal year is: {temporal_ctx['fy']}",  
           f"- The current fiscal quarter is: {temporal_ctx['fq']}",
       ]
       
       # Add metadata context if available
       if metadata_ctx.get("schemas"):
           prefix_parts.append("\nAvailable Data Tables:")
           for table_name, table_info in metadata_ctx["schemas"].items():
               prefix_parts.append(f"- {table_name}: {table_info.get('comment', 'SEC financial data')}")
               # Optionally include key columns for better routing decisions
       
       return "\n".join(prefix_parts) + "\n\n"
   ```

### Approach 2: Configuration-Based Solution (Simple Alternative)

**Advantages:**

- Full control over exposed metadata
- No API call overhead during agent execution
- Easy to customize and filter information
- Can include trusted assets documentation manually

**Implementation Steps:**

1. **Extend configs.yaml**

   ```yaml
   genie_space_metadata:
     tables:
       balance_sheet:
         description: "SEC Balance Sheet data from 2003-2022"
         columns:
           - name: "revenue"
             type: "double"
             description: "Total revenue for the period"
       income_statement:
         description: "SEC Income Statement data from 2003-2022"
         # ... more columns
     trusted_assets:
       - name: "Revenue Growth YoY"
         description: "Calculate year-over-year revenue growth"
         sql_template: "SELECT ..."
   ```

2. **Runtime Metadata Loading**

   ```python
   def load_metadata_from_config() -> dict:
       """Load Genie space metadata from configuration"""
       with open('configs.yaml', 'r') as f:
           config = yaml.safe_load(f)
       return config.get('genie_space_metadata', {})
   ```

3. **Supervisor Prompt Enhancement**
   ```python
   def enhance_supervisor_prompt_with_metadata(base_prompt: str, metadata: dict) -> str:
       metadata_context = format_metadata_for_prompt(metadata)
       return f"{base_prompt}\n\nAvailable Data Context:\n{metadata_context}"
   ```

## Updated Implementation Plan

### Phase 1: Module-Level Metadata Loading (Core Implementation)

- [ ] Add synchronous metadata retrieval functions to `multiagent-genie.py`
- [ ] Implement graceful error handling for API failures (print warnings, continue without metadata)
- [ ] Load metadata once at module level alongside existing config loading
- [ ] Add context formatting helper that combines temporal and metadata context
- [ ] Test metadata loading in development environment

### Phase 2: Supervisor Integration with Enhanced Context

- [ ] Modify `supervisor_agent` to use combined temporal + metadata context prefix
- [ ] Update both system prompt and research prompt with enhanced context
- [ ] Test routing decisions with metadata-enhanced prompts using sample questions
- [ ] Add configuration options for metadata verbosity levels
- [ ] Validate improved routing accuracy through evaluation framework

### Phase 3: Production Readiness and Optimization

- [ ] Add comprehensive error handling and fallback behavior for production
- [ ] Update documentation in `CLAUDE.md`, `README.md`, and `optimization-guide.md`
- [ ] Add metadata context to evaluation criteria and test cases
- [ ] Create troubleshooting guide for metadata loading failures
- [ ] Monitor metadata impact on routing decisions via MLflow traces

## Code Integration Points

### Current Architecture Integration (Post-Asyncio)

1. **multiagent-genie.py** - Module-level metadata loading

   ```python
   # Add after existing config loading (line ~40)
   METADATA_CONTEXT = load_metadata_context()
   
   # Modify supervisor_agent function (line ~143)
   def supervisor_agent(state):
       temporal_ctx = get_temporal_context()
       context_prefix = build_enhanced_context_prefix(temporal_ctx, METADATA_CONTEXT)
       
       enhanced_system_prompt = context_prefix + SYSTEM_PROMPT
       enhanced_research_prompt = context_prefix + SYSTEM_PROMPT + "\n\n" + RESEARCH_PROMPT
   ```

2. **LangGraphChatAgent Class** - No changes needed

   ```python
   # Current constructor remains unchanged - compatible with asyncio implementation
   class LangGraphChatAgent(ChatAgent):
       def __init__(self, agent: CompiledStateGraph):
           self.agent = agent
   ```

3. **configs.yaml** - Optional metadata configuration

   ```yaml
   # Add optional metadata verbosity controls
   metadata_config:
     include_column_descriptions: true
     include_table_comments: true
     include_genie_instructions: false  # May be too verbose for prompts
   ```

4. **Integration with Temporal Context** - Enhanced context building

   ```python
   # Replaces current temporal_prefix in supervisor_agent
   def build_enhanced_context_prefix(temporal_ctx: dict, metadata_ctx: dict) -> str:
       # Combines temporal context (existing) with metadata context (new)
       # Returns unified context prefix for all supervisor prompts
   ```

## Expected Benefits

### For Supervisor Agent

- **Better Routing Decisions:** Understanding available tables and columns helps determine complexity
- **Reduced Trial-and-Error:** Knowing data structure prevents impossible queries
- **Context-Aware Planning:** Can plan parallel queries based on actual available data

### For System Performance  

- **Fewer Failed Queries:** Better understanding prevents invalid SQL generation
- **Optimized Query Planning:** Knowledge of indexes and relationships improves performance
- **Reduced Iterations:** More accurate initial routing reduces back-and-forth

### For User Experience

- **More Accurate Responses:** Better context leads to more relevant answers
- **Faster Response Times:** Reduced need for query corrections and retries
- **Enhanced Coverage:** Understanding of all available data sources

## Implementation Considerations

### Performance Impact

- **Startup Only**: Metadata loading happens once at module import - zero runtime impact
- **Memory Usage**: Static metadata context stored in memory (minimal for financial tables)
- **Simple and Fast**: No caching logic, TTL management, or refresh mechanisms needed
- **Asyncio Compatible**: Metadata loading is synchronous at startup, asyncio handles runtime parallel execution

### Security and Permissions

- Ensure PAT token has `workspace-access` and `genie-access` permissions
- Handle permission errors gracefully during module loading (print warnings, continue)
- Use same authentication pattern as existing Genie agent setup
- Log metadata loading success/failure for troubleshooting

### Maintenance Requirements

- **Minimal**: Financial data schemas change infrequently
- **Module Reload**: If schemas change, restart notebook/agent to reload module-level metadata
- **No Complex Sync**: Eliminates cache invalidation and refresh logic
- **Compatible with Deployment**: Works seamlessly with Databricks model serving restarts

### Fallback Strategy

- **Graceful Degradation**: If metadata loading fails at module load, continue without metadata context
- **Warning Logging**: Print warnings for metadata loading failures for troubleshooting  
- **Full Functionality**: Agent works normally with asyncio parallel execution, just without enhanced routing context
- **Development Friendly**: Easy to disable/enable metadata loading for testing

### Architecture Compatibility

- **Asyncio Integration**: Module-level metadata loading is independent of asyncio parallel execution
- **Temporal Context**: Metadata context integrates seamlessly with existing temporal context
- **Evaluation Framework**: Enhanced context can be included in evaluation criteria
- **MLflow Tracing**: Metadata impact on routing decisions visible in traces

## Next Steps

1. **Proof of Concept:** Implement metadata retrieval functions and test at module level
2. **Context Integration:** Combine metadata with temporal context in supervisor prompts  
3. **Routing Validation:** Test routing improvements using evaluation framework
4. **Production Enhancement:** Add comprehensive error handling and documentation updates

## Summary

This updated plan provides a practical, low-complexity approach to exposing Genie space metadata to your supervisor agent. The **module-level loading strategy** aligns perfectly with your current architecture while providing:

✅ **Zero runtime performance impact** - metadata loaded once at module import  
✅ **Seamless asyncio compatibility** - independent of parallel execution improvements  
✅ **Enhanced routing context** - combines temporal and metadata information  
✅ **Simple fallback behavior** - graceful degradation when metadata unavailable  
✅ **Easy implementation** - minimal changes to existing codebase  

The approach maintains all benefits of metadata-enhanced routing decisions while integrating naturally with your recent asyncio improvements and temporal context features.
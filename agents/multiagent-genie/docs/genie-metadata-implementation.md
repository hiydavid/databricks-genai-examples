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

### Approach 1: One-Time Initialization Retrieval (Recommended)

**Advantages:**
- Simple and reliable - no complex caching logic needed
- Zero runtime performance impact - metadata loaded once at startup
- Compatible with existing synchronous architecture
- Perfect for stable financial data schemas

**Key Insight:** For financial SEC data, table schemas are stable. Load once at initialization, use throughout session.

**Implementation Steps:**

1. **Add Synchronous Genie Space Metadata Retrieval**
   ```python
   def get_genie_space_metadata(client: WorkspaceClient, space_id: str) -> dict:
       """Retrieve Genie space metadata once at initialization"""
       try:
           response = client.api_client.do("GET", f"/api/2.0/genie/spaces/{space_id}")
           return response.json() if response else {}
       except Exception as e:
           logger.warning(f"Failed to load metadata: {e}")
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
               logger.warning(f"Failed to get schema for {table}: {e}")
       return schemas
   ```

3. **One-Time Metadata Loading in Agent**
   ```python
   class LangGraphChatAgent:
       def __init__(self, config: ModelConfig):
           # Load metadata ONCE at initialization
           self.metadata_context = self._load_metadata_at_startup(config)
           
       def _load_metadata_at_startup(self, config: ModelConfig) -> dict:
           """Load all needed metadata once - no refresh needed"""
           genie_metadata = get_genie_space_metadata(
               self.workspace_client, config.get("genie_space_id")
           )
           table_schemas = get_table_schemas(
               self.workspace_client, 
               config.get("catalog_name"), 
               config.get("schema_name")
           )
           return {"genie": genie_metadata, "schemas": table_schemas}
   ```

4. **Enhance Supervisor with Static Context**
   ```python
   def supervisor_agent(state: AgentState) -> AgentState:
       # Use pre-loaded metadata context
       enhanced_prompt = f"{base_system_prompt}\n\nData Context:\n{format_metadata_context(agent.metadata_context)}"
       # ... rest of supervisor logic
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

## Simplified Implementation Plan

### Phase 1: One-Time Metadata Loading (Core Implementation)
- [ ] Add synchronous metadata retrieval functions using existing `WorkspaceClient`
- [ ] Implement graceful error handling for API failures
- [ ] Load metadata once in `LangGraphChatAgent.__init__()`
- [ ] Add metadata context formatting for supervisor prompts

### Phase 2: Supervisor Integration
- [ ] Modify `supervisor_agent` system prompt to include static metadata context
- [ ] Test routing decisions with metadata-enhanced prompts
- [ ] Add configuration options for metadata verbosity
- [ ] Validate improved routing accuracy

### Phase 3: Production Readiness
- [ ] Add comprehensive error handling and fallback behavior
- [ ] Document new configuration options in CLAUDE.md
- [ ] Add startup metadata loading to deployment process
- [ ] Create troubleshooting guide for metadata loading failures

## Code Integration Points

### Current Architecture Modifications

1. **LangGraphChatAgent Class** (driver.py) - One-time loading
   ```python
   def __init__(self, config: ModelConfig):
       # Load metadata once at startup
       self.metadata_context = self._load_metadata_at_startup(config)
   ```

2. **multiagent-genie.py:100** - `supervisor_agent` function
   ```python
   def supervisor_agent(state: AgentState) -> AgentState:
       # Use pre-loaded metadata in system prompt
       enhanced_prompt = build_metadata_prompt(base_prompt, agent.metadata_context)
   ```

3. **configs.yaml** - Simple configuration
   ```yaml
   # No complex caching config needed
   metadata_config:
     include_column_descriptions: true
     include_table_comments: true
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
- **Startup Only**: Metadata loading happens once at initialization - zero runtime impact
- **Memory Usage**: Static metadata context stored in memory (minimal for financial tables)
- **Simple and Fast**: No caching logic, TTL management, or refresh mechanisms needed

### Security and Permissions
- Ensure PAT token has `workspace-access` and `genie-access` permissions
- Handle permission errors gracefully during startup
- Log metadata loading success/failure for troubleshooting

### Maintenance Requirements
- **Minimal**: Financial data schemas change infrequently
- **Restart to Refresh**: If schemas change, restart agent deployment to reload
- **No Complex Sync**: Eliminates cache invalidation and refresh logic

### Fallback Strategy
- **Simple**: If metadata loading fails at startup, continue without metadata context
- **Logging**: Log metadata loading failures for troubleshooting
- **Degraded Mode**: Agent works normally, just without enhanced routing context

## Next Steps

1. **Proof of Concept:** Implement basic API client and test metadata retrieval
2. **Integration Testing:** Add metadata to supervisor prompt and test routing improvements  
3. **Performance Optimization:** Implement caching and measure impact
4. **Production Deployment:** Add monitoring, error handling, and documentation

This plan provides a comprehensive roadmap for exposing Genie space metadata to your supervisor agent, improving its ability to make informed routing decisions and provide more accurate responses.
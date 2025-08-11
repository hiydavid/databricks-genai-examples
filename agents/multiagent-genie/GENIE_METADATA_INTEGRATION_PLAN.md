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

### Approach 1: Direct API Integration (Recommended)

**Advantages:**
- Real-time metadata access
- Always synchronized with Genie space configuration
- No manual maintenance required
- Comprehensive metadata coverage

**Implementation Steps:**

1. **Add Genie Space Metadata Retrieval**
   ```python
   async def get_genie_space_metadata(space_id: str) -> dict:
       """Retrieve Genie space metadata including table/column descriptions"""
       response = await databricks_client.get(f"/api/2.0/genie/spaces/{space_id}")
       return response.json()
   ```

2. **Add Unity Catalog Table Metadata**
   ```python
   async def get_table_metadata(full_table_name: str) -> dict:
       """Get detailed table metadata from Unity Catalog"""
       response = await databricks_client.get(f"/api/2.1/unity-catalog/tables/{full_table_name}")
       return response.json()
   ```

3. **Supervisor Context Enhancement**
   - Modify `supervisor_agent` function (multiagent-genie.py:100) to include metadata in system prompt
   - Cache metadata to avoid repeated API calls
   - Add metadata about available tables, columns, data types, and descriptions

4. **Metadata Caching System**
   ```python
   class MetadataCache:
       def __init__(self, ttl_minutes: int = 30):
           self.cache = {}
           self.ttl = ttl_minutes
       
       async def get_genie_metadata(self, space_id: str) -> dict:
           # Check cache first, then fetch if expired
           pass
   ```

### Approach 2: Configuration-Based Solution (Alternative)

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

## Detailed Implementation Plan

### Phase 1: API Client Development
- [ ] Create Databricks API client wrapper for Genie and Unity Catalog endpoints
- [ ] Implement authentication handling with PAT token
- [ ] Add error handling and retry logic
- [ ] Create data models for API responses

### Phase 2: Metadata Caching System
- [ ] Implement TTL-based caching to minimize API calls
- [ ] Add cache invalidation triggers
- [ ] Create metadata refresh capabilities
- [ ] Add logging for cache performance monitoring

### Phase 3: Supervisor Agent Integration
- [ ] Modify `supervisor_agent` system prompt template to include metadata placeholders
- [ ] Add metadata injection logic at agent initialization
- [ ] Update routing logic to leverage table/column context
- [ ] Add configuration options for metadata verbosity

### Phase 4: Testing and Validation
- [ ] Test with sample queries across different complexity levels
- [ ] Validate improved routing decisions with metadata context
- [ ] Monitor performance impact of metadata loading
- [ ] Compare routing accuracy before/after metadata integration

### Phase 5: Documentation and Maintenance
- [ ] Document new configuration options in CLAUDE.md
- [ ] Add troubleshooting guide for metadata issues
- [ ] Create maintenance procedures for keeping metadata synchronized

## Code Integration Points

### Current Architecture Modifications

1. **multiagent-genie.py:100** - `supervisor_agent` function
   - Add metadata loading at initialization
   - Enhance system prompt with data context
   - Include table/column information in routing decisions

2. **configs.yaml** - Configuration
   - Add `genie_space_metadata` section (Approach 2)
   - Add `metadata_cache_ttl` configuration option
   - Include API endpoint configurations

3. **LangGraphChatAgent Class** - Driver integration
   - Initialize metadata cache in constructor
   - Refresh metadata on space configuration changes

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
- API calls for metadata retrieval add initial latency
- Caching strategy crucial for production deployment
- Consider async loading for large metadata sets

### Security and Permissions
- Ensure PAT token has appropriate permissions for both Genie and Unity Catalog APIs
- Handle permission errors gracefully
- Consider service principal authentication for production

### Maintenance Requirements
- Metadata may need periodic refresh as Genie space evolves
- Monitor for API changes in Databricks platform
- Keep documentation synchronized with actual data structure

### Fallback Strategies
- Graceful degradation when metadata is unavailable
- Default routing behavior when API calls fail
- Logging and alerting for metadata synchronization issues

## Next Steps

1. **Proof of Concept:** Implement basic API client and test metadata retrieval
2. **Integration Testing:** Add metadata to supervisor prompt and test routing improvements  
3. **Performance Optimization:** Implement caching and measure impact
4. **Production Deployment:** Add monitoring, error handling, and documentation

This plan provides a comprehensive roadmap for exposing Genie space metadata to your supervisor agent, improving its ability to make informed routing decisions and provide more accurate responses.
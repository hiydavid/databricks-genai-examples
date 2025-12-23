# Genie Space Migration with Databricks Asset Bundles

Migrate Databricks Genie Spaces between Azure workspaces using the Genie Management APIs.

Designed for regulatory-compliant environments requiring:

- **No PAT tokens** - Service Principal authentication only
- **Git-based deployment** - all configurations in version control
- **Full audit trail** - git commits + CI/CD logs for change management

## Why This Pattern?

As of December 2025, Databricks Asset Bundles don't natively support Genie Spaces ([GitHub issue #3008](https://github.com/databricks/cli/issues/3008)). This pattern uses the public Genie Management APIs (CreateSpace, UpdateSpace, GetSpace) as a workaround.

## Getting Started

See [docs/SETUP_GUIDE.md](docs/SETUP_GUIDE.md) for the complete setup walkthrough.

## Quick Start (DAB)

```bash
# Deploy bundle to your workspace
databricks bundle deploy --target dev --var warehouse_id=<your-warehouse-id>

# Export a Genie Space
databricks bundle run export_genie_space \
    --var source_space_id=<space-id-to-export>

# Deploy a Genie Space (first time - creates new)
databricks bundle run deploy_genie_space \
    --var warehouse_id=<warehouse-id> \
    --var genie_config=genie_spaces/my_space.json

# Deploy a Genie Space (subsequent - updates existing)
databricks bundle run deploy_genie_space \
    --var warehouse_id=<warehouse-id> \
    --var genie_config=genie_spaces/my_space.json \
    --var space_id=<existing-space-id>
```

## Project Structure

```text
├── databricks.yml              # DAB bundle configuration (jobs, artifacts)
├── pyproject.toml              # Python package config with entry points
├── azure-pipelines.yml         # Azure DevOps CI/CD pipeline
├── scripts/
│   ├── __init__.py             # Package marker
│   ├── export_genie_space.py   # Export from source workspace
│   └── deploy_genie_space.py   # Deploy to destination workspace
├── genie_spaces/               # Exported Genie Space JSON configs
│   └── sample_space.json       # Example config structure
├── docs/
│   └── SETUP_GUIDE.md          # Step-by-step setup guide
└── requirements.txt            # Python dependencies
```

## How It Works

### The `serialized_space` Field

When exporting a Genie Space with `include_serialized_space=True`, the API returns a JSON string containing the complete space configuration:

- `version` - Schema version
- `config` - Sample questions, settings
- `data_sources` - Tables available to the space
- `instructions` - Context and rules for Genie

This field can be passed directly to CreateSpace/UpdateSpace APIs to recreate the space in another workspace.

### Deployment Behavior

| Scenario                                  | Behavior                            |
| ----------------------------------------- | ----------------------------------- |
| First deploy (no `--space-id`)            | Creates new space, outputs new ID   |
| Subsequent deploys (with `--space-id`)    | Updates existing space              |
| No `--space-id` but matching title exists | Updates by title match (fallback)   |

## Known Limitations

| Limitation              | Impact                                                      |
| ----------------------- | ----------------------------------------------------------- |
| New Space ID            | Destination workspace gets a different space_id than source |
| No conversation history | Conversations are workspace-specific and not migrated       |
| Permissions separate    | Unity Catalog grants must be configured separately          |
| Rate limits             | 5 queries/minute/workspace for Genie API                    |

## Compliance

This pattern supports regulatory requirements:

- **Version Control** - All configs stored in git with full history
- **Audit Trail** - Git commits + CI/CD logs document all changes
- **Approval Gates** - PR approval can be required before production
- **Immutable Artifacts** - `serialized_space` JSON captures exact state
- **No Manual Changes** - All changes flow through git → CI/CD → workspace

## References

- [Genie API Documentation](https://docs.databricks.com/aws/en/genie/conversation-api)
- [DABs Authentication (Azure)](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/authentication)
- [Databricks Python SDK - Genie](https://databricks-sdk-py.readthedocs.io/en/stable/workspace/dashboards/genie.html)
- [GitHub Issue #3008 - DABs Genie Support](https://github.com/databricks/cli/issues/3008)

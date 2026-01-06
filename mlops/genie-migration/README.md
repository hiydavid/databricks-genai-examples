# Genie Space Migration with Databricks Asset Bundles

Migrate Databricks Genie Spaces between Azure workspaces using the Genie Management APIs.

Designed for regulatory-compliant environments requiring:

- **No PAT tokens** - Service Principal authentication only
- **Git-based deployment** - all configurations in version control
- **Full audit trail** - git commits + CI/CD logs for change management

## Why This Pattern?

As of December 2025, Databricks Asset Bundles don't natively support Genie Spaces ([GitHub issue #3008](https://github.com/databricks/cli/issues/3008)). This pattern uses the public Genie Management APIs (CreateSpace, UpdateSpace, GetSpace) as a workaround.

## Cross-Workspace Migration Workflow

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                           SOURCE WORKSPACE                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│  1. databricks bundle validate --target dev                                 │
│  2. databricks bundle deploy --target dev                                   │
│  3. databricks bundle run export_genie_space --target dev                   │
│     → Writes JSON to /Workspace/Shared/genie_exports/<title>.json           │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           LOCAL MACHINE                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│  4. databricks workspace export \                                           │
│       /Workspace/Shared/genie_exports/<title>.json \                        │
│       --file ./genie_spaces/<filename>.json                                 │
│     (Edit JSON here if catalog/schema names differ between environments)    │
│                                                                             │
│  5. git add genie_spaces/<filename>.json                                    │
│  6. git commit -m "Export Genie Space: <title>"                             │
│  7. git push                                                                │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           TARGET WORKSPACE                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│  8. databricks bundle validate --target prod                                │
│  9. databricks bundle deploy --target prod \                                │
│       --var deploy_config_path=/Workspace/Shared/.bundle/.../genie_spaces/x.json│
│     → Syncs files + sets job parameters                                     │
│ 10. databricks bundle run deploy_genie_space --target prod                  │
│     → Creates/updates Genie Space from synced JSON                          │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Setup

### 1. Configure `databricks.yml`

```bash
cp databricks.yml.template databricks.yml
```

Key settings:

```yaml
variables:
  target_warehouse_id:
    default: "<your-warehouse-id>"

  run_as_service_principal:
    default: "<your-sp-application-id>"  # Use application ID, not display name

  source_space_id:
    default: "<genie-space-id-to-export>"

targets:
  dev:  # Source workspace
    workspace:
      host: https://adb-<source-workspace-id>.<region>.azuredatabricks.net
      root_path: /Shared/.bundle/${bundle.name}/${bundle.target}

  prod:  # Target workspace
    workspace:
      host: https://adb-<target-workspace-id>.<region>.azuredatabricks.net
      root_path: /Shared/.bundle/${bundle.name}/${bundle.target}
```

### 2. Service Principal Setup (Both Workspaces)

Supports both **Databricks managed SPs** and **Microsoft Entra ID SPs**. See [docs/SETUP_GUIDE.md](docs/SETUP_GUIDE.md) for detailed creation steps.

1. **Add SP to workspace**: Admin Settings → Service principals → Add
2. **Grant SP entitlements**: "Workspace access" and "Databricks SQL access"
3. **Grant yourself "User" role on SP**: SP → Permissions → Grant access → Add yourself with "User" role
   - Required to create jobs with `run_as` referencing this SP
4. **Grant SP permissions**:
   - Source: CAN EDIT on Genie Space (required for export)
   - Target: CAN USE on SQL Warehouse (required for deploy)
   - Target: CAN MANAGE on `target_parent_path` folder (required to create Genie Spaces)

## Command Reference

### Export (Source Workspace)

```bash
# Validate and deploy bundle
databricks bundle validate --target dev
databricks bundle deploy --target dev

# Run export job
databricks bundle run export_genie_space --target dev

# Download exported JSON to local repo
databricks workspace export \
    /Workspace/Shared/genie_exports/<title>.json \
    --file ./genie_spaces/<filename>.json
```

> **Note:** If your target workspace uses different catalog/schema names, edit the JSON file before committing.

### Deploy (Target Workspace)

```bash
# Commit the exported JSON
git add genie_spaces/<filename>.json
git commit -m "Export Genie Space"
git push

# Validate and deploy bundle (syncs JSON to workspace)
# NOTE: --var must be on deploy, not run (base_parameters are set at deploy time)
databricks bundle validate --target prod
databricks bundle deploy --target prod \
    --var deploy_config_path=/Workspace/Shared/.bundle/genie-migration/prod/files/genie_spaces/<filename>.json

# Run deploy job (first time - creates new space)
databricks bundle run deploy_genie_space --target prod

# Run deploy job (subsequent - updates existing space)
databricks bundle deploy --target prod \
    --var deploy_config_path=/Workspace/Shared/.bundle/genie-migration/prod/files/genie_spaces/<filename>.json \
    --var target_space_id=<existing-space-id>
databricks bundle run deploy_genie_space --target prod
```

## Project Structure

```text
├── databricks.yml              # DAB bundle configuration
├── databricks.yml.template     # Template for new setups
├── scripts/
│   ├── export_genie_space.py   # Export notebook
│   └── deploy_genie_space.py   # Deploy notebook
├── genie_spaces/               # Exported JSON configs (committed to git)
│   └── <space_name>.json
└── docs/
    └── SETUP_GUIDE.md
```

## How It Works

### The `serialized_space` Field

The Genie API's `get_space(include_serialized_space=True)` returns a JSON string containing:

- `version` - Schema version
- `config` - Sample questions, settings
- `data_sources` - Tables available to the space
- `instructions` - Context and rules for Genie

This field is portable and passed directly to `create_space()` or `update_space()`.

### Deployment Behavior

| Scenario                          | Behavior                          |
|-----------------------------------|-----------------------------------|
| No `--var target_space_id`        | Creates new space, outputs new ID |
| With `--var target_space_id=<id>` | Updates existing space            |

**Note:** Target workspace gets a **different space_id** than source. Save the new ID for subsequent deployments.

## Known Limitations

| Limitation              | Impact                                               |
|-------------------------|------------------------------------------------------|
| No conversation history | Conversations are workspace-specific                 |
| Permissions separate    | Unity Catalog grants must be configured separately   |
| Table names unchanged   | `serialized_space` deployed as-is; edit JSON before commit if catalogs differ |

## References

- [Genie API Documentation](https://docs.databricks.com/aws/en/genie/conversation-api)
- [DABs Authentication (Azure)](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/authentication)
- [Databricks Python SDK - Genie](https://databricks-sdk-py.readthedocs.io/en/stable/workspace/dashboards/genie.html)
- [GitHub Issue #3008 - DABs Genie Support](https://github.com/databricks/cli/issues/3008)

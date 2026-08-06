# ⚠️ Deprecated: Genie Space Migration

This example has been **deprecated** and is no longer maintained.

## Recommended replacement

Use the maintained, standalone reference implementation instead:

> **[anhhchu/genie-agent-cicd](https://github.com/anhhchu/genie-agent-cicd)** —
> Reference implementation for managing a Databricks Genie Agent and Unity Catalog
> metric view as code using Databricks Asset Bundles (DABs).

**Blog post:** [anhcodes.dev/blog/genie-agent-cicd](https://anhcodes.dev/blog/genie-agent-cicd/)

## Why the switch?

This example was originally a **workaround**: at the time it was written,
Databricks Asset Bundles did not natively support Genie Spaces
([databricks/cli#3008](https://github.com/databricks/cli/issues/3008)). It worked
around that gap by calling the Genie Management APIs (`get_space` /
`create_space` / `update_space`) directly from notebook jobs to export and
re-import Genie Space configurations as `serialized_space` JSON.

Native DABs support for Genie has since shipped. The replacement project uses the
supported `databricks bundle generate genie-space` flow and provides a more
complete pattern — managing both the **Genie Agent** *and* its backing
**UC metric view** as code, with multi-environment promotion (`dev` → `prod`).

| This example (deprecated)                                  | [genie-agent-cicd](https://github.com/anhhchu/genie-agent-cicd) (recommended) |
|------------------------------------------------------------|-------------------------------------------------------------------------------|
| Genie Spaces migrated via Genie Management APIs            | Genie Agent managed natively via DABs resource definitions                    |
| `serialized_space` JSON exported/imported by hand          | `databricks bundle generate genie-space` + committed `*.geniespace.json`      |
| Export + deploy jobs run as notebooks                      | Declarative `databricks bundle deploy` with `--target` per environment        |
| No metric view management                                   | UC metric view managed as code (`src/metric-view.yaml`)                       |
| Single export/deploy workflow                               | Full CI/CD reference with code review and reproducible deploys                 |

## Archived code

The original implementation has been preserved under [`archive/`](./archive/) for
historical reference. It is **not maintained** and may break against newer versions
of the Databricks CLI / SDK.

- [`archive/README.md`](./archive/README.md) — original runbook and workflow diagrams
- [`archive/scripts/`](./archive/scripts/) — `export_genie_space.py` / `deploy_genie_space.py` notebooks
- [`archive/docs/SP_SETUP_GUIDE.md`](./archive/docs/SP_SETUP_GUIDE.md) — service principal provisioning and permissions
- [`archive/databricks.yml.template`](./archive/databricks.yml.template) — bundle template
- [`archive/genie_spaces/`](./archive/genie_spaces/) — sample exported JSON template

## Migration notes

If you are currently using this example:

1. Adopt [genie-agent-cicd](https://github.com/anhhchu/genie-agent-cicd) for new work.
2. Import each existing Genie Space into the new bundle with the native generator:

   ```bash
   databricks bundle generate genie-space \
     --existing-id <SPACE_ID> \
     --key <your_space_name>
   ```

   This produces `src/<key>.geniespace.json` and `resources/<key>.genie_space.yml`,
   which replace the hand-exported `serialized_space` JSON used here.

3. Recreate any custom service principal provisioning steps from
   [`archive/docs/SP_SETUP_GUIDE.md`](./archive/docs/SP_SETUP_GUIDE.md) as needed —
   the new bundle uses standard `databricks auth` / bundle targets instead of
   per-job `run_as` service principals.

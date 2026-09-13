# Managed Memory for Agents — Support Copilot Demo

This example shows how a Databricks support copilot gains memory in layers, using
**Managed agent memory** (Beta) as the only durable store — no custom memory
infrastructure, vector database, or memory framework.

The demo builds one copilot across four memory types:

| Phase | Memory type | Durability |
| --- | --- | --- |
| 1 | Working memory (short-term context) | None — client-side only |
| 2 | Episodic memory (support cases) | Per-case conversations |
| 3 | Semantic memory (customer preferences) | Cross-case entries |
| 4 | Procedural memory (shared playbooks) | Team-scoped entries |

Phases 1 and 2 (this folder's current state) cover working and episodic memory:
why a stateless agent fails on follow-up questions, that a client-managed history
list only survives the session, and that Managed Memory conversations and entries
give support cases durable, resumable history — including recall of a resolved
case from a later one.

## Notebooks

| Notebook | What it shows |
| --- | --- |
| `00_setup_foundations` | Idempotent setup: memory store, synthetic `support_orders` Delta table, read-only `lookup_order` tool, and a `chat` helper. |
| `01_stateless_baseline` | A stateless copilot answers a status question, then fails on a follow-up that references "that order". |
| `02_client_managed_working_memory` | The client keeps a `messages` list: follow-ups work within the session, then fail again once the list is cleared. |
| `03_episodic_memory` | One managed conversation per support case: history survives client recreation, resolved cases are saved as entries, and a new case recalls a prior case summary. |

## Prerequisites

- A Databricks workspace with Unity Catalog and the **Managed agent memory** preview
  (Beta) enabled — see [Manage Databricks previews](https://docs.databricks.com/aws/en/release-notes/previews).
- `CREATE MEMORY STORE` privilege on the target schema (default `main.default`). If you
  override the `schema` widget to a schema that does not exist yet, you also need
  `CREATE SCHEMA` on the catalog.
- Can-query access to a pay-per-token foundation model endpoint. The demo defaults to
  `databricks-glm-5-3-flash` (Zhipu GLM-5.3-Flash); change the widget in
  `00_setup_foundations` to use any OpenAI-compatible chat-completions endpoint.
- Databricks Runtime 15.4+ (all Python dependencies are preinstalled).

## How to run

1. Import this folder into a Databricks workspace (Repos or folder import).
2. Run `00_setup_foundations` once (it is safe to rerun).
3. Run `01_stateless_baseline`, then `02_client_managed_working_memory`, then
   `03_episodic_memory`. Notebooks 01–03 are also safe to rerun.

All resource names are configurable through the widgets in `00_setup_foundations`;
all data is synthetic. See `PLAN.md` for the full phased roadmap.

## Documentation

- [Managed agent memory](https://docs.databricks.com/aws/en/agents/agent-memory/managed-memory)
- [Memory API reference](https://docs.databricks.com/aws/en/agents/agent-memory/memory-store-api)
- [Foundation Model APIs](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis/)

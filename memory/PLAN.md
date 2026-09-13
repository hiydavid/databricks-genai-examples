# Managed Memory Demo — Phased Implementation

## Status

- **Phase 1: COMPLETE** (commit `1c1eea4`, reviewed and fixed 2026-09-12).
  Deliverables: `README.md`, `AGENTS.md`, `requirements.txt`,
  `00_setup_foundations`, `01_stateless_baseline`,
  `02_client_managed_working_memory`. Post-review fixes: memory-store create
  body uses `description` (not `comment`), and `CREATE SCHEMA IF NOT EXISTS`
  moved ahead of memory-store creation so a fresh `schema` widget value works.
- **Phase 2: COMPLETE** (2026-09-12). Deliverable: `03_episodic_memory`
  (REST helper + per-case conversations + case-summary entries). README and
  AGENTS updated; no new dependencies.
- **Phase 3: NEXT** — start here in a new session.
- Phases 4–5: not started.

Conventions, hard rules, and run order live in `AGENTS.md`; the user-facing
overview is `README.md`. Both are current as of Phase 2.

## Phase 1: Foundation and Short-Term Context — COMPLETE

Build the shared foundation and demonstrate why memory is needed.

- Add a concise README, local guidance, dependencies, and setup notebook.
- Create the Managed Memory store and synthetic `support_orders` Delta table idempotently.
- Implement the read-only `lookup_order(order_id)` tool.
- Add the stateless and client-managed working-memory notebooks.
- Verify that history works within a session but disappears when cleared.

## Phase 2: Episodic Memory — COMPLETE

Added durable support-case continuity.

- Added the minimal Managed Memory REST helper.
- Created one managed conversation per support case.
- Resumed a case after client recreation using its conversation ID.
- Saved resolved case summaries under `/memories/cases/{case_id}.md`.
- Retrieved a previous case summary from a new case conversation.
- Clarified that conversations preserve one case; entries enable cross-case recall.

Verified API facts (re-checked against the full HTML of
`docs.databricks.com/aws/en/agents/agent-memory/memory-store-api` on
2026-09-12, including the curl examples and field tables):

- No Python SDK for these APIs; `w.api_client.do(...)` supports a `query`
  dict for query parameters (verified against databricks-sdk 0.67).
- Conversations: create `POST /api/2.1/unity-catalog/conversations` with body
  `{"memory_store": {"name": <3-part name>}, "scope": {"kind", "value"},
  "metadata"?, "items"?}` — `kind` is e.g. `user` or `user_defined`; `items`
  seeds up to 20 initial items and "items without a type are stored as message
  items". Update = `POST .../conversations/{id}` with `{"metadata": ...}`
  (replaces metadata). Item list is OpenAI-compatible: `GET
  .../conversations/{id}/items?limit=&order=&after=` returning `data`,
  `last_id`, `has_more`.
- Conversation items: OpenAI shape — add via
  `POST .../conversations/{id}/items` with body `{"items": [{"role",
  "content"}, ...]}`; returned items may carry `type` and content parts
  (`{"type": "input_text"/"output_text", "text"}`).
- Entries: create `POST .../memory-stores/{full_name}/entries?scope=<scope>`
  with body `{path, contents, description?}` (path must start `/memories/`);
  get `GET .../entries:get?scope=&path=`; list `GET .../entries?scope=` (omits
  `contents`, supports `path_prefix`, `page_size`, `page_token`); update
  `PATCH .../entries` with body `{scope, path, one of str_replace|insert|
  replace_all, description?}`; delete `DELETE .../entries?scope=&path=`;
  search `POST .../entries:search` with `{scope, query, path_prefix?, top_k?}`
  (keyword-based across path, contents, description; top_k default 10, max 50).
- Entry scopes are plain strings (the partition); conversation scopes are
  `{kind, value}` objects.

Phase 3 implementation notes for the next session:

- The REST helper lives in `03_episodic_memory` (conversation_* + entry_create /
  entry_get / entry_upsert). Promote it into a shared location (still inside
  this folder) when Phase 3 needs entries too, and extend it with entry_list,
  entry_search, entry_delete, and str_replace updates.
- Scope naming so far: conversations `{"kind": "user_defined", "value":
  <customer_id>}`, entries scope = `<customer_id>`, case summaries under
  `/memories/cases/{case_id}.md`. Phase 3 picks and documents the default
  scope convention (customer vs trusted team scopes — the managed-memory doc
  recommends setting scopes in trusted application code, never model-chosen).
- Notebook 03 could not be run against a live preview workspace locally; its
  first live run should be verified on a preview-enabled workspace (Phase 5
  covers the full verification pass).

## Phase 3: Semantic Memory

Add durable customer preferences.

- Store `/memories/preferences.md` under the trusted customer scope.
- Demonstrate save, get, list, keyword search, update, and delete.
- Retrieve preferences from an unrelated new case.
- Add an optional extraction example that proposes—but does not automatically persist—a memory.
- Highlight that Managed Memory search is keyword-based, not vector semantic retrieval.

## Phase 4: Procedural Memory

Add shared support procedures and the complete copilot flow.

- Store an approved delayed-replacement playbook under a controlled team scope.
- Retrieve the current order through `lookup_order`.
- Retrieve and apply the shared playbook.
- Return "Recommended actions" and a "Draft response to the customer."
- Explain that Managed Memory stores procedures but does not score, decay, prune, or learn them.

## Phase 5: Validation and Documentation

Finish the example without adding deployment or custom memory infrastructure.

- Add unit tests for REST paths, pagination, updates, and scope enforcement.
- Compile all exported Python notebooks and helpers.
- Add disabled-by-default cleanup for the exact demo resources.
- Verify the full notebook run order on a preview-enabled workspace.
- Keep notebook markdown short and link to Databricks documentation instead of reproducing it.
- Document Beta requirements, trusted scopes, keyword-search limitations, and when custom memory infrastructure becomes necessary.

## Phase Completion Criteria

Each phase must:

- Run after the preceding phase without manual code changes beyond configuration.
- Use only synthetic data and configurable resource names.
- Keep all changes inside `/memory`.
- Preserve Databricks notebook cell markers.
- Avoid secrets, workspace URLs, emails, and customer identifiers.
- Leave the repository in a usable state before the next phase begins.

# Managed Memory Demo — Phased Implementation

## Status

- **Phase 1: COMPLETE** (commit `1c1eea4`, reviewed and fixed 2026-09-12).
  Deliverables: `README.md`, `AGENTS.md`, `requirements.txt`,
  `00_setup_foundations`, `01_stateless_baseline`,
  `02_client_managed_working_memory`. Post-review fixes: memory-store create
  body uses `description` (not `comment`), and `CREATE SCHEMA IF NOT EXISTS`
  moved ahead of memory-store creation so a fresh `schema` widget value works.
- **Phase 2: NEXT** — start here in a new session.
- Phases 3–5: not started.

Conventions, hard rules, and run order live in `AGENTS.md`; the user-facing
overview is `README.md`. Both are current as of Phase 1.

## Phase 1: Foundation and Short-Term Context — COMPLETE

Build the shared foundation and demonstrate why memory is needed.

- Add a concise README, local guidance, dependencies, and setup notebook.
- Create the Managed Memory store and synthetic `support_orders` Delta table idempotently.
- Implement the read-only `lookup_order(order_id)` tool.
- Add the stateless and client-managed working-memory notebooks.
- Verify that history works within a session but disappears when cleared.

## Phase 2: Episodic Memory

Add durable support-case continuity.

- Add the minimal Managed Memory REST helper.
- Create one managed conversation per support case.
- Resume a case after client recreation using its conversation ID.
- Save resolved case summaries under `/memories/cases/{case_id}.md`.
- Retrieve a previous case summary from a new case conversation.
- Clarify that conversations preserve one case; entries enable cross-case recall.

Verified API facts (checked against
`docs.databricks.com/aws/en/agents/agent-memory/memory-store-api` on
2026-09-12 — re-verify before coding, the feature is Beta):

- There is no Python SDK for these APIs; use `WorkspaceClient().api_client.do(...)`
  as `00_setup_foundations` already does for the store.
- Conversations: create `POST /api/2.1/unity-catalog/conversations`,
  get `GET .../conversations/{conversation_id}`,
  update `POST .../conversations/{conversation_id}`,
  delete `DELETE .../conversations/{conversation_id}`,
  add items `POST .../conversations/{conversation_id}/items`,
  get/list items `GET .../conversations/{conversation_id}/items[/{item_id}]`,
  delete item `DELETE .../conversations/{conversation_id}/items/{item_id}`.
  All require `WRITE MEMORY STORE` (create/update/delete) or
  `READ MEMORY STORE` (read) on the target store — the conversation body
  references the memory store; check the API reference for the exact
  create-conversation body fields before writing the helper.
- Entries (used from Phase 3 on, listed for context): create
  `POST /api/2.1/unity-catalog/memory-stores/{full_name}/entries`,
  get `GET .../entries:get`, list `GET .../entries`,
  search `POST .../entries:search`.

Phase 2 implementation notes for the next session:

- New notebook `03_episodic_memory`, run after `02`; it gets configuration via
  `%run ./00_setup_foundations` (exposes `w`, `chat`, `lookup_order`,
  `MEMORY_STORE_NAME`, `json`).
- Keep the REST helper in a cell inside `03` for now; promote it to a shared
  helper only when Phase 3 needs it too.
- Names so far: store `support_agent_memory`, table `support_orders`,
  default scope naming starts in Phase 3 — pick and document it there.

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

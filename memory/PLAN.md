# Managed Memory Demo — Phased Implementation

## Phase 1: Foundation and Short-Term Context

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

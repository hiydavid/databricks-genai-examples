# Databricks notebook source

# MAGIC %md
# MAGIC # 03 — Episodic memory: support cases that survive the client
# MAGIC
# MAGIC Notebook 02 ended with the problem: a client-held `messages` list gives
# MAGIC continuity inside one session and nothing more. **Episodic memory** fixes that —
# MAGIC a durable record of *what happened in this support case*, stored server-side.
# MAGIC
# MAGIC Managed Memory gives this two pieces:
# MAGIC
# MAGIC 1. **A managed conversation per support case** — every turn is persisted to the
# MAGIC    memory store under the customer's scope. A new client resumes the case from
# MAGIC    its conversation ID alone.
# MAGIC 2. **A memory entry per resolved case** — once a case closes, its summary is
# MAGIC    saved as an entry (`/memories/cases/{case_id}.md`), so a *different*
# MAGIC    conversation can recall what happened in an earlier case.
# MAGIC
# MAGIC This is the pattern to watch: conversations preserve a single case; entries
# MAGIC enable recall across cases.

# COMMAND ----------

# MAGIC %run ./00_setup_foundations

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Managed Memory REST helper
# MAGIC
# MAGIC There is no Python SDK for these APIs yet, so the helper wraps
# MAGIC `WorkspaceClient().api_client.do(...)` — the same approach `00_setup_foundations`
# MAGIC uses for the memory store. Two surfaces matter here:
# MAGIC
# MAGIC - **Conversations** — created against the memory store and pinned to a scope
# MAGIC   (`{"kind": ..., "value": ...}`). Items follow the OpenAI conversation-item
# MAGIC   shape and list with OpenAI-compatible pagination (`after`, `limit`,
# MAGIC   `has_more`).
# MAGIC - **Entries** — identified by a plain-string `scope` (the partition, here the
# MAGIC   customer) plus a `path` that must start with `/memories/`.
# MAGIC
# MAGIC Full reference: [Memory API reference](https://docs.databricks.com/aws/en/agents/agent-memory/memory-store-api) (Beta — re-verify when upgrading).

# COMMAND ----------

from databricks.sdk.errors import DatabricksError

_CONVERSATIONS = "/api/2.1/unity-catalog/conversations"
_ENTRIES = f"/api/2.1/unity-catalog/memory-stores/{MEMORY_STORE_NAME}/entries"


def _do_or_none(method: str, path: str, **kwargs):
    """Run a REST call, returning None when the target does not exist."""
    try:
        return w.api_client.do(method, path, **kwargs)
    except DatabricksError as e:
        if getattr(e, "error_code", "") in ("RESOURCE_DOES_NOT_EXIST", "NOT_FOUND"):
            return None
        raise


def conversation_create(scope_kind: str, scope_value: str, metadata: dict = None) -> dict:
    """Create a conversation backed by the demo memory store, pinned to a scope."""
    body = {
        "memory_store": {"name": MEMORY_STORE_NAME},
        "scope": {"kind": scope_kind, "value": scope_value},
    }
    if metadata:
        body["metadata"] = metadata
    return w.api_client.do("POST", _CONVERSATIONS, body=body)


def conversation_get(conversation_id: str) -> dict:
    return _do_or_none("GET", f"{_CONVERSATIONS}/{conversation_id}")


def conversation_update(conversation_id: str, metadata: dict) -> dict:
    """Replace the conversation's caller-controlled metadata."""
    return w.api_client.do(
        "POST", f"{_CONVERSATIONS}/{conversation_id}", body={"metadata": metadata}
    )


def conversation_items_add(conversation_id: str, items: list) -> dict:
    """Persist OpenAI-shaped conversation items (up to 20 per call)."""
    return w.api_client.do(
        "POST", f"{_CONVERSATIONS}/{conversation_id}/items", body={"items": items}
    )


def conversation_items_list(conversation_id: str, limit: int = 100) -> list:
    """All items of a conversation in creation order, following cursor pagination."""
    items, after = [], None
    while True:
        query = {"limit": limit, "order": "asc"}
        if after:
            query["after"] = after
        page = w.api_client.do(
            "GET", f"{_CONVERSATIONS}/{conversation_id}/items", query=query
        )
        data = page.get("data") or []
        items.extend(data)
        after = page.get("last_id")
        if not page.get("has_more") or not after or not data:
            return items


def entry_create(scope: str, path: str, contents: str, description: str = None) -> dict:
    """Create a memory entry in the given scope. Paths must start with /memories/."""
    body = {"path": path, "contents": contents}
    if description:
        body["description"] = description
    return w.api_client.do("POST", _ENTRIES, query={"scope": scope}, body=body)


def entry_get(scope: str, path: str) -> dict:
    return _do_or_none("GET", f"{_ENTRIES}:get", query={"scope": scope, "path": path})


def entry_upsert(scope: str, path: str, contents: str, description: str = None) -> dict:
    """Create the entry, or replace its contents if it already exists.

    Get-first keeps notebook reruns idempotent without guessing the
    conflict-error code of a Beta API.
    """
    if entry_get(scope, path) is None:
        return entry_create(scope, path, contents, description)
    body = {"scope": scope, "path": path, "replace_all": {"contents": contents}}
    if description:
        body["description"] = description
    return w.api_client.do("PATCH", _ENTRIES, body=body)

# COMMAND ----------

# MAGIC %md
# MAGIC ## The case session
# MAGIC
# MAGIC The scenario: customer **Priya Nair** (`CUST-002`) opens a case about her delayed
# MAGIC USB-C dock (order `SO-1003`, on warehouse backorder). One managed conversation
# MAGIC is created per case and pinned to the customer's scope; the entry scope is the
# MAGIC same customer, so case summaries land in the customer's memory partition. Scope
# MAGIC naming gets formalized in Phase 3.
# MAGIC
# MAGIC The system prompt stays client-side — it is configuration, not case history. The
# MAGIC `messages` list below is only a local cache for the model call; the source of
# MAGIC truth is the managed conversation.

# COMMAND ----------

CASE_SYSTEM_PROMPT = """You are a support copilot for an online store.
Answer customer questions about orders using only the order data and case
context provided in this conversation. If the order data you need is not in
this conversation, say you don't know and ask the customer for the missing
detail. Never guess."""

CUSTOMER_ID = "CUST-002"
CUSTOMER_NAME = "Priya Nair"
CASE_ID = "CASE-2025-0141"

# Conversations take an object scope; entries take a plain-string scope.
CONVERSATION_SCOPE = {"kind": "user_defined", "value": CUSTOMER_ID}
ENTRY_SCOPE = CUSTOMER_ID


def new_case_session(case_id: str) -> dict:
    """Open a support case: one managed conversation, case metadata attached."""
    conv = conversation_create(
        CONVERSATION_SCOPE["kind"],
        CONVERSATION_SCOPE["value"],
        metadata={"case_id": case_id, "customer_id": CUSTOMER_ID},
    )
    print(f"opened {case_id} -> conversation {conv['id']}")
    return {
        "case_id": case_id,
        "conversation_id": conv["id"],
        "messages": [{"role": "system", "content": CASE_SYSTEM_PROMPT}],
    }


def case_ask(case: dict, question: str, order_id: str = None, context: str = None) -> str:
    """One turn: optional order lookup and prior-case context, reply persisted
    to the managed conversation alongside the user message."""
    content = question
    if order_id is not None:
        content = (
            f"Order data from lookup_order:\n"
            f"{json.dumps(lookup_order(order_id), indent=2)}\n\nQuestion: {question}"
        )
    if context is not None:
        content = f"{context}\n\n{content}"

    reply = chat(case["messages"] + [{"role": "user", "content": content}])
    case["messages"].append({"role": "user", "content": content})
    case["messages"].append({"role": "assistant", "content": reply})
    conversation_items_add(
        case["conversation_id"],
        [
            {"role": "user", "content": content},
            {"role": "assistant", "content": reply},
        ],
    )
    return reply

# COMMAND ----------

# MAGIC %md
# MAGIC ## Session 1: two turns, history stored server-side
# MAGIC
# MAGIC Every turn is mirrored into the managed conversation — question, embedded order
# MAGIC data, and reply — so the case history accumulates in the memory store, not just
# MAGIC in this notebook's process.

# COMMAND ----------

case = new_case_session(CASE_ID)

question_1 = "Where is my USB-C dock? It's been two weeks."
answer_1 = case_ask(case, question_1, order_id="SO-1003")
print(f"Customer: {question_1}\n")
print(f"Copilot : {answer_1}\n")

question_2 = "What did it cost again?"
answer_2 = case_ask(case, question_2)
print(f"Customer: {question_2}\n")
print(f"Copilot : {answer_2}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The client is recreated — the case is not
# MAGIC
# MAGIC In notebook 02 this was the failure point: clearing the `messages` list lost
# MAGIC everything. Here the only value that needs to survive the client is the
# MAGIC **conversation ID** — the thing a real deployment would store in its ticketing
# MAGIC system. Everything else is rebuilt from the memory store: list the
# MAGIC conversation's items, convert them back into chat messages, and continue.

# COMMAND ----------

def _item_text(item: dict) -> str:
    """Extract plain text from an OpenAI conversation item (string or parts list)."""
    content = item.get("content", "")
    if isinstance(content, str):
        return content
    return "\n".join(
        part.get("text", "") for part in content if isinstance(part, dict)
    )


def resume_case_session(conversation_id: str, case_id: str) -> dict:
    """Rebuild a case session from server-side items after client recreation."""
    items = conversation_items_list(conversation_id)
    messages = [{"role": "system", "content": CASE_SYSTEM_PROMPT}]
    for item in items:
        if item.get("type", "message") != "message":
            continue  # tool calls and other item types are not chat turns here
        role = item.get("role")
        if role in ("user", "assistant", "system"):
            messages.append({"role": role, "content": _item_text(item)})
    print(f"resumed {case_id}: {len(items)} items restored from the memory store")
    return {
        "case_id": case_id,
        "conversation_id": conversation_id,
        "messages": messages,
    }


# Everything below this line knows only the conversation ID.
CONVERSATION_ID = case["conversation_id"]

case = resume_case_session(CONVERSATION_ID, CASE_ID)

question_3 = "What was the new ETA you mentioned for the dock?"
answer_3 = case_ask(case, question_3)
print(f"\nCustomer: {question_3}\n")
print(f"Copilot : {answer_3}")
print("\nThe copilot answered from server-side history — the local session was rebuilt, not remembered.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resolve the case: summarize and store it as an entry
# MAGIC
# MAGIC The conversation holds the full transcript — useful while the case is alive,
# MAGIC too heavy to replay into every future case. When the case resolves, condense
# MAGIC it into a summary and store it as a **memory entry** under
# MAGIC `/memories/cases/{case_id}.md` in the customer's scope. The save is an upsert
# MAGIC (get-first, then create or `replace_all`), so reruns stay idempotent. The
# MAGIC conversation's metadata is stamped `resolved` to close the loop.

# COMMAND ----------

CASE_SUMMARY_PATH = f"/memories/cases/{CASE_ID}.md"

summary = chat(
    [
        {
            "role": "system",
            "content": (
                "You summarize support cases. Produce a compact case record in "
                "markdown with: customer, order, problem, outcome, key dates. "
                "No preamble."
            ),
        },
        {
            "role": "user",
            "content": "\n\n".join(
                m["content"] for m in case["messages"] if m["role"] != "system"
            ),
        },
    ]
)
print(summary)

entry_upsert(
    ENTRY_SCOPE,
    CASE_SUMMARY_PATH,
    summary,
    description=f"{CASE_ID}: delayed order SO-1003, backorder with new ETA (resolved)",
)
conversation_update(
    CONVERSATION_ID,
    {"case_id": CASE_ID, "customer_id": CUSTOMER_ID, "status": "resolved"},
)
print(f"\nsaved case summary to {CASE_SUMMARY_PATH} (scope '{ENTRY_SCOPE}')")

# COMMAND ----------

# MAGIC %md
# MAGIC ## A new case recalls an old one
# MAGIC
# MAGIC Priya opens a second case about a different order (`SO-1004`, laptop stand).
# MAGIC A new case gets a **new conversation** — conversations track one case, and
# MAGIC continuing the old one would mix two cases' history. Cross-case recall works
# MAGIC through the entry layer: fetch the previous case's summary by scope and path,
# MAGIC and hand it to the copilot as context for the new conversation.

# COMMAND ----------

NEXT_CASE_ID = "CASE-2025-0142"

next_case = new_case_session(NEXT_CASE_ID)

prior_summary = entry_get(ENTRY_SCOPE, CASE_SUMMARY_PATH)
print(f"retrieved prior case summary ({CASE_SUMMARY_PATH}):\n\n{prior_summary['contents']}")

question_4 = (
    "My laptop stand hasn't shipped either. Is this going to be another "
    "repeat of the dock situation?"
)
answer_4 = case_ask(
    next_case,
    question_4,
    order_id="SO-1004",
    context=(
        f"Summary of this customer's previous support case {CASE_ID}, "
        f"retrieved from memory:\n{prior_summary['contents']}"
    ),
)
print(f"\nCustomer: {question_4}\n")
print(f"Copilot : {answer_4}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What this establishes
# MAGIC
# MAGIC - **Conversations give a case durable history.** Turns persisted to the memory
# MAGIC   store survive client recreation; resuming needs only the conversation ID.
# MAGIC - **Entries give cases cross-case recall.** A resolved case becomes a compact,
# MAGIC   scoped record that any later conversation can retrieve and use as context.
# MAGIC - **The copilot did not remember by itself.** The application fetched the entry
# MAGIC   and passed it in — Managed Memory stores and partitions memory; the recall
# MAGIC   logic is the app's. (Entry search arrives in Phase 3.)
# MAGIC - Case summaries are markdown under `/memories/cases/`, scoped per customer —
# MAGIC   the naming convention that Phase 3 extends to preferences.
# MAGIC
# MAGIC **Next:** Phase 3 adds **semantic memory** — durable customer preferences as
# MAGIC entries, with list, keyword search, update, and delete.

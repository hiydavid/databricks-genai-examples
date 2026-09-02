# Databricks notebook source

# MAGIC %md
# MAGIC # 02 — Client-managed working memory: history works … until it doesn't
# MAGIC
# MAGIC The simplest fix for the stateless baseline: the **client** keeps the conversation
# MAGIC as a running `messages` list and sends the whole list on every call. That list is
# MAGIC the agent's *working memory* (short-term context).
# MAGIC
# MAGIC This notebook shows both sides of the pattern:
# MAGIC 1. With the history list, a follow-up question works — the copilot remembers
# MAGIC    which order "that order" refers to.
# MAGIC 2. Clear the list (a new client / a restarted session), and the memory is gone.
# MAGIC    Working memory lives only in the client process; nothing durable was written.

# COMMAND ----------

# MAGIC %run ./00_setup_foundations

# COMMAND ----------

SYSTEM_PROMPT = """You are a support copilot for an online store.
Answer customer questions about orders using only the order data provided
earlier in this conversation. If the order data you need is not in this
conversation, say you don't know and ask the customer for the missing
detail. Never guess."""

# Start the session with an empty history: just the system prompt.
messages = [{"role": "system", "content": SYSTEM_PROMPT}]


def ask(question: str, order_id: str = None) -> str:
    """One turn: optionally look up an order, then ask and record both sides."""
    content = question
    if order_id is not None:
        order_data = lookup_order(order_id)
        content = f"Order data from lookup_order:\n{json.dumps(order_data, indent=2)}\n\nQuestion: {question}"
    messages.append({"role": "user", "content": content})
    reply = chat(messages)
    messages.append({"role": "assistant", "content": reply})
    return reply


# COMMAND ----------

# MAGIC %md
# MAGIC ## Turn 1: same status question as the baseline
# MAGIC
# MAGIC `ask` appends the user message (with the tool result) to `messages`, calls the
# MAGIC model with the full list, and appends the reply. The history starts growing.

# COMMAND ----------

question_1 = "What's the status of my order SO-1001? When will it arrive?"
answer_1 = ask(question_1, order_id="SO-1001")

print(f"Customer: {question_1}\n")
print(f"Copilot : {answer_1}\n")
print(f"messages list now has {len(messages)} entries (system + user + assistant)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Turn 2: the follow-up that failed before
# MAGIC
# MAGIC No order ID, no tool call — just the question, appended to the same history.

# COMMAND ----------

question_2 = "What was the total on that order?"
answer_2 = ask(question_2)

print(f"Customer: {question_2}\n")
print(f"Copilot : {answer_2}\n")
print("The copilot remembered the order from turn 1 — it was in the messages list.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Now clear the history — the session ends
# MAGIC
# MAGIC The `messages` list only existed in this notebook's process. Recreate the client
# MAGIC (new browser tab, restarted server, next day at work) and the list is back to
# MAGIC just the system prompt. Simulate that by resetting it, then re-ask the same
# MAGIC follow-up.

# COMMAND ----------

# A new session: fresh client, empty history.
messages = [{"role": "system", "content": SYSTEM_PROMPT}]

question_3 = "What was the total on that order?"
answer_3 = ask(question_3)

print(f"Customer: {question_3}\n")
print(f"Copilot : {answer_3}\n")
print("Without the history, the copilot cannot know which order was discussed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What this establishes
# MAGIC
# MAGIC - **Working memory works within a session.** Keeping the `messages` list and
# MAGIC   resending it gives continuity between turns.
# MAGIC - **It disappears when the session ends.** The list lives in client memory
# MAGIC   only — nothing was persisted, and a new session starts blank.
# MAGIC - Resending ever-growing history also does not scale: long sessions inflate
# MAGIC   cost and latency, and eventually hit context limits.
# MAGIC
# MAGIC **What's needed is durable memory** that survives sessions and can be shared
# MAGIC across them. That is exactly what Managed Memory conversations provide — the
# MAGIC next phase (`PLAN.md`, Phase 2) stores conversation state server-side under a
# MAGIC memory store and scope, so a support case resumes even after the client is
# MAGIC recreated.

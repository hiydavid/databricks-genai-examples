# Databricks notebook source

# MAGIC %md
# MAGIC # 01 — The stateless baseline: why the copilot needs memory
# MAGIC
# MAGIC An LLM has **no built-in memory**. Every API call is independent: whatever the
# MAGIC model said or was told on a previous call is gone. This notebook shows that
# MAGIC failure mode directly.
# MAGIC
# MAGIC The setup: a support copilot answers customer questions using `lookup_order`.
# MAGIC Here the copilot is *stateless* — each turn sends only the system prompt, the
# MAGIC tool result for the current question, and the question itself. Watch the second
# MAGIC turn fail.

# COMMAND ----------

# MAGIC %run ./00_setup_foundations

# COMMAND ----------

# MAGIC %md
# MAGIC ## The copilot's system prompt

# COMMAND ----------

SYSTEM_PROMPT = """You are a support copilot for an online store.
Answer customer questions about orders using only the order data provided.
If the order data you need is not in this conversation, say you don't know
and ask the customer for the missing detail. Never guess."""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Turn 1: a simple status question
# MAGIC The customer asks about order `SO-1001`. We call `lookup_order` ourselves and
# MAGIC hand the result to the model alongside the question.

# COMMAND ----------

question_1 = "What's the status of my order SO-1001? When will it arrive?"
order_data = lookup_order("SO-1001")

messages_turn_1 = [
    {"role": "system", "content": SYSTEM_PROMPT},
    {
        "role": "user",
        "content": f"Order data from lookup_order:\n{json.dumps(order_data, indent=2)}\n\nQuestion: {question_1}",
    },
]

answer_1 = chat(messages_turn_1)
print(f"Customer: {question_1}\n")
print(f"Copilot : {answer_1}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Turn 2: a follow-up question
# MAGIC Seconds later, the customer asks a natural follow-up: *"What was the total on
# MAGIC that order?"* — no order ID, because they already gave it. A stateless copilot
# MAGIC makes a fresh API call with no access to turn 1.

# COMMAND ----------

question_2 = "What was the total on that order?"

messages_turn_2 = [
    {"role": "system", "content": SYSTEM_PROMPT},
    {"role": "user", "content": question_2},
]

answer_2 = chat(messages_turn_2)
print(f"Customer: {question_2}\n")
print(f"Copilot : {answer_2}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What just happened
# MAGIC
# MAGIC Turn 1 worked because everything the model needed was in the request. Turn 2
# MAGIC failed because the model was never told which order "that order" refers to — the
# MAGIC order ID, the status lookup, everything from turn 1 simply does not exist in the
# MAGIC new request. A well-tuned model refuses to guess, which is the best failure you can
# MAGIC hope for; a worse-tuned model would hallucinate an order. (The system prompt leans
# MAGIC heavily on the model here — check the printed output above to see how yours behaves.)
# MAGIC
# MAGIC **The fix is working memory** — keeping the recent conversation history and
# MAGIC letting the model see it. Run `02_client_managed_working_memory` next.

# Databricks notebook source
# MAGIC %md
# MAGIC # Async Research Job
# MAGIC
# MAGIC This notebook is executed by Databricks Lakeflow Job to perform research asynchronously.
# MAGIC It receives a research plan via job parameters and saves results to UC Volume.

# COMMAND ----------

# MAGIC %pip install databricks-sdk databricks-mcp openai pydantic pyyaml nest_asyncio --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
import sys
import os
import yaml
import nest_asyncio

# Enable nested event loops (required for MCP client in notebooks)
nest_asyncio.apply()

# Auto-detect src path from notebook location
SRC_PATH = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.insert(0, SRC_PATH)

# Load configuration
config_path = os.path.join(SRC_PATH, "config.yaml")
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

print(f"Loaded config from: {config_path}")

# COMMAND ----------

# Get research plan from job parameters
research_plan_b64 = dbutils.widgets.get("research_plan_b64")
print(f"Received research plan (base64 length: {len(research_plan_b64)})")

# COMMAND ----------

# Parse the research plan
from models.research_plan import ResearchPlan

plan = ResearchPlan.from_base64(research_plan_b64)

print(f"Research Topic: {plan.topic}")
print(f"Questions: {len(plan.research_questions)}")
for i, q in enumerate(plan.research_questions, 1):
    print(f"  {i}. {q}")
print(f"Output Path: {plan.full_output_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC Update these values for your environment:

# COMMAND ----------

# Configuration - loaded from config.yaml
LLM_ENDPOINT = config["llm"]["endpoint_name"]
MCP_CATALOG = config["mcp"]["catalog"]
MCP_SCHEMA = config["mcp"]["schema"]

print(f"LLM Endpoint: {LLM_ENDPOINT}")
print(f"MCP: {MCP_CATALOG}.{MCP_SCHEMA}")

# COMMAND ----------

# Initialize the researcher agent
from databricks.sdk import WorkspaceClient
from researcher_agent import ResearcherAgent

ws = WorkspaceClient()
mcp_url = f"{ws.config.host}/api/2.0/mcp/functions/{MCP_CATALOG}/{MCP_SCHEMA}"

print(f"Initializing ResearcherAgent...")
print(f"  LLM Endpoint: {LLM_ENDPOINT}")
print(f"  MCP URL: {mcp_url}")

researcher = ResearcherAgent(
    llm_endpoint=LLM_ENDPOINT,
    mcp_server_url=mcp_url,
    workspace_client=ws,
)

print(f"Available MCP tools: {list(researcher.tools.keys())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Research

# COMMAND ----------

# Execute the research plan
print(f"Starting research on: {plan.topic}")
print("-" * 50)

report = researcher.execute_research_plan(plan)

print("-" * 50)
print(f"Research complete. Report length: {len(report)} characters")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Report to UC Volume

# COMMAND ----------

# Save report to Unity Catalog Volume
output_path = plan.full_output_path

researcher.save_report(report, output_path)
print(f"Report saved to: {output_path}")

# COMMAND ----------

# Preview the report
print("=" * 60)
print("REPORT PREVIEW (first 2000 chars)")
print("=" * 60)
print(report[:2000])
if len(report) > 2000:
    print(f"\n... [{len(report) - 2000} more characters]")

# COMMAND ----------

# Exit with success status
result = {
    "status": "SUCCESS",
    "output_path": output_path,
    "report_length": len(report),
    "questions_researched": len(plan.research_questions),
}

dbutils.notebook.exit(json.dumps(result))

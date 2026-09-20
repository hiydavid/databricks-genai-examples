# Databricks GenAI Examples

Hands-on examples for building, evaluating, and deploying AI applications on Databricks, with supporting examples for feature engineering and custom model serving.

Each example is independent, with its own notebooks, dependencies, configuration, and prerequisites. Start with the linked README or notebook for the example you want to run.

## Getting started

1. **Choose an example** from the catalog below and read its prerequisites. Depending on the example, you may need Unity Catalog, a SQL warehouse, Model Serving, Vector Search, Genie, or access to a preview feature.
2. **Open the example in the right environment.** Import or clone notebook examples into a Databricks workspace. For local development or Databricks Asset Bundles (DABs), follow the example's setup and authentication instructions.
3. **Configure your resources.** Fill in the supplied configuration templates, notebook widgets, or setup variables for your catalog, schema, endpoints, and other resources. Keep credentials in environment variables or Databricks secrets, never in committed files.
4. **Install the example's dependencies and follow its run order.** Use its README, notebook setup cells, `requirements.txt`, or `pyproject.toml`. There is no repository-wide install, build, or test command.

For Databricks skills and coding-assistant setup, see the [Databricks AI Dev Kit](https://github.com/databricks-solutions/ai-dev-kit).

## Example catalog

- [Agents](#agents)
- [AI/BI Genie](#aibi-genie)
- [Batch inference and document processing](#batch-inference-and-document-processing)
- [Context engineering](#context-engineering)
- [Evaluation](#evaluation)
- [Feature engineering](#feature-engineering)
- [Fine-tuning and prompt optimization](#fine-tuning-and-prompt-optimization)
- [Agent memory](#agent-memory)
- [Model serving](#model-serving)
- [Vector Search](#vector-search)

### Agents

| Example | What it covers |
| --- | --- |
| [Agent Bricks](agents/agent-bricks/) | Build a Multi-Agent Supervisor that coordinates a Genie Space and a Knowledge Assistant. |
| [LangGraph research assistant](agents/langgraph-research-assistant/) | Validate company names, plan research, and retrieve financial documents with a multi-agent graph. |
| [LangGraph Genie deep research](agents/langgraph-genie-deep-research/) | Route questions to Genie and coordinate parallel queries for financial analysis. |
| [Genie agent with OpenAI-compatible tool calling](agents/openai-genie-agent/) | Combine Genie queries and Unity Catalog function tools in an MLflow `ResponsesAgent`. |
| [Knowledge assistant](agents/openai-knowledge-assistant/) | Combine Vector Search document retrieval, Genie queries, and Unity Catalog tools. |
| [Agent with Databricks managed MCP tools](agents/openai-multiagent-mcp/) | Connect a tool-calling agent to Genie, Vector Search, and Unity Catalog through Model Context Protocol (MCP) servers. |
| [Agent deployment with DABs](agents/agent-dabs/) | Develop, evaluate, register, and deploy a LangGraph agent using Databricks Asset Bundles. |
| [Retrieval agent with DABs](agents/openai-retrieval-agent-dabs/) | Parse PDFs with `ai_parse_document`, build a Vector Search index, evaluate, and deploy a retrieval agent. |
| [Text-to-SQL with managed MCP](agents/text2sql-agent/) | Discover schemas and execute SQL through Unity Catalog function tools. **Experimental.** |
| [Text-to-SQL with LangChain](agents/text2sql-agent-langchain/text2sql_langchain.ipynb) | Use LangChain's SQL toolkit with Databricks SQL. |
| [Async agent workflows with Lakeflow Jobs](agents/agent-job-run/) | Hand research plans to background jobs, poll for completion, and save reports to a Unity Catalog Volume. **WIP.** |
| [Web search tool](agents/agent-tools/websearch-tool.ipynb) | Create a Unity Catalog connection and function for web search through the You.com API. |

### AI/BI Genie

| Example | What it covers |
| --- | --- |
| [Genie optimization workflow](aibi/genie-optimization-workflow/) | **WIP.** Snapshot a space, validate benchmarks, measure baseline accuracy, optimize with Genie Code tasks, and record an audit trail; requires the Genie Code Job Task preview. |
| [Genie latency guide](aibi/genie-latency-guide/) | Diagnose query-generation and execution latency using API timing, MLflow tracing, and tuning guidance. |
| [Genie demo data](aibi/genie-demo-data/) | Generate synthetic banking, talent advisory, healthcare, retail, SaaS, and wind turbine datasets, with benchmark loaders. |
| [Genie query caching](aibi/genie-query-caching/) | Explore Lakebase + pgvector, Vector Search, and hybrid caching strategies. **WIP; not ready for use.** |
| [Genie space migration](aibi/genie-migration/) | **Deprecated.** The README points to a replacement; the original implementation is retained under `archive/`. |

### Batch inference and document processing

| Example | What it covers |
| --- | --- |
| [CPU endpoint batch inference](batch-inference/custom-batch-inference/batch-inference-cpu-endpoint.py) | Submit concurrent batches to a custom Model Serving endpoint. |
| [Auto Loader with `ai_query`](batch-inference/ai-query-with-streaming/ai-query-with-streaming.py) | Process incoming text files with Structured Streaming and write model responses to Delta. |
| [Entity resolution](batch-inference/entity-resolution/) | Match merchant-name variations using Vector Search and `ai_query`, then evaluate accuracy. |
| [Document parsing](batch-inference/ai-parse-document/) | Parse PDFs with `ai_parse_document`, including a notebook and helper for debugging parsed output. |
| [End-to-end entity extraction](batch-inference/e2e-entity-extraction/) | Ingest medical-claims PDFs, classify and extract fields, and evaluate results with MLflow; orchestrate with DABs and Lakeflow Jobs. |

### Context engineering

| Example | What it covers |
| --- | --- |
| [Claude financial analyst](context-engineering/claude-financial-analyst/) | Use a Claude Code skill and MCP-based SQL access to analyze financial data in Databricks. |

### Evaluation

| Example | What it covers |
| --- | --- |
| [Entity extraction with MLflow](evals/eval-entity-extraction/) | Build an evaluation dataset and compare predefined scorers, custom guidelines, LLM judges, code scorers, and expert review. Start with `00_setup.ipynb`. |
| [Multi-turn Genie evaluation](evals/genie-mlflow-eval/) | Run scripted Genie conversations, trace them as MLflow sessions, and evaluate completed multi-turn interactions. |

### Feature engineering

| Example | What it covers |
| --- | --- |
| [Declarative Feature Engineering](feature-store/) | Generate synthetic credit-card data, define and register features, create training sets, and demonstrate batch and online inference. See the README for preview and runtime requirements. |

### Fine-tuning and prompt optimization

| Example | What it covers |
| --- | --- |
| [Structured extraction with LLMs](fine-tuning/structured-extraction-with-llm/00_introduction.ipynb) | Extract fields from lease agreements, generate synthetic training data, fine-tune a model, and compare results. |
| [Prompt optimization with GEPA](fine-tuning/prompt-optimization/) | Establish a structured-extraction baseline, optimize prompts with MLflow's GEPA optimizer, and evaluate the optimized prompt. |

### Agent memory

| Example | What it covers |
| --- | --- |
| [Managed memory](memory/) | **WIP.** Compare a stateless baseline, client-managed working memory, and durable episodic memory across support cases. The current notebooks cover working and episodic memory; semantic and procedural memory are planned. Requires the Managed agent memory preview. |

### Model serving

| Example | What it covers |
| --- | --- |
| [FLUX.1-Dev](model-serving/flux1-dev/) | Register and serve an image-generation model on a custom GPU endpoint, with Databricks and local setup notebooks. |
| [Custom GLM](model-serving/custom-glm/Deploy-Custom-GLM.ipynb) | Train a GLM, wrap its parameters in a custom Python model, and deploy it to Model Serving. |

### Vector Search

| Example | What it covers |
| --- | --- |
| [Multi-index search](vector-search/multi-index-search.py) | Query multiple indexes concurrently, merge ranked results, and expose search through an MLflow Python model. |

## Disclaimer and license

The content provided here is for reference and educational purposes only. It is not officially supported by Databricks under any Service Level Agreements (SLAs). All materials are provided AS IS, without any guarantees or warranties, and are not intended for production use without proper review and testing.

See [LICENSE.md](LICENSE.md) for terms of use.

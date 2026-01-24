# SPEC

## Setup

- .py file but with Databricks enabled formatting. it needs to be able to run on Databricks
- use Databricks FMAPI models, default to Claude Sonnet 4.5
- Use latest Databricks SDK to get Genie space: w.genie.get_space() with include_serialized_space = True
- All in one notebook .py

## Features

- section 1: review serialized space
    - get serialized Genie space using the Databricks SDK
    - use HTML rendering in the notebook to show the serialized space JSON by each section

- section 2: analyze each space section
    - use LLM (from databricks FMAPI) to analyze each section
    - for each section, compare space configuration vs. a checklist of items
    - use a new html rendering to show analysis of each section

- section 3: optimize the space
    - use the benchmark question section of the serialized space
    - for each benchmark question, display expected SQL
    - use the Genie API on the question
    - retrieve generated SQL
    - compare expected SQL to generated SQL
    - also compare results by running genereated SQL vs. expected SQL 

## Relevant docs:

- Databricks workspace API on Genie: https://databricks-sdk-py.readthedocs.io/en/latest/workspace/dashboards/genie.html
- Databricks workspace API for statement execution: https://databricks-sdk-py.readthedocs.io/en/latest/workspace/sql/statement_execution.html

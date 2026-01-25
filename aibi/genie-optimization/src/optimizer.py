# Databricks notebook source

# MAGIC %md
# MAGIC # Genie Space Optimizer
# MAGIC Analyzes and optimizes Databricks Genie Spaces through:
# MAGIC 1. Space visualization - Display serialized space JSON by section
# MAGIC 2. LLM-powered checklist analysis - Evaluate configuration against best practices
# MAGIC 3. Benchmark testing - Run Genie conversations and compare SQL results
# MAGIC
# MAGIC **Requirements:**
# MAGIC - CAN EDIT permission on the Genie Space
# MAGIC - Access to a SQL warehouse (for benchmark execution)
# MAGIC - Access to Foundation Model API endpoint
# MAGIC
# MAGIC **Parameters:**
# MAGIC - `space_id`: The Genie Space ID to analyze
# MAGIC - `warehouse_id`: Optional SQL warehouse ID for benchmark execution
# MAGIC - `llm_endpoint`: LLM endpoint for analysis (default: databricks-claude-sonnet-4-5)

# COMMAND ----------
# MAGIC %pip install databricks-sdk>=0.76.0 openai --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import sys

# Add the current notebook directory to Python path for imports
notebook_path = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .notebookPath()
    .get()
)
notebook_dir = "/Workspace" + "/".join(notebook_path.split("/")[:-1])
if notebook_dir not in sys.path:
    sys.path.insert(0, notebook_dir)

from utils import (
    BenchmarkRunner,
    GenieSpaceClient,
    HTMLRenderer,
    LLMAnalyzer,
    load_config,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC Loads settings from config.yaml in the project root.

# COMMAND ----------

config = load_config()

space_id = config["space_id"]
warehouse_id = config.get("warehouse_id") or None
llm_endpoint = config.get("llm_endpoint", "databricks-claude-sonnet-4-5")

if not space_id:
    raise ValueError("space_id is required in config.yaml")

print(f"Space ID: {space_id}")
print(f"Warehouse ID: {warehouse_id or '(will use space default)'}")
print(f"LLM Endpoint: {llm_endpoint}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Import Space Configuration
# MAGIC Fetches and displays the serialized Genie Space configuration by section.

# COMMAND ----------

genie_client = GenieSpaceClient()
space_data = genie_client.get_serialized_space(space_id)

print(f"\nSpace Title: {space_data['title']}")
print(f"Description: {space_data['description'] or '(none)'}")

# Parse the serialized space
serialized = space_data["serialized_space"]

# Display each section
sections = [
    ("data_sources", serialized.get("data_sources")),
    ("instructions", serialized.get("instructions")),
    ("benchmarks", serialized.get("benchmarks")),
]

html_output = """
<div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;">
    <h2 style="color: #1e3a5f; border-bottom: 2px solid #667eea; padding-bottom: 8px;">
        Serialized Space Configuration
    </h2>
"""

for section_name, section_data in sections:
    html_output += HTMLRenderer.render_json_section(
        title=section_name,
        data=section_data,
        collapsed=True,
    )

html_output += "</div>"
displayHTML(html_output)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Checklist Analysis
# MAGIC Evaluates the space configuration against best practice checklists using LLM analysis.

# COMMAND ----------

analyzer = LLMAnalyzer(dbutils, config)

print("\nAnalyzing data_sources section...")
data_sources_analysis = analyzer.analyze_data_sources(serialized.get("data_sources"))

html_output = """
<div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;">
    <h2 style="color: #1e3a5f; border-bottom: 2px solid #667eea; padding-bottom: 8px;">
        Checklist Analysis
    </h2>
"""
html_output += HTMLRenderer.render_checklist_analysis(
    "Data Sources", data_sources_analysis
)
html_output += "</div>"
displayHTML(html_output)

# COMMAND ----------

print("Analyzing instructions section...")
instructions_analysis = analyzer.analyze_instructions(serialized.get("instructions"))

html_output = """
<div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;">
"""
html_output += HTMLRenderer.render_checklist_analysis(
    "Instructions", instructions_analysis
)
html_output += "</div>"
displayHTML(html_output)

# COMMAND ----------

print("Analyzing benchmarks section...")
benchmarks_analysis = analyzer.analyze_benchmarks(serialized.get("benchmarks"))

html_output = """
<div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;">
"""
html_output += HTMLRenderer.render_checklist_analysis("Benchmarks", benchmarks_analysis)
html_output += "</div>"
displayHTML(html_output)

# COMMAND ----------

# Combine all analyses
all_items = []
for analysis in [data_sources_analysis, instructions_analysis, benchmarks_analysis]:
    all_items.extend(analysis.get("items", []))

pass_count = sum(1 for item in all_items if item.get("status") == "pass")
fail_count = sum(1 for item in all_items if item.get("status") == "fail")
warning_count = sum(1 for item in all_items if item.get("status") == "warning")
na_count = sum(1 for item in all_items if item.get("status") == "na")

summary_html = f"""
<div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 20px 0;">
    <div style="background: linear-gradient(135deg, #1e3a5f 0%, #2d5a87 100%); color: white; padding: 20px; border-radius: 8px;">
        <h3 style="margin: 0 0 16px 0;">Overall Checklist Summary</h3>
        <div style="display: flex; gap: 20px; flex-wrap: wrap;">
            <div style="background: rgba(255,255,255,0.1); padding: 12px 20px; border-radius: 6px;">
                <div style="font-size: 24px; font-weight: bold; color: #22c55e;">{pass_count}</div>
                <div style="font-size: 12px; opacity: 0.8;">Passed</div>
            </div>
            <div style="background: rgba(255,255,255,0.1); padding: 12px 20px; border-radius: 6px;">
                <div style="font-size: 24px; font-weight: bold; color: #ef4444;">{fail_count}</div>
                <div style="font-size: 12px; opacity: 0.8;">Failed</div>
            </div>
            <div style="background: rgba(255,255,255,0.1); padding: 12px 20px; border-radius: 6px;">
                <div style="font-size: 24px; font-weight: bold; color: #f59e0b;">{warning_count}</div>
                <div style="font-size: 12px; opacity: 0.8;">Warnings</div>
            </div>
            <div style="background: rgba(255,255,255,0.1); padding: 12px 20px; border-radius: 6px;">
                <div style="font-size: 24px; font-weight: bold; color: #6b7280;">{na_count}</div>
                <div style="font-size: 12px; opacity: 0.8;">N/A</div>
            </div>
        </div>
    </div>
</div>
"""
displayHTML(summary_html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Benchmark Testing
# MAGIC Select which benchmark questions to run, then execute the next cell.

# COMMAND ----------

benchmarks_data = serialized.get("benchmarks", {})
all_questions = benchmarks_data.get("questions", [])


def get_question_text(q):
    """Extract question text, handling list format."""
    text = q.get("question", "")
    if isinstance(text, list):
        text = text[0] if text else ""
    return text


if not all_questions:
    displayHTML(
        """
    <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; padding: 20px; background: #fef3c7; border: 1px solid #f59e0b; border-radius: 8px; margin: 20px 0;">
        <strong style="color: #92400e;">No Benchmarks Configured</strong>
        <p style="color: #78350f; margin: 8px 0 0 0;">
            This Genie Space has no benchmark questions defined. Add benchmark Q&A pairs in the space configuration to enable automated testing.
        </p>
    </div>
    """
    )
else:
    import ipywidgets as widgets
    from IPython.display import display

    # Create checkboxes for each question
    checkboxes = []
    for i, q in enumerate(all_questions):
        cb = widgets.Checkbox(
            value=False,
            description="",
            indent=False,
            layout=widgets.Layout(width="30px"),
        )
        checkboxes.append(cb)

    # Create styled labels
    question_widgets = []
    for i, (cb, q) in enumerate(zip(checkboxes, all_questions)):
        label = widgets.HTML(
            value=f"<span style='font-family: -apple-system, sans-serif;'><strong>Q{i}:</strong> {get_question_text(q)}</span>"
        )
        row = widgets.HBox([cb, label], layout=widgets.Layout(margin="4px 0"))
        question_widgets.append(row)

    # Select all / Deselect all buttons
    def select_all(b):
        for cb in checkboxes:
            cb.value = True

    def deselect_all(b):
        for cb in checkboxes:
            cb.value = False

    select_all_btn = widgets.Button(
        description="Select All",
        button_style="info",
        layout=widgets.Layout(width="100px"),
    )
    deselect_all_btn = widgets.Button(
        description="Clear All",
        button_style="warning",
        layout=widgets.Layout(width="100px"),
    )
    select_all_btn.on_click(select_all)
    deselect_all_btn.on_click(deselect_all)

    header = widgets.HTML(
        value=f"""<div style="font-family: -apple-system, sans-serif; margin-bottom: 10px;">
            <h3 style="color: #1e3a5f; margin: 0;">Select Benchmark Questions ({len(all_questions)} available)</h3>
            <p style="color: #666; margin: 4px 0 0 0;">Check the questions you want to run, then execute the next cell.</p>
        </div>"""
    )

    button_row = widgets.HBox(
        [select_all_btn, deselect_all_btn], layout=widgets.Layout(margin="0 0 10px 0")
    )
    question_box = widgets.VBox(
        question_widgets,
        layout=widgets.Layout(
            max_height="400px",
            overflow_y="auto",
            padding="10px",
            border="1px solid #ddd",
            border_radius="4px",
        ),
    )

    display(widgets.VBox([header, button_row, question_box]))

    # Store checkboxes globally for next cell
    _benchmark_checkboxes = checkboxes

# COMMAND ----------

# Run selected benchmarks
benchmark_results = []  # Initialize before conditional to ensure variable exists

if not all_questions:
    print("No benchmark questions available.")
elif "_benchmark_checkboxes" not in dir():
    print("Please run the previous cell first to display the question selector.")
else:
    selected_indices = [i for i, cb in enumerate(_benchmark_checkboxes) if cb.value]

    if not selected_indices:
        print("No questions selected. Please select at least one question above.")
    else:
        questions_to_run = [all_questions[i] for i in selected_indices]
        print(
            f"Running {len(questions_to_run)} selected benchmark(s): Q{', Q'.join(map(str, selected_indices))}"
        )

        # Ensure analyzer exists for LLM-based SQL comparison
        if "analyzer" not in dir():
            analyzer = LLMAnalyzer(dbutils, config)

        benchmark_runner = BenchmarkRunner(genie_client, warehouse_id, llm_analyzer=analyzer)
        benchmark_results = benchmark_runner.run_all_benchmarks(
            space_id, questions_to_run
        )

        # Display individual results
        html_output = """
        <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;">
            <h2 style="color: #1e3a5f; border-bottom: 2px solid #667eea; padding-bottom: 8px;">
                Benchmark Test Results
            </h2>
        """

        for result in benchmark_results:
            details = {}
            if result.expected_row_count is not None:
                details["Expected rows"] = result.expected_row_count
            if result.generated_row_count is not None:
                details["Generated rows"] = result.generated_row_count
            if result.error:
                details["Error"] = result.error

            html_output += HTMLRenderer.render_benchmark_result(
                question=result.question,
                expected_sql=result.expected_sql,
                generated_sql=result.generated_sql,
                passed=result.passed,
                details=details,
            )

        html_output += "</div>"
        displayHTML(html_output)

# COMMAND ----------

if benchmark_results:
    # Prepare summary data
    summary_data = [
        {
            "question": r.question,
            "passed": r.passed,
            "summary": r.summary,
        }
        for r in benchmark_results
    ]

    html_output = """
    <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;">
    """
    html_output += HTMLRenderer.render_summary_table(summary_data)
    html_output += "</div>"
    displayHTML(html_output)

    # Print final stats
    passed = sum(1 for r in benchmark_results if r.passed)
    total = len(benchmark_results)
    print(f"\nBenchmark Results: {passed}/{total} passed ({passed/total*100:.0f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Benchmark Analysis & Recommendations
# MAGIC Uses LLM to compare expected vs generated SQL and provide actionable recommendations.

# COMMAND ----------

if benchmark_results:
    # Ensure analyzer exists (in case checklist section was skipped)
    if "analyzer" not in dir():
        analyzer = LLMAnalyzer(dbutils, config)

    print("Analyzing benchmark results with LLM...")
    benchmark_analysis = analyzer.analyze_benchmark_results(
        benchmark_results=benchmark_results,
        serialized_space=serialized,
    )
    html_output = HTMLRenderer.render_benchmark_analysis(benchmark_analysis)
    displayHTML(html_output)
else:
    print("No benchmark results to analyze.")

# Databricks notebook source

# MAGIC %md
# MAGIC # Genie Space Optimizer
# MAGIC
# MAGIC End-to-end workflow for improving Databricks Genie Spaces:
# MAGIC 1. Import and inspect serialized space configuration
# MAGIC 2. Analyze checklist quality (data sources, instructions, benchmarks, config)
# MAGIC 3. Run benchmark evaluation with hybrid SQL judging
# MAGIC 4. Generate and optionally apply optimization changes
# MAGIC 5. Validate and optionally create an optimized copy of the space

# COMMAND ----------
# MAGIC %pip install "databricks-sdk>=0.85" openai sqlglot --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import sys
from datetime import date

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
    apply_optimization_changes,
    generate_optimization_changes,
    load_config,
    validate_serialized_space,
)


# COMMAND ----------


def _as_bool(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)


def _join_text(value):
    if value is None:
        return ""
    if isinstance(value, list):
        return " ".join(str(v).strip() for v in value if str(v).strip()).strip()
    return str(value).strip()


def _build_config_report(space_id, analyses):
    sections = ["data_sources", "instructions", "benchmarks", "config"]
    status_counts = {"pass": 0, "fail": 0, "warning": 0, "na": 0}

    lines = [
        f"# Config Analysis: {space_id}",
        "",
        f"**Date:** {date.today().isoformat()}",
        "",
    ]

    for section in sections:
        analysis = analyses.get(section, {})
        items = analysis.get("items", [])
        lines.append(f"## {section}")
        lines.append("")
        lines.append("| Item | Status | Explanation | Fix |")
        lines.append("|------|--------|-------------|-----|")
        for item in items:
            status = str(item.get("status", "na")).lower()
            status = status if status in status_counts else "na"
            status_counts[status] += 1
            name = str(item.get("name", "")).replace("|", "\\|")
            explanation = str(item.get("explanation", "")).replace("|", "\\|")
            fix = str(item.get("fix", "")).replace("|", "\\|")
            lines.append(f"| {name} | {status} | {explanation} | {fix} |")
        lines.append("")
        lines.append(f"Summary: {analysis.get('summary', '')}")
        lines.append("")

    lines.insert(
        4,
        (
            "**Counts:** "
            f"pass={status_counts['pass']} | "
            f"fail={status_counts['fail']} | "
            f"warning={status_counts['warning']} | "
            f"na={status_counts['na']}"
        ),
    )

    return "\n".join(lines)


def _build_benchmark_report(space_id, results):
    verdict_counts = {"correct": 0, "partial": 0, "incorrect": 0, "error": 0}
    weighted = 0.0

    lines = [
        f"# Benchmark Analysis: {space_id}",
        "",
        f"**Date:** {date.today().isoformat()}",
        f"**Questions tested:** {len(results)}",
        "",
    ]

    for result in results:
        verdict = str(result.verdict).lower()
        verdict = verdict if verdict in verdict_counts else "error"
        verdict_counts[verdict] += 1
        weighted += float(result.score)

    percent = (weighted / len(results) * 100) if results else 0.0

    lines.extend(
        [
            "## Summary",
            "",
            "| Verdict | Count |",
            "|---------|-------|",
            f"| Correct | {verdict_counts['correct']} |",
            f"| Partial | {verdict_counts['partial']} |",
            f"| Incorrect | {verdict_counts['incorrect']} |",
            f"| Error | {verdict_counts['error']} |",
            f"| **Score** | **{weighted:.1f}/{len(results)} ({percent:.0f}%)** |",
            "",
            "## Detailed Results",
            "",
        ]
    )

    for idx, result in enumerate(results, start=1):
        lines.extend(
            [
                f"### {idx}. {result.question}",
                "",
                f"**Verdict:** {result.verdict}",
                f"**Score:** {result.score:.1f} (deterministic={result.deterministic_score:.2f})",
                "",
                "**Expected SQL:**",
                "```sql",
                result.expected_sql or "",
                "```",
                "",
                "**Generated SQL:**",
                "```sql",
                result.generated_sql or "",
                "```",
                "",
                f"**Summary:** {result.summary}",
                f"**Dimensions:** {result.dimension_results}",
                "",
                "---",
                "",
            ]
        )

    return "\n".join(lines)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC Load runtime settings from `config.yaml`.

# COMMAND ----------

config = load_config()

space_id = config["space_id"]
warehouse_id = config.get("warehouse_id") or None
llm_endpoint = config.get("llm_endpoint", "databricks-claude-sonnet-4-5")

workflow_mode = str(config.get("workflow_mode", "all")).strip().lower()
if workflow_mode not in {"all", "analyze", "benchmark", "optimize"}:
    workflow_mode = "all"

benchmark_judge_mode = str(config.get("benchmark_judge_mode", "hybrid")).strip().lower()
if benchmark_judge_mode not in {"hybrid", "llm", "execution"}:
    benchmark_judge_mode = "hybrid"

benchmark_timeout_minutes = int(config.get("benchmark_timeout_minutes", 5) or 5)
create_new_space = _as_bool(config.get("create_new_space", False))
optimized_title_prefix = str(config.get("optimized_title_prefix", "[Optimized] "))
enable_execution_diagnostics = _as_bool(config.get("enable_execution_diagnostics", False))

approved_for_apply = False

if not space_id:
    raise ValueError("space_id is required in config.yaml")

print(f"Space ID: {space_id}")
print(f"Configured Warehouse ID: {warehouse_id or '(none)'}")
print(f"LLM Endpoint: {llm_endpoint}")
print(f"Workflow mode: {workflow_mode}")
print(f"Benchmark judge mode: {benchmark_judge_mode}")
print(f"Benchmark timeout (minutes): {benchmark_timeout_minutes}")
print(f"Create new space: {create_new_space}")

config_analysis_report = ""
benchmark_analysis_report = ""
optimization_report = ""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Import Space Configuration
# MAGIC Fetches and displays serialized Genie Space configuration by section.

# COMMAND ----------

genie_client = GenieSpaceClient()
space_data = genie_client.get_serialized_space(space_id)

print(f"\nSpace Title: {space_data['title']}")
print(f"Description: {space_data['description'] or '(none)'}")

serialized = space_data["serialized_space"]
space_default_warehouse_id = space_data.get("warehouse_id") or None
effective_warehouse_id = warehouse_id or space_default_warehouse_id or None

print(f"Space default warehouse_id: {space_default_warehouse_id or '(none)'}")
print(f"Effective warehouse_id for benchmarks: {effective_warehouse_id or '(none)'}")

sections = [
    ("config", serialized.get("config")),
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
# MAGIC ## Section 2: Checklist Analysis (Option A)
# MAGIC Runs for workflow modes: `all`, `analyze`, `optimize`.

# COMMAND ----------

analyzer = None
checklist_analyses = {}

if workflow_mode in {"all", "analyze", "optimize"}:
    analyzer = LLMAnalyzer(dbutils, config)

    print("Analyzing data_sources section...")
    data_sources_analysis = analyzer.analyze_data_sources(serialized.get("data_sources"))
    print("Analyzing instructions section...")
    instructions_analysis = analyzer.analyze_instructions(serialized.get("instructions"))
    print("Analyzing benchmarks section...")
    benchmarks_analysis = analyzer.analyze_benchmarks(serialized.get("benchmarks"))
    print("Analyzing config section...")
    config_section_analysis = analyzer.analyze_config(serialized.get("config"))

    checklist_analyses = {
        "data_sources": data_sources_analysis,
        "instructions": instructions_analysis,
        "benchmarks": benchmarks_analysis,
        "config": config_section_analysis,
    }

    html_output = """
    <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;">
        <h2 style="color: #1e3a5f; border-bottom: 2px solid #667eea; padding-bottom: 8px;">
            Checklist Analysis
        </h2>
    """

    html_output += HTMLRenderer.render_checklist_analysis("Data Sources", data_sources_analysis)
    html_output += HTMLRenderer.render_checklist_analysis("Instructions", instructions_analysis)
    html_output += HTMLRenderer.render_checklist_analysis("Benchmarks", benchmarks_analysis)
    html_output += HTMLRenderer.render_checklist_analysis("Config", config_section_analysis)

    all_items = []
    for analysis in checklist_analyses.values():
        all_items.extend(analysis.get("items", []))

    pass_count = sum(1 for item in all_items if item.get("status") == "pass")
    fail_count = sum(1 for item in all_items if item.get("status") == "fail")
    warning_count = sum(1 for item in all_items if item.get("status") == "warning")
    na_count = sum(1 for item in all_items if item.get("status") == "na")

    html_output += f"""
    <div style="margin: 18px 0; background: linear-gradient(135deg, #1e3a5f 0%, #2d5a87 100%); color: white; padding: 20px; border-radius: 8px;">
        <h3 style="margin: 0 0 14px 0;">Overall Checklist Summary</h3>
        <div style="display: flex; gap: 14px; flex-wrap: wrap;">
            <span style="background: rgba(255,255,255,0.14); padding: 8px 12px; border-radius: 6px;">Passed: <strong>{pass_count}</strong></span>
            <span style="background: rgba(255,255,255,0.14); padding: 8px 12px; border-radius: 6px;">Failed: <strong>{fail_count}</strong></span>
            <span style="background: rgba(255,255,255,0.14); padding: 8px 12px; border-radius: 6px;">Warnings: <strong>{warning_count}</strong></span>
            <span style="background: rgba(255,255,255,0.14); padding: 8px 12px; border-radius: 6px;">N/A: <strong>{na_count}</strong></span>
        </div>
    </div>
    """

    html_output += HTMLRenderer.render_priority_recommendations(all_items)
    html_output += "</div>"

    displayHTML(html_output)

    config_analysis_report = _build_config_report(space_id, checklist_analyses)
else:
    print("Checklist analysis skipped for workflow_mode='benchmark'.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Benchmark Selection (Option B)
# MAGIC Runs for workflow modes: `all`, `benchmark`, `optimize`.

# COMMAND ----------

benchmarks_data = serialized.get("benchmarks", {})
all_questions = []

if workflow_mode in {"all", "benchmark", "optimize"}:
    all_questions = benchmarks_data.get("questions", [])

    if not all_questions:
        displayHTML(
            """
        <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; padding: 20px; background: #fef3c7; border: 1px solid #f59e0b; border-radius: 8px; margin: 20px 0;">
            <strong style="color: #92400e;">No Benchmarks Configured</strong>
            <p style="color: #78350f; margin: 8px 0 0 0;">
                This Genie Space has no benchmark questions defined.
            </p>
        </div>
        """
        )
    else:
        import ipywidgets as widgets
        from IPython.display import display

        checkboxes = []
        for _ in all_questions:
            cb = widgets.Checkbox(
                value=False,
                description="",
                indent=False,
                layout=widgets.Layout(width="30px"),
            )
            checkboxes.append(cb)

        question_widgets = []
        for i, (cb, q) in enumerate(zip(checkboxes, all_questions)):
            question_text = _join_text(q.get("question"))
            label = widgets.HTML(
                value=f"<span style='font-family: -apple-system, sans-serif;'><strong>Q{i}</strong>: {question_text}</span>"
            )
            row = widgets.HBox([cb, label], layout=widgets.Layout(margin="4px 0"))
            question_widgets.append(row)

        def select_all(_):
            for cb in checkboxes:
                cb.value = True

        def deselect_all(_):
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
            [select_all_btn, deselect_all_btn],
            layout=widgets.Layout(margin="0 0 10px 0"),
        )
        question_box = widgets.VBox(
            question_widgets,
            layout=widgets.Layout(
                max_height="420px",
                overflow_y="auto",
                padding="10px",
                border="1px solid #ddd",
                border_radius="4px",
            ),
        )

        display(widgets.VBox([header, button_row, question_box]))
        _benchmark_checkboxes = checkboxes
else:
    print("Benchmark selection skipped for workflow_mode='analyze'.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Benchmark Execution + Hybrid Judge (Option B)

# COMMAND ----------

benchmark_results = []
benchmark_analysis = {
    "benchmark_analyses": [],
    "overall_recommendations": [],
    "summary": "",
}

if workflow_mode in {"all", "benchmark", "optimize"}:
    if not all_questions:
        print("No benchmark questions available.")
    else:
        if "_benchmark_checkboxes" in dir():
            selected_indices = [i for i, cb in enumerate(_benchmark_checkboxes) if cb.value]
        elif workflow_mode == "optimize":
            selected_indices = list(range(len(all_questions)))
            print("No interactive selector found; optimize mode defaults to all benchmarks.")
        else:
            selected_indices = []
            print("Please run the benchmark selection cell first.")

        if not selected_indices:
            print("No benchmark questions selected.")
        else:
            questions_to_run = [all_questions[i] for i in selected_indices]
            print(
                f"Running {len(questions_to_run)} selected benchmark(s): "
                + ", ".join(f"Q{i}" for i in selected_indices)
            )

            if analyzer is None:
                analyzer = LLMAnalyzer(dbutils, config)

            benchmark_runner = BenchmarkRunner(
                genie_client=genie_client,
                warehouse_id=effective_warehouse_id,
                llm_analyzer=analyzer,
                judge_mode=benchmark_judge_mode,
                timeout_minutes=benchmark_timeout_minutes,
                enable_execution_diagnostics=enable_execution_diagnostics,
            )

            benchmark_results = benchmark_runner.run_all_benchmarks(space_id, questions_to_run)

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
                details["Deterministic score"] = f"{result.deterministic_score:.2f}"
                if result.error:
                    details["Error"] = result.error

                html_output += HTMLRenderer.render_benchmark_result(
                    question=result.question,
                    expected_sql=result.expected_sql,
                    generated_sql=result.generated_sql,
                    verdict=result.verdict,
                    score=result.score,
                    summary=result.summary,
                    dimension_results=result.dimension_results,
                    details=details,
                )

            html_output += "</div>"
            displayHTML(html_output)

            summary_data = [
                {
                    "question": r.question,
                    "verdict": r.verdict,
                    "score": r.score,
                    "summary": r.summary,
                }
                for r in benchmark_results
            ]

            displayHTML(
                "<div style='font-family: -apple-system, BlinkMacSystemFont, \"Segoe UI\", Roboto, sans-serif;'>"
                + HTMLRenderer.render_summary_table(summary_data)
                + "</div>"
            )

            weighted_score = sum(r.score for r in benchmark_results)
            weighted_percent = (
                weighted_score / len(benchmark_results) * 100 if benchmark_results else 0
            )
            print(
                f"Weighted benchmark score: {weighted_score:.1f}/{len(benchmark_results)} ({weighted_percent:.0f}%)"
            )

            benchmark_analysis_report = _build_benchmark_report(space_id, benchmark_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: Benchmark Diagnosis & Recommendations
# MAGIC LLM-generated diagnosis and configuration recommendations.

# COMMAND ----------

if benchmark_results and workflow_mode in {"all", "benchmark", "optimize"}:
    if analyzer is None:
        analyzer = LLMAnalyzer(dbutils, config)

    print("Analyzing benchmark results with LLM...")
    benchmark_analysis = analyzer.analyze_benchmark_results(
        benchmark_results=benchmark_results,
        serialized_space=serialized,
    )
    displayHTML(HTMLRenderer.render_benchmark_analysis(benchmark_analysis))

    if benchmark_analysis.get("overall_recommendations"):
        benchmark_analysis_report += "\n\n## Patterns & Recommendations\n\n"
        for recommendation in benchmark_analysis["overall_recommendations"]:
            benchmark_analysis_report += f"- {recommendation}\n"
elif workflow_mode in {"all", "benchmark", "optimize"}:
    print("No benchmark results to analyze.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: Proposed Optimization Plan (Option C)
# MAGIC Runs for workflow modes: `all`, `optimize`.

# COMMAND ----------

optimization_changes = []
optimized_serialized_space = None
validation_result = None
created_space_result = None

if workflow_mode in {"all", "optimize"}:
    if not checklist_analyses:
        print("Optimization planning requires checklist analyses (Option A).")
    else:
        optimization_changes = generate_optimization_changes(
            serialized_space=serialized,
            checklist_analyses=checklist_analyses,
            benchmark_results=benchmark_results,
            benchmark_analysis=benchmark_analysis,
        )

        displayHTML(HTMLRenderer.render_optimization_plan(optimization_changes))

        approved_for_apply = False
        print("Approval gate reset. Set approved_for_apply = True and run the next cell to apply changes.")

        category_counts = {}
        for change in optimization_changes:
            category = change.get("category", "other")
            category_counts[category] = category_counts.get(category, 0) + 1

        optimization_report_lines = [
            f"# Optimization Report: {space_data['title']}",
            "",
            f"**Original Space ID:** `{space_id}`",
            f"**Date:** {date.today().isoformat()}",
            "",
            "## Proposed Changes",
            "",
        ]
        for category, count in sorted(category_counts.items()):
            optimization_report_lines.append(f"- {category}: {count}")
        optimization_report_lines.append("")
        optimization_report_lines.append("## Detailed Changes")
        optimization_report_lines.append("")
        for idx, change in enumerate(optimization_changes, start=1):
            optimization_report_lines.append(
                f"{idx}. `{change.get('action')}` at `{change.get('path')}`"
            )
            optimization_report_lines.append(
                f"   - Rationale: {change.get('rationale', '')}"
            )
            optimization_report_lines.append(
                f"   - Before: {change.get('before', '')}"
            )
            optimization_report_lines.append(
                f"   - After: {change.get('after', '')}"
            )
        optimization_report = "\n".join(optimization_report_lines)
else:
    print("Optimization planning skipped for workflow_mode='analyze' or 'benchmark'.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7: Apply + Validate + Optional Create (Option C)
# MAGIC Set `approved_for_apply = True` before running this cell.

# COMMAND ----------

if workflow_mode in {"all", "optimize"}:
    if not optimization_changes:
        print("No optimization changes to apply.")
    elif not approved_for_apply:
        print("Approval gate not satisfied. Set approved_for_apply = True and rerun this cell.")
    else:
        optimized_serialized_space = apply_optimization_changes(serialized, optimization_changes)
        validation_result = validate_serialized_space(optimized_serialized_space)

        displayHTML(HTMLRenderer.render_validation_report(validation_result))

        if validation_result.get("valid"):
            displayHTML(
                "<div style='font-family: -apple-system, BlinkMacSystemFont, \"Segoe UI\", Roboto, sans-serif;'>"
                + HTMLRenderer.render_json_section(
                    title="optimized_serialized_space",
                    data=optimized_serialized_space,
                    collapsed=True,
                )
                + "</div>"
            )

            if create_new_space:
                created_space_result = genie_client.create_optimized_space(
                    original_space_id=space_id,
                    updated_config=optimized_serialized_space,
                    title_prefix=optimized_title_prefix,
                )
                displayHTML(
                    "<div style='font-family: -apple-system, BlinkMacSystemFont, \"Segoe UI\", Roboto, sans-serif;'>"
                    + HTMLRenderer.render_json_section(
                        title="created_optimized_space",
                        data=created_space_result,
                        collapsed=False,
                    )
                    + "</div>"
                )
                print(
                    f"Created optimized space: {created_space_result['new_space_id']}\n"
                    f"URL: {created_space_result['new_space_url']}"
                )

                optimization_report += "\n\n## Creation Result\n\n"
                optimization_report += (
                    f"- New Space ID: `{created_space_result['new_space_id']}`\n"
                    f"- New Space URL: `{created_space_result['new_space_url']}`\n"
                )
            else:
                print("create_new_space is false. Stopping at validated optimized configuration preview.")
        else:
            print("Validation failed. Resolve errors before creating an optimized space.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generated Reports
# MAGIC Report strings are available in these variables for downstream notebook cells:
# MAGIC - `config_analysis_report`
# MAGIC - `benchmark_analysis_report`
# MAGIC - `optimization_report`

# COMMAND ----------

print("config_analysis_report length:", len(config_analysis_report or ""))
print("benchmark_analysis_report length:", len(benchmark_analysis_report or ""))
print("optimization_report length:", len(optimization_report or ""))

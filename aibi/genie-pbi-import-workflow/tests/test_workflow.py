"""Run notebook control flow locally; no Databricks credentials or Spark required."""

import contextlib
import io
import itertools
import json
import os
from pathlib import Path
import runpy
import sys
import tempfile
import types
import unittest
from unittest.mock import Mock, patch
import zipfile


NOTEBOOK_DIR = Path(__file__).resolve().parents[1] / "notebooks"
NOTEBOOKS = ("setup_and_preflight.py", "validate_metric_view.py", "audit_and_report.py")
PARAMS = {
    "run_id": "test'run\\id",
    "pbit_volume_path": "/Volumes/source_catalog/source_schema/pbi_files",
    "metric_view_catalog": "test_catalog",
    "metric_view_schema": "test_schema",
    "pbit_filename": "model.pbit",
    "metric_view_name": "mv_test",
    "create_agent": "true",
    "agent_name": "Test Agent",
    "warehouse_id": "test-warehouse",
}
MV_FQN = "test_catalog.test_schema.mv_test"
PBIT_PATH = "/Volumes/source_catalog/source_schema/pbi_files/model.pbit"

# Power BI model with one table per source kind the preflight distinguishes.
DATA_MODEL = {"model": {"tables": [
    {"name": "Sales", "partitions": [{"source": {"type": "m", "expression": [
        'let Source = Databricks.Catalogs("host", "path"),',
        '  c = Source{[Name="src_catalog",Kind="Database"]}[Data],',
        '  s = c{[Name="src_schema",Kind="Schema"]}[Data],',
        '  t = s{[Name="fact_sales",Kind="Table"]}[Data]',
        "in t",
    ]}}], "measures": [{"name": "Total Sales", "expression": "SUM(Sales[amount])"},
                       {"name": "Margin %", "expression": "DIVIDE([Profit], [Total Sales])"}]},
    {"name": "Dim Customer", "partitions": [{"source": {"type": "m", "expression": "Value.NativeQuery(...)"}}]},
    {"name": "Calendar", "partitions": [{"source": {"type": "calculated", "expression": "CALENDARAUTO()"}}]},
]}}
# YAML is a superset of JSON, so the stub yaml module below can parse this with json.
MV_DEFINITION = {
    "version": 1.1,
    "source": "src_catalog.src_schema.fact_sales",
    "joins": [{"name": "customer", "source": "dim_customer", "on": "source.cid = customer.cid",
               "joins": [{"name": "region", "source": "other.dim_region", "on": "customer.rid = region.rid"}]}],
    "dimensions": [{"name": "region_name", "expr": "region.name", "display_name": "Region"}],
    "measures": [
        {"name": "total_sales", "expr": "SUM(amount)", "display_name": "Total Sales"},
        {"name": "margin_pct", "expr": "SUM(profit) / SUM(amount)", "display_name": "Margin %"},
    ],
}


class NotebookExit(BaseException):
    """Model a normal dbutils.notebook.exit separately from task failures."""


class Widgets:
    def __init__(self, values):
        self.values = dict(values)

    def text(self, name, default):
        self.values.setdefault(name, default)

    def get(self, name):
        return self.values[name]


class Spark:
    """Stand-in for the artifact table and metric view queries, keeping bound values verbatim.

    This exercises handoffs, not Spark's SQL parser. A workspace smoke test is still
    needed to verify SQL execution, /importBI, and actual Genie API responses.
    """

    def __init__(self):
        self.rows = []
        self.calls = []
        self.failing_measures = set()

    def seed(self, artifact_type, payload):
        self.rows.append({
            "run_id": PARAMS["run_id"],
            "artifact_type": artifact_type,
            "payload": payload if isinstance(payload, str) else json.dumps(payload),
            "created_at": len(self.rows),
        })

    def latest(self, artifact_type):
        return json.loads(next(row["payload"] for row in reversed(self.rows)
                               if row["artifact_type"] == artifact_type))

    def sql(self, query, args=None):
        self.calls.append((query, args))
        statement = " ".join(query.split())
        result = types.SimpleNamespace(collect=lambda: [])
        if statement.startswith("CREATE TABLE"):
            return result
        if "MEASURE(" in statement:
            if any(f"MEASURE(`{m}`)" in statement for m in self.failing_measures):
                raise RuntimeError("simulated measure failure")
            return result
        if not args or "run_id" not in args:
            raise AssertionError("Artifact reads and writes must bind run_id")
        if args["run_id"] in query:
            raise AssertionError("run_id was interpolated into SQL")
        if statement.startswith("INSERT INTO"):
            if ":payload" not in query or args["payload"] in query:
                raise AssertionError("JSON payload must be bound as a value")
            self.rows.append({**args, "created_at": len(self.rows)})
            return result
        if statement.startswith("SELECT"):
            rows = [row for row in self.rows if row["run_id"] == args["run_id"]]
            return types.SimpleNamespace(collect=lambda: rows)
        raise AssertionError(f"Unexpected SQL: {statement}")


class WorkflowTests(unittest.TestCase):
    def setUp(self):
        self.spark = Spark()
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.pbit = os.path.join(self.tmp.name, "model.pbit")
        self.write_pbit(DATA_MODEL)

        self.existing_tables = {"src_catalog.src_schema.fact_sales",
                                "test_catalog.test_schema.dim_customer", "test_catalog.other.dim_region"}
        self.tables = Mock()
        self.tables.exists.side_effect = lambda full_name: types.SimpleNamespace(
            table_exists=full_name in self.existing_tables)
        self.tables.get.return_value = types.SimpleNamespace(
            table_type=types.SimpleNamespace(value="METRIC_VIEW"),
            view_definition=json.dumps(MV_DEFINITION),
        )
        self.genie = Mock()
        self.genie.get_space.return_value = types.SimpleNamespace(
            serialized_space=json.dumps({"data_sources": {"tables": [{"identifier": MV_FQN}]}}),
        )
        self.client = Mock(return_value=types.SimpleNamespace(tables=self.tables, genie=self.genie))

    def write_pbit(self, model, entry="DataModelSchema"):
        with zipfile.ZipFile(self.pbit, "w") as z:
            z.writestr(entry, "﻿".encode("utf-16-le") + json.dumps(model).encode("utf-16-le"))

    def run_notebook(self, filename, **overrides):
        sdk = types.ModuleType("databricks.sdk")
        sdk.WorkspaceClient = self.client
        yaml = types.ModuleType("yaml")
        yaml.safe_load = json.loads

        def exit_notebook(payload):
            raise NotebookExit(payload)

        # Redirect the notebook's /Volumes path to the local fixture.
        real_exists, real_zipfile = os.path.exists, zipfile.ZipFile
        local = lambda path: self.pbit if path == PBIT_PATH else path

        dbutils = types.SimpleNamespace(
            widgets=Widgets({**PARAMS, **overrides}),
            notebook=types.SimpleNamespace(exit=exit_notebook),
        )
        modules = {"databricks": types.ModuleType("databricks"), "databricks.sdk": sdk, "yaml": yaml}
        with patch.dict(sys.modules, modules), \
                patch("os.path.exists", lambda path: real_exists(local(path))), \
                patch("zipfile.ZipFile", lambda path, *a, **k: real_zipfile(local(path), *a, **k)), \
                contextlib.redirect_stdout(io.StringIO()):
            try:
                runpy.run_path(str(NOTEBOOK_DIR / filename), init_globals={"dbutils": dbutils, "spark": self.spark})
            except NotebookExit as e:
                return json.loads(str(e))
        self.fail("Notebook did not exit")

    def assert_no_calls(self):
        self.client.assert_not_called()
        self.assertEqual(self.spark.calls, [])

    # --- all notebooks ---

    def test_all_partial_configurations_are_dry_runs_without_calls(self):
        for filename in NOTEBOOKS:
            for present in itertools.product((False, True), repeat=3):
                if all(present):
                    continue
                config = {key: PARAMS[key] if keep else ""
                          for key, keep in zip(("metric_view_catalog", "metric_view_schema", "pbit_filename"), present)}
                with self.subTest(filename=filename, config=config):
                    self.assertEqual(self.run_notebook(filename, **config)["status"], "DRY_RUN")
        self.assert_no_calls()

    def test_configured_runs_require_a_run_id(self):
        for filename in NOTEBOOKS:
            with self.subTest(filename=filename), self.assertRaisesRegex(ValueError, "run_id is required"):
                self.run_notebook(filename, run_id="")
        self.assert_no_calls()

    # --- setup_and_preflight ---

    def test_setup_records_model_and_source_resolution(self):
        result = self.run_notebook("setup_and_preflight.py")
        self.assertEqual(result["measures_found"], 2)
        config = self.spark.latest("setup_config")
        self.assertEqual(config["run_id"], PARAMS["run_id"])
        self.assertEqual(config["pbit_volume_path"], PARAMS["pbit_volume_path"])
        self.assertEqual(config["pbit_path"], PBIT_PATH)
        self.assertEqual(config["metric_view_fqn"], MV_FQN)
        self.assertEqual([m["name"] for m in config["pbi_measures"]], ["Total Sales", "Margin %"])
        details = {d["pbi_table"]: d for d in config["source_table_validation"]["details"]}
        self.assertEqual(details["Sales"]["uc_table"], "src_catalog.src_schema.fact_sales")
        self.assertEqual(details["Sales"]["match"], "m_navigation")
        self.assertTrue(details["Sales"]["uc_exists"])
        self.assertEqual(details["Dim Customer"]["uc_table"], "test_catalog.test_schema.dim_customer")
        self.assertEqual(details["Dim Customer"]["match"], "name_convention")
        self.assertEqual(details["Calendar"]["match"], "calculated")
        self.assertNotIn("uc_exists", details["Calendar"])

    def test_setup_missing_source_tables_are_only_a_warning(self):
        self.existing_tables = set()
        self.run_notebook("setup_and_preflight.py")
        self.assertEqual(self.spark.latest("setup_config")["source_table_validation"]["tables_missing_from_uc"], 2)

    def test_setup_rejects_invalid_parameters_before_any_calls(self):
        cases = (
            ({"metric_view_name": ""}, "metric_view_name"),
            ({"pbit_volume_path": ""}, "pbit_volume_path"),
            ({"pbit_volume_path": "dbfs:/Volumes/c/s/v"}, "/Volumes/"),
            ({"pbit_filename": "nested/model.pbit"}, "file name"),
            ({"pbit_filename": "model.pbix"}, ".pbit"),
            ({"create_agent": "yes"}, "create_agent"),
            ({"agent_name": ""}, "agent_name"),
            ({"warehouse_id": ""}, "warehouse_id"),
        )
        for override, message in cases:
            with self.subTest(override=override), self.assertRaisesRegex(ValueError, message):
                self.run_notebook("setup_and_preflight.py", **override)
        self.assert_no_calls()

    def test_setup_does_not_require_agent_settings_without_an_agent(self):
        self.run_notebook("setup_and_preflight.py", create_agent="false", agent_name="", warehouse_id="")
        self.assertFalse(self.spark.latest("setup_config")["create_agent"])

    def test_setup_fails_without_a_readable_model(self):
        os.remove(self.pbit)
        with self.assertRaises(FileNotFoundError):
            self.run_notebook("setup_and_preflight.py")
        self.write_pbit(DATA_MODEL, entry="Report/Layout")
        with self.assertRaisesRegex(ValueError, "DataModelSchema"):
            self.run_notebook("setup_and_preflight.py")
        self.assertFalse(any(row["artifact_type"] == "setup_config" for row in self.spark.rows))

    # --- validate_metric_view ---

    def test_validation_passes_and_resolves_every_source(self):
        result = self.run_notebook("validate_metric_view.py")
        self.assertEqual(result["overall_status"], "PASS")
        validation = self.spark.latest("validation")
        self.assertEqual(validation["measure_count"], 2)
        self.assertEqual(validation["dimension_count"], 1)
        tables = {s["table"]: s["role"] for s in validation["source_table_validation"]["details"]}
        self.assertEqual(tables, {
            "src_catalog.src_schema.fact_sales": "source",
            "test_catalog.test_schema.dim_customer": "join:customer",
            "test_catalog.other.dim_region": "join:region",
        })
        # One query per measure plus one grouped query.
        self.assertEqual(len(validation["smoke_tests"]), 3)

    def test_validation_accepts_a_wrapped_definition_and_query_sources(self):
        definition = {**MV_DEFINITION, "source": "SELECT * FROM src_catalog.src_schema.fact_sales", "joins": []}
        self.tables.get.return_value.view_definition = f"CREATE VIEW x AS $${json.dumps(definition)}$$"
        self.run_notebook("validate_metric_view.py")
        self.assertEqual(self.spark.latest("validation")["source_table_validation"]["tables_checked"], 0)

    def test_validation_parses_quoted_source_names(self):
        self.existing_tables.add("cat.my.schema.t")
        definition = {**MV_DEFINITION, "source": "`cat`.`my.schema`.`t`", "joins": []}
        self.tables.get.return_value.view_definition = json.dumps(definition)
        self.run_notebook("validate_metric_view.py")
        details = self.spark.latest("validation")["source_table_validation"]["details"]
        self.assertEqual([(d["table"], d["exists"]) for d in details], [("cat.my.schema.t", True)])
        self.tables.exists.assert_called_once_with(full_name="cat.my.schema.t")

    def test_validation_failures_write_diagnostics_and_fail(self):
        def not_found():
            self.tables.get.side_effect = RuntimeError("simulated not found")

        def wrong_type():
            self.tables.get.return_value.table_type = types.SimpleNamespace(value="VIEW")

        def missing_source():
            self.existing_tables.discard("test_catalog.other.dim_region")

        def broken_measure():
            self.spark.failing_measures.add("margin_pct")

        def four_part_source():
            self.tables.get.return_value.view_definition = json.dumps({**MV_DEFINITION, "source": "a.b.c.d"})

        def no_measures():
            self.tables.get.return_value.view_definition = json.dumps({**MV_DEFINITION, "measures": []})

        cases = (
            (not_found, "Could not read metric view"),
            (wrong_type, "not a METRIC_VIEW"),
            (missing_source, "other.dim_region"),
            (broken_measure, "margin_pct"),
            (four_part_source, "not found: not a valid"),
            (no_measures, "no measures"),
        )
        for arrange, message in cases:
            with self.subTest(case=arrange.__name__):
                self.setUp()
                arrange()
                with self.assertRaisesRegex(RuntimeError, message):
                    self.run_notebook("validate_metric_view.py")
                validation = self.spark.latest("validation")
                self.assertEqual(validation["overall_status"], "FAIL")
                self.assertTrue(any(message in e for e in validation["errors"]))

    # --- audit_and_report ---

    def seed_complete_audit(self, with_agent=True):
        self.run_notebook("setup_and_preflight.py")
        self.spark.seed("import_result", {
            "metric_view_fqn": MV_FQN, "status": "SUCCESS", "measures_created": 2,
            "dimensions_created": 1, "measures_not_translated": [], "notes": 'Quotes "ok" \\ and\nnewlines',
        })
        self.run_notebook("validate_metric_view.py")
        if with_agent:
            self.spark.seed("agent_result", {
                "agent_name": "Test Agent", "space_id": "space-1", "status": "SUCCESS",
                "sample_questions_added": 6, "error": None,
            })

    def test_complete_audit_is_successful(self):
        self.seed_complete_audit()
        result = self.run_notebook("audit_and_report.py")
        self.assertEqual(result["status"], "SUCCESS")
        self.assertTrue(result["audit_complete"])
        self.assertEqual(result["agent_space_id"], "space-1")
        # Measures match on display_name after /importBI renamed them.
        self.assertEqual(result["pbi_measures_unmatched"], [])
        self.genie.get_space.assert_called_once_with(space_id="space-1", include_serialized_space=True)

    def test_audit_accepts_equivalent_metric_view_names(self):
        self.seed_complete_audit()
        quoted = "`TEST_CATALOG`.`test_schema`.`MV_test`"
        self.spark.seed("import_result", {**self.spark.latest("import_result"), "metric_view_fqn": quoted})
        self.genie.get_space.return_value = types.SimpleNamespace(
            serialized_space=json.dumps({"data_sources": {"metric_views": [{"identifier": quoted}]}}))
        self.assertTrue(self.run_notebook("audit_and_report.py")["audit_complete"])

    def test_audit_does_not_expect_an_agent_after_failed_validation(self):
        self.seed_complete_audit(with_agent=False)
        self.spark.seed("validation", {**self.spark.latest("validation"), "overall_status": "FAIL"})
        with self.assertRaisesRegex(RuntimeError, "validation did not pass") as ctx:
            self.run_notebook("audit_and_report.py")
        self.assertNotIn("agent_result", str(ctx.exception))
        self.genie.get_space.assert_not_called()

    def test_audit_without_an_agent_does_not_need_agent_result(self):
        self.seed_complete_audit(with_agent=False)
        result = self.run_notebook("audit_and_report.py", create_agent="false")
        self.assertTrue(result["audit_complete"])
        self.assertIsNone(result["agent_space_id"])
        self.genie.get_space.assert_not_called()

    def test_audit_reports_measures_missing_from_the_metric_view(self):
        self.seed_complete_audit()
        definition = {**MV_DEFINITION, "measures": MV_DEFINITION["measures"][:1]}
        self.tables.get.return_value.view_definition = json.dumps(definition)
        self.run_notebook("validate_metric_view.py")
        result = self.run_notebook("audit_and_report.py")
        self.assertTrue(result["audit_complete"])
        self.assertEqual(result["pbi_measures_unmatched"], ["Margin %"])

    def test_missing_required_artifacts_make_audit_incomplete(self):
        for missing in ("setup_config", "import_result", "validation", "agent_result"):
            with self.subTest(missing=missing):
                self.setUp()
                self.seed_complete_audit()
                self.spark.rows = [row for row in self.spark.rows if row["artifact_type"] != missing]
                with self.assertRaisesRegex(RuntimeError, f"Missing required artifact: {missing}"):
                    self.run_notebook("audit_and_report.py")
                summary = self.spark.latest("audit_summary")
                self.assertEqual(summary["status"], "INCOMPLETE")
                self.assertFalse(summary["audit_complete"])

    def test_audit_rejects_unsuccessful_handoffs(self):
        cases = (
            ("import_result", {"status": "FAILED"}, "import_result reports status"),
            ("import_result", {"metric_view_fqn": "other.mv"}, "different metric view"),
            ("validation", {"overall_status": "FAIL"}, "validation did not pass"),
            ("agent_result", {"status": "FAILED", "error": "boom"}, "boom"),
            ("agent_result", {"space_id": ""}, "no space_id"),
        )
        for artifact_type, override, message in cases:
            with self.subTest(artifact_type=artifact_type, override=override):
                self.setUp()
                self.seed_complete_audit()
                self.spark.seed(artifact_type, {**self.spark.latest(artifact_type), **override})
                with self.assertRaisesRegex(RuntimeError, message):
                    self.run_notebook("audit_and_report.py")
                self.assertFalse(self.spark.latest("audit_summary")["audit_complete"])

    def test_audit_verifies_the_agent_against_the_genie_api(self):
        cases = (
            ("unreadable", RuntimeError("simulated space failure"), "Could not read Genie space"),
            ("other source", types.SimpleNamespace(serialized_space='{"data_sources": {}}'), "does not use"),
            ("prefix match", types.SimpleNamespace(serialized_space=json.dumps(
                {"data_sources": {"tables": [{"identifier": MV_FQN + "_v2"}]}})), "does not use"),
            ("extra source", types.SimpleNamespace(serialized_space=json.dumps(
                {"data_sources": {"tables": [{"identifier": MV_FQN}, {"identifier": "c.s.other"}]}})), "only data source"),
        )
        for name, response, message in cases:
            with self.subTest(case=name):
                self.setUp()
                self.seed_complete_audit()
                if isinstance(response, Exception):
                    self.genie.get_space.side_effect = response
                else:
                    self.genie.get_space.return_value = response
                with self.assertRaisesRegex(RuntimeError, message):
                    self.run_notebook("audit_and_report.py")
                self.assertIsNone(self.spark.latest("audit_summary")["agent_space_id"])

    def test_malformed_artifacts_are_reported(self):
        for payload in ('{"broken":', "[]"):
            with self.subTest(payload=payload):
                self.setUp()
                self.seed_complete_audit()
                self.spark.seed("import_result", payload)
                with self.assertRaisesRegex(RuntimeError, "Invalid import_result artifact"):
                    self.run_notebook("audit_and_report.py")

    def test_audit_recomputes_instead_of_reusing_a_previous_summary(self):
        self.seed_complete_audit()
        self.spark.seed("audit_summary", {"status": "SUCCESS", "audit_complete": True})
        self.genie.get_space.side_effect = RuntimeError("simulated space failure")
        with self.assertRaisesRegex(RuntimeError, "Audit incomplete"):
            self.run_notebook("audit_and_report.py")
        summary = self.spark.latest("audit_summary")
        self.assertFalse(summary["audit_complete"])
        self.assertNotIn("audit_summary", summary["artifacts_collected"])


if __name__ == "__main__":
    unittest.main()

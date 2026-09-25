"""Run notebook control flow locally; no Databricks credentials or Spark required."""

import contextlib
import io
import itertools
import json
from pathlib import Path
import runpy
import sys
import types
import unittest
from unittest.mock import Mock, patch


NOTEBOOK_DIR = Path(__file__).resolve().parents[1] / "notebooks"
NOTEBOOKS = ("intake_and_snapshot.py", "begin_baseline_run.py", "publish_and_audit.py")
PARAMS = {
    "run_id": "test'run\\id",
    "space_id": "test-space",
    "catalog": "test_catalog",
    "schema": "test_schema",
    "triggered_by": "test provenance with 'quotes' and \\slashes",
}
APPROVED_IDS = [f"question-{i}" for i in range(20)]
SERIALIZED_SPACE = json.dumps({
    "version": 2,
    "instructions": {"text_instructions": [{"content": ['Use "net revenue".\nOwner: O\'Reilly; regex: \\d+; café.']}]},
})


class NotebookExit(BaseException):
    """Model a normal dbutils.notebook.exit separately from task failures."""


class Widgets:
    def __init__(self, values):
        self.values = dict(values)

    def text(self, name, default):
        self.values.setdefault(name, default)

    def get(self, name):
        return self.values[name]


class ArtifactStore:
    """Small stand-in for the artifact table, preserving bound values verbatim.

    This exercises handoffs, not Spark's SQL parser. A workspace smoke test is
    still needed to verify SQL execution and actual Genie API responses.
    """

    def __init__(self):
        self.rows = []
        self.calls = []
        self.fail_reads = False
        self.fail_writes = set()

    def seed(self, artifact_type, payload):
        self.rows.append({
            "run_id": PARAMS["run_id"],
            "artifact_type": artifact_type,
            "payload": json.dumps(payload),
            "created_at": len(self.rows),
        })

    def latest(self, artifact_type):
        return json.loads(next(row["payload"] for row in reversed(self.rows)
                               if row["artifact_type"] == artifact_type))

    def sql(self, query, args=None):
        self.calls.append((query, args))
        statement = " ".join(query.split())
        if statement.startswith("CREATE TABLE"):
            return types.SimpleNamespace(collect=lambda: [])
        if not args or "run_id" not in args:
            raise AssertionError("Artifact reads and writes must bind run_id")
        if args["run_id"] in query:
            raise AssertionError("run_id was interpolated into SQL")
        if statement.startswith("INSERT INTO"):
            artifact_type = args["artifact_type"]
            if artifact_type in self.fail_writes:
                raise RuntimeError("simulated artifact write failure")
            if ":payload" not in query or args["payload"] in query:
                raise AssertionError("JSON payload must be bound as a value")
            self.rows.append({**args, "created_at": len(self.rows)})
            return types.SimpleNamespace(collect=lambda: [])
        if statement.startswith("SELECT"):
            if self.fail_reads:
                raise RuntimeError("simulated artifact read failure")
            rows = [row for row in self.rows if row["run_id"] == args["run_id"]]
            if "artifact_type = 'benchmark_qc'" in query:
                rows = [row for row in rows if row["artifact_type"] == "benchmark_qc"][-1:]
            return types.SimpleNamespace(collect=lambda: rows)
        raise AssertionError(f"Unexpected SQL: {statement}")


def evaluation(status="DONE", num_questions=20, num_correct=18):
    return types.SimpleNamespace(
        eval_run_id="test-eval", eval_run_status=types.SimpleNamespace(value=status),
        num_questions=num_questions, num_correct=num_correct,
        num_needs_review=0, num_done=num_questions if status == "DONE" else 0,
    )


class WorkflowTests(unittest.TestCase):
    def setUp(self):
        self.store = ArtifactStore()
        self.genie = Mock()
        self.genie.get_space.return_value = types.SimpleNamespace(
            title="Test space", description="Test description", serialized_space=SERIALIZED_SPACE,
        )
        self.genie.genie_create_eval_run.return_value = evaluation("RUNNING")
        self.genie.genie_get_eval_run.return_value = evaluation()
        self.jobs = Mock()
        self.jobs.get_run.return_value = types.SimpleNamespace(
            creator_user_name="runner@example.com", trigger=types.SimpleNamespace(value="ONE_TIME"),
        )
        self.client = Mock(return_value=types.SimpleNamespace(genie=self.genie, jobs=self.jobs))

    def run_notebook(self, filename, **overrides):
        sdk = types.ModuleType("databricks.sdk")
        sdk.WorkspaceClient = self.client

        def exit_notebook(payload):
            raise NotebookExit(payload)

        dbutils = types.SimpleNamespace(
            widgets=Widgets({**PARAMS, **overrides}),
            notebook=types.SimpleNamespace(exit=exit_notebook),
        )
        with patch.dict(sys.modules, {"databricks": types.ModuleType("databricks"), "databricks.sdk": sdk}), \
                patch("time.sleep"), contextlib.redirect_stdout(io.StringIO()):
            try:
                runpy.run_path(str(NOTEBOOK_DIR / filename), init_globals={"dbutils": dbutils, "spark": self.store})
            except NotebookExit as e:
                return json.loads(str(e))
        self.fail("Notebook did not exit")

    def test_intake_records_the_user_who_started_the_run(self):
        self.run_notebook("intake_and_snapshot.py", triggered_by="", job_run_id="123")
        self.jobs.get_run.assert_called_once_with(run_id=123)
        manifest = self.store.latest("run_manifest")
        self.assertEqual(manifest["triggered_by"], "runner@example.com")
        self.assertEqual(manifest["trigger_type"], "ONE_TIME")

    def test_intake_does_not_fail_when_run_lookup_fails(self):
        self.jobs.get_run.side_effect = RuntimeError("simulated lookup failure")
        self.run_notebook("intake_and_snapshot.py", triggered_by="", job_run_id="123")
        self.assertIsNone(self.store.latest("run_manifest")["triggered_by"])

    def seed_complete_audit(self, final_accuracy=0.95):
        self.store.seed("run_manifest", {"run_id": PARAMS["run_id"], "space_id": PARAMS["space_id"]})
        self.store.seed("space_config_snapshot", {
            "space_id": PARAMS["space_id"], "serialized_space": SERIALIZED_SPACE,
        })
        self.store.seed("benchmark_qc", {"approved_benchmark_question_ids": APPROVED_IDS})
        self.store.seed("baseline_run", {
            "status": "SUCCESS", "eval_run_id": "test-eval", "eval_run_status": "DONE",
            "num_questions": 20, "accuracy": 0.9,
        })
        self.store.seed("optimization_result", {
            "status": "SUCCESS", "starting_accuracy": 0.9, "final_accuracy": final_accuracy,
            "final_eval_run_id": "final-eval", "rounds_executed": 1,
            "changes_per_round": [{"round": 1, "summary": "Added a description"}],
            "remaining_failures": [],
        })
        self.genie.genie_get_eval_run.return_value = evaluation(num_correct=round(final_accuracy * 20))

    def test_successful_handoffs_preserve_nested_json_and_bound_values(self):
        self.run_notebook("intake_and_snapshot.py")
        self.assertEqual(self.store.latest("space_config_snapshot")["serialized_space"], SERIALIZED_SPACE)
        self.assertEqual(self.store.latest("run_manifest")["triggered_by"], PARAMS["triggered_by"])
        self.store.seed("benchmark_qc", {"approved_benchmark_question_ids": APPROVED_IDS})
        baseline = self.run_notebook("begin_baseline_run.py")
        self.assertEqual(baseline["accuracy"], 0.9)
        self.genie.genie_create_eval_run.assert_called_once_with(
            space_id=PARAMS["space_id"], benchmark_question_ids=APPROVED_IDS,
        )
        self.store.seed("optimization_result", {
            "status": "SUCCESS", "starting_accuracy": 0.9, "final_accuracy": 0.9,
            "final_eval_run_id": baseline["eval_run_id"],
            "rounds_executed": 0, "changes_per_round": [], "remaining_failures": [],
        })
        result = self.run_notebook("publish_and_audit.py")
        self.assertEqual(result["status"], "SUCCESS")
        self.assertTrue(result["audit_complete"])
        self.assertTrue(result["target_met"])
        self.assertIn("space_config_post_opt", result["artifacts_collected"])
        self.assertEqual(self.store.latest("space_config_post_opt")["serialized_space"], SERIALIZED_SPACE)

    def test_all_partial_configurations_are_dry_runs_without_calls(self):
        for filename in NOTEBOOKS:
            for present in itertools.product((False, True), repeat=3):
                if all(present):
                    continue
                config = {key: PARAMS[key] if keep else ""
                          for key, keep in zip(("space_id", "catalog", "schema"), present)}
                with self.subTest(filename=filename, config=config):
                    self.assertEqual(self.run_notebook(filename, **config)["status"], "DRY_RUN")
        self.client.assert_not_called()
        self.assertEqual(self.store.calls, [])

    def test_configured_runs_require_a_run_id(self):
        for filename in NOTEBOOKS:
            with self.subTest(filename=filename), self.assertRaisesRegex(ValueError, "run_id is required"):
                self.run_notebook(filename, run_id="")
        self.client.assert_not_called()
        self.assertEqual(self.store.calls, [])

    def test_initial_snapshot_failure_stops_before_artifact_writes(self):
        self.genie.get_space.side_effect = RuntimeError("simulated snapshot failure")
        with self.assertRaisesRegex(RuntimeError, "snapshot failure"):
            self.run_notebook("intake_and_snapshot.py")
        self.assertEqual(self.store.calls, [])

    def test_missing_invalid_or_empty_qc_never_starts_evaluation(self):
        cases = (None, {}, [], {"approved_benchmark_question_ids": None},
                 {"approved_benchmark_question_ids": []}, {"approved_benchmark_question_ids": [""]},
                 {"approved_benchmark_question_ids": [123]})
        for qc in cases:
            with self.subTest(qc=qc):
                self.store = ArtifactStore()
                if qc is not None:
                    self.store.seed("benchmark_qc", qc)
                with self.assertRaisesRegex(RuntimeError, "benchmark_qc"):
                    self.run_notebook("begin_baseline_run.py")
                diagnostic = self.store.latest("baseline_run")
                self.assertEqual(diagnostic["status"], "FAILED")
                self.assertEqual(diagnostic["eval_run_status"], "NOT_STARTED")
                self.assertIsNone(diagnostic["accuracy"])
        self.client.assert_not_called()

    def test_unreadable_qc_never_starts_evaluation(self):
        self.store.fail_reads = True
        with self.assertRaisesRegex(RuntimeError, "artifact read failure"):
            self.run_notebook("begin_baseline_run.py")
        self.assertEqual(self.store.latest("baseline_run")["status"], "FAILED")
        self.client.assert_not_called()

    def test_unsuccessful_and_unfinished_evaluations_save_diagnostics_and_fail(self):
        for status in ("EVALUATION_FAILED", "EVALUATION_CANCELLED", "EVALUATION_TIMEOUT", "RUNNING"):
            with self.subTest(status=status):
                self.store = ArtifactStore()
                self.store.seed("benchmark_qc", {"approved_benchmark_question_ids": APPROVED_IDS})
                self.genie.genie_get_eval_run.return_value = evaluation(status)
                with self.assertRaisesRegex(RuntimeError, "Baseline"):
                    self.run_notebook("begin_baseline_run.py")
                diagnostic = self.store.latest("baseline_run")
                self.assertEqual(diagnostic["status"], "FAILED")
                self.assertEqual(diagnostic["eval_run_status"], status)
                self.assertEqual(diagnostic["eval_run_id"], "test-eval")
                self.assertIsNone(diagnostic["accuracy"])

    def test_start_and_poll_errors_save_diagnostics(self):
        for method in ("genie_create_eval_run", "genie_get_eval_run"):
            with self.subTest(method=method):
                self.setUp()
                self.store.seed("benchmark_qc", {"approved_benchmark_question_ids": APPROVED_IDS})
                getattr(self.genie, method).side_effect = RuntimeError("simulated API failure")
                with self.assertRaisesRegex(RuntimeError, "API failure"):
                    self.run_notebook("begin_baseline_run.py")
                diagnostic = self.store.latest("baseline_run")
                self.assertEqual(diagnostic["status"], "FAILED")
                self.assertIsNone(diagnostic["accuracy"])
                if method == "genie_get_eval_run":
                    self.assertEqual(diagnostic["eval_run_id"], "test-eval")

    def test_completed_evaluations_require_valid_counts(self):
        for total, correct in ((None, 0), (0, 0), (20, None), (20, 21), (20, -1)):
            with self.subTest(total=total, correct=correct):
                self.store = ArtifactStore()
                self.store.seed("benchmark_qc", {"approved_benchmark_question_ids": APPROVED_IDS})
                self.genie.genie_get_eval_run.return_value = evaluation(num_questions=total, num_correct=correct)
                with self.assertRaisesRegex(RuntimeError, "accuracy counts"):
                    self.run_notebook("begin_baseline_run.py")
                self.assertIsNone(self.store.latest("baseline_run")["accuracy"])

    def test_zero_accuracy_is_a_successful_completed_evaluation(self):
        self.store.seed("benchmark_qc", {"approved_benchmark_question_ids": APPROVED_IDS})
        self.genie.genie_get_eval_run.return_value = evaluation(num_correct=0)
        result = self.run_notebook("begin_baseline_run.py")
        self.assertEqual(result["status"], "SUCCESS")
        self.assertEqual(result["accuracy"], 0)

    def test_missing_required_artifacts_make_audit_incomplete(self):
        for missing in ("run_manifest", "space_config_snapshot", "benchmark_qc", "baseline_run", "optimization_result"):
            with self.subTest(missing=missing):
                self.store = ArtifactStore()
                self.seed_complete_audit()
                self.store.rows = [row for row in self.store.rows if row["artifact_type"] != missing]
                with self.assertRaisesRegex(RuntimeError, "Audit incomplete"):
                    self.run_notebook("publish_and_audit.py")
                summary = self.store.latest("run_summary")
                self.assertEqual(summary["status"], "INCOMPLETE")
                self.assertFalse(summary["audit_complete"])
                self.assertIsNone(summary["target_met"])
                if missing == "optimization_result":
                    self.assertIsNone(summary["final_accuracy"])

    def test_audit_snapshot_failure_cannot_reuse_previous_success(self):
        self.seed_complete_audit()
        self.store.seed("space_config_post_opt", {"space_id": PARAMS["space_id"], "serialized_space": SERIALIZED_SPACE})
        self.store.seed("run_summary", {"status": "SUCCESS", "target_met": True})
        self.genie.get_space.side_effect = RuntimeError("simulated snapshot failure")
        with self.assertRaisesRegex(RuntimeError, "snapshot failure"):
            self.run_notebook("publish_and_audit.py")
        summary = self.store.latest("run_summary")
        self.assertFalse(summary["audit_complete"])
        self.assertIsNone(summary["target_met"])
        self.assertNotIn("space_config_post_opt", summary["artifacts_collected"])

    def test_audit_snapshot_write_failure_is_recorded(self):
        self.seed_complete_audit()
        self.store.fail_writes.add("space_config_post_opt")
        with self.assertRaisesRegex(RuntimeError, "artifact write failure"):
            self.run_notebook("publish_and_audit.py")
        self.assertFalse(self.store.latest("run_summary")["audit_complete"])

    def test_malformed_audit_artifacts_are_reported(self):
        for payload in ('{"broken":', '[]', '{"final_accuracy": "0.95"}'):
            with self.subTest(payload=payload):
                self.store = ArtifactStore()
                self.seed_complete_audit()
                self.store.rows[-1]["payload"] = payload
                with self.assertRaisesRegex(RuntimeError, "Audit incomplete"):
                    self.run_notebook("publish_and_audit.py")
                self.assertIsNone(self.store.latest("run_summary")["target_met"])

    def test_completed_audit_below_target_is_successful(self):
        self.seed_complete_audit(final_accuracy=0.85)
        result = self.run_notebook("publish_and_audit.py")
        self.assertEqual(result["status"], "SUCCESS")
        self.assertTrue(result["audit_complete"])
        self.assertFalse(result["target_met"])

    def test_audit_requires_success_even_when_reported_accuracy_meets_target(self):
        for artifact_type in ("baseline_run", "optimization_result"):
            for status in ("FAILED", "RUNNING", None):
                with self.subTest(artifact_type=artifact_type, status=status):
                    self.store = ArtifactStore()
                    self.seed_complete_audit()
                    payload = self.store.latest(artifact_type)
                    payload["status"] = status
                    self.store.seed(artifact_type, payload)
                    with self.assertRaisesRegex(RuntimeError, "does not report success"):
                        self.run_notebook("publish_and_audit.py")
                    self.assertIsNone(self.store.latest("run_summary")["target_met"])

    def test_audit_verifies_final_accuracy_against_the_named_eval_run(self):
        cases = (
            ("missing id", {"final_eval_run_id": None}, None, "final_eval_run_id"),
            ("not done", {}, evaluation("EVALUATION_FAILED"), "has status"),
            ("bad counts", {}, evaluation(num_questions=0), "invalid accuracy counts"),
            ("mismatch", {}, evaluation(num_correct=10), "does not match the final evaluation"),
            ("other questions", {}, evaluation(num_questions=40, num_correct=38), "did not cover"),
            ("reused baseline", {"final_eval_run_id": "test-eval"}, None, "baseline eval run"),
        )
        for name, override, final_eval, message in cases:
            with self.subTest(case=name):
                self.setUp()
                self.seed_complete_audit()
                payload = {**self.store.latest("optimization_result"), **override}
                self.store.seed("optimization_result", payload)
                if final_eval is not None:
                    self.genie.genie_get_eval_run.return_value = final_eval
                with self.assertRaisesRegex(RuntimeError, message):
                    self.run_notebook("publish_and_audit.py")
                self.assertIsNone(self.store.latest("run_summary")["target_met"])

    def test_audit_uses_api_accuracy_when_reported_value_is_rounded(self):
        self.seed_complete_audit()
        self.genie.genie_get_eval_run.return_value = evaluation(num_questions=20, num_correct=17)
        payload = {**self.store.latest("optimization_result"), "final_accuracy": 0.86}
        self.store.seed("optimization_result", payload)
        result = self.run_notebook("publish_and_audit.py")
        self.assertTrue(result["audit_complete"])
        self.assertEqual(self.store.latest("run_summary")["final_accuracy"], 0.85)

    def test_audit_rejects_negative_max_rounds(self):
        self.seed_complete_audit()
        with self.assertRaisesRegex(ValueError, "max_rounds"):
            self.run_notebook("publish_and_audit.py", max_rounds="-1")


if __name__ == "__main__":
    unittest.main()

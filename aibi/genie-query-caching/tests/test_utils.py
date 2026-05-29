import pathlib
import sys
from types import SimpleNamespace

import pytest

EXAMPLE_DIR = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(EXAMPLE_DIR))

import utils  # noqa: E402


def _valid_config():
    return {
        "catalog": "main",
        "schema": "genie_cache",
        "data_catalog": "main",
        "data_schema": "horizon_bank",
        "cache_version": "v1",
        "genie_space_id": "space_123",
        "genie_timeout_minutes": 5,
        "vs_endpoint": "genie-cache-vs-endpoint",
        "embedding_model": "databricks-qwen3-embedding-0-6b",
        "embedding_dimension": 1024,
        "result_row_limit": 100,
        "lakebase": {
            "host": "lakebase.example.databricks.com",
            "port": 5432,
            "database": "cache",
            "secret_scope": "scope",
            "secret_key_username": "username",
            "secret_key_password": "password",
        },
        "thresholds": {
            "lakebase_similarity": 0.92,
            "lakebase_hybrid": 0.93,
            "vs_auto_execute": 0.90,
            "vs_confirm": 0.75,
        },
    }


def test_load_config_validates_required_values(tmp_path):
    config_path = tmp_path / "configs.yaml"
    config_path.write_text(
        """
catalog: main
schema: genie_cache
data_schema: horizon_bank
cache_version: v1
genie_space_id: space_123
vs_endpoint: genie-cache-vs-endpoint
lakebase:
  host: lakebase.example.databricks.com
  database: cache
  secret_scope: scope
  secret_key_username: username
  secret_key_password: password
thresholds:
  vs_auto_execute: 0.90
  vs_confirm: 0.75
""",
        encoding="utf-8",
    )

    loaded = utils.load_config(str(config_path))

    assert loaded["catalog"] == "main"
    assert utils.build_cache_context(loaded).data_schema == "horizon_bank"


def test_validate_config_rejects_unsafe_uc_identifier():
    config = _valid_config()
    config["catalog"] = "main;DROP"

    with pytest.raises(ValueError, match="catalog"):
        utils.validate_config(config)


def test_validate_config_rejects_bad_threshold_ordering():
    config = _valid_config()
    config["thresholds"]["vs_confirm"] = 0.95
    config["thresholds"]["vs_auto_execute"] = 0.90

    with pytest.raises(ValueError, match="vs_confirm"):
        utils.validate_config(config)


def test_normalize_question_removes_punctuation_and_collapses_space():
    assert utils.normalize_question("  What was revenue?!  ") == "what was revenue"
    assert utils.normalize_question("A   B\nC") == "a b c"


def test_deterministic_cache_id_is_stable_and_session_scoped():
    config = _valid_config()

    first = utils.deterministic_cache_id(config, "What was revenue?")
    second = utils.deterministic_cache_id(config, "what was revenue")
    session_a = utils.deterministic_cache_id(
        config, "What was revenue?", cache_scope=utils.SESSION_SCOPE, session_id="a"
    )
    session_b = utils.deterministic_cache_id(
        config, "What was revenue?", cache_scope=utils.SESSION_SCOPE, session_id="b"
    )

    assert first == second
    assert session_a != session_b
    assert first != session_a
    with pytest.raises(ValueError, match="session_id"):
        utils.deterministic_cache_id(config, "What was revenue?", cache_scope=utils.SESSION_SCOPE)


def test_generate_embedding_uses_extra_params(monkeypatch):
    calls = {}

    class FakeServingEndpoints:
        def query(self, **kwargs):
            calls.update(kwargs)
            return SimpleNamespace(data=[SimpleNamespace(embedding=[0.1, 0.2])])

    fake_client = SimpleNamespace(serving_endpoints=FakeServingEndpoints())
    monkeypatch.setattr(utils, "_workspace_client", fake_client)

    embedding = utils.generate_embedding(
        "hello",
        model="embedding-model",
        instruction="Represent this question:",
        dimensions=128,
    )

    assert embedding == [0.1, 0.2]
    assert calls == {
        "name": "embedding-model",
        "input": ["hello"],
        "extra_params": {
            "instruction": "Represent this question:",
            "dimensions": 128,
        },
    }


def test_coerce_text_handles_psycopg_jsonb_dicts():
    assert utils._coerce_text("plain") == "plain"
    assert utils._coerce_text({"sql": "SELECT 1", "text": "answer"}) == "answer"
    assert utils._coerce_text(None) is None


def test_normalize_cached_sql_rejects_non_read_only_sql():
    assert utils._normalize_cached_sql(" SELECT 1; ") == "SELECT 1"
    assert utils._normalize_cached_sql("WITH x AS (SELECT 1) SELECT * FROM x").startswith("WITH")
    with pytest.raises(ValueError, match="read-only"):
        utils._normalize_cached_sql("DELETE FROM table")

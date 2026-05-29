"""
Shared utilities for Genie Query Caching notebooks.

The cache stores generated SQL plus metadata. Cache hits re-execute SQL under
the current Databricks context instead of replaying prior answer text.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import random
import re
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import MessageStatus

DEFAULT_EMBEDDING_MODEL = "databricks-qwen3-embedding-0-6b"
DEFAULT_EMBEDDING_DIMENSION = 1024
DEFAULT_RESULT_ROW_LIMIT = 100
GLOBAL_SCOPE = "global"
SESSION_SCOPE = "session"
_UC_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_ENDPOINT_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]+$")


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class CacheContext:
    """Stable cache context derived from Genie space and data version."""

    space_id: str
    data_catalog: str
    data_schema: str
    cache_version: str
    cache_context_key: str


@dataclass
class GenieResult:
    """Parsed result from a Genie API call."""

    conversation_id: str
    message_id: str
    generated_sql: Optional[str] = None
    response_text: Optional[str] = None
    latency_seconds: float = 0.0

    @property
    def cacheable(self) -> bool:
        return bool(self.generated_sql and self.generated_sql.strip())


@dataclass
class CacheLookupResult:
    """Result from a cache lookup."""

    hit_type: Optional[str]
    row_id: Optional[str] = None
    question_text: Optional[str] = None
    generated_sql: Optional[str] = None
    genie_response_text: Optional[str] = None
    score: float = 0.0
    embedding: Optional[list[float]] = None
    tier: Optional[str] = None

    @property
    def hit(self) -> bool:
        return self.hit_type is not None


@dataclass
class SqlExecutionResult:
    """Preview result from executing cached SQL."""

    rows: list[dict[str, Any]]
    row_count: int
    truncated: bool
    latency_seconds: float


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def load_config(path: str = "./configs.yaml") -> dict:
    """Load and validate YAML configuration."""
    with open(path) as f:
        config = yaml.safe_load(f) or {}
    validate_config(config)
    return config


def validate_config(config: dict) -> None:
    """Fail fast for missing placeholders, unsafe identifiers, and bad thresholds."""
    required = ["catalog", "schema", "data_schema", "genie_space_id", "vs_endpoint", "cache_version"]
    missing = [key for key in required if not _has_real_value(config.get(key))]
    if missing:
        raise ValueError(f"Missing required config values: {', '.join(missing)}")

    for key in ["catalog", "schema", "data_schema"]:
        _validate_uc_identifier(config[key], key)
    if "data_catalog" in config and config["data_catalog"]:
        _validate_uc_identifier(config["data_catalog"], "data_catalog")

    if not _ENDPOINT_NAME_RE.match(str(config["vs_endpoint"])):
        raise ValueError("vs_endpoint may only contain letters, numbers, '.', '_', and '-'")

    dim = int(config.get("embedding_dimension", DEFAULT_EMBEDDING_DIMENSION))
    if dim < 32 or dim > 4096:
        raise ValueError("embedding_dimension must be between 32 and 4096")

    row_limit = int(config.get("result_row_limit", DEFAULT_RESULT_ROW_LIMIT))
    if row_limit < 1 or row_limit > 10000:
        raise ValueError("result_row_limit must be between 1 and 10000")

    thresholds = config.get("thresholds", {})
    vs_confirm = float(thresholds.get("vs_confirm", 0.75))
    vs_auto = float(thresholds.get("vs_auto_execute", 0.90))
    if vs_confirm > vs_auto:
        raise ValueError("thresholds.vs_confirm must be <= thresholds.vs_auto_execute")

    lakebase = config.get("lakebase", {})
    for key in ["host", "database", "secret_scope", "secret_key_username", "secret_key_password"]:
        if not _has_real_value(lakebase.get(key)):
            raise ValueError(f"Missing required Lakebase config value: lakebase.{key}")


def _has_real_value(value: Any) -> bool:
    if value is None:
        return False
    text = str(value).strip()
    return bool(text) and not text.startswith("your_") and text != "TODO"


def _validate_uc_identifier(value: str, field_name: str) -> None:
    if not _UC_IDENTIFIER_RE.match(str(value)):
        raise ValueError(
            f"{field_name} must be a simple Unity Catalog identifier "
            "(letters, numbers, and underscores only)."
        )


def build_cache_context(config: dict) -> CacheContext:
    """Build a stable cache context key for a Genie/data semantic version."""
    space_id = config["genie_space_id"]
    data_catalog = config.get("data_catalog") or config["catalog"]
    data_schema = config["data_schema"]
    cache_version = str(config["cache_version"])
    raw = "|".join([space_id, data_catalog, data_schema, cache_version])
    context_key = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]
    return CacheContext(
        space_id=space_id,
        data_catalog=data_catalog,
        data_schema=data_schema,
        cache_version=cache_version,
        cache_context_key=context_key,
    )


def table_name(config: dict, table: str) -> str:
    """Return a validated three-part cache table name."""
    _validate_uc_identifier(table, "table")
    return f"{config['catalog']}.{config['schema']}.{table}"


def data_schema_name(config: dict) -> str:
    """Return the configured data catalog/schema used by cached SQL examples."""
    context = build_cache_context(config)
    return f"{context.data_catalog}.{context.data_schema}"


# ---------------------------------------------------------------------------
# Optional MLflow tracing
# ---------------------------------------------------------------------------

def mlflow_tracing_enabled(config: dict) -> bool:
    """Return True when optional MLflow tracing should be attempted."""
    mlflow_cfg = config.get("mlflow", {})
    return bool(
        mlflow_cfg.get("enable_tracing", False)
        and os.getenv("MLFLOW_TRACKING_URI")
        and os.getenv("MLFLOW_EXPERIMENT_ID")
    )


@contextlib.contextmanager
def trace_operation(
    config: dict,
    name: str,
    span_type: str = "UNKNOWN",
    inputs: Optional[dict[str, Any]] = None,
):
    """Create an MLflow span when tracing is configured, otherwise no-op."""
    if not mlflow_tracing_enabled(config):
        yield None
        return

    try:
        import mlflow
        from mlflow.entities import SpanType

        span_type_value = getattr(SpanType, span_type, SpanType.UNKNOWN)
        with mlflow.start_span(name=name, span_type=span_type_value) as span:
            if inputs:
                span.set_inputs(_safe_trace_payload(inputs))
            yield span
    except Exception as e:
        if config.get("mlflow", {}).get("debug", False):
            print(f"  MLflow tracing disabled for span '{name}': {e}")
        yield None


def _safe_trace_payload(payload: dict[str, Any]) -> dict[str, Any]:
    safe = {}
    for key, value in payload.items():
        if key.lower() in {"password", "token", "secret"}:
            safe[key] = "<redacted>"
        elif isinstance(value, str) and len(value) > 2000:
            safe[key] = value[:2000] + "...<truncated>"
        else:
            safe[key] = value
    return safe


# ---------------------------------------------------------------------------
# Question normalization and cache IDs
# ---------------------------------------------------------------------------

def normalize_question(question: str) -> str:
    """Normalize a question for exact-match dedup."""
    q = question.lower().strip()
    q = re.sub(r"[^\w\s]", "", q)
    q = re.sub(r"\s+", " ", q)
    return q


def deterministic_cache_id(
    config: dict,
    question: str,
    cache_scope: str = GLOBAL_SCOPE,
    session_id: Optional[str] = None,
) -> str:
    """Return a deterministic UUID string for a cache entry."""
    context = build_cache_context(config)
    session_key = _session_key(cache_scope, session_id)
    raw = "|".join(
        [
            context.space_id,
            context.cache_context_key,
            cache_scope,
            session_key,
            normalize_question(question),
        ]
    )
    return str(uuid.uuid5(uuid.NAMESPACE_URL, raw))


def _session_key(cache_scope: str, session_id: Optional[str]) -> str:
    if cache_scope == SESSION_SCOPE:
        if not session_id:
            raise ValueError("session_id is required for session-scoped cache entries")
        return session_id
    if cache_scope != GLOBAL_SCOPE:
        raise ValueError(f"Unsupported cache_scope: {cache_scope}")
    return ""


def utcnow() -> datetime:
    """Return the current UTC time."""
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Embedding generation (Foundation Model API)
# ---------------------------------------------------------------------------

_workspace_client: Optional[WorkspaceClient] = None


def _get_workspace_client() -> WorkspaceClient:
    global _workspace_client
    if _workspace_client is None:
        _workspace_client = WorkspaceClient()
    return _workspace_client


def generate_embedding(
    text: str,
    model: str = DEFAULT_EMBEDDING_MODEL,
    instruction: Optional[str] = None,
    dimensions: Optional[int] = None,
) -> list[float]:
    """Generate an embedding vector via the Databricks Foundation Model API."""
    w = _get_workspace_client()
    extra_params: dict[str, Any] = {}
    if instruction:
        extra_params["instruction"] = instruction
    if dimensions:
        extra_params["dimensions"] = dimensions

    kwargs: dict[str, Any] = {"name": model, "input": [text]}
    if extra_params:
        kwargs["extra_params"] = extra_params
    response = w.serving_endpoints.query(**kwargs)
    return response.data[0].embedding


def generate_embedding_from_config(config: dict, text: str) -> list[float]:
    """Generate an embedding using model settings from config."""
    with trace_operation(config, "generate_embedding", "EMBEDDING", {"text": text[:200]}):
        return generate_embedding(
            text,
            model=config.get("embedding_model", DEFAULT_EMBEDDING_MODEL),
            instruction=config.get("embedding_instruction"),
            dimensions=config.get("embedding_dimension", DEFAULT_EMBEDDING_DIMENSION),
        )


# ---------------------------------------------------------------------------
# Retry with decorrelated jitter
# ---------------------------------------------------------------------------

def retry_with_backoff(
    fn,
    max_attempts: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
):
    """Execute *fn* with decorrelated-jitter backoff."""
    last_delay = base_delay
    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except Exception as e:
            if attempt == max_attempts:
                raise
            delay = min(max_delay, random.uniform(base_delay, last_delay * 3))
            last_delay = delay
            print(f"  Attempt {attempt}/{max_attempts} failed: {e}")
            print(f"  Retrying in {delay:.1f}s...")
            time.sleep(delay)


# ---------------------------------------------------------------------------
# Genie API wrapper
# ---------------------------------------------------------------------------

def call_genie(space_id: str, question: str, timeout_minutes: int = 5) -> GenieResult:
    """Send a question to a Genie Space and return generated SQL/text."""
    w = _get_workspace_client()
    start = time.time()

    message = w.genie.start_conversation_and_wait(
        space_id=space_id,
        content=question,
        timeout=timedelta(minutes=timeout_minutes),
    )

    if message.status == MessageStatus.FAILED:
        error_msg = message.error.message if message.error else "Unknown error"
        raise RuntimeError(f"Genie failed: {error_msg}")

    generated_sql = None
    response_text = None

    if message.attachments:
        for att in message.attachments:
            if att.query and att.query.query:
                generated_sql = att.query.query
            if att.text and att.text.content:
                response_text = att.text.content

    return GenieResult(
        conversation_id=message.conversation_id,
        message_id=message.id,
        generated_sql=generated_sql,
        response_text=response_text,
        latency_seconds=time.time() - start,
    )


def call_genie_with_retry(config: dict, question: str) -> GenieResult:
    """Call the Genie API with configurable retry/backoff and optional tracing."""
    retry_cfg = config.get("retry", {})

    def _call():
        with trace_operation(config, "call_genie", "LLM", {"question": question}):
            return call_genie(
                space_id=config["genie_space_id"],
                question=question,
                timeout_minutes=config.get("genie_timeout_minutes", 5),
            )

    return retry_with_backoff(
        _call,
        max_attempts=retry_cfg.get("max_attempts", 5),
        base_delay=retry_cfg.get("base_delay", 1.0),
        max_delay=retry_cfg.get("max_delay", 60.0),
    )


# ---------------------------------------------------------------------------
# SQL execution
# ---------------------------------------------------------------------------

def execute_cached_sql(sql: str, row_limit: int = DEFAULT_RESULT_ROW_LIMIT) -> SqlExecutionResult:
    """Execute cached SQL under the current Spark/Databricks identity."""
    cleaned_sql = _normalize_cached_sql(sql)
    start = time.time()

    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active Spark session is available for SQL execution")

    df = spark.sql(cleaned_sql)
    preview_rows = df.limit(row_limit + 1).collect()
    truncated = len(preview_rows) > row_limit
    rows = [row.asDict(recursive=True) for row in preview_rows[:row_limit]]
    return SqlExecutionResult(
        rows=rows,
        row_count=len(rows),
        truncated=truncated,
        latency_seconds=time.time() - start,
    )


def execute_cached_sql_with_trace(
    config: dict,
    sql: str,
    row_limit: Optional[int] = None,
) -> SqlExecutionResult:
    """Execute cached SQL with optional MLflow tracing."""
    limit = row_limit or int(config.get("result_row_limit", DEFAULT_RESULT_ROW_LIMIT))
    with trace_operation(config, "execute_cached_sql", "TOOL", {"row_limit": limit, "sql": sql}):
        return execute_cached_sql(sql, limit)


def _normalize_cached_sql(sql: str) -> str:
    if not sql or not sql.strip():
        raise ValueError("Cannot execute empty cached SQL")
    cleaned = sql.strip().rstrip(";")
    first_token = re.sub(r"^\s*/\*.*?\*/", "", cleaned, flags=re.DOTALL).lstrip().split(None, 1)[0].lower()
    if first_token not in {"select", "with"}:
        raise ValueError("Cached SQL must be read-only and start with SELECT or WITH")
    return cleaned


# ---------------------------------------------------------------------------
# Lakebase (PostgreSQL + pgvector) connectivity
# ---------------------------------------------------------------------------

_lakebase_conn = None


def get_lakebase_connection(config: dict):
    """Return a cached psycopg connection to Lakebase with pgvector support."""
    global _lakebase_conn

    import psycopg
    from pgvector.psycopg import register_vector

    if _lakebase_conn is not None:
        try:
            _lakebase_conn.execute("SELECT 1")
            return _lakebase_conn
        except Exception:
            try:
                _lakebase_conn.close()
            except Exception:
                pass
            _lakebase_conn = None

    lb = config["lakebase"]

    from pyspark.dbutils import DBUtils
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active Spark session is available for Databricks Secrets")
    dbutils = DBUtils(spark)

    username = dbutils.secrets.get(scope=lb["secret_scope"], key=lb["secret_key_username"])
    password = dbutils.secrets.get(scope=lb["secret_scope"], key=lb["secret_key_password"])

    conn = psycopg.connect(
        host=lb["host"],
        port=lb.get("port", 5432),
        dbname=lb["database"],
        user=username,
        password=password,
        sslmode="require",
        autocommit=False,
    )
    register_vector(conn)
    _lakebase_conn = conn
    return conn


def close_lakebase_connection():
    """Explicitly close the cached Lakebase connection."""
    global _lakebase_conn
    if _lakebase_conn is not None:
        try:
            _lakebase_conn.close()
        except Exception:
            pass
        _lakebase_conn = None


# ---------------------------------------------------------------------------
# Lakebase cache operations
# ---------------------------------------------------------------------------

def lakebase_cache_lookup(
    config: dict,
    question: str,
    threshold: float,
    cache_scope: str = GLOBAL_SCOPE,
    session_id: Optional[str] = None,
) -> CacheLookupResult:
    """Check Lakebase for exact or vector cache hits."""
    context = build_cache_context(config)
    session_key = _session_key(cache_scope, session_id)
    normalized = normalize_question(question)

    with trace_operation(
        config,
        "lakebase_cache_lookup",
        "RETRIEVER",
        {"question": question, "cache_scope": cache_scope, "session_id": session_key},
    ):
        conn = get_lakebase_connection(config)
        with conn.cursor() as cur:
            exact_params = [
                context.space_id,
                context.cache_context_key,
                cache_scope,
                session_key,
                normalized,
            ]
            cur.execute(
                """
                SELECT id, question_text, generated_sql, genie_response_text
                FROM genie_cache
                WHERE space_id = %s
                  AND cache_context_key = %s
                  AND cache_scope = %s
                  AND session_id = %s
                  AND question_normalized = %s
                LIMIT 1
                """,
                exact_params,
            )
            row = cur.fetchone()
            if row:
                _lakebase_increment_hit(cur, row[0])
                conn.commit()
                return CacheLookupResult(
                    hit_type="exact",
                    row_id=str(row[0]),
                    question_text=row[1],
                    generated_sql=row[2],
                    genie_response_text=_coerce_text(row[3]),
                    score=1.0,
                )

            embedding = generate_embedding_from_config(config, question)
            vector_params = [
                embedding,
                context.space_id,
                context.cache_context_key,
                cache_scope,
                session_key,
                embedding,
            ]
            cur.execute(
                """
                SELECT id, question_text, generated_sql, genie_response_text,
                       1 - (embedding <=> %s::vector) AS similarity
                FROM genie_cache
                WHERE space_id = %s
                  AND cache_context_key = %s
                  AND cache_scope = %s
                  AND session_id = %s
                  AND embedding IS NOT NULL
                ORDER BY embedding <=> %s::vector
                LIMIT 1
                """,
                vector_params,
            )
            row = cur.fetchone()
            if row and row[4] is not None and row[4] >= threshold:
                _lakebase_increment_hit(cur, row[0])
                conn.commit()
                return CacheLookupResult(
                    hit_type="vector",
                    row_id=str(row[0]),
                    question_text=row[1],
                    generated_sql=row[2],
                    genie_response_text=_coerce_text(row[3]),
                    score=float(row[4]),
                    embedding=embedding,
                )

        return CacheLookupResult(hit_type=None, score=0.0, embedding=embedding)


def lakebase_cache_write(
    config: dict,
    question: str,
    sql: str,
    response_text: str = "",
    cache_scope: str = GLOBAL_SCOPE,
    session_id: Optional[str] = None,
    embedding: Optional[list[float]] = None,
    validated: bool = False,
    validation_source: Optional[str] = None,
) -> str:
    """Upsert a generated SQL cache entry into Lakebase."""
    if not sql or not sql.strip():
        raise ValueError("Only Genie responses with generated SQL can be cached")

    context = build_cache_context(config)
    session_key = _session_key(cache_scope, session_id)
    normalized = normalize_question(question)
    row_id = deterministic_cache_id(config, question, cache_scope, session_id)
    if embedding is None:
        embedding = generate_embedding_from_config(config, question)

    with trace_operation(
        config,
        "lakebase_cache_write",
        "TOOL",
        {"question": question, "cache_scope": cache_scope, "session_id": session_key},
    ):
        conn = get_lakebase_connection(config)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO genie_cache (
                    id, space_id, cache_context_key, cache_scope, session_id,
                    question_text, question_normalized, embedding, generated_sql,
                    genie_response_text, validated, validation_source
                )
                VALUES (
                    %s::uuid, %s, %s, %s, %s,
                    %s, %s, %s::vector, %s,
                    %s, %s, %s
                )
                ON CONFLICT (
                    space_id, cache_context_key, cache_scope, session_id, question_normalized
                ) DO UPDATE SET
                    question_text = EXCLUDED.question_text,
                    embedding = EXCLUDED.embedding,
                    generated_sql = EXCLUDED.generated_sql,
                    genie_response_text = EXCLUDED.genie_response_text,
                    validated = genie_cache.validated OR EXCLUDED.validated,
                    validation_source = COALESCE(EXCLUDED.validation_source, genie_cache.validation_source),
                    updated_at = now()
                """,
                (
                    row_id,
                    context.space_id,
                    context.cache_context_key,
                    cache_scope,
                    session_key,
                    question,
                    normalized,
                    embedding,
                    sql,
                    response_text or "",
                    validated,
                    validation_source,
                ),
            )
        conn.commit()
    return row_id


def _lakebase_increment_hit(cur, row_id: str) -> None:
    cur.execute(
        """
        UPDATE genie_cache
        SET hit_count = hit_count + 1,
            last_hit_at = now(),
            updated_at = now()
        WHERE id = %s::uuid
        """,
        (str(row_id),),
    )


def _coerce_text(value: Any) -> Optional[str]:
    """Handle text and old JSONB values safely."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        return value.get("text") or json.dumps(value)
    return str(value)


# ---------------------------------------------------------------------------
# Delta cache operations
# ---------------------------------------------------------------------------

def delta_cache_upsert(
    spark,
    table: str,
    config: dict,
    question: str,
    sql: str,
    response_text: str = "",
    cache_scope: str = GLOBAL_SCOPE,
    session_id: Optional[str] = None,
    validated: bool = False,
    validation_source: Optional[str] = None,
) -> str:
    """Upsert a deterministic cache row into a Delta table."""
    if not sql or not sql.strip():
        raise ValueError("Only Genie responses with generated SQL can be cached")

    from pyspark.sql import Row

    context = build_cache_context(config)
    session_key = _session_key(cache_scope, session_id)
    row_id = deterministic_cache_id(config, question, cache_scope, session_id)
    now = utcnow()
    view_name = f"_genie_cache_upsert_{uuid.uuid4().hex}"
    row = Row(
        id=row_id,
        space_id=context.space_id,
        cache_context_key=context.cache_context_key,
        cache_scope=cache_scope,
        session_id=session_key,
        question_text=question,
        question_normalized=normalize_question(question),
        generated_sql=sql,
        genie_response_text=response_text or "",
        validated=bool(validated),
        validation_source=validation_source,
        created_at=now,
        updated_at=now,
        last_hit_at=None,
        hit_count=0,
    )
    spark.createDataFrame([row]).createOrReplaceTempView(view_name)

    with trace_operation(
        config,
        "delta_cache_upsert",
        "TOOL",
        {"table": table, "question": question, "cache_scope": cache_scope},
    ):
        spark.sql(
            f"""
            MERGE INTO {table} AS target
            USING {view_name} AS source
            ON target.id = source.id
            WHEN MATCHED THEN UPDATE SET
                target.question_text = source.question_text,
                target.question_normalized = source.question_normalized,
                target.generated_sql = source.generated_sql,
                target.genie_response_text = source.genie_response_text,
                target.validated = target.validated OR source.validated,
                target.validation_source = COALESCE(source.validation_source, target.validation_source),
                target.updated_at = source.updated_at
            WHEN NOT MATCHED THEN INSERT (
                id,
                space_id,
                cache_context_key,
                cache_scope,
                session_id,
                question_text,
                question_normalized,
                generated_sql,
                genie_response_text,
                validated,
                validation_source,
                created_at,
                updated_at,
                last_hit_at,
                hit_count
            ) VALUES (
                source.id,
                source.space_id,
                source.cache_context_key,
                source.cache_scope,
                source.session_id,
                source.question_text,
                source.question_normalized,
                source.generated_sql,
                source.genie_response_text,
                source.validated,
                source.validation_source,
                source.created_at,
                source.updated_at,
                source.last_hit_at,
                source.hit_count
            )
            """
        )
    return row_id


def delta_increment_hit_count(spark, table: str, row_id: str) -> None:
    """Increment hit metadata for a Delta cache row."""
    spark.sql(
        f"""
        UPDATE {table}
        SET hit_count = hit_count + 1,
            last_hit_at = current_timestamp(),
            updated_at = current_timestamp()
        WHERE id = '{row_id}'
        """
    )


# ---------------------------------------------------------------------------
# Vector Search helpers
# ---------------------------------------------------------------------------

def vector_search_filters(
    config: dict,
    cache_scope: str = GLOBAL_SCOPE,
    session_id: Optional[str] = None,
    validated_only: bool = True,
) -> dict[str, Any]:
    """Return Vector Search filters for the current cache context."""
    context = build_cache_context(config)
    filters: dict[str, Any] = {
        "space_id": context.space_id,
        "cache_context_key": context.cache_context_key,
        "cache_scope": cache_scope,
    }
    if cache_scope == SESSION_SCOPE:
        filters["session_id"] = _session_key(cache_scope, session_id)
    if validated_only:
        filters["validated"] = True
    return filters


def sync_vs_index_and_wait(vsc, endpoint_name: str, index_name: str, timeout_minutes: int = 10):
    """Trigger a VS index sync and wait for completion."""
    index = vsc.get_index(endpoint_name=endpoint_name, index_name=index_name)
    index.sync()
    print("  VS index sync triggered...")

    start = time.time()
    timeout_seconds = timeout_minutes * 60

    while True:
        status = index.describe().get("status", {})
        if status.get("ready", False):
            print("  VS index sync complete")
            return
        if time.time() - start > timeout_seconds:
            print("  WARNING: Sync timed out; new entries may not be searchable yet")
            return
        time.sleep(5)


# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------

def print_sql_preview(result: SqlExecutionResult, max_rows: int = 5) -> None:
    """Print a compact preview of SQL execution rows."""
    print(
        f"  SQL executed in {result.latency_seconds:.3f}s "
        f"({result.row_count} preview row(s){', truncated' if result.truncated else ''})"
    )
    for row in result.rows[:max_rows]:
        print(f"    {row}")


def print_summary_table(results: list[dict], columns: list[str]):
    """Print a formatted summary table from a list of result dicts."""
    header = f"{columns[0]:<50}" + "".join(f"{c:>14}" for c in columns[1:])
    print(header)
    print("-" * len(header))
    for row in results:
        line = f"{str(row[columns[0]])[:50]:<50}"
        for c in columns[1:]:
            val = row.get(c)
            if isinstance(val, float):
                line += f"{val:>14.3f}"
            else:
                line += f"{str(val):>14}"
        print(line)

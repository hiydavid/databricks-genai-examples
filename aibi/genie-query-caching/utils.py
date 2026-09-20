"""
Shared utilities for Genie Query Caching notebooks.

Provides: config loading, question normalization, embedding generation,
retry with exponential backoff, Genie API wrapper, Lakebase connectivity,
cache lookup/write for Lakebase pgvector, and VS index sync.
"""

import json
import random
import re
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import MessageStatus

DEFAULT_EMBEDDING_MODEL = "databricks-qwen3-embedding-0-6b"


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def load_config(path: str = "./configs.yaml") -> dict:
    """Load YAML configuration file."""
    with open(path) as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# Question normalization
# ---------------------------------------------------------------------------

def normalize_question(question: str) -> str:
    """Normalize a question for exact-match dedup.

    Lowercases, removes a trailing question mark, and collapses whitespace.
    Preserve meaningful punctuation: '1.5', '15', '<', and '>' must not
    collapse to the same cache key.
    """
    q = question.lower().strip().rstrip("?").rstrip()
    q = re.sub(r"\s+", " ", q)
    return q


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
) -> list[float]:
    """Generate an embedding vector via the Databricks Foundation Model API.

    Uses ``WorkspaceClient().serving_endpoints.query()`` which returns an
    OpenAI-compatible response with ``data[0].embedding``.

    Parameters
    ----------
    instruction : str, optional
        Task-specific instruction for instruction-aware models (e.g.,
        ``databricks-qwen3-embedding-0-6b``).  Can improve retrieval by 1-5%.
        ``query()`` has no dedicated kwarg for it, so it is passed through
        ``extra_params``.
    """
    w = _get_workspace_client()
    extra_params = {"instruction": instruction} if instruction is not None else None
    response = w.serving_endpoints.query(name=model, input=[text], extra_params=extra_params)
    return response.data[0].embedding


# ---------------------------------------------------------------------------
# Retry with exponential backoff + decorrelated jitter
# ---------------------------------------------------------------------------

def _is_retryable(e: Exception) -> bool:
    """Return True only for transient errors worth retrying.

    Auth failures, NOT_FOUND, malformed requests etc. fail immediately.
    The SDK maps HTTP statuses to typed exceptions, so retryability is
    decided from the exception class; ``retry_after_secs`` is set whenever
    the API returns a Retry-After header.
    """
    if isinstance(e, (TimeoutError, ConnectionError)):
        return True
    try:
        from databricks.sdk.errors import (
            DatabricksError,
            DeadlineExceeded,
            InternalError,
            TemporarilyUnavailable,
            TooManyRequests,
        )
    except ImportError:
        return False
    if isinstance(e, DatabricksError):
        return (
            e.retry_after_secs is not None
            or isinstance(e, (TooManyRequests, InternalError, TemporarilyUnavailable, DeadlineExceeded))
        )
    return False


def retry_with_backoff(
    fn,
    max_attempts: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
):
    """Execute *fn* with exponential backoff and decorrelated jitter.

    Jitter formula: ``delay = min(max_delay, uniform(base_delay, prev_delay * 3))``
    This avoids thundering-herd effects in distributed systems.

    Only the Genie API call should be wrapped with this — cache lookups should
    fail fast without retry.  Non-retryable errors (auth, NOT_FOUND, ...) are
    raised on the first attempt rather than retried.
    """
    last_delay = base_delay
    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except Exception as e:
            if attempt == max_attempts or not _is_retryable(e):
                raise
            delay = min(max_delay, random.uniform(base_delay, last_delay * 3))
            last_delay = delay
            print(f"  Attempt {attempt}/{max_attempts} failed: {e}")
            print(f"  Retrying in {delay:.1f}s...")
            time.sleep(delay)


# ---------------------------------------------------------------------------
# Genie API wrapper
# ---------------------------------------------------------------------------
# We use the Databricks Python SDK (start_conversation_and_wait) rather than:
#   - REST API: Would require manual polling of POST /api/2.0/genie/spaces/
#     {space_id}/start-conversation every 5-10s with no benefit over the SDK.
#   - Databricks Managed MCP: Designed for AI agent tool use, not programmatic
#     caching.  MCP output is not meant to be parsed programmatically, which
#     conflicts with our need to extract SQL and response text from attachments.
# The SDK handles long-polling automatically and provides typed response objects.
# See: https://docs.databricks.com/aws/en/genie/conversation-api

@dataclass
class GenieResult:
    """Parsed result from a Genie API call."""

    conversation_id: str
    message_id: str
    generated_sql: Optional[str] = None
    response_text: Optional[str] = None
    latency_seconds: float = 0.0


def call_genie(
    space_id: str,
    question: str,
    timeout_minutes: int = 5,
) -> GenieResult:
    """Send a question to a Genie Space and return the parsed result.

    Uses ``start_conversation_and_wait`` from the Databricks SDK.  On failure
    the SDK raises, which the caller can catch or wrap with ``retry_with_backoff``.
    """
    w = _get_workspace_client()
    start = time.time()

    message = w.genie.start_conversation_and_wait(
        space_id=space_id,
        content=question,
        timeout=timedelta(minutes=timeout_minutes),
    )

    if message.status == MessageStatus.FAILED:
        error_msg = message.error.error if message.error else "Unknown error"
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
    """Call the Genie API with configurable retry / backoff.

    Reads ``genie_space_id``, ``genie_timeout_minutes``, and ``retry.*``
    from *config*.
    """
    retry_cfg = config.get("retry", {})

    def _call():
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
# Lakebase (PostgreSQL + pgvector) connectivity
# ---------------------------------------------------------------------------

_lakebase_conn = None


def get_lakebase_connection(config: dict):
    """Return a cached ``psycopg`` connection to Lakebase with pgvector support.

    The connection is created once and reused across calls within a notebook
    session.  If the connection is closed or broken, a new one is created.

    Credentials are fetched from Databricks Secrets using the scope and keys
    defined in ``config["lakebase"]``.
    """
    global _lakebase_conn

    import psycopg
    from pgvector.psycopg import register_vector

    # Return cached connection if still open
    if _lakebase_conn is not None:
        try:
            _lakebase_conn.execute("SELECT 1")
            return _lakebase_conn
        except Exception:
            # Connection is broken — recreate
            try:
                _lakebase_conn.close()
            except Exception:
                pass
            _lakebase_conn = None

    lb = config["lakebase"]

    # Import dbutils at call time (only available on Databricks clusters)
    from pyspark.dbutils import DBUtils
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
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
        # Autocommit keeps the SELECT 1 health check above from leaving a
        # dangling idle transaction on every cache call.
        autocommit=True,
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

def _session_key(session_id: Optional[str]) -> str:
    """Map a nullable session id to its storage key.

    ``session_id`` is stored NOT NULL with '' as the sentinel for global
    (non-session-scoped) entries, so Postgres unique constraints treat two
    global writes of the same question as a conflict.
    """
    return session_id or ""


def lakebase_cache_lookup(
    config: dict,
    question: str,
    threshold: float,
    session_id: Optional[str] = None,
):
    """Check Lakebase for a cached response.

    1. Exact match on normalized question text.
    2. If no exact match, cosine similarity search via pgvector.

    When *session_id* is provided, queries are scoped to that session;
    otherwise they run against the global ('') entries.

    Returns ``(hit_type, cached_sql, cached_response, score, embedding,
    hit_count)`` where hit_type is ``"exact"``, ``"vector"``, or ``None`` on
    miss, and hit_count is the entry's hit count AFTER this hit (0 on miss).
    The embedding is returned so callers can reuse it for cache writes.
    """
    from pgvector import Vector

    normalized = normalize_question(question)
    embedding_model = config.get("embedding_model", DEFAULT_EMBEDDING_MODEL)
    embedding_instruction = config.get("embedding_instruction")
    session_key = _session_key(session_id)

    conn = get_lakebase_connection(config)
    with conn.cursor() as cur:
        # --- Exact match ---
        cur.execute(
            "SELECT cached_sql, cached_response FROM genie_cache "
            "WHERE question_normalized = %s AND session_id = %s LIMIT 1",
            (normalized, session_key),
        )
        row = cur.fetchone()
        if row:
            cur.execute(
                "UPDATE genie_cache SET hit_count = hit_count + 1 "
                "WHERE question_normalized = %s AND session_id = %s RETURNING hit_count",
                (normalized, session_key),
            )
            hit_count = cur.fetchone()[0]
            # psycopg decodes JSONB into Python objects when reading the row.
            resp = row[1]
            return "exact", row[0], resp, 1.0, None, hit_count

        # --- Vector similarity ---
        embedding = generate_embedding(
            question, model=embedding_model, instruction=embedding_instruction,
        )
        # pgvector's psycopg adapter only registers dumpers for Vector and
        # numpy.ndarray — a plain list would be adapted as a PG array.
        vec = Vector(embedding)

        cur.execute(
            """
            SELECT question_normalized, cached_sql, cached_response,
                   1 - (embedding <=> %s) AS similarity
            FROM genie_cache
            WHERE session_id = %s AND embedding IS NOT NULL
            ORDER BY embedding <=> %s
            LIMIT 1
            """,
            (vec, session_key, vec),
        )
        row = cur.fetchone()

        if row and row[3] is not None and row[3] >= threshold:
            cur.execute(
                "UPDATE genie_cache SET hit_count = hit_count + 1 "
                "WHERE question_normalized = %s AND session_id = %s RETURNING hit_count",
                (row[0], session_key),
            )
            hit_count = cur.fetchone()[0]
            resp = row[2]
            return "vector", row[1], resp, float(row[3]), embedding, hit_count

    return None, None, None, 0.0, embedding, 0


def lakebase_cache_write(
    config: dict,
    question: str,
    sql: str,
    response_text: str,
    session_id: Optional[str] = None,
    embedding: Optional[list[float]] = None,
):
    """Write a Genie response to the Lakebase cache.

    Uses ``ON CONFLICT (question_normalized, session_id)`` to upsert, so a
    session's entry is never overwritten by another session asking the same
    question.

    If *embedding* is provided it is reused; otherwise a new one is generated.
    """
    from pgvector import Vector

    normalized = normalize_question(question)
    embedding_model = config.get("embedding_model", DEFAULT_EMBEDDING_MODEL)
    embedding_instruction = config.get("embedding_instruction")
    if embedding is None:
        embedding = generate_embedding(
            question, model=embedding_model, instruction=embedding_instruction,
        )
    response_json = json.dumps({"sql": sql, "text": response_text})

    conn = get_lakebase_connection(config)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO genie_cache
                (question_normalized, embedding, cached_sql, cached_response, session_id)
            VALUES (%s, %s, %s, %s::jsonb, %s)
            ON CONFLICT (question_normalized, session_id) DO UPDATE SET
                cached_sql = EXCLUDED.cached_sql,
                cached_response = EXCLUDED.cached_response,
                embedding = EXCLUDED.embedding
            """,
            (normalized, Vector(embedding), sql, response_json, _session_key(session_id)),
        )


def evict_expired_l1(config: dict, ttl_minutes: int):
    """Delete session-scoped L1 entries older than the TTL.

    Global entries (session_id = '') are the durable cache and are never
    evicted by this; only per-session rows have a lifetime.
    """
    conn = get_lakebase_connection(config)
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM genie_cache WHERE session_id <> '' "
            "AND created_at < now() - make_interval(mins => %s)",
            (ttl_minutes,),
        )
        deleted = cur.rowcount
    if deleted:
        print(f"  Evicted {deleted} expired L1 entries (TTL {ttl_minutes} min)")
    return deleted


# ---------------------------------------------------------------------------
# Vector Search helpers
# ---------------------------------------------------------------------------

def sync_vs_index_and_wait(vsc, endpoint_name: str, index_name: str, timeout_minutes: int = 10):
    """Trigger a VS index sync and wait for completion."""
    index = vsc.get_index(endpoint_name=endpoint_name, index_name=index_name)
    index.sync()
    print("  VS index sync triggered...")

    # An ONLINE index may still be serving the previous snapshot during sync.
    # Let the SDK wait for pending updates too, and propagate timeout/failure
    # instead of letting the notebooks continue with an incomplete index.
    index.wait_until_ready(
        timeout=timedelta(minutes=timeout_minutes), wait_for_updates=True,
    )
    print("  VS index sync complete")


def execute_cached_sql(spark, sql: str):
    """Return materialized pandas results, or None if cached SQL fails.

    These demos return small aggregates, so collecting them to pandas is
    appropriate. This executes the query inside the error handler and lets
    notebook display() reuse its results without re-running the source SQL.
    For larger result sets, use a bounded result-serving path instead.
    """
    try:
        return spark.sql(sql).toPandas()
    except Exception as e:
        print(f"  Cached SQL failed to execute ({type(e).__name__}) — falling back to Genie")
        return None


# ---------------------------------------------------------------------------
# Convenience helpers
# ---------------------------------------------------------------------------

def generate_id() -> str:
    """Return a new UUID string for cache row IDs."""
    return str(uuid.uuid4())


def utcnow() -> datetime:
    """Return the current UTC time (timezone-aware)."""
    return datetime.now(timezone.utc)


def print_summary_table(results: list[dict], columns: list[str]):
    """Print a formatted summary table from a list of result dicts.

    *columns* is a list of keys to display.  The first column is left-aligned;
    the rest are right-aligned with 12-char width.
    """
    header = f"{columns[0]:<50}" + "".join(f"{c:>14}" for c in columns[1:])
    print(header)
    print("-" * len(header))
    for row in results:
        line = f"{str(row[columns[0]])[:50]:<50}"
        for c in columns[1:]:
            val = row[c]
            if isinstance(val, float):
                line += f"{val:>14.3f}"
            else:
                line += f"{str(val):>14}"
        print(line)

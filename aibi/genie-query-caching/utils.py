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

DEFAULT_EMBEDDING_MODEL = "databricks-gte-large-en"


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

    Lowercases, strips punctuation, and collapses whitespace so that trivially
    different phrasings (trailing '?', extra spaces) map to the same key.
    """
    q = question.lower().strip()
    q = re.sub(r"[^\w\s]", "", q)
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


def generate_embedding(text: str, model: str = DEFAULT_EMBEDDING_MODEL) -> list[float]:
    """Generate an embedding vector via the Databricks Foundation Model API.

    Uses ``WorkspaceClient().serving_endpoints.query()`` which returns an
    OpenAI-compatible response with ``data[0].embedding``.
    """
    w = _get_workspace_client()
    response = w.serving_endpoints.query(
        name=model,
        input=[text],
    )
    return response.data[0].embedding


# ---------------------------------------------------------------------------
# Retry with exponential backoff + decorrelated jitter
# ---------------------------------------------------------------------------

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
    fail fast without retry.
    """
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
    session_id: Optional[str] = None,
):
    """Check Lakebase for a cached response.

    1. Exact match on normalized question text.
    2. If no exact match, cosine similarity search via pgvector.

    When *session_id* is provided, queries are scoped to that session.

    Returns ``(hit_type, cached_sql, cached_response, score, embedding)``
    where hit_type is ``"exact"``, ``"vector"``, or ``None`` on miss.
    The embedding is returned so callers can reuse it for cache writes.
    """
    normalized = normalize_question(question)
    embedding_model = config.get("embedding_model", DEFAULT_EMBEDDING_MODEL)

    conn = get_lakebase_connection(config)
    with conn.cursor() as cur:
        # --- Exact match ---
        session_filter = "AND session_id = %s" if session_id else ""
        params: list = [normalized]
        if session_id:
            params.append(session_id)

        cur.execute(
            f"SELECT cached_sql, cached_response FROM genie_cache "
            f"WHERE question_normalized = %s {session_filter} LIMIT 1",
            params,
        )
        row = cur.fetchone()
        if row:
            cur.execute(
                f"UPDATE genie_cache SET hit_count = hit_count + 1 "
                f"WHERE question_normalized = %s {session_filter}",
                params,
            )
            conn.commit()
            resp = json.loads(row[1]) if row[1] else None
            return "exact", row[0], resp, 1.0, None

        # --- Vector similarity ---
        embedding = generate_embedding(question, model=embedding_model)
        vs_params: list = [embedding, session_id, embedding] if session_id else [embedding, embedding]
        session_where = "WHERE session_id = %s AND embedding IS NOT NULL" if session_id else "WHERE embedding IS NOT NULL"

        cur.execute(
            f"""
            SELECT question_normalized, cached_sql, cached_response,
                   1 - (embedding <=> %s::vector) AS similarity
            FROM genie_cache
            {session_where}
            ORDER BY embedding <=> %s::vector
            LIMIT 1
            """,
            vs_params,
        )
        row = cur.fetchone()

        if row and row[3] is not None and row[3] >= threshold:
            cur.execute(
                f"UPDATE genie_cache SET hit_count = hit_count + 1 "
                f"WHERE question_normalized = %s {session_filter}",
                [row[0]] + ([session_id] if session_id else []),
            )
            conn.commit()
            resp = json.loads(row[2]) if row[2] else None
            return "vector", row[1], resp, float(row[3]), embedding

    return None, None, None, 0.0, embedding


def lakebase_cache_write(
    config: dict,
    question: str,
    sql: str,
    response_text: str,
    session_id: Optional[str] = None,
    embedding: Optional[list[float]] = None,
):
    """Write a Genie response to the Lakebase cache.

    Uses ``ON CONFLICT`` to upsert.  When *session_id* is provided, the
    ``session_id`` column is also set.

    If *embedding* is provided it is reused; otherwise a new one is generated.
    """
    normalized = normalize_question(question)
    embedding_model = config.get("embedding_model", DEFAULT_EMBEDDING_MODEL)
    if embedding is None:
        embedding = generate_embedding(question, model=embedding_model)
    response_json = json.dumps({"sql": sql, "text": response_text})

    conn = get_lakebase_connection(config)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO genie_cache
                (question_normalized, embedding, cached_sql, cached_response, session_id)
            VALUES (%s, %s::vector, %s, %s::jsonb, %s)
            ON CONFLICT (question_normalized) DO UPDATE SET
                cached_sql = EXCLUDED.cached_sql,
                cached_response = EXCLUDED.cached_response,
                embedding = EXCLUDED.embedding,
                session_id = EXCLUDED.session_id
            """,
            (normalized, embedding, sql, response_json, session_id),
        )
    conn.commit()


# ---------------------------------------------------------------------------
# Vector Search helpers
# ---------------------------------------------------------------------------

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
            print("  WARNING: Sync timed out — new entries may not be searchable yet")
            return
        time.sleep(5)


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

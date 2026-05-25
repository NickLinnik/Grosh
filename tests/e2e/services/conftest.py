"""E2E test session + per-test lifecycle.

The e2e tests run against a separate `grosh-test` compose stack (postgres on
host 5433, redpanda on 19093, api on 8010, ingestion on 8011). That stack must
be running before tests start — bring it up with ``make test-stack-up``. Tests
never touch the dev stack.

Session lifecycle:
  1. Probe http://localhost:8010/health and http://localhost:8011/health.
     Fail fast with a clear message if the test stack isn't running.
  2. That's it. Migrations are applied by ``make test-stack-up``; we don't
     re-run them per session.

Per-test lifecycle:
  1. TRUNCATE all user-scoped tables (preserves seed rows from migrations
     0002 + 0005).
  2. Delete-and-recreate the 3 Redpanda topics so each test starts with a
     clean Kafka log and no stale consumer offsets.

This conftest does NOT manage containers. The compose stack is the user's
responsibility (``make test-stack-up`` / ``make test-stack-down``).
"""

from __future__ import annotations

import time
from collections.abc import AsyncGenerator
from pathlib import Path

import asyncpg
import httpx
import pytest_asyncio
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import dotenv_values

REPO_ROOT = Path(__file__).resolve().parents[3]
ENV_TEST_PATH = REPO_ROOT / "infra" / ".env.test"

# Load the test stack's env vars into a dict. We deliberately do NOT
# `load_dotenv` (which would pollute os.environ and could clash with whatever
# is set when running tests from a shell that sourced infra/.env). The values
# we need from .env.test are only the admin DSN and the test stack endpoints,
# all read explicitly below.
_TEST_ENV = dotenv_values(ENV_TEST_PATH)

# Host-side endpoints — these are the ports exposed by the test stack.
API_BASE = "http://localhost:8010"
INGESTION_BASE = "http://localhost:8011"
KAFKA_BOOTSTRAP = "localhost:19093"

# Admin DSN — points at the test postgres (host port 5433) using the
# password from .env.test. Built explicitly because the DSN in .env.test
# uses the in-container hostname ``postgres``, which isn't reachable from
# the host.
_ADMIN_PASSWORD = _TEST_ENV["POSTGRES_PASSWORD"]
_ADMIN_USER = _TEST_ENV["POSTGRES_USER"]
_DB_NAME = _TEST_ENV["POSTGRES_DB"]
ADMIN_DSN = f"postgresql://{_ADMIN_USER}:{_ADMIN_PASSWORD}@localhost:5433/{_DB_NAME}"

TEST_TOPICS = (
    "raw_transactions.monobank",
    "raw_transactions.manual",
    "normalized_transactions",
)

# Tables that are user-scoped and accumulate test data. Truncated between
# tests. Order doesn't matter — TRUNCATE ... CASCADE handles FK chains.
# Seeded tables (``networks``, ``rate_source_config``, the seeded admin in
# ``users``) are handled separately to preserve migration-0002/0005 state.
_USER_SCOPED_TABLES = (
    "transactions",
    "transfer_match_anomalies",
    "staging_normalized_transactions",
    "reprocessing_backups",
    "reprocessing_locks",
    "accounts",
    "bank_integrations",
    "categories",
    "user_settings",
    "refresh_tokens",
    "revoked_tokens",
    "ml_labels",
    "merchant_rules",
    "currency_rates",
)


def _probe_endpoint(url: str, *, timeout: float = 2.0) -> bool:
    """Return True if url returns 200 within timeout, False otherwise."""
    try:
        r = httpx.get(url, timeout=timeout)
        return r.status_code == 200
    except Exception:
        return False


def _assert_test_stack_running() -> None:
    """Fail fast with a clear message if the test stack isn't reachable."""
    api_ok = _probe_endpoint(f"{API_BASE}/health")
    ingestion_ok = _probe_endpoint(f"{INGESTION_BASE}/health")
    if api_ok and ingestion_ok:
        return
    missing = []
    if not api_ok:
        missing.append(f"api ({API_BASE}/health)")
    if not ingestion_ok:
        missing.append(f"ingestion ({INGESTION_BASE}/health)")
    raise RuntimeError(
        "E2E test stack is not running — could not reach "
        + ", ".join(missing)
        + ".\nRun `make test-stack-up` before running these tests."
    )


def _kafka_admin() -> AdminClient:
    return AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})


def _recreate_topics() -> None:
    """Delete and recreate the test topics.

    Idempotent: if a topic doesn't exist the delete is a no-op; if it does, we
    wait briefly and recreate it. Treating "already exists" as success makes
    this safe to call repeatedly even when consumers auto-create topics
    between calls.
    """
    admin = _kafka_admin()
    futs = admin.delete_topics(list(TEST_TOPICS), operation_timeout=10)
    for _, fut in futs.items():
        try:
            fut.result(timeout=10)
        except Exception:
            # Topic may not exist on the first run — that's fine.
            pass
    # Deletion is async in Kafka — wait briefly for metadata to settle before
    # recreate, otherwise the broker may still see the old topic.
    time.sleep(0.5)
    new_topics = [
        NewTopic(t, num_partitions=1, replication_factor=1) for t in TEST_TOPICS
    ]
    create_futs = admin.create_topics(new_topics, operation_timeout=10)
    for _, fut in create_futs.items():
        try:
            fut.result(timeout=10)
        except Exception as exc:
            msg = str(exc).lower()
            # All flavors of "topic exists" are fine — a consumer beat us to it.
            if "already" not in msg and "exists" not in msg:
                raise


async def _truncate_user_data(conn: asyncpg.Connection) -> None:
    """TRUNCATE all user-scoped tables; preserve seeded admin user.

    Migrations 0002 + 0005 seed: 1 admin user, 1 family network,
    2 rate_source_config rows. ``networks`` and ``rate_source_config`` aren't
    in _USER_SCOPED_TABLES so they aren't truncated. The seeded admin user is
    re-protected by the ``WHERE role != 'admin'`` clause below.
    """
    existing = await conn.fetch(
        """
        SELECT tablename FROM pg_tables
        WHERE schemaname = 'public' AND tablename = ANY($1::text[])
        """,
        list(_USER_SCOPED_TABLES),
    )
    existing_names = [r["tablename"] for r in existing]
    if existing_names:
        await conn.execute(
            f"TRUNCATE {', '.join(existing_names)} RESTART IDENTITY CASCADE"
        )
    await conn.execute("DELETE FROM users WHERE role != 'admin'")
    # Reset reprocess rate-limit timestamp on the seeded admin so reprocess
    # tests don't fail spuriously after a previous reprocess test claimed it.
    await conn.execute("UPDATE users SET last_reprocess_started_at = NULL")


# ---------------------------------------------------------------------------
# Session fixture: probe the stack
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture(loop_scope="session", scope="session", autouse=True)
async def _e2e_session_lifecycle() -> AsyncGenerator[None, None]:
    """Verify the test stack is reachable + recreate Kafka topics once per session.

    Migrations are owned by `make test-stack-up`. Topic recreation lives here
    (not per-test) for two reasons:
    1. Topics are append-only event logs; per-test isolation is handled by DB
       truncation + the consumer pipeline's UUID5 + ON CONFLICT DO NOTHING
       deduplication on persistence. Per-test topic resets add no isolation.
    2. Deleting subscribed topics mid-session triggers UNKNOWN_TOPIC_OR_PART
       in consumers. They tolerate it (see kafka.py budgets), but the
       behavior is observable in logs and slows tests. Once-per-session
       recreate avoids the noise entirely while still giving us a clean
       slate for each `pytest` invocation.
    """
    _assert_test_stack_running()
    _recreate_topics()
    yield
    # Nothing to tear down — the stack stays up between sessions.


# ---------------------------------------------------------------------------
# Per-test fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture(loop_scope="session", scope="function")
async def pg() -> AsyncGenerator[asyncpg.Connection, None]:
    """asyncpg connection as grosh_admin against the test postgres.

    The grosh_admin role is the table owner, so RLS policies don't apply.
    Use this for factory inserts in tests' arrange phase and for direct DB
    assertions in their assert phase.
    """
    conn = await asyncpg.connect(ADMIN_DSN)
    try:
        yield conn
    finally:
        await conn.close()


@pytest_asyncio.fixture(loop_scope="session", scope="function", autouse=True)
async def _isolate_test(pg: asyncpg.Connection) -> AsyncGenerator[None, None]:
    """Per-test isolation: truncate user data only.

    Kafka topic recreation is session-scoped (see _e2e_session_lifecycle).
    Per-test topic resets would churn consumer offsets and broker metadata
    without providing meaningful isolation — the pipeline already
    deduplicates by deterministic UUID5 at persistence.
    """
    await _truncate_user_data(pg)
    yield
    # No after-test cleanup — the next test's setup truncates.


@pytest_asyncio.fixture(loop_scope="session", scope="function")
async def client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """httpx client. Tests build their own URLs using API_BASE / INGESTION_BASE."""
    async with httpx.AsyncClient(follow_redirects=False, timeout=10.0) as c:
        yield c

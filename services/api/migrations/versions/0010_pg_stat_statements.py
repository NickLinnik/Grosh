"""Enable pg_stat_statements for query-level observability

Revision ID: 0010
Revises: 0009
Create Date: 2026-05-19

Loads the ``pg_stat_statements`` extension so the database tracks per-query
execution stats (calls, total/mean/min/max time, rows, shared-buffer hits/
misses). This is the canonical Postgres source-of-truth for "which queries
spent the most time" — used both for ad-hoc profiling and for ongoing
observability once we wire it into Grafana.

Overhead is 1-3% of query time in published benchmarks, dominated by
query-text normalization on each statement; comfortably below noise at our
3-user scale and worth keeping enabled in production.

The C library must also be loaded at Postgres startup via
``shared_preload_libraries`` — configured in ``infra/docker-compose.yml`` and
in the production k3s Postgres manifest. Without that, ``CREATE EXTENSION``
fails at execute time; with it, the extension's view (``pg_stat_statements``)
becomes available to any connection after this migration runs.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0010"
down_revision: str | Sequence[str] | None = "0009"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")


def downgrade() -> None:
    op.execute("DROP EXTENSION IF EXISTS pg_stat_statements")

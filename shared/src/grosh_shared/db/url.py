"""Helpers for converting PostgreSQL connection strings between asyncpg
and SQLAlchemy dialect formats.

asyncpg accepts only ``postgresql://`` URLs. SQLAlchemy needs the
``postgresql+asyncpg://`` dialect prefix to pick the async driver. The
same ``DATABASE_URL`` env var is read by both FastAPI (asyncpg) and
Alembic (SQLAlchemy), so one side always has to convert.
"""


def for_asyncpg(url: str) -> str:
    return url.replace("postgresql+asyncpg://", "postgresql://", 1)


def for_sqlalchemy_async(url: str) -> str:
    if url.startswith("postgresql+asyncpg://"):
        return url
    return url.replace("postgresql://", "postgresql+asyncpg://", 1)

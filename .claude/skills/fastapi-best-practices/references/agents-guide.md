# FastAPI Best Practices — Agent Reference

Adapted from [zhanymkanov/fastapi-best-practices](https://github.com/zhanymkanov/fastapi-best-practices)
for the Grosh project conventions.

## Compatibility Matrix

| Dependency        | Minimum | Notes                                                  |
|-------------------|---------|--------------------------------------------------------|
| Python            | 3.12    | Project standard; `StrEnum`, `X \| Y` union syntax     |
| FastAPI           | 0.111   | `Annotated[T, Depends(...)]` is the idiomatic form     |
| Pydantic          | 2.7     | v1 APIs (`json_encoders`, `.dict()`) are removed        |
| pydantic-settings | 2.4     | Separate package since Pydantic v2                      |
| SQLAlchemy        | 2.0     | Async API (`AsyncSession`, `async_sessionmaker`)        |
| Alembic           | 1.13    | Async-aware migrations                                  |
| httpx             | 0.27    | Use `ASGITransport` for in-process tests                |
| PyJWT             | 2.8     | Use this, **not** the unmaintained `python-jose`        |
| ruff              | 0.4     | Replaces black, isort, autoflake                        |

## Project Structure

Organize by **domain**, not by file type. One package per bounded context.

```
src/grosh_api/
├── {domain}/           # e.g., auth/, transactions/, accounts/
│   ├── router.py       # API endpoints
│   ├── schemas.py      # Pydantic models
│   ├── models.py       # SQLAlchemy ORM models
│   ├── service.py      # Business logic
│   ├── dependencies.py # Route dependencies
│   ├── config.py       # Domain-scoped BaseSettings
│   ├── constants.py    # Constants and error codes
│   ├── exceptions.py   # Domain-specific exceptions
│   └── utils.py        # Helper functions
├── config.py           # Global BaseSettings
├── models.py           # Shared Pydantic / ORM bases
├── exceptions.py       # Global exceptions
├── database.py         # Async engine + session factory
└── main.py             # FastAPI app + lifespan
```

**Cross-domain imports**: always use the explicit module name. Never `from grosh_api.auth import *`.

```python
from grosh_api.auth import constants as auth_constants
from grosh_api.notifications import service as notification_service
from grosh_api.transactions.constants import ErrorCode as TxnErrorCode
```

## Async Routes

### Decision rule

| Route does this                        | Use                                               |
|----------------------------------------|---------------------------------------------------|
| `await`-able non-blocking I/O          | `async def`                                       |
| Blocking I/O (no async client exists)  | `def` (sync, runs in threadpool)                  |
| Mix of both                            | `async def` + `run_in_threadpool` for blocking part|
| CPU-bound work (>50 ms compute)        | Offload to a worker process (Celery / RQ / Arq)   |

### Do / Don't

```python
# DON'T — blocking call inside async route freezes the entire event loop
@router.get("/bad")
async def bad():
    time.sleep(10)            # blocks every request on this worker
    return {"ok": True}

# DO — sync route lets FastAPI run it in a threadpool
@router.get("/sync-ok")
def sync_ok():
    time.sleep(10)            # blocks one threadpool worker, not the loop
    return {"ok": True}

# DO — async route with awaitable sleep
@router.get("/async-ok")
async def async_ok():
    await asyncio.sleep(10)   # yields control, loop keeps serving requests
    return {"ok": True}

# DO — async route that has to call a sync library
from fastapi.concurrency import run_in_threadpool

@router.get("/wrap")
async def wrap():
    result = await run_in_threadpool(legacy_sync_client.fetch, "id")
    return result
```

### Threadpool caveats

- Default Starlette threadpool size is 40. Saturating it slows every sync route.
- Threads cost more than coroutines. Don't use sync routes "just because."
- CPU-intensive tasks: neither async nor threadpool help (GIL). Offload to a process pool or task queue.

## Pydantic

### Use built-in validators

```python
from enum import StrEnum
from pydantic import AnyUrl, BaseModel, EmailStr, Field


class MusicBand(StrEnum):
    AEROSMITH = "AEROSMITH"
    QUEEN = "QUEEN"
    ACDC = "AC/DC"


class UserCreate(BaseModel):
    first_name: str = Field(min_length=1, max_length=128)
    username: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_-]+$")
    email: EmailStr
    age: int = Field(ge=18)                     # required, must be >= 18
    favorite_band: MusicBand | None = None
    website: AnyUrl | None = None
```

> **Don't** write `Field(ge=18, default=None)`. The constraint and the default contradict
> each other. Decide: required (`Field(ge=18)`) or optional (`int | None = Field(default=None, ge=18)`).

### Custom base model — modern serialization

`json_encoders` is **deprecated** in Pydantic v2. Use `@field_serializer` for per-field rules,
or annotate a custom type with `PlainSerializer`.

```python
from datetime import datetime
from zoneinfo import ZoneInfo
from pydantic import BaseModel, ConfigDict, field_serializer


class CustomModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    @field_serializer("*", when_used="json", check_fields=False)
    def _serialize_datetimes(self, value):
        if isinstance(value, datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=ZoneInfo("UTC"))
            return value.strftime("%Y-%m-%dT%H:%M:%S%z")
        return value
```

### Split BaseSettings by domain

```python
# grosh_api/auth/config.py
from datetime import timedelta
from pydantic_settings import BaseSettings, SettingsConfigDict


class AuthConfig(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="AUTH_", env_file=".env", extra="ignore")

    JWT_ALG: str
    JWT_SECRET: str
    JWT_EXP_MINUTES: int = 5
    REFRESH_TOKEN_KEY: str
    REFRESH_TOKEN_EXP: timedelta = timedelta(days=30)
    SECURE_COOKIES: bool = True


auth_settings = AuthConfig()
```

## Dependencies

### Use Annotated, not default-arg `Depends(...)`

`Annotated[T, Depends(...)]` is the idiomatic form since FastAPI 0.95 and avoids
gotchas with default values.

```python
# DO — modern Annotated form
from typing import Annotated
from fastapi import Depends

PostDep = Annotated[dict, Depends(valid_post_id)]

@router.get("/posts/{post_id}")
async def get_post(post: PostDep):
    return post

# AVOID — default-argument form (still works, but legacy)
@router.get("/posts/{post_id}")
async def get_post(post: dict = Depends(valid_post_id)):
    return post
```

### Validate inside dependencies (not just inject)

```python
async def valid_post_id(post_id: UUID4) -> dict:
    post = await service.get_by_id(post_id)
    if not post:
        raise PostNotFound()
    return post
```

### Chain dependencies for reuse

```python
async def valid_owned_post(
    post: Annotated[dict, Depends(valid_post_id)],
    token_data: Annotated[dict, Depends(parse_jwt_data)],
) -> dict:
    if post["creator_id"] != token_data["user_id"]:
        raise UserNotOwner()
    return post
```

### Rules

- Dependencies are **cached per request**. Same `Depends(x)` called 5 times in one request → `x` runs once.
- Prefer `async def` dependencies. Sync deps run in the threadpool — wasted overhead for small CPU-only checks.
- Use **the same path-variable name** across endpoints when sharing a dependency (e.g., `profile_id` in both `/profiles/{profile_id}` and `/creators/{profile_id}`).

## Authentication — JWT

Use **`PyJWT`**, not `python-jose` (unmaintained).

```python
import jwt  # PyJWT
from jwt.exceptions import InvalidTokenError

def decode_token(token: str) -> dict:
    try:
        return jwt.decode(token, settings.JWT_SECRET, algorithms=[settings.JWT_ALG])
    except InvalidTokenError as exc:
        raise InvalidCredentials() from exc
```

## Database — SQLAlchemy 2.0 async

Prefer SQLAlchemy 2.0's async API. `encode/databases` is in maintenance mode — don't pick it for new projects.

```python
# grosh_api/database.py
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

engine = create_async_engine(str(settings.DATABASE_URL), pool_pre_ping=True)
SessionFactory = async_sessionmaker(engine, expire_on_commit=False)


async def get_db() -> AsyncSession:
    async with SessionFactory() as session:
        yield session
```

### Naming conventions

- `lower_case_snake`
- Singular tables: `post`, `user`, `post_like`
- Group with prefix: `payment_account`, `payment_bill`
- `_at` suffix for `datetime`, `_date` suffix for `date`
- Use the same FK column name everywhere it appears

### Index naming convention

```python
from sqlalchemy import MetaData

POSTGRES_INDEXES_NAMING_CONVENTION = {
    "ix": "%(column_0_label)s_idx",
    "uq": "%(table_name)s_%(column_0_name)s_key",
    "ck": "%(table_name)s_%(constraint_name)s_check",
    "fk": "%(table_name)s_%(column_0_name)s_fkey",
    "pk": "%(table_name)s_pkey",
}
metadata = MetaData(naming_convention=POSTGRES_INDEXES_NAMING_CONVENTION)
```

### SQL-first, Pydantic-second

- Do joins, aggregation, and JSON shaping in SQL — Postgres is faster than CPython at this.
- Hydrate the result into Pydantic only for response validation, not for transformation.

## Background Work — BackgroundTasks vs Task Queue

| Use BackgroundTasks when…                 | Use Celery / Arq / RQ when…                         |
|-------------------------------------------|------------------------------------------------------|
| Task is < 1 second                        | Task takes seconds to minutes                        |
| Failure can be silently dropped           | You need retries, dead-letter, or visibility         |
| Task is in-process (send email, log row)  | Task is CPU-heavy or needs a separate pool           |
| You don't need scheduling                 | You need cron, ETA, or rate limiting                 |

```python
from fastapi import BackgroundTasks

@router.post("/signup")
async def signup(data: SignupIn, bg: BackgroundTasks):
    user = await service.create_user(data)
    bg.add_task(send_welcome_email, user.email)   # fire-and-forget, in-process
    return user
```

> BackgroundTasks run **after the response is sent, in the same worker process**. If the
> worker dies, the task is lost. There is no retry. Don't use them for anything you'd
> page on.

## Response Serialization Gotcha

FastAPI first converts the returned object to a dict via `jsonable_encoder`, then validates
against `response_model`, then serializes to JSON. This means your Pydantic model is
**constructed twice** — once by you, once by FastAPI.

Be aware of this when using expensive validators or side effects in response models.

## ValueError Becomes ValidationError

If you raise a `ValueError` inside a Pydantic validator used in a request body, FastAPI returns
it as a **detailed validation error response** to the client. Keep validation messages
user-friendly and avoid leaking implementation details.

## Testing

### Async client from day one

```python
import pytest
from httpx import AsyncClient, ASGITransport

from grosh_api.main import app


@pytest.fixture
async def client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest.mark.asyncio
async def test_create_post(client: AsyncClient):
    resp = await client.post("/posts", json={"title": "hi"})
    assert resp.status_code == 201
```

> **Don't** use `async_asgi_testclient` — it's unmaintained. httpx + `ASGITransport` is
> the supported path.

### Override dependencies in tests

Don't monkeypatch internals. Use FastAPI's built-in `dependency_overrides`.

```python
from grosh_api.deps import get_current_user
from grosh_api.main import app


def fake_user():
    return User(id=uuid4(), email="test@test.com", role=UserRole.member, is_active=True)


@pytest.fixture(autouse=True)
def _override_auth():
    app.dependency_overrides[get_current_user] = fake_user
    yield
    app.dependency_overrides.clear()
```

## Migrations (Alembic)

- Migrations must be static and reversible.
- Descriptive filenames:
  ```ini
  # alembic.ini
  file_template = %%(year)d-%%(month).2d-%%(day).2d_%%(slug)s
  ```
  → `2026-04-14_add_post_content_idx.py`

## API Documentation

### Hide docs outside selected envs

```python
from fastapi import FastAPI

SHOW_DOCS_IN = {"local", "staging"}
app_kwargs = {"title": "Grosh API"}
if settings.ENVIRONMENT not in SHOW_DOCS_IN:
    app_kwargs["openapi_url"] = None    # disables /docs and /redoc

app = FastAPI(**app_kwargs)
```

### Document endpoints fully

```python
@router.post(
    "/items",
    response_model=ItemResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create an item",
    description="Creates an item owned by the authenticated user.",
    tags=["items"],
    responses={
        status.HTTP_400_BAD_REQUEST: {"model": ErrorResponse, "description": "Validation error"},
        status.HTTP_409_CONFLICT:    {"model": ErrorResponse, "description": "Slug already exists"},
    },
)
async def create_item(payload: ItemCreate) -> ItemResponse: ...
```

## Linting

```shell
ruff check --fix src
ruff format src
```

Always read linting rules from `pyproject.toml` / `ruff.toml` before suggesting fixes —
projects define their own rule sets and overrides.

## Anti-Patterns — Common AI-Agent Mistakes

Check diffs for these. Each is a real failure mode agents introduce.

| Anti-pattern                                                | Why it's wrong                                         | Fix                                                         |
|-------------------------------------------------------------|--------------------------------------------------------|-------------------------------------------------------------|
| `requests.get(...)` inside `async def`                      | Blocks the event loop. `requests` is sync.             | Use `httpx.AsyncClient` or `await run_in_threadpool(...)`.  |
| `time.sleep` / `open()` / sync DB driver inside `async def` | Same — blocks the loop.                               | Use async equivalent (`asyncio.sleep`, `aiofiles`, async driver). |
| `from jose import jwt`                                      | `python-jose` is unmaintained.                         | `import jwt` (PyJWT).                                       |
| `from async_asgi_testclient import TestClient`              | Unmaintained.                                          | `httpx.AsyncClient` + `ASGITransport`.                      |
| `model_config = ConfigDict(json_encoders={...})`            | Deprecated in Pydantic v2.                             | `@field_serializer` or `Annotated[T, PlainSerializer(...)]`.|
| `Field(ge=18, default=None)`                                | Constraint contradicts the default.                    | Pick required or optional, not both.                        |
| `def get_user(id: int = Depends(...))`                      | Legacy default-arg form; gotchas with default values.  | `user: Annotated[User, Depends(...)]`.                      |
| Catching `Exception` around a route body                    | Hides bugs and turns 500s into silent 200s.            | Catch specific exception; raise `HTTPException`.            |
| `BackgroundTasks` for anything you'd page on                | No retry, dies with the worker.                        | Use Celery / Arq / RQ.                                     |
| Calling a sync ORM session inside `async def`               | Blocks the loop, may deadlock the pool.                | Use `AsyncSession`.                                         |
| Returning Pydantic model + same `response_model=`           | Model gets constructed twice (validate + serialize).   | Return dict/ORM row and let `response_model` validate.      |
| Importing via deep paths (`from grosh_api.auth.service.user import ...`) | Tight coupling, hard to refactor.         | `from grosh_api.auth import service as auth_service`.       |
| One `BaseSettings` for the whole app                        | Hard to reason about, every domain reads every var.    | One `BaseSettings` per domain.                              |
| Mocking the DB in integration tests                         | Mock/prod divergence fires in prod.                    | Real DB + `dependency_overrides` for auth/external services.|

## Quick Reference

| Scenario                             | Solution                                           |
|--------------------------------------|----------------------------------------------------|
| Non-blocking I/O                     | `async def` route with `await`                     |
| Blocking I/O (no async client)       | `def` route (sync, runs in threadpool)             |
| Sync library inside async route      | `await run_in_threadpool(fn, *args)`               |
| CPU-intensive work                   | Worker process (Celery / Arq / RQ)                 |
| Request validation against DB        | Dependency that loads + validates + returns        |
| Reuse validation across routes       | Chain dependencies                                 |
| Inject dependency in modern style    | `Annotated[T, Depends(...)]`                       |
| Per-request dep caching              | Default behavior — same `Depends(x)` runs once     |
| Per-domain config                    | One `BaseSettings` subclass per domain             |
| Custom datetime serialization        | `@field_serializer`                                |
| Fire-and-forget short task           | `BackgroundTasks`                                  |
| Reliable / scheduled / heavy task    | Celery / Arq / RQ                                  |
| JWT decode                           | `PyJWT` (`import jwt`)                             |
| Async DB                             | SQLAlchemy 2.0 async (`AsyncSession`)              |
| HTTP test client                     | `httpx.AsyncClient` + `ASGITransport`              |
| Swap dep in tests                    | `app.dependency_overrides[dep] = fake`             |
| Lint + format                        | `ruff check --fix` + `ruff format`                 |

---
name: fastapi-best-practices
description: >-
  FastAPI best practices and conventions. Use when writing, reviewing, or refactoring
  FastAPI applications — route handlers, Pydantic schemas, dependency injection,
  project structure, async patterns, testing, or API documentation. Triggers on tasks
  involving FastAPI routers, endpoints, request validation, response models, or
  application configuration. Does not cover general Python syntax or typing — see
  modern-python-development for that.
version: 0.2.0
---

# FastAPI Best Practices

Opinionated conventions for building production FastAPI applications, adapted from
[zhanymkanov/fastapi-best-practices](https://github.com/zhanymkanov/fastapi-best-practices).
General Python idioms (naming, type hints, error handling, dataclasses) are covered by
`modern-python-development` — this skill focuses on FastAPI-specific patterns.

All rules, code examples, decision tables, and the anti-patterns checklist live in a single
reference file:

| Reference                        | Contents                                                        |
|----------------------------------|-----------------------------------------------------------------|
| `references/agents-guide.md`    | Full guide: compatibility matrix, async routes, Pydantic, dependencies, DB, testing, anti-patterns |

## Quick Reference

### Async Routes

- `async def` — use ONLY with non-blocking `await` calls; blocks event loop otherwise
- `def` (sync) — use for blocking I/O; runs in threadpool automatically
- CPU-intensive — offload to process pool or task queue, not threads
- Sync SDK in async route — use `run_in_threadpool()` from Starlette

### Dependencies

- Use `Annotated[T, Depends(...)]` — the modern idiomatic form, not `= Depends(...)`
- Use for **request validation** (DB lookups, auth), not just DI
- Chain dependencies to compose validation without repetition
- Dependencies are **cached per request** — same dependency runs once per request
- Prefer `async` dependencies to avoid threadpool overhead

### Pydantic

- Use built-in validators (`Field`, `EmailStr`, `AnyUrl`) before writing custom ones
- Use `@field_serializer` for custom serialization — `json_encoders` is deprecated in v2
- Split `BaseSettings` by domain — one per module, not a single global config
- `ValueError` in validators becomes a 422 response — keep messages user-friendly

### Database

- Table names: `lower_case_snake`, singular (`post`, `user`, `post_like`)
- DateTime columns: `_at` suffix; date columns: `_date` suffix
- SQL-first — complex joins and JSON aggregation belong in the database
- Use SQLAlchemy 2.0 async API (`AsyncSession`, `async_sessionmaker`)

### Testing

- Async test client from day one: `httpx.AsyncClient` + `ASGITransport`
- Override dependencies with `app.dependency_overrides`, not monkeypatching
- Don't use `async_asgi_testclient` — it's unmaintained

### Anti-Patterns to Watch For

- Sync I/O inside `async def` (blocks event loop)
- `python-jose` instead of `PyJWT`
- `json_encoders` instead of `@field_serializer`
- `= Depends(...)` instead of `Annotated[T, Depends(...)]`
- Catching bare `Exception` around route bodies
- Mocking the DB in integration tests

## How to Use

Read `references/agents-guide.md` for the full guide with code examples, decision matrices,
and the complete anti-patterns checklist.

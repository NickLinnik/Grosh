# Alembic Migration Conventions

Conventions for writing raw SQL Alembic migrations in this project. All migrations use `op.get_bind()` and raw SQL — no SQLAlchemy ORM constructs.

## Use `bind = op.get_bind()` once per function

Call `op.get_bind()` once at the top of each migration function and reuse the `bind` local variable. Never call `op.get_bind()` multiple times, and never use `op.execute()` directly — it discards results and does not accept bind parameters.

**Incorrect:**
```python
def upgrade() -> None:
    op.execute(text("INSERT INTO networks (name) VALUES (:name)"), {"name": "Family"})  # op.execute() ignores params
    op.get_bind().execute(text("DELETE FROM users WHERE id = :id"), {"id": some_id})
    op.get_bind().execute(text("UPDATE users SET active = false"))  # repeated get_bind() calls
```

**Correct:**
```python
def upgrade() -> None:
    bind = op.get_bind()
    bind.execute(text("INSERT INTO networks (name) VALUES (:name)"), {"name": "Family"})
    bind.execute(text("DELETE FROM users WHERE id = :id"), {"id": some_id})
    bind.execute(text("UPDATE users SET active = false"))
```

## Use `sqlalchemy.text()` with named bind parameters

Always wrap SQL in `text()` and use named parameters (`:`-prefixed). Never interpolate variables into SQL strings.

**Incorrect:**
```python
bind.execute(text(f"INSERT INTO users (email) VALUES ('{email}')"))  # SQL injection risk
```

**Correct:**
```python
bind.execute(text("INSERT INTO users (email) VALUES (:email)"), {"email": email})
```

## Use `.scalar_one()` to retrieve a single returned value

When a statement has `RETURNING` and you need one value back, chain `.scalar_one()` on the result. It raises if zero or more than one row is returned — a useful built-in sanity check.

```python
network_id = bind.execute(
    text("INSERT INTO networks (name) VALUES (:name) RETURNING id"),
    {"name": "Family"},
).scalar_one()
```

## Write raw SQL — no ORM constructs

`target_metadata = None` in `env.py`. Use `bind.execute(text(...))` for everything — no `op.create_table()`, `op.add_column()`, or other ORM-style helpers unless there is a specific reason.

## Downgrade must reverse upgrade exactly

Every `upgrade()` must have a matching `downgrade()` that undoes it in reverse order. Use the same `bind = op.get_bind()` pattern.

```python
def downgrade() -> None:
    bind = op.get_bind()
    bind.execute(text("DROP TABLE IF EXISTS network_members"))
    bind.execute(text("DROP TABLE IF EXISTS networks"))
```

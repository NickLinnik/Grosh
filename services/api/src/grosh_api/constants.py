"""Shared protocol-level identifiers used across layers.

These names represent agreements between code that doesn't share imports —
backend and frontend, FastAPI and database, Python and SQL. Centralizing
them here makes the agreement explicit and renaming safe.
"""

# Cookie name agreed between FastAPI and the browser. The frontend never
# touches this cookie directly (it's httpOnly), so there is no frontend
# mirror — the browser sends it automatically with credentials: 'include'.
REFRESH_TOKEN_COOKIE = "refresh_token"

# PostgreSQL session variable used by RLS policies to scope queries to the
# current user. Set in deps.get_current_user via set_config(); read by RLS
# policies defined in migrations/0001_initial_schema.py. The migration
# embeds this string as raw SQL and cannot import the constant — if you
# rename it here, update the migration's policy USING clause too.
CURRENT_USER_ID_SESSION_VAR = "app.current_user_id"

# HTTP Bearer auth header — RFC 6750.
AUTH_HEADER = "Authorization"
BEARER_PREFIX = "Bearer "

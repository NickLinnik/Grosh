"""Protocol-level identifiers used across layers of the API service.

These names represent agreements between code that doesn't share imports —
backend and frontend, FastAPI and database, Python and SQL. Centralizing
them here makes the agreement explicit and renaming safe.

JWT auth constants (AUTH_HEADER, BEARER_PREFIX, CURRENT_USER_ID_SESSION_VAR,
JWT_ALGORITHM) live in grosh_shared.http.auth so the ingestion service can import
them without depending on this package.
"""

# Cookie name agreed between FastAPI and the browser. The frontend never
# touches this cookie directly (it's httpOnly), so there is no frontend
# mirror — the browser sends it automatically with credentials: 'include'.
REFRESH_TOKEN_COOKIE = "refresh_token"

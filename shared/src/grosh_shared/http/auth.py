"""JWT decode utilities shared across Python services.

Provides token decoding without environment coupling — the caller supplies
the secret. This allows both the API service (which also issues tokens) and
read-only consumers like the ingestion service to validate tokens without
sharing implementation.
"""

from uuid import UUID

import jwt

# HTTP Bearer auth — RFC 6750.
AUTH_HEADER = "Authorization"
BEARER_PREFIX = "Bearer "

# Single algorithm for all JWT operations in the platform.
JWT_ALGORITHM = "HS256"


class InvalidAccessTokenError(Exception):
    """Token is malformed, tampered, or expired."""

    def __init__(self, message: str = "Invalid token.") -> None:
        self.message = message
        super().__init__(message)


def decode_access_token(token: str, secret: str) -> dict[str, object]:
    """Decode and verify a JWT access token.

    Args:
        token:  Raw JWT string (without the ``Bearer `` prefix).
        secret: The HMAC secret used to sign tokens. Supplied by the caller
                so this module stays free of environment coupling.

    Returns:
        Decoded payload dict.

    Raises:
        InvalidAccessTokenError: Token is expired or otherwise invalid.
    """
    try:
        return jwt.decode(token, secret, algorithms=[JWT_ALGORITHM])
    except jwt.ExpiredSignatureError as exc:
        raise InvalidAccessTokenError("Token has expired.") from exc
    except jwt.InvalidTokenError as exc:
        raise InvalidAccessTokenError("Invalid token.") from exc


def extract_user_id(payload: dict[str, object]) -> UUID:
    """Extract and validate the ``sub`` claim as a UUID.

    Args:
        payload: Decoded JWT payload as returned by :func:`decode_access_token`.

    Returns:
        The user's UUID.

    Raises:
        InvalidAccessTokenError: ``sub`` claim is missing or not a valid UUID.
    """
    try:
        return UUID(str(payload["sub"]))
    except (KeyError, ValueError) as exc:
        raise InvalidAccessTokenError("Token subject is missing or invalid.") from exc

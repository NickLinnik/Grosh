import hashlib
import os
import secrets
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import asyncpg
import bcrypt
import jwt
from grosh_shared.db.rls import set_rls_user_email, set_rls_user_id
from grosh_shared.http.auth import JWT_ALGORITHM
from grosh_shared.http.auth import (
    InvalidAccessTokenError as SharedInvalidAccessTokenError,
)
from grosh_shared.http.auth import decode_access_token as shared_decode_access_token

from grosh_api.repositories.revoked_token_repo import RevokedTokenRepo
from grosh_api.repositories.token_repo import TokenRepo
from grosh_api.repositories.user_repo import UserRepo
from grosh_api.services import DomainError

_ACCESS_TOKEN_TTL_MINUTES = 15
_REFRESH_TOKEN_TTL_DAYS = 30
_REFRESH_TOKEN_BYTES = 32

# Pre-computed bcrypt hash used as a constant-time decoy when login is
# attempted against an unknown email. Without this, login would only run
# bcrypt for existing users, leaking account existence via response time.
# Computed once at import (~100 ms).
_DUMMY_PASSWORD_HASH = bcrypt.hashpw(b"dummy", bcrypt.gensalt()).decode()


class InvalidCredentialsError(DomainError):
    """Login attempted with unknown email, wrong password, or inactive user."""


class InvalidAccessTokenError(DomainError):
    """Access token is malformed, tampered, or expired."""


class SessionExpiredError(DomainError):
    """Refresh token is missing, unknown, expired, or belongs to an inactive user."""


class AuthService:
    def __init__(
        self,
        user_repo: UserRepo,
        token_repo: TokenRepo,
        revoked_token_repo: RevokedTokenRepo,
    ) -> None:
        self._user_repo = user_repo
        self._token_repo = token_repo
        self._revoked_token_repo = revoked_token_repo

    # -- JWT ------------------------------------------------------------------

    def encode_access_token(self, user_id: UUID) -> str:
        now = datetime.now(UTC)
        payload = {
            "sub": str(user_id),
            "jti": str(uuid4()),
            "iat": now,
            "exp": now + timedelta(minutes=_ACCESS_TOKEN_TTL_MINUTES),
        }
        return jwt.encode(payload, os.environ["JWT_SECRET"], algorithm=JWT_ALGORITHM)

    def decode_access_token(self, token: str) -> dict[str, object]:
        try:
            return shared_decode_access_token(token, os.environ["JWT_SECRET"])
        except SharedInvalidAccessTokenError as exc:
            raise InvalidAccessTokenError(exc.message) from exc

    # -- Password -------------------------------------------------------------

    def hash_password(self, plain: str) -> str:
        return bcrypt.hashpw(plain.encode(), bcrypt.gensalt()).decode()

    def verify_password(self, plain: str, hashed: str) -> bool:
        return bcrypt.checkpw(plain.encode(), hashed.encode())

    # -- Refresh token --------------------------------------------------------

    @staticmethod
    def _hash_token(token_bytes: bytes) -> str:
        return hashlib.sha256(token_bytes).hexdigest()

    async def _create_refresh_token(
        self, conn: asyncpg.Connection, user_id: UUID
    ) -> str:
        raw_bytes = secrets.token_bytes(_REFRESH_TOKEN_BYTES)
        raw_token = raw_bytes.hex()
        token_hash = self._hash_token(raw_bytes)
        expires_at = datetime.now(UTC) + timedelta(days=_REFRESH_TOKEN_TTL_DAYS)

        await self._token_repo.create(conn, user_id, token_hash, expires_at)

        return raw_token

    # -- Auth flows -----------------------------------------------------------

    async def login(
        self, conn: asyncpg.Connection, email: str, password: str
    ) -> tuple[str, str]:
        await set_rls_user_email(conn, email)
        record = await self._user_repo.get_by_email(conn, email)

        # Constant-time guard against user enumeration: always run bcrypt,
        # even when the user doesn't exist, so response time is independent
        # of whether the email is registered.
        if record is None:
            self.verify_password(password, _DUMMY_PASSWORD_HASH)
            raise InvalidCredentialsError()

        if not self.verify_password(password, record.password_hash):
            raise InvalidCredentialsError()

        if not record.is_active:
            raise InvalidCredentialsError()

        access_token = self.encode_access_token(record.id)
        refresh_token = await self._create_refresh_token(conn, record.id)
        await self._user_repo.touch_last_active_at(conn, record.id)

        return access_token, refresh_token

    async def refresh(
        self, conn: asyncpg.Connection, raw_token: str
    ) -> tuple[str, str]:
        try:
            token_hash = self._hash_token(bytes.fromhex(raw_token))
        except ValueError:
            raise SessionExpiredError()
        token = await self._token_repo.get_by_hash(conn, token_hash)

        if token is None or token.expires_at < datetime.now(UTC):
            raise SessionExpiredError()

        await set_rls_user_id(conn, token.user_id)
        user = await self._user_repo.get_by_id(conn, token.user_id)
        if user is None or not user.is_active:
            raise SessionExpiredError()

        async with conn.transaction():
            await self._token_repo.delete(conn, token.id)
            new_raw = await self._create_refresh_token(conn, user.id)
            await self._user_repo.touch_last_active_at(conn, user.id)

        access_token = self.encode_access_token(user.id)
        return access_token, new_raw

    async def _revoke_access_token(
        self, conn: asyncpg.Connection, access_token: str
    ) -> None:
        try:
            payload = self.decode_access_token(access_token)
            jti = UUID(str(payload["jti"]))
            exp = datetime.fromtimestamp(float(str(payload["exp"])), tz=UTC)
            await self._revoked_token_repo.insert(conn, jti, exp)
        except (InvalidAccessTokenError, KeyError, ValueError):
            pass  # malformed token — nothing to revoke

    async def is_token_revoked(self, conn: asyncpg.Connection, jti: UUID) -> bool:
        return await self._revoked_token_repo.is_revoked(conn, jti)

    async def logout(
        self,
        conn: asyncpg.Connection,
        raw_refresh_token: str,
        access_token: str | None = None,
    ) -> None:
        try:
            token_hash = self._hash_token(bytes.fromhex(raw_refresh_token))
        except ValueError:
            pass
        else:
            token = await self._token_repo.get_by_hash(conn, token_hash)
            if token is not None:
                await self._token_repo.delete(conn, token.id)

        if access_token:
            await self._revoke_access_token(conn, access_token)

    async def logout_all(
        self,
        conn: asyncpg.Connection,
        user_id: UUID,
        access_token: str | None = None,
    ) -> None:
        """Revoke every refresh token belonging to ``user_id``.

        Use case: user suspects their account is compromised and wants to
        kick every active session out of every device. Also revokes the
        current access token immediately if provided.
        """
        await self._token_repo.delete_by_user(conn, user_id)
        if access_token:
            await self._revoke_access_token(conn, access_token)

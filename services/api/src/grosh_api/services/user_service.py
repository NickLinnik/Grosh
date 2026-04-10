from uuid import UUID

import asyncpg
from grosh_shared.models import UserRole

from grosh_api.repositories.token_repo import TokenRepo
from grosh_api.repositories.user_repo import UserRecord, UserRepo
from grosh_api.services import DomainError
from grosh_api.services.auth_service import AuthService


class UserAlreadyExistsError(DomainError):
    """Attempted to create a user with an email that is already registered."""


class UserNotFoundError(DomainError):
    """Target user id does not exist."""


class CannotDeleteSelfError(DomainError):
    """Admin attempted to delete their own account."""


class UserService:
    def __init__(
        self,
        user_repo: UserRepo,
        token_repo: TokenRepo,
        auth_service: AuthService,
    ) -> None:
        self._user_repo = user_repo
        self._token_repo = token_repo
        self._auth_service = auth_service

    async def create_user(
        self,
        conn: asyncpg.Connection,
        email: str,
        password: str,
        display_name: str,
        role: UserRole,
    ) -> UserRecord:
        if await self._user_repo.exists_by_email(conn, email):
            raise UserAlreadyExistsError()
        password_hash = self._auth_service.hash_password(password)
        return await self._user_repo.create(
            conn, email, password_hash, display_name, role
        )

    async def delete_user(
        self,
        conn: asyncpg.Connection,
        admin_id: UUID,
        target_id: UUID,
    ) -> None:
        if admin_id == target_id:
            raise CannotDeleteSelfError()
        record = await self._user_repo.get_by_id(conn, target_id)
        if record is None:
            raise UserNotFoundError()
        await self._user_repo.set_active(conn, target_id, False)
        await self._token_repo.delete_by_user(conn, target_id)

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

import asyncpg
from grosh_shared.models import User, UserRole

_USER_COLUMNS = (
    "id, email, password_hash, display_name, role, is_active, created_at, updated_at"
)


@dataclass(frozen=True)
class UserRecord:
    id: UUID
    email: str
    password_hash: str
    display_name: str
    role: UserRole
    is_active: bool
    created_at: datetime
    updated_at: datetime

    def to_user(self) -> User:
        return User(
            id=self.id,
            email=self.email,
            display_name=self.display_name,
            role=self.role,
            is_active=self.is_active,
        )


class UserRepo:
    @staticmethod
    def _row_to_record(row: asyncpg.Record) -> UserRecord:
        return UserRecord(
            id=row["id"],
            email=row["email"],
            password_hash=row["password_hash"],
            display_name=row["display_name"],
            role=UserRole(row["role"]),
            is_active=row["is_active"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    async def get_by_email(
        self, conn: asyncpg.Connection, email: str
    ) -> UserRecord | None:
        row = await conn.fetchrow(
            f"""
            SELECT {_USER_COLUMNS}
            FROM users
            WHERE email = $1
            """,
            email,
        )
        if row is None:
            return None
        return self._row_to_record(row)

    async def get_by_id(
        self, conn: asyncpg.Connection, user_id: UUID
    ) -> UserRecord | None:
        row = await conn.fetchrow(
            f"""
            SELECT {_USER_COLUMNS}
            FROM users
            WHERE id = $1
            """,
            user_id,
        )
        if row is None:
            return None
        return self._row_to_record(row)

    async def create(
        self,
        conn: asyncpg.Connection,
        email: str,
        password_hash: str,
        display_name: str,
        role: UserRole,
    ) -> UserRecord:
        row = await conn.fetchrow(
            f"""
            INSERT INTO users (email, password_hash, display_name, role)
            VALUES ($1, $2, $3, $4)
            RETURNING {_USER_COLUMNS}
            """,
            email,
            password_hash,
            display_name,
            role.value,
        )
        return self._row_to_record(row)

    async def set_active(
        self, conn: asyncpg.Connection, user_id: UUID, is_active: bool
    ) -> None:
        await conn.execute(
            """
            UPDATE users
            SET is_active = $2
            WHERE id = $1
            """,
            user_id,
            is_active,
        )

    async def exists_by_email(self, conn: asyncpg.Connection, email: str) -> bool:
        result = await conn.fetchval(
            """
            SELECT EXISTS(
                SELECT 1 FROM users WHERE email = $1
            )
            """,
            email,
        )
        return bool(result)

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


@dataclass(frozen=True)
class AdminUserRow:
    id: UUID
    email: str
    role: UserRole
    created_at: datetime
    last_active_at: datetime | None


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

    async def touch_last_active_at(
        self, conn: asyncpg.Connection, user_id: UUID
    ) -> None:
        await conn.execute(
            """
            UPDATE users
            SET last_active_at = now()
            WHERE id = $1
            """,
            user_id,
        )

    async def count_admin_users(self, conn: asyncpg.Connection) -> int:
        result = await conn.fetchval(
            """
            SELECT count(*)
            FROM users
            """
        )
        return int(result)

    async def list_admin_users(
        self,
        conn: asyncpg.Connection,
        *,
        limit: int,
        cursor_created_at: datetime | None = None,
        cursor_id: UUID | None = None,
    ) -> list[AdminUserRow]:
        if cursor_created_at is not None and cursor_id is not None:
            rows = await conn.fetch(
                """
                SELECT
                    id,
                    email,
                    role,
                    created_at,
                    last_active_at
                FROM users
                WHERE (created_at, id) < ($1::timestamptz, $2::uuid)
                ORDER BY
                    created_at DESC,
                    id DESC
                LIMIT $3
                """,
                cursor_created_at,
                cursor_id,
                limit,
            )
        else:
            rows = await conn.fetch(
                """
                SELECT
                    id,
                    email,
                    role,
                    created_at,
                    last_active_at
                FROM users
                ORDER BY
                    created_at DESC,
                    id DESC
                LIMIT $1
                """,
                limit,
            )
        return [
            AdminUserRow(
                id=row["id"],
                email=row["email"],
                role=UserRole(row["role"]),
                created_at=row["created_at"],
                last_active_at=row["last_active_at"],
            )
            for row in rows
        ]

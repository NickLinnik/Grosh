import json
from typing import Any
from uuid import UUID

import asyncpg
from grosh_shared.models import BankSource


class IntegrationRepo:
    async def create_integration(
        self,
        conn: asyncpg.Connection,
        user_id: UUID,
        bank: BankSource,
        config: dict[str, Any],
    ) -> UUID:
        row = await conn.fetchrow(
            """
            INSERT INTO bank_integrations
                (user_id, bank, config, status)
            VALUES
                ($1, $2, $3::jsonb, 'active')
            RETURNING id
            """,
            user_id,
            str(bank),
            json.dumps(config),
        )
        return row["id"]

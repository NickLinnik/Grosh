import json
from uuid import UUID

import asyncpg
from grosh_shared.normalized import TransactionRow


class TransactionReadRepo:
    """Read-only access to the transactions table for the reprocess flow.

    Only exposes select_for_user — write methods live in
    grosh_pipeline.repositories.transaction_repo (pipeline service).
    """

    async def select_for_user(
        self,
        conn: asyncpg.Connection,
        user_id: UUID,
    ) -> list[TransactionRow]:
        """Return all transactions for a user ordered by (time, id) for replay."""
        rows = await conn.fetch(
            """
            SELECT
                id,
                source,
                source_id,
                user_id,
                account_id,
                time,
                amount_cents,
                operation_amount_cents,
                operation_currency_code,
                description,
                mcc,
                cashback_amount_cents,
                balance_cents,
                hold,
                direction,
                counterparty_iban,
                rate_source,
                metadata
            FROM transactions
            WHERE user_id = $1
            ORDER BY
                time,
                id
            """,
            user_id,
        )
        return [
            TransactionRow(
                id=row["id"],
                source=row["source"],
                source_id=row["source_id"],
                user_id=row["user_id"],
                account_id=row["account_id"],
                time=row["time"],
                amount_cents=row["amount_cents"],
                operation_amount_cents=row["operation_amount_cents"],
                operation_currency_code=row["operation_currency_code"],
                description=row["description"],
                mcc=row["mcc"],
                cashback_amount_cents=row["cashback_amount_cents"] or 0,
                balance_cents=row["balance_cents"],
                hold=row["hold"],
                direction=row["direction"],
                counterparty_iban=row["counterparty_iban"],
                rate_source=row["rate_source"],
                metadata=json.loads(row["metadata"])
                if isinstance(row["metadata"], str)
                else row["metadata"],
            )
            for row in rows
        ]

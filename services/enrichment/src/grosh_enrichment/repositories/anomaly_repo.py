"""Anomaly recording repository for transfer detection.

Owns all SQL related to transfer_match_anomalies.
"""

from uuid import UUID

import asyncpg


class AnomalyRepo:
    async def record_anomaly(
        self,
        conn: asyncpg.Connection,
        transaction_id: UUID,
        candidate_ids: list[UUID],
        reason_code: str,
        reason_detail: str | None,
    ) -> None:
        """Insert a transfer match anomaly.

        The table has a UNIQUE constraint on transaction_id — only one
        anomaly per transaction is allowed.  ON CONFLICT DO NOTHING handles
        Kafka redelivery: if the consumer crashes after recording the anomaly
        but before committing the offset, the retry silently skips the
        duplicate (the anomaly from the first attempt is already correct).
        """
        await conn.execute(
            """
            INSERT INTO transfer_match_anomalies (
                transaction_id,
                candidate_ids,
                reason_code,
                reason_detail
            ) VALUES ($1, $2, $3, $4)
            ON CONFLICT (transaction_id) DO NOTHING
            """,
            transaction_id,
            candidate_ids,
            reason_code,
            reason_detail,
        )

    async def delete_unpaired_anomalies_for_transactions(
        self,
        conn: asyncpg.Connection,
        tx_ids: list[UUID],
    ) -> None:
        """Delete unpaired_* anomalies when a partner arrives and the pair succeeds.

        Only deletes auto-resolvable anomaly types (unpaired_from_description,
        unpaired_to_description). Terminal anomalies (ambiguous_*, description_*)
        persist for manual investigation — they represent rejected decisions,
        not transient states.
        """
        await conn.execute(
            """
            DELETE FROM transfer_match_anomalies
            WHERE transaction_id = ANY($1::uuid[])
              AND reason_code IN (
                  'unpaired_from_description',
                  'unpaired_to_description'
              )
            """,
            tx_ids,
        )

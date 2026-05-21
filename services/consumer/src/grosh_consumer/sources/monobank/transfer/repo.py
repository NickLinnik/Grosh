"""Monobank transfer detection repository.

Lives under `sources/monobank/transfer/` rather than the generic
`repositories/` directory because every query encodes a Monobank-specific
assumption: the `mcc = '4829'` gate, the `±2 seconds` window, and the
two-clause amount predicate that exists solely to handle Monobank's
FOP↔FOP cross-currency `op_amount` asymmetry (ADR §3 "Why two amount
predicates"). Other banks (PUMB, Revolut on the roadmap) will need
different MCCs / windows / predicates; when the second bank lands,
factor common pieces out then.

Three methods consumed by `MonobankTransferDetection`:

  - `exists(conn, tx_id) -> bool` — idempotency gate.
  - `find_universal_candidates(conn, tx) -> list[CandidateRow]` — single
    `SELECT … FOR UPDATE SKIP LOCKED` over the unclaimed candidate set.
  - `claim_pair(conn, *, existing_id, new_id, pair_metadata_block)` —
    UPDATE the partner row's `related_transaction_id`, `special_category`,
    and `metadata.layer.transfer.pair`.

SQL is triple-quoted, one item per line per project SQL style.
The `claim_pair` UPDATE uses `jsonb_set(COALESCE(metadata, '{}'::jsonb), …)`
to defend against partner rows with NULL or partial metadata.
"""

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg
from grosh_shared.mcc import MccCode
from grosh_shared.models import TransactionDirection
from grosh_shared.normalized import NormalizedTransaction


@dataclass(frozen=True)
class CandidateRow:
    """Subset of `transactions` columns the decision module needs.

    The universal-fetch SQL selects exactly these fields — `metadata`,
    `cashback_amount_cents`, etc. are not pulled because the strategy
    has no use for them.

    `direction` is typed as the full TransactionDirection enum even
    though the SQL filters on MCC 4829 (which is never `zero` in
    practice — transfers always have a non-zero amount). Encoding the
    narrower "income | expense" subspace at the type level would
    require a separate sub-enum that buys no clarity at this scale.
    """

    id: UUID
    account_id: UUID
    direction: TransactionDirection
    counterparty_iban: str | None
    description: str | None
    amount_cents: int
    operation_amount_cents: int | None
    time: datetime


class TransferQueryRepo:
    async def transaction_exists(self, conn: asyncpg.Connection, tx_id: UUID) -> bool:
        row = await conn.fetchval(
            """
            SELECT 1
            FROM transactions
            WHERE id = $1
            """,
            tx_id,
        )
        return row is not None

    async def find_universal_candidates(
        self,
        conn: asyncpg.Connection,
        tx: NormalizedTransaction,
    ) -> list[CandidateRow]:
        """Single SELECT replacing the v1 three-tier ladder.

        Filters: same user, opposite direction, MCC 4829, different
        account, unclaimed, ±2s window. The two-clause amount predicate
        handles Monobank's FOP↔FOP cross-currency `op_amount` asymmetry
        (per ADR §3 "Why two amount predicates"):

          - clause 1: `cand.amount_cents = COALESCE(incoming.op_amount,
            incoming.amount_cents)` — finds the partner from the expense
            side of an asymmetric pair.
          - clause 2: `cand.operation_amount_cents = incoming.amount_cents`
            — finds the partner from the income side of the same pair.

        `FOR UPDATE SKIP LOCKED` so concurrent same-millisecond pair
        workers don't double-claim — the late one's fetch returns 0 and
        the standard at-least-once retry handles the eventual claim.
        """
        opposite_direction = (
            TransactionDirection.income
            if tx.direction == TransactionDirection.expense
            else TransactionDirection.expense
        )
        first_clause_amount = (
            tx.operation_amount_cents
            if tx.operation_amount_cents is not None
            else tx.amount_cents
        )
        rows = await conn.fetch(
            """
            SELECT
                id,
                account_id,
                direction,
                counterparty_iban,
                description,
                amount_cents,
                operation_amount_cents,
                time
            FROM transactions
            WHERE user_id = $1
              AND direction = $2::transaction_direction
              AND mcc = $3
              AND account_id != $4
              AND related_transaction_id IS NULL
              AND (
                    amount_cents = $5
                 OR operation_amount_cents = $6
              )
              AND time BETWEEN $7::timestamptz - interval '2 seconds'
                          AND $7::timestamptz + interval '2 seconds'
            FOR UPDATE SKIP LOCKED
            """,
            tx.user_id,
            opposite_direction,
            MccCode.WIRE_TRANSFER.code,
            tx.account_id,
            first_clause_amount,
            tx.amount_cents,
            tx.time,
        )
        return [
            CandidateRow(
                id=row["id"],
                account_id=row["account_id"],
                direction=row["direction"],
                counterparty_iban=row["counterparty_iban"],
                description=row["description"],
                amount_cents=row["amount_cents"],
                operation_amount_cents=row["operation_amount_cents"],
                time=row["time"],
            )
            for row in rows
        ]

    async def claim_pair(
        self,
        conn: asyncpg.Connection,
        *,
        existing_id: UUID,
        new_id: UUID,
        pair_metadata_block: dict[str, Any],
    ) -> None:
        """Claim the existing row as the partner of the new row.

        Sets `related_transaction_id` to `new_id`, `special_category` to
        `'transfer'`, and writes `pair_metadata_block` under
        `metadata.layer.transfer.pair` on the existing row.

        Uses `jsonb_set(COALESCE(metadata, '{}'::jsonb), ...,
        create_missing => true)` to defend against partner rows with
        `metadata IS NULL`, `metadata = '{}'`, or `metadata` containing
        only the `source` namespace (no `layer` key yet). The
        `metadata.source` sub-tree, when present, is preserved.
        """
        # Ensure metadata.layer and metadata.layer.transfer exist as objects
        # before setting the leaf .pair, since jsonb_set's create_missing
        # only creates the LEAF key, not intermediate path elements.
        await conn.execute(
            """
            UPDATE transactions
            SET
                related_transaction_id = $1,
                special_category = 'transfer'::special_category,
                metadata = jsonb_set(
                    jsonb_set(
                        jsonb_set(
                            COALESCE(metadata, '{}'::jsonb),
                            '{layer}',
                            COALESCE(metadata -> 'layer', '{}'::jsonb),
                            true
                        ),
                        '{layer,transfer}',
                        COALESCE(metadata -> 'layer' -> 'transfer', '{}'::jsonb),
                        true
                    ),
                    '{layer,transfer,pair}',
                    $3::jsonb,
                    true
                )
            WHERE id = $2
            """,
            new_id,
            existing_id,
            json.dumps(pair_metadata_block),
        )

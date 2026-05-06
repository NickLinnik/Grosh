"""Monobank 3-tier transfer detection strategy.

Implements TransferDetectionStrategy for Monobank transactions.

Detection tiers (executed in order):
  A — IBAN lookup: incoming tx has counterparty_iban → find own account → query partner.
  B — Reverse IBAN: no incoming IBAN, but partner stored this account's IBAN
      as counterparty.
  C — Amount fallback: both IBANs NULL; match by operation_amount + description guard.

Description guard acts as a CANARY on Tiers A/B (pair proceeds regardless of mismatch,
anomaly recorded) and as a GATE on Tier C (invalid descriptions disqualify candidates).
"""

from uuid import UUID

import asyncpg
from grosh_shared.mcc import MccCode

from grosh_consumer.models.normalized import NormalizedTransaction
from grosh_consumer.repositories.account_property_repo import AccountPropertyRepo
from grosh_consumer.repositories.anomaly_repo import AnomalyRepo
from grosh_consumer.repositories.transfer_repo import TransferQueryRepo
from grosh_consumer.services.transfer_detection import AnomalyRecord, TransferResult
from grosh_consumer.sources.monobank.descriptions import (
    is_transfer_description,
    parse_description,
    validate_pair_descriptions,
)

_WINDOW_SECONDS = 2

# Anomaly codes that are auto-resolved when a pair is found.
# Terminal anomaly codes (ambiguous_*, description_*) are never auto-deleted.
_AUTO_RESOLVE_CODES = frozenset(
    {"unpaired_from_description", "unpaired_to_description"}
)


def _opposite_direction(direction: str) -> str:
    if direction == "income":
        return "expense"
    if direction == "expense":
        return "income"
    return direction


def _assign_sides(
    direction: str,
    tx_desc: str | None,
    tx_acc_type: str,
    tx_acc_currency: str,
    partner_desc: str | None,
    partner_acc_type: str,
    partner_acc_currency: str,
) -> tuple[str | None, str | None, str, str, str, str]:
    """Sort incoming tx and partner into (income_desc, expense_desc,
    income_type, income_currency, expense_type, expense_currency)
    based on the incoming tx's direction."""
    if direction == "income":
        return (
            tx_desc,
            partner_desc,
            tx_acc_type,
            tx_acc_currency,
            partner_acc_type,
            partner_acc_currency,
        )
    return (
        partner_desc,
        tx_desc,
        partner_acc_type,
        partner_acc_currency,
        tx_acc_type,
        tx_acc_currency,
    )


class MonobankTransferDetection:
    """3-tier transfer detection for Monobank transactions."""

    def __init__(
        self,
        transaction_repo: TransferQueryRepo,
        account_repo: AccountPropertyRepo,
        anomaly_repo: AnomalyRepo,
    ) -> None:
        self._tx_repo = transaction_repo
        self._acc_repo = account_repo
        self._anomaly_repo = anomaly_repo

    async def detect_and_pair(
        self,
        conn: asyncpg.Connection,
        tx: NormalizedTransaction,
    ) -> TransferResult:
        # --- Idempotency guard ---
        if await self._tx_repo.exists(conn, tx.id):
            return TransferResult()

        # --- MCC filter ---
        if tx.mcc != MccCode.WIRE_TRANSFER.code:
            return TransferResult()

        opposite = _opposite_direction(tx.direction)

        # --- Tier A: counterparty IBAN present ---
        if tx.counterparty_iban is not None:
            result = await self._tier_a(conn, tx, opposite)
            if result is not None:
                return result
            # Tier A found 0 candidates — counterparty_iban may point to a
            # non-immediate hop (e.g. final destination in a 3-hop transfer).
            # Fall through to Tier B.

        # --- Tier B: reverse IBAN lookup ---
        (
            acc_type,
            acc_currency,
            acc_iban,
        ) = await self._acc_repo.get_account_with_properties(conn, tx.account_id)
        if acc_iban is not None:
            result = await self._tier_b(
                conn, tx, opposite, acc_type, acc_currency, acc_iban
            )
            if result is not None:
                return result

        # --- Tier C: both IBANs NULL, description + amount fallback ---
        return await self._tier_c(conn, tx, opposite, acc_type, acc_currency)

    # ------------------------------------------------------------------
    # Tier A
    # ------------------------------------------------------------------

    async def _tier_a(
        self,
        conn: asyncpg.Connection,
        tx: NormalizedTransaction,
        opposite: str,
    ) -> TransferResult | None:
        if tx.counterparty_iban is None:
            raise ValueError("Tier A called without counterparty_iban")

        # Look up the target account — must belong to the same user.
        target = await self._acc_repo.get_user_account_by_iban(
            conn, tx.counterparty_iban, tx.user_id
        )
        if target is None:
            # IBAN is external — not an internal transfer.
            return TransferResult()

        target_account_id, target_type, target_currency = target

        candidates = await self._tx_repo.find_unclaimed_partner_tier_a(
            conn,
            user_id=tx.user_id,
            target_account_id=target_account_id,
            opposite_type=opposite,
            time=tx.time,
            window_seconds=_WINDOW_SECONDS,
        )

        if len(candidates) == 0:
            return None

        if len(candidates) > 1:
            anomaly = AnomalyRecord(
                transaction_id=tx.id,
                candidate_ids=[c["id"] for c in candidates],
                reason_code="ambiguous_iban_match",
                reason_detail=(
                    f"Found {len(candidates)} unclaimed partners on account"
                    f" {target_account_id} within ±{_WINDOW_SECONDS}s"
                ),
            )
            return TransferResult(anomalies=[anomaly])

        partner = candidates[0]
        partner_id: UUID = partner["id"]

        # Resolve incoming account properties for description validation.
        inc_type, inc_currency, _ = await self._acc_repo.get_account_with_properties(
            conn, tx.account_id
        )

        (
            income_desc,
            expense_desc,
            income_type,
            income_currency,
            expense_type,
            expense_currency,
        ) = _assign_sides(
            tx.direction,
            tx.description,
            inc_type,
            inc_currency,
            partner["description"],
            target_type,
            target_currency,
        )

        anomalies: list[AnomalyRecord] = []
        descriptions_valid = validate_pair_descriptions(
            income_desc=income_desc,
            expense_desc=expense_desc,
            income_account_type=income_type,
            income_account_currency=income_currency,
            expense_account_type=expense_type,
            expense_account_currency=expense_currency,
        )
        if not descriptions_valid:
            anomalies.append(
                AnomalyRecord(
                    transaction_id=tx.id,
                    candidate_ids=[partner_id],
                    reason_code="description_consistency_mismatch",
                    reason_detail=(
                        f"Income desc={income_desc!r} / expense desc={expense_desc!r}"
                        f" inconsistent with account types"
                        f" income={income_type}/{income_currency}"
                        f" expense={expense_type}/{expense_currency}"
                    ),
                )
            )

        await self._claim(conn, partner_id, tx.id)
        return TransferResult(
            special_category="transfer",
            related_transaction_id=partner_id,
            anomalies=anomalies,
        )

    # ------------------------------------------------------------------
    # Tier B
    # ------------------------------------------------------------------

    async def _tier_b(
        self,
        conn: asyncpg.Connection,
        tx: NormalizedTransaction,
        opposite: str,
        acc_type: str,
        acc_currency: str,
        acc_iban: str,
    ) -> TransferResult | None:
        """Return a TransferResult if found, None to fall through to Tier C."""
        candidates = await self._tx_repo.find_unclaimed_partner_tier_b(
            conn,
            user_id=tx.user_id,
            account_iban=acc_iban,
            opposite_type=opposite,
            time=tx.time,
            window_seconds=_WINDOW_SECONDS,
        )

        if len(candidates) == 0:
            return None

        if len(candidates) > 1:
            anomaly = AnomalyRecord(
                transaction_id=tx.id,
                candidate_ids=[c["id"] for c in candidates],
                reason_code="ambiguous_reverse_iban",
                reason_detail=(
                    f"Found {len(candidates)} unclaimed transactions with"
                    f" counterparty_iban={acc_iban!r} within ±{_WINDOW_SECONDS}s"
                ),
            )
            return TransferResult(anomalies=[anomaly])

        partner = candidates[0]
        partner_id: UUID = partner["id"]
        (
            partner_acc_type,
            partner_acc_currency,
            _,
        ) = await self._acc_repo.get_account_with_properties(
            conn, partner["account_id"]
        )

        (
            income_desc,
            expense_desc,
            income_type,
            income_currency,
            expense_type,
            expense_currency,
        ) = _assign_sides(
            tx.direction,
            tx.description,
            acc_type,
            acc_currency,
            partner["description"],
            partner_acc_type,
            partner_acc_currency,
        )

        anomalies: list[AnomalyRecord] = []
        descriptions_valid = validate_pair_descriptions(
            income_desc=income_desc,
            expense_desc=expense_desc,
            income_account_type=income_type,
            income_account_currency=income_currency,
            expense_account_type=expense_type,
            expense_account_currency=expense_currency,
        )
        if not descriptions_valid:
            anomalies.append(
                AnomalyRecord(
                    transaction_id=tx.id,
                    candidate_ids=[partner_id],
                    reason_code="description_consistency_mismatch",
                    reason_detail=(
                        f"Income desc={income_desc!r} / expense desc={expense_desc!r}"
                        f" inconsistent with account types"
                        f" income={income_type}/{income_currency}"
                        f" expense={expense_type}/{expense_currency}"
                    ),
                )
            )

        await self._claim(conn, partner_id, tx.id)
        return TransferResult(
            special_category="transfer",
            related_transaction_id=partner_id,
            anomalies=anomalies,
        )

    # ------------------------------------------------------------------
    # Tier C
    # ------------------------------------------------------------------

    async def _tier_c(
        self,
        conn: asyncpg.Connection,
        tx: NormalizedTransaction,
        opposite: str,
        acc_type: str,
        acc_currency: str,
    ) -> TransferResult:
        # Description guard: only known transfer descriptions enter Tier C.
        constraint = parse_description(tx.description)
        if constraint is None:
            # Not in the known set — check prefix to decide on anomaly type.
            if is_transfer_description(tx.description):
                reason = (
                    "unpaired_from_description"
                    if tx.direction == "income"
                    else "unpaired_to_description"
                )
                anomaly = AnomalyRecord(
                    transaction_id=tx.id,
                    candidate_ids=[],
                    reason_code=reason,
                    reason_detail=(
                        f"Description {tx.description!r} has transfer prefix"
                        " but is not in the known set; no Tier C attempt"
                    ),
                )
                return TransferResult(anomalies=[anomaly])
            # Plain non-transfer description (e.g. person name) — no anomaly.
            return TransferResult()

        # The incoming operation_amount_cents equals what the partner received
        # as amount_cents (the cross-match criterion).
        incoming_op_amount = tx.operation_amount_cents
        if incoming_op_amount is None:
            incoming_op_amount = tx.amount_cents

        all_candidates = await self._tx_repo.find_unclaimed_partner_tier_c(
            conn,
            user_id=tx.user_id,
            operation_amount_cents=incoming_op_amount,
            opposite_type=opposite,
            time=tx.time,
            incoming_account_id=tx.account_id,
            window_seconds=_WINDOW_SECONDS,
        )

        # Batch-fetch account properties for all candidates in one query.
        candidate_account_ids = [c["account_id"] for c in all_candidates]
        account_props = await self._acc_repo.get_accounts_with_properties(
            conn, candidate_account_ids
        )

        # Filter candidates by description validation.
        valid_candidates = []
        invalid_candidates = []
        for candidate in all_candidates:
            props = account_props.get(candidate["account_id"])
            if props is None:
                continue
            cand_acc_type, cand_acc_currency, _ = props

            (
                income_desc,
                expense_desc,
                income_type,
                income_currency,
                expense_type,
                expense_currency,
            ) = _assign_sides(
                tx.direction,
                tx.description,
                acc_type,
                acc_currency,
                candidate["description"],
                cand_acc_type,
                cand_acc_currency,
            )

            if validate_pair_descriptions(
                income_desc=income_desc,
                expense_desc=expense_desc,
                income_account_type=income_type,
                income_account_currency=income_currency,
                expense_account_type=expense_type,
                expense_account_currency=expense_currency,
            ):
                valid_candidates.append((candidate, cand_acc_type, cand_acc_currency))
            else:
                invalid_candidates.append(candidate)

        if len(valid_candidates) == 0:
            if invalid_candidates:
                anomaly = AnomalyRecord(
                    transaction_id=tx.id,
                    candidate_ids=[c["id"] for c in invalid_candidates],
                    reason_code="description_account_mismatch",
                    reason_detail=(
                        f"Candidates found but description validation failed;"
                        f" incoming desc={tx.description!r}"
                        f" account={acc_type}/{acc_currency}"
                    ),
                )
                return TransferResult(anomalies=[anomaly])
            # No candidates at all — record unpaired anomaly if description is known.
            if tx.direction == "income":
                reason = "unpaired_from_description"
            else:
                reason = "unpaired_to_description"
            anomaly = AnomalyRecord(
                transaction_id=tx.id,
                candidate_ids=[],
                reason_code=reason,
                reason_detail=(
                    f"No Tier C candidates for desc={tx.description!r}"
                    f" op_amount={incoming_op_amount}"
                    f" within ±{_WINDOW_SECONDS}s"
                ),
            )
            return TransferResult(anomalies=[anomaly])

        if len(valid_candidates) > 1:
            anomaly = AnomalyRecord(
                transaction_id=tx.id,
                candidate_ids=[c["id"] for c, _, _ in valid_candidates],
                reason_code="ambiguous_amount_match",
                reason_detail=(
                    f"Found {len(valid_candidates)} valid candidates"
                    f" for op_amount={incoming_op_amount}"
                    f" within ±{_WINDOW_SECONDS}s"
                ),
            )
            return TransferResult(anomalies=[anomaly])

        partner, _, _ = valid_candidates[0]
        partner_id: UUID = partner["id"]
        await self._claim(conn, partner_id, tx.id)
        return TransferResult(
            special_category="transfer",
            related_transaction_id=partner_id,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _claim(
        self,
        conn: asyncpg.Connection,
        existing_tx_id: UUID,
        new_tx_id: UUID,
    ) -> None:
        """Update the existing partner and auto-resolve any unpaired anomalies.

        Cleans up both legs: the existing partner may have had an unpaired_*
        anomaly while waiting, and the new incoming tx may have one from a
        prior partial run (Kafka retry after crash).
        """
        await self._tx_repo.claim_pair(conn, existing_tx_id, new_tx_id)
        await self._anomaly_repo.delete_unpaired_anomalies_for_transactions(
            conn, [existing_tx_id, new_tx_id]
        )

"""MonobankTransferDetection — the v2 strategy orchestrator.

Reads top-down as the 7-step algorithm pass from
`references/adr-transfer-detection-v2.md` §1:

  1. MCC + idempotency gate
  2. Compute row flags (description_matched, multi_hop_description, cp_iban_status)
  3a. External-IBAN short-circuit (skip universal fetch; emit row block; maybe anomaly)
  3. Universal candidate fetch (one SELECT … FOR UPDATE SKIP LOCKED)
  4. Hard IBAN consistency filter
  5. Per-pair evidence classification
  6. Decide (count-and-decide + bucket-locked)
  7. Translate Decision → TransferResult (claim → claim_pair + auto-resolve;
     anomaly → record; skip → return)

No SQL, no description-map literals, no anomaly literals — all delegated to
the helper modules under `transfer/` and the repos.

The strategy implements `TransferDetectionStrategy.detect_and_pair`. It runs
inside the orchestrator's per-event transaction; `claim_pair` and
`anomaly_repo.record_anomaly` commit atomically with the orchestrator's
INSERT of the new row.
"""

from typing import Any
from uuid import UUID

import asyncpg
from grosh_shared.mcc import MccCode
from grosh_shared.models import TransactionDirection
from grosh_shared.normalized import NormalizedTransaction

from grosh_enrichment.repositories.account_repo import AccountProps, AccountRepo
from grosh_enrichment.repositories.anomaly_repo import AnomalyRepo
from grosh_enrichment.services.transfer_detection import (
    AnomalyRecord,
    TransferResult,
)
from grosh_enrichment.sources.monobank.descriptions import (
    is_transfer_description,
)
from grosh_enrichment.sources.monobank.transfer import anomalies
from grosh_enrichment.sources.monobank.transfer.decision import (
    Anomaly,
    Claim,
    Skip,
    decide,
)
from grosh_enrichment.sources.monobank.transfer.flags import (
    CpIbanStatus,
    RowFlags,
    compute_row_flags,
)
from grosh_enrichment.sources.monobank.transfer.iban_classifier import (
    PairEvidence,
    classify_pair_evidence,
    is_consistent,
)
from grosh_enrichment.sources.monobank.transfer.metadata import (
    build_pair_block,
    build_row_block,
)
from grosh_enrichment.sources.monobank.transfer.repo import (
    CandidateRow,
    TransferQueryRepo,
)


class MonobankTransferDetection:
    def __init__(
        self,
        transfer_repo: TransferQueryRepo,
        account_repo: AccountRepo,
        anomaly_repo: AnomalyRepo,
    ) -> None:
        self._transfer_repo = transfer_repo
        self._account_repo = account_repo
        self._anomaly_repo = anomaly_repo

    async def detect_and_pair(
        self,
        conn: asyncpg.Connection,
        tx: NormalizedTransaction,
    ) -> TransferResult:
        # Step 1: MCC + idempotency gate.
        # Zero-amount rows have no opposite direction to search for; skip them
        # before the universal fetch so we don't probe the candidate set with
        # a nonsensical direction parameter.
        if tx.mcc != MccCode.WIRE_TRANSFER.code:
            return TransferResult()
        if tx.direction not in (
            TransactionDirection.income,
            TransactionDirection.expense,
        ):
            return TransferResult()
        if await self._transfer_repo.transaction_exists(conn, tx.id):
            return TransferResult()

        # Step 2: compute row flags. Pre-resolve incoming.cp_iban so the
        # sync compute_row_flags can stay synchronous.
        incoming_iban_to_account = await self._batch_resolve_ibans(
            conn=conn,
            user_id=tx.user_id,
            ibans={tx.counterparty_iban} if tx.counterparty_iban else set(),
        )
        flags = compute_row_flags(tx, incoming_iban_to_account)
        transfer_row_metadata = build_row_block(flags)

        # Step 3a: unlinked-partner short-circuit (cp_iban resolves to no
        # account the user has linked to the platform; no pairing row exists).
        if flags.cp_iban_status == CpIbanStatus.UNLINKED:
            return self._handle_unlinked(tx, flags, transfer_row_metadata)

        # Step 3: universal candidate fetch.
        raw_candidates = await self._transfer_repo.find_universal_candidates(conn, tx)

        # Steps 4 + 5: consistency filter + evidence classification.
        scored = await self._filter_and_classify(
            conn=conn,
            tx=tx,
            flags=flags,
            candidates=raw_candidates,
            incoming_iban_to_account=incoming_iban_to_account,
        )

        # Step 6: decide.
        incoming_acc_props = await self._load_account(conn, tx.account_id)
        partner_account_ids = [cand.account_id for cand, _ev in scored]
        partner_acc_props_by_id = await self._load_accounts(conn, partner_account_ids)
        decision = decide(
            incoming_id=tx.id,
            candidates=scored,
            incoming_description=tx.description,
            incoming_account_id=tx.account_id,
            incoming_acc_props=incoming_acc_props,
            account_props_by_id=partner_acc_props_by_id,
        )

        # Step 7: translate Decision → TransferResult.
        return await self._apply_decision(
            conn=conn,
            tx=tx,
            decision=decision,
            row_block=transfer_row_metadata,
        )

    async def _filter_and_classify(
        self,
        *,
        conn: asyncpg.Connection,
        tx: NormalizedTransaction,
        flags: RowFlags,
        candidates: list[CandidateRow],
        incoming_iban_to_account: dict[str, UUID],
    ) -> list[tuple[CandidateRow, PairEvidence]]:
        if not candidates:
            return []

        # Pre-resolve every candidate IBAN the consistency filter and
        # evidence classifier will look up. The incoming IBAN was already
        # resolved before computing flags; reuse that cache.
        candidate_ibans: set[str] = {
            cand.counterparty_iban for cand in candidates if cand.counterparty_iban
        }
        new_resolutions = await self._batch_resolve_ibans(
            conn=conn, user_id=tx.user_id, ibans=candidate_ibans
        )
        iban_to_account = {**incoming_iban_to_account, **new_resolutions}

        scored: list[tuple[CandidateRow, PairEvidence]] = []
        for cand in candidates:
            # compute_row_flags accepts both NormalizedTransaction (above)
            # and CandidateRow (here) via the _RowFlagInputs Protocol — both
            # expose description / direction / counterparty_iban.
            cand_flags = compute_row_flags(cand, iban_to_account)
            if not is_consistent(
                incoming_flags=flags,
                candidate_flags=cand_flags,
                incoming_cp_iban=tx.counterparty_iban,
                candidate_cp_iban=cand.counterparty_iban,
                incoming_account_id=tx.account_id,
                candidate_account_id=cand.account_id,
                iban_to_account=iban_to_account,
            ):
                continue
            evidence = classify_pair_evidence(
                incoming_flags=flags,
                candidate_flags=cand_flags,
                incoming_cp_iban=tx.counterparty_iban,
                candidate_cp_iban=cand.counterparty_iban,
                incoming_account_id=tx.account_id,
                candidate_account_id=cand.account_id,
                iban_to_account=iban_to_account,
            )
            scored.append((cand, evidence))
        return scored

    async def _batch_resolve_ibans(
        self,
        *,
        conn: asyncpg.Connection,
        user_id: UUID,
        ibans: set[str],
    ) -> dict[str, UUID]:
        """Resolve each IBAN to its owning account_id (absent if unlinked)."""
        result: dict[str, UUID] = {}
        for iban in ibans:
            props = await self._account_repo.find_by_iban(conn, iban, user_id)
            if props is not None:
                result[iban] = props.id
        return result

    async def _load_account(
        self,
        conn: asyncpg.Connection,
        account_id: UUID,
    ) -> AccountProps:
        return await self._account_repo.get_by_id(conn, account_id)

    async def _load_accounts(
        self,
        conn: asyncpg.Connection,
        account_ids: list[UUID],
    ) -> dict[UUID, AccountProps]:
        return await self._account_repo.get_many_by_ids(conn, list(set(account_ids)))

    def _handle_unlinked(
        self,
        tx: NormalizedTransaction,
        flags: RowFlags,
        row_block: dict[str, Any],
    ) -> TransferResult:
        """§3a unlinked-partner short-circuit — emit row block; maybe an anomaly.

        The cp_iban resolves to an account the user hasn't linked to the
        platform (a friend's card, an unlinked spouse account, a vendor's
        IBAN). No partner row exists in our DB to pair with, so we skip the
        universal fetch entirely. Still record the row block for traceability.
        """
        result_anomalies: list[AnomalyRecord] = []
        if not flags.description_matched and is_transfer_description(tx.description):
            if (tx.description or "").startswith("З "):
                result_anomalies.append(
                    anomalies.unpaired_from_description(
                        transaction_id=tx.id,
                        description=tx.description,
                    )
                )
            elif (tx.description or "").startswith("На "):
                result_anomalies.append(
                    anomalies.unpaired_to_description(
                        transaction_id=tx.id,
                        description=tx.description,
                    )
                )
        return TransferResult(
            anomalies=result_anomalies,
            metadata_block={"row": row_block},
        )

    async def _apply_decision(
        self,
        *,
        conn: asyncpg.Connection,
        tx: NormalizedTransaction,
        decision: Claim | Anomaly | Skip,
        row_block: dict[str, Any],
    ) -> TransferResult:
        if isinstance(decision, Skip):
            return TransferResult(metadata_block={"row": row_block})

        if isinstance(decision, Anomaly):
            # The orchestrator INSERTs the new row after the strategy returns;
            # an FK exists on transfer_match_anomalies.transaction_id, so the
            # strategy MUST NOT record_anomaly here. Anomalies are returned
            # in the result for the orchestrator to persist post-INSERT.
            # decide() already stamped each record with tx.id.
            return TransferResult(
                anomalies=[decision.record],
                metadata_block={"row": row_block},
            )

        # Claim path: claim_pair updates a row that already exists (the partner),
        # and the auto-resolve delete operates on existing transaction IDs only,
        # so both are safe to run synchronously inside the strategy.
        pair_metadata_block = build_pair_block(
            decision.iban_evidence, decision.description_decisive
        )
        await self._transfer_repo.claim_pair(
            conn,
            existing_id=decision.partner_id,
            new_id=tx.id,
            pair_metadata_block=pair_metadata_block,
        )
        await self._anomaly_repo.delete_unpaired_anomalies_for_transactions(
            conn, [tx.id, decision.partner_id]
        )
        result_anomalies: list[AnomalyRecord] = []
        if decision.canary_anomaly is not None:
            # Canary anomaly is on the new (incoming) row; orchestrator
            # records it post-INSERT. decide() already stamped it with tx.id.
            result_anomalies.append(decision.canary_anomaly)
        return TransferResult(
            special_category="transfer",
            related_transaction_id=decision.partner_id,
            anomalies=result_anomalies,
            metadata_block={"row": row_block, "pair": pair_metadata_block},
        )

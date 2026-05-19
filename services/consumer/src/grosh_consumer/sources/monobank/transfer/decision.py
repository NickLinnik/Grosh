"""Count-and-decide branch + bucket-locked principle.

Pure function `decide(...)` returning a `Claim` / `Anomaly` / `Skip`
discriminated union. No DB, no IO. Composes the description-pair
validation from `descriptions.py` and the anomaly builders from
`anomalies.py`.

See `references/adr-transfer-detection-v2.md` §6 (count-and-decide) and
"Principle: bucket-locked evidence".
"""

from dataclasses import dataclass
from typing import Any
from uuid import UUID

from grosh_shared.models import TransactionDirection

from grosh_consumer.services.transfer_detection import AnomalyRecord
from grosh_consumer.sources.monobank.descriptions import (
    parse_description,
    validate_pair_descriptions,
)
from grosh_consumer.sources.monobank.transfer import anomalies
from grosh_consumer.sources.monobank.transfer.iban_classifier import PairEvidence

# Bucket priority — `bilateral > unilateral > none`. The bucket-locked
# principle: commit to the highest non-empty bucket; never fall through.
_BUCKET_PRIORITY: tuple[PairEvidence, ...] = ("bilateral", "unilateral", "none")


@dataclass(frozen=True)
class Claim:
    """Successful pair claim. The partner row will be UPDATEd by the repo."""

    partner_id: UUID
    iban_evidence: PairEvidence
    description_decisive: bool
    canary_anomaly: AnomalyRecord | None = None


@dataclass(frozen=True)
class Anomaly:
    """Pair couldn't be claimed; record the anomaly and persist as plain."""

    record: AnomalyRecord


@dataclass(frozen=True)
class Skip:
    """No claim, no anomaly — persist as plain. Sentinel singleton-shaped."""


Decision = Claim | Anomaly | Skip

# A candidate paired with its consistency-filter-survived evidence level.
ScoredCandidate = tuple[Any, PairEvidence]  # (CandidateRow, evidence)


def decide(
    *,
    incoming_id: UUID,
    candidates: list[ScoredCandidate],
    incoming_description: str | None,
    incoming_account_id: UUID,
    incoming_acc_props: Any,
    account_props_by_id: dict[UUID, Any],
) -> Decision:
    """Decide what to do with the incoming MCC 4829 row given its candidates.

    Returns one of:
    - `Claim(partner_id, iban_evidence, description_decisive, canary_anomaly?)`
      — pair was picked; canary anomaly is attached when the claim
      proceeded despite a description mismatch (count==1 path) or the
      hard-filtered survivor's other side disagrees (count>1 path).
    - `Anomaly(record)` — pair couldn't be picked; record the anomaly.
    - `Skip()` — nothing to do; persist the row as plain.

    `incoming_id` is the UUID of the new (yet-to-be-inserted) row; it's
    stamped into every AnomalyRecord this function builds so callers
    can persist them directly without a post-hoc rewrite.
    """
    if len(candidates) == 0:
        return _decide_count_zero(
            transaction_id=incoming_id,
            incoming_description=incoming_description,
        )

    if len(candidates) == 1:
        cand, evidence = candidates[0]
        return _decide_single(
            transaction_id=incoming_id,
            partner=cand,
            evidence=evidence,
            incoming_description=incoming_description,
            incoming_acc_props=incoming_acc_props,
            account_props_by_id=account_props_by_id,
            description_decisive=False,
        )

    return _decide_many(
        transaction_id=incoming_id,
        candidates=candidates,
        incoming_description=incoming_description,
        incoming_acc_props=incoming_acc_props,
        account_props_by_id=account_props_by_id,
    )


def _decide_count_zero(
    *,
    transaction_id: UUID,
    incoming_description: str | None,
) -> Decision:
    if not incoming_description:
        return Skip()
    if incoming_description.startswith("З "):
        return Anomaly(
            anomalies.unpaired_from_description(
                transaction_id=transaction_id,
                description=incoming_description,
            )
        )
    if incoming_description.startswith("На "):
        return Anomaly(
            anomalies.unpaired_to_description(
                transaction_id=transaction_id,
                description=incoming_description,
            )
        )
    # The explicit allowed phrase ("Переказ на картку") falls through to Skip:
    # per ADR §6, count==0 emits unpaired only on the prefix family.
    return Skip()


def _decide_single(
    *,
    transaction_id: UUID,
    partner: Any,
    evidence: PairEvidence,
    incoming_description: str | None,
    incoming_acc_props: Any,
    account_props_by_id: dict[UUID, Any],
    description_decisive: bool,
) -> Decision:
    """count==1 path: claim with optional canary on description mismatch."""
    # Subscript (not .get) is safe here despite get_many_by_ids' partial-dict
    # contract: the partner row was returned by find_universal_candidates which
    # holds FOR UPDATE on it, and accounts→transactions FK is ON DELETE CASCADE,
    # so any account deletion would either remove the partner before the fetch
    # or block until we release the lock. Within this strategy run the partner's
    # account is guaranteed to be in the dict.
    partner_acc_props = account_props_by_id[partner.account_id]
    canary = _build_canary_if_inconsistent(
        transaction_id=transaction_id,
        partner=partner,
        partner_acc_props=partner_acc_props,
        incoming_description=incoming_description,
        incoming_acc_props=incoming_acc_props,
    )
    return Claim(
        partner_id=partner.id,
        iban_evidence=evidence,
        description_decisive=description_decisive,
        canary_anomaly=canary,
    )


def _decide_many(
    *,
    transaction_id: UUID,
    candidates: list[ScoredCandidate],
    incoming_description: str | None,
    incoming_acc_props: Any,
    account_props_by_id: dict[UUID, Any],
) -> Decision:
    """count>1 path: bucket-locked + description hard filter.

    The hard filter is applied to the chosen bucket regardless of its
    size. A single-member bucket whose description fails IS an anomaly,
    not a canary claim — the canary path is reserved for the original
    count==1 case (single survivor of the universal fetch +
    consistency filter). Falling through to canary semantics from a
    multi-candidate origin would silently accept a strong-IBAN +
    bad-description claim while other plausible candidates exist in
    lower buckets — bucket-locked means we stop, not weaken.
    """
    bucket_evidence, bucket = _select_bucket(candidates)

    # Subscript on account_props_by_id is safe for the same reason as in
    # _decide_single — the candidates were locked by find_universal_candidates
    # and the CASCADE FK guarantees their accounts are in the dict.
    survivors = [
        cand
        for cand in bucket
        if _descriptions_consistent(
            partner=cand,
            partner_acc_props=account_props_by_id[cand.account_id],
            incoming_description=incoming_description,
            incoming_acc_props=incoming_acc_props,
        )
    ]
    bucket_ids = [cand.id for cand in bucket]

    if len(survivors) == 1:
        partner = survivors[0]
        # A single bucket member that survives description gets the canary
        # check on its own description vs partner accounts (no second-side
        # filter — the canary is a freestanding consistency check).
        # description_decisive is True iff bucket size > survivor count
        # (i.e. the description filter actually narrowed something).
        description_decisive = len(bucket) > len(survivors)
        return _decide_single(
            transaction_id=transaction_id,
            partner=partner,
            evidence=bucket_evidence,
            incoming_description=incoming_description,
            incoming_acc_props=incoming_acc_props,
            account_props_by_id=account_props_by_id,
            description_decisive=description_decisive,
        )

    if len(survivors) == 0 and bucket_evidence == "none":
        return Anomaly(
            anomalies.description_account_mismatch(
                transaction_id=transaction_id,
                rejected_candidate_ids=bucket_ids,
                incoming_description=incoming_description,
                bucket_evidence=bucket_evidence,
            )
        )

    return Anomaly(
        anomalies.ambiguous_pair_match(
            transaction_id=transaction_id,
            bucket_candidate_ids=bucket_ids,
            bucket_evidence=bucket_evidence,
            surviving_count=len(survivors),
        )
    )


def _select_bucket(
    candidates: list[ScoredCandidate],
) -> tuple[PairEvidence, list[Any]]:
    """Return the highest non-empty evidence bucket as (level, members).

    Implements the bucket-locked principle: scan in `_BUCKET_PRIORITY`
    order and return the first non-empty bucket; never fall through.
    """
    for level in _BUCKET_PRIORITY:
        bucket = [cand for cand, ev in candidates if ev == level]
        if bucket:
            return level, bucket
    # Unreachable when called with len(candidates) > 0 and evidence values
    # restricted to the three enum members. Defensive return for type safety.
    return "none", []


def _build_canary_if_inconsistent(
    *,
    transaction_id: UUID,
    partner: Any,
    partner_acc_props: Any,
    incoming_description: str | None,
    incoming_acc_props: Any,
) -> AnomalyRecord | None:
    if _descriptions_consistent(
        partner=partner,
        partner_acc_props=partner_acc_props,
        incoming_description=incoming_description,
        incoming_acc_props=incoming_acc_props,
    ):
        return None
    income_desc, expense_desc, income_props, expense_props = _split_by_direction(
        partner=partner,
        partner_acc_props=partner_acc_props,
        incoming_description=incoming_description,
        incoming_acc_props=incoming_acc_props,
    )
    return anomalies.description_consistency_mismatch(
        transaction_id=transaction_id,
        partner_id=partner.id,
        income_desc=income_desc,
        expense_desc=expense_desc,
        income_acc_props=income_props,
        expense_acc_props=expense_props,
    )


def _descriptions_consistent(
    *,
    partner: Any,
    partner_acc_props: Any,
    incoming_description: str | None,
    incoming_acc_props: Any,
) -> bool:
    income_desc, expense_desc, income_props, expense_props = _split_by_direction(
        partner=partner,
        partner_acc_props=partner_acc_props,
        incoming_description=incoming_description,
        incoming_acc_props=incoming_acc_props,
    )
    if (
        parse_description(income_desc) is None
        and parse_description(expense_desc) is None
    ):
        # Both sides outside the known set — vacuously consistent (per §2.6 case 57).
        return True
    return validate_pair_descriptions(
        income_desc=income_desc,
        expense_desc=expense_desc,
        income_account_type=income_props.type,
        income_account_currency=income_props.currency_code,
        expense_account_type=expense_props.type,
        expense_account_currency=expense_props.currency_code,
    )


def _split_by_direction(
    *,
    partner: Any,
    partner_acc_props: Any,
    incoming_description: str | None,
    incoming_acc_props: Any,
) -> tuple[str | None, str | None, Any, Any]:
    """Return (income_desc, expense_desc, income_props, expense_props).

    The validate function expects the income side and expense side as
    separate inputs; we route by `partner.direction`.
    """
    if partner.direction == TransactionDirection.income:
        return (
            partner.description,
            incoming_description,
            partner_acc_props,
            incoming_acc_props,
        )
    return (
        incoming_description,
        partner.description,
        incoming_acc_props,
        partner_acc_props,
    )

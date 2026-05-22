"""Anomaly builders for the Monobank transfer detection strategy.

One builder per `transfer_anomaly_reason` enum value the detector emits.
Each builder formats `reason_detail` from its inputs and returns a typed
`AnomalyRecord`. The detector never constructs an `AnomalyRecord` inline.

See `references/adr-transfer-detection-v2.md` §3 for the anomaly enum.
"""

from typing import Any
from uuid import UUID

from grosh_enrichment.services.transfer_detection import AnomalyRecord


def unpaired_from_description(
    *,
    transaction_id: UUID,
    description: str | None,
) -> AnomalyRecord:
    """Income side has a `З ` prefix but no partner found (yet).

    Auto-cleaned on later partner arrival.
    """
    return AnomalyRecord(
        transaction_id=transaction_id,
        candidate_ids=[],
        reason_code="unpaired_from_description",
        reason_detail=(
            f"Income leg with transfer-prefixed description {description!r} "
            f"found no partner candidate within the ±2s window."
        ),
    )


def unpaired_to_description(
    *,
    transaction_id: UUID,
    description: str | None,
) -> AnomalyRecord:
    """Expense side has a `На ` prefix but no partner found (yet).

    Auto-cleaned on later partner arrival.
    """
    return AnomalyRecord(
        transaction_id=transaction_id,
        candidate_ids=[],
        reason_code="unpaired_to_description",
        reason_detail=(
            f"Expense leg with transfer-prefixed description {description!r} "
            f"found no partner candidate within the ±2s window."
        ),
    )


def description_consistency_mismatch(
    *,
    transaction_id: UUID,
    partner_id: UUID,
    income_desc: str | None,
    expense_desc: str | None,
    income_acc_props: Any,
    expense_acc_props: Any,
) -> AnomalyRecord:
    """Pair was claimed but descriptions disagree with partner account properties.

    Canary signal — pair is still claimed (IBAN evidence was sufficient);
    this anomaly persists for manual investigation.
    """
    return AnomalyRecord(
        transaction_id=transaction_id,
        candidate_ids=[partner_id],
        reason_code="description_consistency_mismatch",
        reason_detail=(
            f"Pair claimed but descriptions disagree: "
            f"income={income_desc!r} on {_acc_summary(income_acc_props)}, "
            f"expense={expense_desc!r} on {_acc_summary(expense_acc_props)}."
        ),
    )


def description_account_mismatch(
    *,
    transaction_id: UUID,
    rejected_candidate_ids: list[UUID],
    incoming_description: str | None,
    bucket_evidence: str,
) -> AnomalyRecord:
    """`none`-evidence bucket; description hard filter rejected all candidates.

    Special case for the count>1 path: only fires when bucket evidence is
    `none` AND every candidate failed description validation.
    """
    return AnomalyRecord(
        transaction_id=transaction_id,
        candidate_ids=list(rejected_candidate_ids),
        reason_code="description_account_mismatch",
        reason_detail=(
            f"Description {incoming_description!r} rejected all "
            f"{len(rejected_candidate_ids)} candidate(s) in the "
            f"{bucket_evidence!r} evidence bucket."
        ),
    )


def ambiguous_pair_match(
    *,
    transaction_id: UUID,
    bucket_candidate_ids: list[UUID],
    bucket_evidence: str,
    surviving_count: int,
) -> AnomalyRecord:
    """After bucket-locking and description filtering, pair couldn't be picked.

    Two sub-cases distinguished in the detail string:
    - 0 survivors with evidence ≥`unilateral`: description eliminated all.
    - >1 survivors: description left multiple candidates ambiguous.
    """
    if surviving_count == 0:
        outcome = "description filter eliminated all"
    else:
        outcome = f"{surviving_count} candidate(s) survived description filter"
    return AnomalyRecord(
        transaction_id=transaction_id,
        candidate_ids=list(bucket_candidate_ids),
        reason_code="ambiguous_pair_match",
        reason_detail=(
            f"Bucket evidence={bucket_evidence!r} with "
            f"{len(bucket_candidate_ids)} candidate(s); {outcome}."
        ),
    )


def _acc_summary(acc: Any) -> str:
    """Render an AccountProps-like object for the human-readable detail string."""
    type_ = getattr(acc, "type", "?")
    currency = getattr(acc, "currency_code", "?")
    return f"{type_}/{currency}"

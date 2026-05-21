"""Pair-level IBAN classification for the Monobank transfer detection strategy.

Two pure functions consumed by the detector after the universal candidate
fetch:

  1. `is_consistent(...)` — hard filter dropping candidates whose
     `honest`/`unlinked` cp_iban contradicts the incoming row's account.
  2. `classify_pair_evidence(...)` — assigns the `iban_evidence` level
     (`bilateral` / `unilateral` / `none`) on surviving candidates.

`null` and `transitive` impose no constraint on either side. `unlinked` on
the candidate side is always dropped (its `cp_iban` definitionally doesn't
resolve to any own account, so it can't resolve to incoming's account).

The `iban_to_account` argument is a pre-resolved dict the caller built;
both functions read it with `.get(iban)` for IBAN-to-account-UUID lookup.

See `references/adr-transfer-detection-v2.md` §4 for the rules.
"""

from typing import Literal
from uuid import UUID

from grosh_pipeline.sources.monobank.transfer.flags import CpIbanStatus, RowFlags

PairEvidence = Literal["bilateral", "unilateral", "none"]


def is_consistent(
    *,
    incoming_flags: RowFlags,
    candidate_flags: RowFlags,
    incoming_cp_iban: str | None,
    candidate_cp_iban: str | None,
    incoming_account_id: UUID,
    candidate_account_id: UUID,
    iban_to_account: dict[str, UUID],
) -> bool:
    """Return True iff the candidate survives the hard IBAN consistency filter.

    Drop conditions (any one is sufficient to drop):

    - `incoming.cp_iban_status == "honest"` AND incoming's `cp_iban` does
      NOT resolve to candidate's `account_id`.
    - `candidate.cp_iban_status IN ("honest", "unlinked")` AND candidate's
      `cp_iban` does NOT resolve to incoming's `account_id`.

    `null` / `transitive` impose no constraint on either side.
    `unlinked` on the candidate side is structurally a hard drop: by
    definition `unlinked` means the cp_iban doesn't resolve to ANY own
    account, so it definitionally doesn't resolve to incoming's account.
    """
    if incoming_flags.cp_iban_status == CpIbanStatus.HONEST:
        if incoming_cp_iban is None:
            return False
        if iban_to_account.get(incoming_cp_iban) != candidate_account_id:
            return False

    if candidate_flags.cp_iban_status in (CpIbanStatus.HONEST, CpIbanStatus.UNLINKED):
        if candidate_cp_iban is None:
            return False
        if iban_to_account.get(candidate_cp_iban) != incoming_account_id:
            return False

    return True


def classify_pair_evidence(
    *,
    incoming_flags: RowFlags,
    candidate_flags: RowFlags,
    incoming_cp_iban: str | None,
    candidate_cp_iban: str | None,
    incoming_account_id: UUID,
    candidate_account_id: UUID,
    iban_to_account: dict[str, UUID],
) -> PairEvidence:
    """Assign the per-pair IBAN evidence level on a consistency-filter survivor.

    - `"bilateral"` — both sides `honest` AND each side's `cp_iban`
      resolves to the other side's account.
    - `"unilateral"` — exactly one side `honest` pointing at the other side
      (the other side is `null` or `transitive`).
    - `"none"` — neither side contributes positive IBAN evidence (both
      `null`/`transitive`).

    Only called on survivors of `is_consistent`, so cases with `unlinked`
    on either side are out of scope here.
    """
    incoming_claims = _side_claims(
        flags=incoming_flags,
        cp_iban=incoming_cp_iban,
        other_account_id=candidate_account_id,
        iban_to_account=iban_to_account,
    )
    candidate_claims = _side_claims(
        flags=candidate_flags,
        cp_iban=candidate_cp_iban,
        other_account_id=incoming_account_id,
        iban_to_account=iban_to_account,
    )
    if incoming_claims and candidate_claims:
        return "bilateral"
    if incoming_claims or candidate_claims:
        return "unilateral"
    return "none"


def _side_claims(
    *,
    flags: RowFlags,
    cp_iban: str | None,
    other_account_id: UUID,
    iban_to_account: dict[str, UUID],
) -> bool:
    """True iff this side contributes a positive `honest` claim pointing at `other`."""
    if flags.cp_iban_status != CpIbanStatus.HONEST:
        return False
    if cp_iban is None:
        return False
    return iban_to_account.get(cp_iban) == other_account_id

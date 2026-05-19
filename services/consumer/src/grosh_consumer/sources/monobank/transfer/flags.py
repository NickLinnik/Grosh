"""Per-row flag computation for the Monobank transfer detection strategy.

`compute_row_flags(row, iban_to_account) -> RowFlags` returns three values
bound for the rest of the strategy pass:
  - description_matched: bool — description is in `_INCOME_MAP`/`_EXPENSE_MAP`.
  - multi_hop_description: bool — description is in the `для переказу на`
    family (subset of `description_matched`).
  - cp_iban_status: 4-value enum — `null` / `transitive` / `unlinked` / `honest`.

The directional transitive rule is encoded here as a single explicit branch:
`direction == "expense" AND multi_hop_description → cp_iban_status = "transitive"`.
The Monobank quirk: multi-hop expense legs report the chain-end IBAN
(final destination card), not the immediate partner — so the IBAN must
be suppressed during evidence classification.

`row` is duck-typed via the `_RowFlagInputs` Protocol: any object exposing
`description`, `direction`, and `counterparty_iban` works. Both
`NormalizedTransaction` (incoming) and `CandidateRow` (universal-fetch
result) satisfy the Protocol, so the same function serves both call sites.

This module is pure: no DB access, no IO. The IBAN-to-account resolution
is passed as a pre-resolved dict the caller built; the function reads it
with `.get(iban)`.
"""

from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol
from uuid import UUID

from grosh_shared.models import TransactionDirection

from grosh_consumer.sources.monobank.descriptions import (
    is_multi_hop_description,
    parse_description,
)


class CpIbanStatus(StrEnum):
    """Classification of an MCC 4829 row's counterparty IBAN.

    StrEnum (PEP 663) — members serialize as their string values when
    written to JSON, so the on-disk `metadata.layer.transfer.row.cp_iban_status`
    shape is exactly the string literal. Comparisons with bare strings
    (`flags.cp_iban_status == "honest"`) work transparently because
    StrEnum members are `str` instances.
    """

    NULL = "null"
    TRANSITIVE = "transitive"
    UNLINKED = "unlinked"
    HONEST = "honest"


@dataclass(frozen=True)
class RowFlags:
    """Per-tx diagnostic facts computed once at the top of the strategy pass.

    Carried into evidence classification and the metadata block; never
    short-circuits the flow except for `cp_iban_status == "unlinked"`
    (handled by `detector.py`, not here).
    """

    description_matched: bool
    multi_hop_description: bool
    cp_iban_status: CpIbanStatus


class _RowFlagInputs(Protocol):
    """Minimum surface compute_row_flags reads from its input row.

    Satisfied by both NormalizedTransaction (incoming events) and
    CandidateRow (universal-fetch results) without either knowing about
    this Protocol — structural typing.
    """

    description: str | None
    direction: TransactionDirection
    counterparty_iban: str | None


def compute_row_flags(
    row: _RowFlagInputs,
    iban_to_account: dict[str, UUID],
) -> RowFlags:
    """Compute the row's transfer-detection flags from its description + IBAN.

    `iban_to_account` is a pre-resolved map of IBAN → owning-account UUID
    for the user's linked accounts. An IBAN absent from the map is treated
    as unlinked.
    """
    description_matched = parse_description(row.description) is not None
    multi_hop = is_multi_hop_description(row.description)
    cp_iban_status = _classify_cp_iban(
        direction=row.direction,
        cp_iban=row.counterparty_iban,
        multi_hop_description=multi_hop,
        iban_to_account=iban_to_account,
    )
    return RowFlags(
        description_matched=description_matched,
        multi_hop_description=multi_hop,
        cp_iban_status=cp_iban_status,
    )


def _classify_cp_iban(
    *,
    direction: TransactionDirection,
    cp_iban: str | None,
    multi_hop_description: bool,
    iban_to_account: dict[str, UUID],
) -> CpIbanStatus:
    if cp_iban is None:
        return CpIbanStatus.NULL
    # Directional transitive rule — fires before the lookup matters.
    # Multi-hop expense legs report the chain-end IBAN, not the immediate
    # partner; suppress to prevent contradicting honest-side evidence.
    if direction == TransactionDirection.expense and multi_hop_description:
        return CpIbanStatus.TRANSITIVE
    if cp_iban not in iban_to_account:
        # UNLINKED: the IBAN points at an account that is NOT one of the
        # user's own linked accounts. The partner exists somewhere on the
        # banking network but is outside this platform's view, so the
        # detector cannot find a pairing row — it short-circuits in
        # detector.py::_handle_unlinked.
        return CpIbanStatus.UNLINKED
    return CpIbanStatus.HONEST

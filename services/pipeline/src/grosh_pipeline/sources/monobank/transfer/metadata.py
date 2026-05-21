"""JSON-shape builders for `metadata.layer.transfer.{row, pair}`.

Centralized here so changes to the layer's JSON shape touch one file.
The detector and orchestrator never construct these dicts inline; they
call the builders.

See `references/adr-transfer-detection-v2.md` §2 for the schema.
"""

from grosh_pipeline.sources.monobank.transfer.flags import RowFlags
from grosh_pipeline.sources.monobank.transfer.iban_classifier import PairEvidence


def build_row_block(flags: RowFlags) -> dict[str, object]:
    """Build the per-tx `metadata.layer.transfer.row` sub-block.

    Always written on every MCC 4829 row; carries diagnostic facts about
    the row itself (independent of pairing outcome).
    """
    return {
        "description_matched": flags.description_matched,
        "multi_hop_description": flags.multi_hop_description,
        "cp_iban_status": flags.cp_iban_status,
    }


def build_pair_block(
    iban_evidence: PairEvidence,
    description_decisive: bool,
) -> dict[str, object]:
    """Build the per-pair `metadata.layer.transfer.pair` sub-block.

    Written on both legs of a successful claim; identical on both legs.
    `iban_evidence` and `description_decisive` are independent — a pair
    can have `bilateral` evidence AND `description_decisive=True`
    simultaneously (multiple bilateral candidates, description picks one).
    """
    return {
        "iban_evidence": iban_evidence,
        "description_decisive": description_decisive,
    }

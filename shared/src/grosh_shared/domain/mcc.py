"""ISO 18245 Merchant Category Codes.

Thin wrapper over the `iso18245` package. Services import from here
rather than depending on the package directly.
"""

from enum import Enum

from iso18245 import get_mcc


class MccCode(Enum):
    """Well-known MCC codes used in business logic.

    Values are full MCC objects from iso18245 — carry description,
    range, and network-specific metadata.
    """

    WIRE_TRANSFER = get_mcc("4829")

    @property
    def code(self) -> str:
        return self.value.mcc

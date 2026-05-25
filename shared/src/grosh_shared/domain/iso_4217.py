"""ISO 4217 currency code utilities using pycountry."""

import pycountry

from grosh_shared.domain.models import Currency

# pycountry has gaps — these are valid ISO 4217 codes it doesn't know about.
_OVERRIDES: dict[str, str] = {
    "975": "BGN",
    "946": "RON",
}


def numeric_to_alpha(code: int) -> str:
    """Convert ISO 4217 numeric currency code to alpha-3 code."""
    padded = str(code).zfill(3)
    override = _OVERRIDES.get(padded)
    if override is not None:
        return override
    currency = pycountry.currencies.get(numeric=padded)
    if currency is None:
        raise ValueError(f"Unknown ISO 4217 numeric code: {code}")
    return currency.alpha_3


def all_alpha_codes() -> list[str]:
    """Return tracked currency codes (used for rate backfill scope)."""
    return [c.value for c in Currency]

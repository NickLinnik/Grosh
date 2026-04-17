from grosh_shared.models import Currency

_NUMERIC_TO_ALPHA: dict[int, Currency] = {
    980: Currency.UAH,
    840: Currency.USD,
    978: Currency.EUR,
    826: Currency.GBP,
    985: Currency.PLN,
    203: Currency.CZK,
    756: Currency.CHF,
    392: Currency.JPY,
    156: Currency.CNY,
    949: Currency.TRY,
}


def numeric_to_alpha(code: int) -> Currency:
    """Convert ISO 4217 numeric currency code to alpha-3 code."""
    alpha = _NUMERIC_TO_ALPHA.get(code)
    if alpha is None:
        raise ValueError(f"Unknown ISO 4217 numeric code: {code}")
    return alpha


def all_alpha_codes() -> list[Currency]:
    """Return all known ISO 4217 alpha-3 currency codes."""
    return list(_NUMERIC_TO_ALPHA.values())

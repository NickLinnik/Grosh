_NUMERIC_TO_ALPHA: dict[int, str] = {
    980: "UAH",
    840: "USD",
    978: "EUR",
    826: "GBP",
    985: "PLN",
    203: "CZK",
    756: "CHF",
    392: "JPY",
    156: "CNY",
    949: "TRY",
}


def numeric_to_alpha(code: int) -> str:
    """Convert ISO 4217 numeric currency code to alpha-3 code."""
    alpha = _NUMERIC_TO_ALPHA.get(code)
    if alpha is None:
        raise ValueError(f"Unknown ISO 4217 numeric code: {code}")
    return alpha

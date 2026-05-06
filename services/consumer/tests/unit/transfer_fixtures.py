"""Shared constants and account fixtures for transfer detection unit tests."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

T = datetime(2025, 6, 1, 12, 0, 0, tzinfo=UTC)
T_PLUS_1S = T + timedelta(seconds=1)
T_MINUS_1S = T - timedelta(seconds=1)
T_PLUS_2S = T + timedelta(seconds=2)
T_MINUS_2S = T - timedelta(seconds=2)
T_PLUS_3S = T + timedelta(seconds=3)
T_MINUS_3S = T - timedelta(seconds=3)
T_MINUS_5S = T - timedelta(seconds=5)

WINDOW_SECONDS = 2
MCC_TRANSFER = "4829"
MCC_GROCERY = "5411"

USER_ID = uuid4()

# Account IDs — stable across tests in this module
_UAH_FOP_ID = uuid4()
_USD_FOP_ID = uuid4()
_EUR_FOP_ID = uuid4()
_UAH_BLACK_ID = uuid4()
_UAH_WHITE_ID = uuid4()
_EUR_CARD_ID = uuid4()

# IBAN strings — arbitrary but realistic-looking
UAH_FOP_IBAN = "UA111000000000000000000000001"
USD_FOP_IBAN = "UA222000000000000000000000002"
EUR_FOP_IBAN = "UA333000000000000000000000003"
UAH_BLACK_IBAN = "UA444000000000000000000000004"
UAH_WHITE_IBAN = "UA555000000000000000000000005"
EUR_CARD_IBAN = "UA666000000000000000000000006"


def seed_accounts(acc_repo) -> dict:
    """Seed a standard set of accounts and return a dict of name → account dict."""
    accounts = {
        "uah_fop": acc_repo.add_account(
            id=_UAH_FOP_ID,
            user_id=USER_ID,
            type="fop",
            currency_code="UAH",
            iban=UAH_FOP_IBAN,
        ),
        "usd_fop": acc_repo.add_account(
            id=_USD_FOP_ID,
            user_id=USER_ID,
            type="fop",
            currency_code="USD",
            iban=USD_FOP_IBAN,
        ),
        "eur_fop": acc_repo.add_account(
            id=_EUR_FOP_ID,
            user_id=USER_ID,
            type="fop",
            currency_code="EUR",
            iban=EUR_FOP_IBAN,
        ),
        "uah_black": acc_repo.add_account(
            id=_UAH_BLACK_ID,
            user_id=USER_ID,
            type="black",
            currency_code="UAH",
            iban=UAH_BLACK_IBAN,
        ),
        "uah_white": acc_repo.add_account(
            id=_UAH_WHITE_ID,
            user_id=USER_ID,
            type="white",
            currency_code="UAH",
            iban=UAH_WHITE_IBAN,
        ),
        "eur_card": acc_repo.add_account(
            id=_EUR_CARD_ID,
            user_id=USER_ID,
            type="black",
            currency_code="EUR",
            iban=EUR_CARD_IBAN,
        ),
    }
    return accounts

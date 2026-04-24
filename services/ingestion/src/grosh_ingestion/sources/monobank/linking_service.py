import logging
import secrets
from uuid import UUID

import asyncpg
from grosh_shared.iso_4217 import numeric_to_alpha
from grosh_shared.models import AccountType, BankSource, TransactionSource

from grosh_ingestion.repositories.account_repo import AccountRepo
from grosh_ingestion.repositories.integration_repo import IntegrationRepo
from grosh_ingestion.sources.monobank.client import MonobankClient

logger = logging.getLogger(__name__)


class MonobankLinkingService:
    def __init__(
        self, integration_repo: IntegrationRepo, account_repo: AccountRepo
    ) -> None:
        self._integration_repo = integration_repo
        self._account_repo = account_repo

    async def link(
        self,
        conn: asyncpg.Connection,
        user_id: UUID,
        token: str,
        webhook_base_url: str,
        encryption_key: str,
        key_version: int,
    ) -> dict:
        """Link a Monobank integration for a user.

        1. Fetch client info (validates the token is live).
        2. Generate a random webhook secret and construct the webhook URL.
        3. Encrypt the token and persist the integration in DB.
        4. Create account rows for each Monobank account.
        5. Register the webhook with Monobank (best-effort).
        """
        async with MonobankClient(token) as client:
            client_info = await client.get_client_info()

            webhook_secret = secrets.token_hex(32)
            webhook_url = f"{webhook_base_url}/{webhook_secret}"

            async with conn.transaction():
                encrypted_token = await conn.fetchval(
                    "SELECT pgp_sym_encrypt($1, $2)", token, encryption_key
                )

                integration_id = await self._integration_repo.create_integration(
                    conn=conn,
                    user_id=user_id,
                    bank=BankSource.monobank,
                    config={
                        "encrypted_token": encrypted_token.hex(),
                        "key_version": key_version,
                        "webhook_secret": webhook_secret,
                        "webhook_url": webhook_url,
                    },
                )

                created_accounts: list[dict] = []
                for mono_account in client_info.accounts:
                    currency_code = numeric_to_alpha(mono_account.currency_code)
                    account_type = AccountType(mono_account.type)
                    masked_pan = (
                        mono_account.masked_pan[0] if mono_account.masked_pan else None
                    )

                    account_id = await self._account_repo.create_account(
                        conn=conn,
                        user_id=user_id,
                        integration_id=integration_id,
                        source=TransactionSource.monobank,
                        account_type=account_type,
                        currency_code=currency_code,
                        masked_pan=masked_pan,
                        iban=mono_account.iban,
                        external_id=mono_account.id,
                        cashback_type=mono_account.cashback_type,
                    )
                    created_accounts.append(
                        {
                            "id": account_id,
                            "type": str(account_type),
                            "currency_code": str(currency_code),
                            "iban": mono_account.iban,
                        }
                    )

            try:
                await client.set_webhook(webhook_url)
            except Exception:
                logger.warning(
                    "Failed to register Monobank webhook for integration %s — "
                    "integration is still active but webhook must be re-registered",
                    integration_id,
                    exc_info=True,
                )

        return {"integration_id": integration_id, "accounts": created_accounts}


_integration_repo = IntegrationRepo()
_account_repo = AccountRepo()
_linking_service = MonobankLinkingService(_integration_repo, _account_repo)


def get_monobank_linking_service() -> MonobankLinkingService:
    return _linking_service

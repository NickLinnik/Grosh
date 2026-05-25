"""Monobank account linking service — idempotent link, unlink, list."""

import json
import logging
import secrets
from typing import Any
from uuid import UUID

import asyncpg
import httpx
from grosh_shared.domain.iso_4217 import numeric_to_alpha
from grosh_shared.domain.models import BankSource, TransactionSource
from grosh_shared.http.errors import ErrorCode, raise_problem
from pydantic import ValidationError

from grosh_ingestion.repositories.account_repo import AccountRepo
from grosh_ingestion.repositories.integration_repo import IntegrationRepo
from grosh_ingestion.sources.monobank.client import MonobankAPIError, MonobankClient
from grosh_ingestion.sources.monobank.repo import BankIntegrationRow, MonobankRepo
from grosh_ingestion.sources.monobank.schemas import (
    MonobankAccountResponse,
    MonobankIntegrationResponse,
    MonobankLinkResponse,
)

logger = logging.getLogger(__name__)


class MonobankLinkingService:
    def __init__(
        self,
        integration_repo: IntegrationRepo,
        account_repo: AccountRepo,
        monobank_repo: MonobankRepo,
    ) -> None:
        self._integration_repo = integration_repo
        self._account_repo = account_repo
        self._monobank_repo = monobank_repo

    async def link(
        self,
        conn: asyncpg.Connection,
        user_id: UUID,
        token: str,
        webhook_base_url: str,
        encryption_key: str,
        key_version: int,
    ) -> MonobankLinkResponse:
        """Link a Monobank integration idempotently on monobank_client_id.

        Step 1 — Validate the token by calling Monobank client-info.
        Step 2 — Look up any existing active integration for this user.
        Step 3a — Same client_id: rotate token, return 200 (is_new=False).
        Step 3b — Different client_id: 409 INTEGRATION_ALREADY_LINKED.
        Step 3c — No existing: insert fresh row, rebind orphan accounts, create new.
        Step 4 — Register webhook (best-effort; failure sets webhook_registered=False).
        Step 5 — Return MonobankLinkResponse.
        """
        # Step 1: validate token via Monobank API. The catch tuple covers
        # transport faults (httpx.HTTPError, TimeoutError), Monobank-side
        # error responses (MonobankAPIError), and malformed payloads
        # (json.JSONDecodeError for non-JSON bodies, pydantic.ValidationError
        # for JSON that doesn't match MonobankClientInfo). All collapse to a
        # clean 502 — letting ValidationError/JSONDecodeError propagate would
        # surface a 500 stack trace to the caller.
        async with MonobankClient(token) as client:
            try:
                client_info = await client.get_client_info()
            except (
                MonobankAPIError,
                httpx.HTTPError,
                TimeoutError,
                json.JSONDecodeError,
                ValidationError,
            ) as exc:
                if isinstance(exc, MonobankAPIError) and exc.status_code in (401, 403):
                    raise_problem(
                        422,
                        ErrorCode.MONOBANK_TOKEN_INVALID,
                        "Token rejected by Monobank.",
                    )
                raise_problem(
                    502,
                    ErrorCode.MONOBANK_API_UNAVAILABLE,
                    "Monobank API unavailable.",
                )

            monobank_client_id = client_info.client_id

            # Step 2: check for an existing integration
            existing = await self._monobank_repo.get_integration_by_client_id(
                conn, user_id, monobank_client_id
            )

            if existing is not None:
                # Step 3a: same client_id — rotate the token, no account changes
                return await self._rotate_existing_integration(
                    conn=conn,
                    existing=existing,
                    token=token,
                    encryption_key=encryption_key,
                    key_version=key_version,
                    client=client,
                    webhook_base_url=webhook_base_url,
                )

            # Step 3b: check for a different-client_id conflict
            conflicting = await self._monobank_repo.get_any_active_integration(
                conn, user_id
            )
            if conflicting is not None:
                raise_problem(
                    409,
                    ErrorCode.INTEGRATION_ALREADY_LINKED,
                    "User has an integration for a different Monobank account;"
                    " DELETE it before linking a new one.",
                )

            # Step 3c: create fresh (ON CONFLICT handles races)
            webhook_secret = secrets.token_hex(32)
            webhook_url = f"{webhook_base_url}/monobank/webhook/{webhook_secret}"

            async with conn.transaction():
                encrypted_token = await self._monobank_repo.encrypt_token(
                    conn, token, encryption_key
                )

                (
                    integration_id,
                    is_new_integration,
                ) = await self._monobank_repo.create_integration_idempotent(
                    conn=conn,
                    user_id=user_id,
                    config={
                        "encrypted_token": encrypted_token.hex(),
                        "key_version": key_version,
                        "webhook_secret": webhook_secret,
                        "webhook_url": webhook_url,
                        "monobank_client_id": monobank_client_id,
                    },
                )

            if not is_new_integration:
                # Concurrent request for the same client_id won the INSERT race.
                # Treat as Step 3a: rotate token, return is_new=False.
                existing = await self._monobank_repo.get_integration_by_client_id(
                    conn, user_id, monobank_client_id
                )
                if existing is not None:
                    return await self._rotate_existing_integration(
                        conn=conn,
                        existing=existing,
                        token=token,
                        encryption_key=encryption_key,
                        key_version=key_version,
                        client=client,
                        webhook_base_url=webhook_base_url,
                    )

            async with conn.transaction():
                # Find orphaned accounts from a prior hard-deleted integration
                # of this same Monobank user (matched by monobank_client_id).
                incoming_external_ids = [acc.id for acc in client_info.accounts]
                orphan_accounts = (
                    await self._monobank_repo.find_orphan_accounts_for_rebind(
                        conn, user_id, monobank_client_id, incoming_external_ids
                    )
                )
                orphan_by_external_id = {a.external_id: a for a in orphan_accounts}

                account_responses: list[MonobankAccountResponse] = []

                for mono_account in client_info.accounts:
                    currency_code = str(numeric_to_alpha(mono_account.currency_code))

                    if mono_account.id in orphan_by_external_id:
                        # Rebind the orphaned account row to the new integration
                        orphan = orphan_by_external_id[mono_account.id]
                        await self._monobank_repo.rebind_account(
                            conn, orphan.id, integration_id
                        )
                        account_responses.append(
                            MonobankAccountResponse(
                                account_id=orphan.id,
                                external_account_id=mono_account.id,
                                currency_code=currency_code,
                                was_rebound=True,
                            )
                        )
                    else:
                        masked_pan = (
                            mono_account.masked_pan[0]
                            if mono_account.masked_pan
                            else None
                        )
                        created = await self._account_repo.create_account(
                            conn=conn,
                            user_id=user_id,
                            integration_id=integration_id,
                            source=TransactionSource.monobank,
                            account_type=mono_account.type,
                            currency_code=currency_code,
                            masked_pan=masked_pan,
                            iban=mono_account.iban,
                            external_id=mono_account.id,
                            cashback_type=mono_account.cashback_type,
                        )
                        account_id = created.id
                        # Tag the new account with monobank_client_id for future rebind.
                        await self._monobank_repo.set_account_monobank_client_id(
                            conn, account_id, monobank_client_id
                        )
                        account_responses.append(
                            MonobankAccountResponse(
                                account_id=account_id,
                                external_account_id=mono_account.id,
                                currency_code=currency_code,
                                was_rebound=False,
                            )
                        )

            # Step 4: register webhook (best-effort)
            webhook_registered = False
            try:
                await client.set_webhook(webhook_url)
                webhook_registered = True
            except Exception:
                logger.warning(
                    "Failed to register Monobank webhook for integration %s —"
                    " integration is active but webhook must be re-registered",
                    integration_id,
                    exc_info=True,
                )

            return MonobankLinkResponse(
                integration_id=integration_id,
                is_new=True,
                webhook_registered=webhook_registered,
                accounts=account_responses,
            )

    async def unlink(
        self,
        conn: asyncpg.Connection,
        integration_id: UUID,
        user_id: UUID,
        encryption_key: str,
    ) -> bool:
        """Hard-delete a Monobank integration.

        Verifies ownership first. Attempts webhook de-registration (best-effort,
        10-second timeout). Accounts and transactions remain untouched.

        Returns True on success, False if the integration was not found or not
        owned by the caller (caller should respond with 404).
        """
        # Fetch decrypted token before deletion — we need it for Monobank call
        token = await self._monobank_repo.decrypt_token(
            conn, integration_id, encryption_key
        )
        if token is None:
            # Either not found or belongs to a different user (RLS enforces ownership
            # because set_rls_user_id was called before this service method).
            return False

        # Best-effort webhook de-registration — outside any DB transaction so we
        # don't hold a connection open during a potentially slow network call.
        try:
            async with MonobankClient(token) as client:
                client.set_timeout(read=10.0, connect=5.0)
                await client.set_webhook("")
        except Exception:
            logger.warning(
                "Failed to de-register Monobank webhook for integration %s —"
                " integration will be deleted anyway",
                integration_id,
                exc_info=True,
            )

        async with conn.transaction():
            deleted = await self._monobank_repo.delete_integration(conn, integration_id)

        return deleted

    async def list_integrations(
        self,
        conn: asyncpg.Connection,
        user_id: UUID,
    ) -> list[MonobankIntegrationResponse]:
        """List all active Monobank integrations for the user with their accounts."""
        integrations = await self._monobank_repo.list_integrations_for_user(
            conn, user_id
        )
        result: list[MonobankIntegrationResponse] = []
        for integration in integrations:
            accounts = await self._monobank_repo.list_accounts_for_integration(
                conn, integration.id
            )
            account_responses = [
                MonobankAccountResponse(
                    account_id=a.id,
                    external_account_id=a.external_id,
                    currency_code=a.currency_code,
                    was_rebound=False,
                )
                for a in accounts
            ]
            result.append(
                MonobankIntegrationResponse(
                    integration_id=integration.id,
                    monobank_client_id=integration.monobank_client_id,
                    accounts=account_responses,
                    created_at=integration.created_at,
                )
            )
        return result

    async def reregister_webhooks(
        self,
        conn: asyncpg.Connection,
        webhook_base_url: str,
        encryption_key: str,
    ) -> list[dict[str, Any]]:
        """Re-register webhooks for all active Monobank integrations.

        Decrypts each stored token, generates a new webhook secret,
        updates the integration config, and re-registers with Monobank.
        Used after changing the app's public domain.
        """
        integrations = await self._integration_repo.list_active(conn)
        results: list[dict[str, Any]] = []

        for row in integrations:
            if row["bank"] != BankSource.monobank:
                continue

            integration_id = row["id"]
            raw_config = row["config"]
            config = (
                json.loads(raw_config)
                if isinstance(raw_config, str)
                else dict(raw_config)
            )

            token_row = await self._monobank_repo.decrypt_token_value(
                conn, config["encrypted_token"], encryption_key
            )
            if token_row is None:
                logger.error(
                    "Failed to decrypt token for integration %s — skipping",
                    integration_id,
                )
                results.append(
                    {
                        "integration_id": integration_id,
                        "status": "error",
                        "detail": "token decryption failed",
                    }
                )
                continue

            webhook_secret = secrets.token_hex(32)
            webhook_url = f"{webhook_base_url}/monobank/webhook/{webhook_secret}"

            new_config = {
                **config,
                "webhook_secret": webhook_secret,
                "webhook_url": webhook_url,
            }
            await self._integration_repo.update_config(conn, integration_id, new_config)

            try:
                async with MonobankClient(token_row) as client:
                    await client.set_webhook(webhook_url)
                results.append(
                    {
                        "integration_id": integration_id,
                        "status": "ok",
                        "webhook_url": webhook_url,
                    }
                )
            except Exception:
                logger.warning(
                    "Failed to register webhook for integration %s"
                    " — config updated but Monobank not notified",
                    integration_id,
                    exc_info=True,
                )
                results.append(
                    {
                        "integration_id": integration_id,
                        "status": "webhook_failed",
                        "webhook_url": webhook_url,
                    }
                )

        return results

    async def _rotate_existing_integration(
        self,
        conn: asyncpg.Connection,
        existing: BankIntegrationRow,
        token: str,
        encryption_key: str,
        key_version: int,
        client: MonobankClient,
        webhook_base_url: str,
    ) -> MonobankLinkResponse:
        """Rotate token + re-register webhook for an existing integration.

        Called from both the happy-path (client_id already known) and the
        concurrent-INSERT race-loss path so neither drifts independently.
        """
        encrypted_token = await self._monobank_repo.encrypt_token(
            conn, token, encryption_key
        )
        async with conn.transaction():
            await self._monobank_repo.update_integration_token(
                conn, existing.id, encrypted_token, key_version
            )

        accounts = await self._monobank_repo.list_accounts_for_integration(
            conn, existing.id
        )
        account_responses = [
            MonobankAccountResponse(
                account_id=a.id,
                external_account_id=a.external_id,
                currency_code=a.currency_code,
                was_rebound=False,
            )
            for a in accounts
        ]

        webhook_registered = await self._try_set_webhook(
            client, existing.config, webhook_base_url, existing.id
        )

        return MonobankLinkResponse(
            integration_id=existing.id,
            is_new=False,
            webhook_registered=webhook_registered,
            accounts=account_responses,
        )

    async def _try_set_webhook(
        self,
        client: MonobankClient,
        config: dict[str, Any],
        webhook_base_url: str,
        integration_id: UUID,
    ) -> bool:
        """Attempt to re-register the webhook for an existing integration.

        Uses the webhook_url already stored in config if present;
        otherwise constructs it from the stored secret.
        Returns True on success, False on any failure.
        """
        webhook_url = config.get("webhook_url") or (
            f"{webhook_base_url}/monobank/webhook/{config.get('webhook_secret', '')}"
        )
        try:
            await client.set_webhook(webhook_url)
            return True
        except Exception:
            logger.warning(
                "Failed to register Monobank webhook for integration %s —"
                " integration is still active but webhook must be re-registered",
                integration_id,
                exc_info=True,
            )
            return False


_integration_repo = IntegrationRepo()
_account_repo = AccountRepo()
_monobank_repo = MonobankRepo()
_linking_service = MonobankLinkingService(
    _integration_repo, _account_repo, _monobank_repo
)


def get_monobank_linking_service() -> MonobankLinkingService:
    return _linking_service

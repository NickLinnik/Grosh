import logging
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID, uuid4

import asyncpg
from confluent_kafka import Producer
from grosh_shared.envelope import TransactionEnvelope
from grosh_shared.id_utils import generate_transaction_id
from grosh_shared.models import Topic, TransactionDirection, TransactionSource

from grosh_ingestion.errors import (
    AccountAlreadyExistsError,
    AccountNotOwnedError,
    InvalidRateSourceError,
)
from grosh_ingestion.kafka import on_delivery
from grosh_ingestion.repositories.account_repo import AccountRepo
from grosh_ingestion.repositories.user_settings_repo import UserSettingsRepo

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ManualTransactionResult:
    """Result returned by ManualService.create_transaction to the router."""

    id: UUID
    source: str
    source_id: str
    account_id: UUID
    time: datetime
    amount_cents: int
    operation_currency_code: str
    description: str | None
    direction: TransactionDirection
    rate_source: str | None


class ManualService:
    def __init__(
        self, account_repo: AccountRepo, settings_repo: UserSettingsRepo
    ) -> None:
        self._account_repo = account_repo
        self._settings_repo = settings_repo

    async def create_account(
        self,
        conn: asyncpg.Connection,
        user_id: UUID,
        account_type: str,
        currency_code: str,
        name: str,
    ) -> UUID:
        if await self._account_repo.manual_name_exists(
            conn, user_id, name, currency_code
        ):
            raise AccountAlreadyExistsError(
                f"Manual account '{name}' ({currency_code}) already exists"
            )

        try:
            return await self._account_repo.create_account(
                conn=conn,
                user_id=user_id,
                source=TransactionSource.manual,
                account_type=account_type,
                currency_code=currency_code,
                name=name,
            )
        except asyncpg.UniqueViolationError:
            raise AccountAlreadyExistsError(
                f"Manual account '{name}' ({currency_code}) already exists"
            )

    async def create_transaction(
        self,
        conn: asyncpg.Connection,
        user_id: UUID,
        account_id: UUID,
        amount_cents: int,
        currency_code: str,
        description: str | None,
        time: datetime,
        direction: TransactionDirection,
        mcc: str | None,
        rate_source: str | None,
        producer: Producer,
        idempotency_key: str | None = None,
    ) -> ManualTransactionResult:
        """Publish a manual transaction envelope to Redpanda.

        Generates a deterministic transaction ID and source_id, includes them
        in the envelope payload so the ManualNormalizer can use them downstream.
        Raises ValueError if account_id does not belong to user_id.
        """
        if not await self._account_repo.belongs_to_user(conn, account_id, user_id):
            raise AccountNotOwnedError(
                f"Account {account_id} does not belong to user {user_id}"
            )

        resolved_rate_source = rate_source
        if resolved_rate_source is None:
            resolved_rate_source = await self._settings_repo.get_default_rate_source(
                conn, user_id
            )

        if resolved_rate_source is not None:
            if not await self._settings_repo.is_valid_rate_source(
                conn, resolved_rate_source
            ):
                raise InvalidRateSourceError(
                    f"Unknown rate source: '{resolved_rate_source}'"
                )

        source_id = idempotency_key if idempotency_key else str(uuid4())
        transaction_id = generate_transaction_id("manual", source_id)

        payload = {
            "id": str(transaction_id),
            "source_id": source_id,
            "amount_cents": amount_cents,
            "operation_currency_code": currency_code,
            "description": description,
            "time": time.isoformat(),
            "direction": direction,
            "mcc": mcc,
            "rate_source": resolved_rate_source,
        }

        envelope = TransactionEnvelope(
            user_id=user_id,
            account_id=account_id,
            source="manual",
            payload=payload,
        )

        # Fire-and-forget: manual transactions are rebuildable from the same input.
        # poll(0) triggers pending delivery callbacks for error logging.
        producer.produce(
            topic=Topic.raw_transactions_manual,
            key=str(user_id).encode(),
            value=envelope.model_dump_json().encode(),
            on_delivery=on_delivery,
        )
        producer.poll(0)

        logger.info(
            "Published manual transaction %s for user %s to %s",
            transaction_id,
            user_id,
            Topic.raw_transactions_manual,
        )

        return ManualTransactionResult(
            id=transaction_id,
            source="manual",
            source_id=source_id,
            account_id=account_id,
            time=time,
            amount_cents=amount_cents,
            operation_currency_code=currency_code,
            description=description,
            direction=direction,
            rate_source=resolved_rate_source,
        )


_service = ManualService(AccountRepo(), UserSettingsRepo())


def get_manual_service() -> ManualService:
    return _service

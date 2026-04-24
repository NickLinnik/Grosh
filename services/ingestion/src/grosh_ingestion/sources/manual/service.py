import logging
from datetime import datetime
from uuid import UUID, uuid4

import asyncpg
from confluent_kafka import Producer
from grosh_shared.events import RawTransactionEvent
from grosh_shared.id_utils import generate_transaction_id
from grosh_shared.models import AccountType, Topic, TransactionSource, TransactionType

from grosh_ingestion.kafka import on_delivery
from grosh_ingestion.repositories.account_repo import AccountRepo
from grosh_ingestion.repositories.user_settings_repo import UserSettingsRepo

logger = logging.getLogger(__name__)


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
        account_type: AccountType,
        currency_code: str,
        name: str,
    ) -> UUID:
        return await self._account_repo.create_account(
            conn=conn,
            user_id=user_id,
            source=TransactionSource.manual,
            account_type=account_type,
            currency_code=currency_code,
            masked_pan=name,
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
        transaction_type: TransactionType,
        mcc: int | None,
        rate_source: str | None,
        producer: Producer,
    ) -> RawTransactionEvent:
        """Build a RawTransactionEvent for a manual transaction and publish to Redpanda.

        If rate_source is None, the user's default from user_settings is used.
        Raises ValueError if account_id does not belong to user_id.
        """
        if not await self._account_repo.belongs_to_user(conn, account_id, user_id):
            raise PermissionError(
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
                raise ValueError(f"Unknown rate source: '{resolved_rate_source}'")

        source_id = f"manual:{uuid4()}"
        transaction_id = generate_transaction_id("manual", source_id)

        event = RawTransactionEvent(
            id=transaction_id,
            source=TransactionSource.manual,
            source_id=source_id,
            user_id=user_id,
            account_id=account_id,
            time=time,
            amount_cents=amount_cents,
            operation_amount_cents=amount_cents,
            currency_code=currency_code,
            description=description,
            mcc=mcc,
            cashback_amount_cents=0,
            hold=False,
            transaction_type=transaction_type,
            rate_source=resolved_rate_source,
        )

        # Fire-and-forget: manual transactions are rebuildable from the same input.
        # poll(0) triggers pending delivery callbacks for error logging.
        producer.produce(
            topic=Topic.raw_transactions,
            key=str(user_id).encode(),
            value=event.model_dump_json().encode(),
            on_delivery=on_delivery,
        )
        producer.poll(0)

        logger.info(
            "Published manual transaction %s for user %s to %s",
            event.id,
            user_id,
            Topic.raw_transactions,
        )

        return event


_service = ManualService(AccountRepo(), UserSettingsRepo())


def get_manual_service() -> ManualService:
    return _service

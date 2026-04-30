from datetime import UTC, datetime
from uuid import UUID

from grosh_shared.events import RawTransactionEvent
from grosh_shared.id_utils import generate_transaction_id
from grosh_shared.iso_4217 import numeric_to_alpha
from grosh_shared.models import RateSource, TransactionSource, TransactionType

from grosh_ingestion.sources.monobank.models import MonobankStatementItem


def to_raw_transaction_event(
    item: MonobankStatementItem,
    user_id: UUID,
    account_id: UUID,
) -> RawTransactionEvent:
    if item.amount > 0:
        transaction_type = TransactionType.income
    elif item.amount < 0:
        transaction_type = TransactionType.expense
    else:
        transaction_type = TransactionType.check

    raw_metadata: dict[str, str] = {}
    for key, value in {
        "comment": item.comment,
        "receipt_id": item.receipt_id,
        "invoice_id": item.invoice_id,
        "counter_edrpou": item.counter_edrpou,
        "counter_name": item.counter_name,
    }.items():
        if value is not None:
            raw_metadata[key] = value

    return RawTransactionEvent(
        id=generate_transaction_id("monobank", item.id),
        source=TransactionSource.monobank,
        source_id=item.id,
        user_id=user_id,
        account_id=account_id,
        time=datetime.fromtimestamp(item.time, tz=UTC),
        amount_cents=abs(item.amount),
        operation_amount_cents=abs(item.operation_amount),
        operation_currency_code=numeric_to_alpha(item.currency_code),
        description=item.description,
        mcc=item.mcc,
        cashback_amount_cents=item.cashback_amount,
        balance_cents=item.balance,
        hold=item.hold,
        transaction_type=transaction_type,
        counterparty_iban=item.counter_iban,
        metadata=raw_metadata if raw_metadata else None,
        rate_source=RateSource.monobank,
    )

from datetime import UTC, datetime

from grosh_shared.envelope import TransactionEnvelope
from grosh_shared.id_utils import generate_transaction_id
from grosh_shared.iso_4217 import numeric_to_alpha
from grosh_shared.models import RateSource, TransactionDirection

from grosh_consumer.models.normalized import NormalizedTransaction
from grosh_consumer.sources.monobank.models import MonobankStatementItem


class MonobankNormalizer:
    """Converts a raw Monobank envelope into a NormalizedTransaction.

    Applies the same canonicalization rules as the old transaction_adapter:
    - abs(amount) — amount_cents is always positive
    - direction inferred from sign of amount
    - ISO 4217 numeric currency code → alpha-3
    - Deterministic UUID5 from source + source_id
    - Extracts optional metadata fields (comment, receipt_id, etc.)
    """

    def normalize(self, envelope: TransactionEnvelope) -> NormalizedTransaction:
        item = MonobankStatementItem.model_validate(envelope.payload)

        if item.amount > 0:
            direction = TransactionDirection.income
        elif item.amount < 0:
            direction = TransactionDirection.expense
        else:
            direction = TransactionDirection.zero

        source_fields: dict[str, str] = {}
        for key, value in {
            "comment": item.comment,
            "receipt_id": item.receipt_id,
            "invoice_id": item.invoice_id,
            "counter_edrpou": item.counter_edrpou,
            "counter_name": item.counter_name,
        }.items():
            if value is not None:
                source_fields[key] = value

        metadata: dict[str, object] | None = (
            {"source": source_fields} if source_fields else None
        )

        return NormalizedTransaction(
            id=generate_transaction_id("monobank", item.id),
            source="monobank",
            source_id=item.id,
            user_id=envelope.user_id,
            account_id=envelope.account_id,
            time=datetime.fromtimestamp(item.time, tz=UTC),
            amount_cents=abs(item.amount),
            operation_amount_cents=abs(item.operation_amount),
            operation_currency_code=numeric_to_alpha(item.currency_code),
            description=item.description,
            mcc=str(item.mcc),
            cashback_amount_cents=item.cashback_amount,
            balance_cents=item.balance,
            hold=item.hold,
            direction=direction,
            counterparty_iban=item.counter_iban,
            metadata=metadata,
            rate_source=RateSource.monobank,
        )

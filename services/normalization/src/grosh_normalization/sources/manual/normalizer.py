from grosh_shared.domain.normalized import NormalizedTransaction
from grosh_shared.messaging.envelope import TransactionEnvelope

from grosh_normalization.sources.manual.models import ManualTransactionPayload


class ManualNormalizer:
    """Converts a manual transaction envelope into a NormalizedTransaction.

    Manual transactions are already in canonical form — the ingestion service
    fills in all required fields. This normalizer just validates and repackages.
    """

    def normalize(self, envelope: TransactionEnvelope) -> NormalizedTransaction:
        payload = ManualTransactionPayload.model_validate(envelope.payload)

        return NormalizedTransaction(
            id=payload.id,
            source="manual",
            source_id=payload.source_id,
            user_id=envelope.user_id,
            account_id=envelope.account_id,
            time=payload.time,
            amount_cents=payload.amount_cents,
            operation_amount_cents=payload.amount_cents,
            operation_currency_code=payload.operation_currency_code,
            description=payload.description,
            mcc=payload.mcc,
            cashback_amount_cents=0,
            balance_cents=None,
            hold=False,
            direction=payload.direction,
            counterparty_iban=None,
            rate_source=payload.rate_source,
            metadata=None,
        )

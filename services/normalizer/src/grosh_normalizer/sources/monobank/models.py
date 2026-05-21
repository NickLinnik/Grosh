from pydantic import BaseModel, ConfigDict, Field


class MonobankStatementItem(BaseModel):
    """Raw Monobank statement item as received in the transaction envelope payload.

    This is an independent copy of the ingestion-side model — they may diverge
    as the consumer may add stricter validation over time.
    """

    model_config = ConfigDict(frozen=True, populate_by_name=True)

    id: str
    time: int
    description: str
    mcc: int
    original_mcc: int = Field(alias="originalMcc")
    hold: bool
    amount: int
    operation_amount: int = Field(alias="operationAmount")
    currency_code: int = Field(alias="currencyCode")
    cashback_amount: int = Field(alias="cashbackAmount")
    balance: int
    comment: str | None = Field(default=None)
    receipt_id: str | None = Field(alias="receiptId", default=None)
    invoice_id: str | None = Field(alias="invoiceId", default=None)
    counter_edrpou: str | None = Field(alias="counterEdrpou", default=None)
    counter_iban: str | None = Field(alias="counterIban", default=None)
    counter_name: str | None = Field(alias="counterName", default=None)

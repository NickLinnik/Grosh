"""Source provider registries.

Both main.py (for rate polling loop) and backfill scripts (for dispatch)
import from here. Adding a new source means adding entries to these registries.
"""

from grosh_shared.models import RateSource

from grosh_ingestion.models import (
    RateKind,
    RateProviderConfig,
    TransactionBackfillProvider,
    WebhookReregistrationProvider,
)
from grosh_ingestion.sources.monobank.backfill import MonobankBackfillProvider
from grosh_ingestion.sources.monobank.linking_service import (
    get_monobank_linking_service,
)
from grosh_ingestion.sources.monobank.rates_provider import (
    fetch_rates as monobank_fetch_rates,
)
from grosh_ingestion.sources.nbu.rates_provider import (
    fetch_historical_rates as nbu_fetch_historical_rates,
)
from grosh_ingestion.sources.nbu.rates_provider import (
    fetch_rates as nbu_fetch_rates,
)

RATE_PROVIDERS: dict[str, RateProviderConfig] = {
    RateSource.monobank: RateProviderConfig(
        source=RateSource.monobank,
        fetch=monobank_fetch_rates,
        interval_seconds=300,
        kind=RateKind.POLLED,
    ),
    RateSource.nbu: RateProviderConfig(
        source=RateSource.nbu,
        fetch=nbu_fetch_rates,
        interval_seconds=86400,
        kind=RateKind.HISTORICAL,
        fetch_historical=nbu_fetch_historical_rates,
    ),
}

TRANSACTION_BACKFILL_PROVIDERS: dict[str, TransactionBackfillProvider] = {
    "monobank": MonobankBackfillProvider(),
}

WEBHOOK_REREGISTRATION_PROVIDERS: dict[str, WebhookReregistrationProvider] = {
    "monobank": get_monobank_linking_service(),
}

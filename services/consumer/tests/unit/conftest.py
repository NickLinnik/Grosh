"""Unit test fixtures for grosh-consumer.

Contains InMemoryRateRepo (existing) and the new in-memory repos for
transfer detection tests: InMemoryTransactionRepo, InMemoryAccountRepo,
InMemoryAnomalyRepo.
"""

from datetime import timedelta
from decimal import Decimal
from uuid import UUID, uuid4

import pytest

from grosh_consumer.repositories.currency_rate_repo import (
    RateRow,
    RateSourceChainError,
    SourceConfig,
)
from grosh_consumer.services.currency_conversion_service import (
    CurrencyConversionService,
)

_MAX_CLOSEST_RATE_AGE = timedelta(days=7)
_MAX_CHAIN_DEPTH = 10


class InMemoryRateRepo:
    """In-memory implementation of CurrencyRateRepo for unit tests.

    Does not inherit from CurrencyRateRepo and does not touch conn.
    conn is accepted for interface compatibility and ignored.
    """

    def __init__(self):
        self._sources: dict[str, dict] = {}  # source -> {fallback, base_currencies}
        self._rates: list[dict] = []
        self._next_id = 1

    # -- Seed helpers --

    def add_source(self, source, fallback=None, base_currencies=("UAH",)):
        self._sources[source] = {
            "fallback": fallback,
            "base_currencies": list(base_currencies),
        }

    def add_rate(
        self,
        source,
        from_,
        to_,
        *,
        mid,
        buy=None,
        sell=None,
        valid_from,
        valid_to=None,
        polled=None,
        interval=None,
    ):
        self._rates.append(
            {
                "id": UUID(int=self._next_id),
                "source": source,
                "currency_from": from_,
                "currency_to": to_,
                "rate_mid": Decimal(str(mid)),
                "rate_buy": Decimal(str(buy)) if buy is not None else None,
                "rate_sell": Decimal(str(sell)) if sell is not None else None,
                "valid_from": valid_from,
                "valid_to": valid_to,
                "last_polled_at": polled,
                "update_cadence_seconds": interval,
            }
        )
        self._next_id += 1

    # -- Repo interface --

    async def find_fresh_rate(
        self, conn, source, currency_from, currency_to, at_time, poll_tolerance
    ):
        best = None
        for r in self._rates:
            if r["source"] != source:
                continue
            if r["currency_from"] != currency_from or r["currency_to"] != currency_to:
                continue
            if r["last_polled_at"] is None or r["update_cadence_seconds"] is None:
                continue
            if r["valid_from"] > at_time:
                continue
            if r["valid_to"] is not None and r["valid_to"] <= at_time:
                continue
            grace = r["last_polled_at"] + timedelta(
                seconds=poll_tolerance * r["update_cadence_seconds"]
            )
            if grace < at_time:
                continue
            if best is None or r["valid_from"] > best["valid_from"]:
                best = r
        if best is None:
            return None
        return self._to_row(best)

    async def find_closest_rate(
        self, conn, currency_from, currency_to, at_time, sources
    ):
        candidates = []
        for r in self._rates:
            if r["currency_from"] != currency_from or r["currency_to"] != currency_to:
                continue
            if r["source"] not in sources:
                continue
            proximity = self._compute_proximity(r, at_time)
            if proximity is None:
                continue
            if proximity > _MAX_CLOSEST_RATE_AGE.total_seconds():
                continue
            source_idx = sources.index(r["source"])
            candidates.append((proximity, source_idx, r["id"], r))

        if not candidates:
            return None

        candidates.sort(key=lambda x: (x[0], x[1], x[2]))
        _, _, _, best = candidates[0]
        proximity_seconds = int(candidates[0][0])
        return self._to_row(best, proximity_seconds=proximity_seconds)

    async def load_source_chain(self, conn, entry_source):
        if entry_source not in self._sources:
            return []

        chain = []
        visited = set()
        current = entry_source
        while current is not None and len(chain) < _MAX_CHAIN_DEPTH:
            if current not in self._sources:
                break
            if current in visited:
                chain.append(
                    SourceConfig(
                        source=current,
                        base_currencies=self._sources[current]["base_currencies"],
                    )
                )
                break
            visited.add(current)
            cfg = self._sources[current]
            chain.append(
                SourceConfig(source=current, base_currencies=cfg["base_currencies"])
            )
            current = cfg["fallback"]

        if len(chain) == _MAX_CHAIN_DEPTH:
            last_source = chain[-1].source
            if (
                last_source in self._sources
                and self._sources[last_source]["fallback"] is not None
            ):
                raise RateSourceChainError(
                    f"Fallback chain for source={entry_source!r} hit depth cap "
                    f"({_MAX_CHAIN_DEPTH}); possible cycle or misconfiguration. "
                    f"Chain: {[c.source for c in chain]}"
                )

        return chain

    # -- Internal --

    @staticmethod
    def _compute_proximity(r, at_time):
        if r["last_polled_at"] is not None:
            vf = r["valid_from"]
            lp = r["last_polled_at"]
            if vf <= at_time <= lp:
                return 0.0
            if at_time > lp:
                return (at_time - lp).total_seconds()
            return (vf - at_time).total_seconds()
        else:
            return abs((at_time - r["valid_from"]).total_seconds())

    @staticmethod
    def _to_row(r, proximity_seconds=None):
        return RateRow(
            id=r["id"],
            source=r["source"],
            rate_mid=r["rate_mid"],
            rate_buy=r["rate_buy"],
            rate_sell=r["rate_sell"],
            last_polled_at=r["last_polled_at"],
            update_cadence_seconds=r["update_cadence_seconds"],
            valid_from=r["valid_from"],
            valid_to=r["valid_to"],
            proximity_seconds=proximity_seconds,
        )


@pytest.fixture
def repo():
    return InMemoryRateRepo()


@pytest.fixture
def service(repo):
    return CurrencyConversionService(repo)


# ---------------------------------------------------------------------------
# Transfer detection in-memory repos
# ---------------------------------------------------------------------------


class InMemoryTransactionRepo:
    """In-memory store for TransferQueryRepo interface.

    conn is accepted on every method but ignored — no DB involved.
    Simulates FOR UPDATE SKIP LOCKED by returning whichever rows are
    currently 'unclaimed' (related_transaction_id is None).
    """

    def __init__(self):
        self._store: list[dict] = []

    # -- Seed helpers --

    def add_transaction(self, **fields) -> dict:
        defaults = {
            "id": uuid4(),
            "user_id": uuid4(),
            "account_id": uuid4(),
            "time": None,
            "amount_cents": 10000,
            "operation_amount_cents": 10000,
            "mcc": "4829",
            "direction": "expense",
            "special_category": None,
            "counterparty_iban": None,
            "related_transaction_id": None,
            "description": None,
        }
        tx = {**defaults, **fields}
        self._store.append(tx)
        return tx

    def get_by_id(self, tx_id: UUID) -> dict | None:
        for tx in self._store:
            if tx["id"] == tx_id:
                return tx
        return None

    # -- Repo interface --

    async def exists(self, conn, tx_id: UUID) -> bool:
        return any(tx["id"] == tx_id for tx in self._store)

    async def find_unclaimed_partner_tier_a(
        self,
        conn,
        user_id: UUID,
        target_account_id: UUID,
        opposite_type: str,
        time,
        window_seconds: int = 2,
    ) -> list[dict]:
        results = []
        for tx in self._store:
            if tx["user_id"] != user_id:
                continue
            if tx["account_id"] != target_account_id:
                continue
            if tx["direction"] != opposite_type:
                continue
            if tx["mcc"] != "4829":
                continue
            if tx["related_transaction_id"] is not None:
                continue
            if abs((tx["time"] - time).total_seconds()) > window_seconds:
                continue
            results.append(tx)
        return results

    async def find_unclaimed_partner_tier_b(
        self,
        conn,
        user_id: UUID,
        account_iban: str,
        opposite_type: str,
        time,
        window_seconds: int = 2,
    ) -> list[dict]:
        results = []
        for tx in self._store:
            if tx["user_id"] != user_id:
                continue
            if tx["counterparty_iban"] != account_iban:
                continue
            if tx["direction"] != opposite_type:
                continue
            if tx["mcc"] != "4829":
                continue
            if tx["related_transaction_id"] is not None:
                continue
            if abs((tx["time"] - time).total_seconds()) > window_seconds:
                continue
            results.append(tx)
        return results

    async def find_unclaimed_partner_tier_c(
        self,
        conn,
        user_id: UUID,
        operation_amount_cents: int,
        opposite_type: str,
        time,
        incoming_account_id: UUID,
        window_seconds: int = 2,
    ) -> list[dict]:
        results = []
        for tx in self._store:
            if tx["user_id"] != user_id:
                continue
            if tx["amount_cents"] != operation_amount_cents:
                continue
            if tx["direction"] != opposite_type:
                continue
            if tx["mcc"] != "4829":
                continue
            if tx["counterparty_iban"] is not None:
                continue
            if tx["related_transaction_id"] is not None:
                continue
            if tx["account_id"] == incoming_account_id:
                continue
            if abs((tx["time"] - time).total_seconds()) > window_seconds:
                continue
            results.append(tx)
        return results

    async def claim_pair(self, conn, existing_tx_id: UUID, new_tx_id: UUID) -> None:
        for tx in self._store:
            if tx["id"] == existing_tx_id:
                tx["related_transaction_id"] = new_tx_id
                tx["special_category"] = "transfer"
                return
        raise ValueError(f"Transaction {existing_tx_id} not found in store")

    async def get_account_with_properties(self, conn, account_id: UUID):
        raise NotImplementedError("use InMemoryAccountRepo")

    async def get_user_account_by_iban(self, conn, iban: str, user_id: UUID):
        raise NotImplementedError("use InMemoryAccountRepo")


class InMemoryAccountRepo:
    """In-memory store for account property lookups.

    Provides the same interface as AccountPropertyRepo in
    sources/monobank/transfer.py.
    """

    def __init__(self):
        self._store: list[dict] = []

    # -- Seed helpers --

    def add_account(
        self,
        id: UUID,
        user_id: UUID,
        type: str,
        currency_code: str,
        iban: str | None = None,
        **extras,
    ) -> dict:
        acc = {
            "id": id,
            "user_id": user_id,
            "type": type,
            "currency_code": currency_code,
            "iban": iban,
            **extras,
        }
        self._store.append(acc)
        return acc

    # -- Repo interface --

    async def get_account_with_properties(
        self, conn, account_id: UUID
    ) -> tuple[str, str, str | None]:
        for acc in self._store:
            if acc["id"] == account_id:
                return acc["type"], acc["currency_code"], acc["iban"]
        raise ValueError(f"Account {account_id} not found in InMemoryAccountRepo")

    async def get_accounts_with_properties(
        self, conn, account_ids: list[UUID]
    ) -> dict[UUID, tuple[str, str, str | None]]:
        result = {}
        for acc in self._store:
            if acc["id"] in account_ids:
                result[acc["id"]] = (acc["type"], acc["currency_code"], acc["iban"])
        return result

    async def get_user_account_by_iban(
        self, conn, iban: str, user_id: UUID
    ) -> tuple[UUID, str, str] | None:
        for acc in self._store:
            if acc["iban"] == iban and acc["user_id"] == user_id:
                return acc["id"], acc["type"], acc["currency_code"]
        return None

    # Compatibility alias used by PipelineOrchestrator's AccountRepo
    async def find_by_iban(self, conn, iban: str, user_id: UUID) -> UUID | None:
        result = await self.get_user_account_by_iban(conn, iban, user_id)
        return result[0] if result is not None else None

    async def get_currency_code(self, conn, account_id: UUID) -> str:
        _, currency, _ = await self.get_account_with_properties(conn, account_id)
        return currency


class InMemoryAnomalyRepo:
    """In-memory store for transfer match anomalies."""

    def __init__(self):
        self._store: list[dict] = []

    # -- Seed helpers --

    def all_anomalies(self) -> list[dict]:
        return list(self._store)

    # -- Repo interface --

    async def record_anomaly(
        self,
        conn,
        transaction_id: UUID,
        candidate_ids: list[UUID],
        reason_code: str,
        reason_detail: str | None,
    ) -> None:
        self._store.append(
            {
                "transaction_id": transaction_id,
                "candidate_ids": list(candidate_ids),
                "reason_code": reason_code,
                "reason_detail": reason_detail,
            }
        )

    async def delete_unpaired_anomalies_for_transactions(
        self, conn, tx_ids: list[UUID]
    ) -> None:
        _AUTO_RESOLVE = {"unpaired_from_description", "unpaired_to_description"}
        tx_id_set = set(tx_ids)
        self._store = [
            a
            for a in self._store
            if not (
                a["transaction_id"] in tx_id_set and a["reason_code"] in _AUTO_RESOLVE
            )
        ]

    async def get_anomaly_for_transaction(
        self, conn, transaction_id: UUID
    ) -> dict | None:
        for a in self._store:
            if a["transaction_id"] == transaction_id:
                return a
        return None


# ---------------------------------------------------------------------------
# Pytest fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def tx_repo():
    return InMemoryTransactionRepo()


@pytest.fixture
def acc_repo():
    return InMemoryAccountRepo()


@pytest.fixture
def anomaly_repo():
    return InMemoryAnomalyRepo()

import uuid
from uuid import UUID

GROSH_NAMESPACE = UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")


def generate_transaction_id(source: str, source_id: str) -> UUID:
    return uuid.uuid5(GROSH_NAMESPACE, f"{source}:{source_id}")

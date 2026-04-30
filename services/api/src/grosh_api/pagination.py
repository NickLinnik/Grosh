import base64
from datetime import datetime

from pydantic import BaseModel


class CursorPage[T](BaseModel):
    items: list[T]
    total: int
    limit: int
    next_cursor: str | None


def encode_cursor(ts: datetime, row_id: str | int) -> str:
    """Encode a (timestamp, id) pair as an opaque URL-safe cursor string."""
    raw = f"{ts.isoformat()}:{row_id}"
    return base64.urlsafe_b64encode(raw.encode()).decode()


def decode_cursor(cursor: str) -> tuple[str, str]:
    """Decode cursor into (timestamp_iso, id_str). Raises ValueError on bad input."""
    raw = base64.urlsafe_b64decode(cursor.encode()).decode()
    ts_str, _, id_str = raw.rpartition(":")
    if not ts_str or not id_str:
        raise ValueError(f"Invalid cursor: '{cursor}'")
    return ts_str, id_str

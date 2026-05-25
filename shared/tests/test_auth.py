from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import jwt
import pytest

from grosh_shared.http.auth import (
    JWT_ALGORITHM,
    InvalidAccessTokenError,
    decode_access_token,
    extract_user_id,
)

_SECRET = "test-secret-value-padded-to-32-chars"


def _encode(payload: dict) -> str:
    return jwt.encode(payload, _SECRET, algorithm=JWT_ALGORITHM)


# -- decode_access_token -------------------------------------------------------


def test_decode_access_token_valid() -> None:
    user_id = uuid4()
    token = _encode(
        {"sub": str(user_id), "exp": datetime.now(UTC) + timedelta(minutes=15)}
    )

    payload = decode_access_token(token, _SECRET)

    assert payload["sub"] == str(user_id)


def test_decode_access_token_expired_raises() -> None:
    token = _encode(
        {"sub": str(uuid4()), "exp": datetime.now(UTC) - timedelta(seconds=1)}
    )

    with pytest.raises(InvalidAccessTokenError, match="expired"):
        decode_access_token(token, _SECRET)


def test_decode_access_token_wrong_secret_raises() -> None:
    token = _encode(
        {"sub": str(uuid4()), "exp": datetime.now(UTC) + timedelta(minutes=15)}
    )

    with pytest.raises(InvalidAccessTokenError):
        decode_access_token(token, "completely-different-secret-32-ch")


def test_decode_access_token_malformed_raises() -> None:
    with pytest.raises(InvalidAccessTokenError):
        decode_access_token("this.is.garbage", _SECRET)


# -- extract_user_id -----------------------------------------------------------


def test_extract_user_id_valid() -> None:
    user_id = uuid4()
    payload: dict = {"sub": str(user_id)}

    result = extract_user_id(payload)

    assert result == user_id
    assert isinstance(result, UUID)


def test_extract_user_id_missing_sub_raises() -> None:
    with pytest.raises(InvalidAccessTokenError):
        extract_user_id({})


def test_extract_user_id_invalid_uuid_raises() -> None:
    with pytest.raises(InvalidAccessTokenError):
        extract_user_id({"sub": "not-a-uuid"})

import hashlib
import uuid
from datetime import UTC, datetime, timedelta

import jwt
import pytest

from grosh_api.repositories.token_repo import TokenRepo
from grosh_api.repositories.user_repo import UserRepo
from grosh_api.services.auth_service import (
    _DUMMY_PASSWORD_HASH,
    AuthService,
    InvalidAccessTokenError,
)


@pytest.fixture(autouse=True)
def set_jwt_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    # At least 32 bytes — RFC 7518 minimum for HS256, silences PyJWT's
    # InsecureKeyLengthWarning during tests.
    monkeypatch.setenv("JWT_SECRET", "test-secret-value-padded-to-32-chars")


@pytest.fixture
def auth_service() -> AuthService:
    return AuthService(UserRepo(), TokenRepo())


# -- JWT ----------------------------------------------------------------------


def test_encode_decode_round_trip(auth_service: AuthService) -> None:
    user_id = uuid.uuid4()

    token = auth_service.encode_access_token(user_id)
    payload = auth_service.decode_access_token(token)

    assert payload["sub"] == str(user_id)


def _make_expired_token(auth_service: AuthService) -> str:
    now = datetime.now(UTC)
    payload = {
        "sub": str(uuid.uuid4()),
        "iat": now - timedelta(minutes=30),
        "exp": now - timedelta(minutes=15),
    }
    return jwt.encode(
        payload, "test-secret-value-padded-to-32-chars", algorithm="HS256"
    )


def _make_tampered_token(auth_service: AuthService) -> str:
    token = auth_service.encode_access_token(uuid.uuid4())
    header_payload, _, _ = token.rsplit(".", 2)
    return header_payload + ".invalidsignature"


@pytest.mark.parametrize(
    ("token_factory", "expected_detail"),
    [
        (_make_expired_token, "Token has expired."),
        (_make_tampered_token, "Invalid token."),
    ],
    ids=["expired", "tampered"],
)
def test_decode_access_token_error_messages(
    auth_service: AuthService,
    token_factory: object,
    expected_detail: str,
) -> None:
    """Pin the exact error detail for each failure mode.

    ``ExpiredSignatureError`` is a subclass of ``InvalidTokenError`` in PyJWT,
    so the ``except`` clauses in ``decode_access_token`` must be ordered
    expired-first. Swapping them would silently degrade the expired-token
    detail to "Invalid token." — this test fails if that happens.
    """
    token = token_factory(auth_service)  # type: ignore[operator]

    with pytest.raises(InvalidAccessTokenError) as exc_info:
        auth_service.decode_access_token(token)

    assert str(exc_info.value) == expected_detail


# -- Password -----------------------------------------------------------------


def test_hash_verify_round_trip(auth_service: AuthService) -> None:
    plain = "super-secret-password"
    hashed = auth_service.hash_password(plain)
    assert auth_service.verify_password(plain, hashed) is True


def test_wrong_password_returns_false(auth_service: AuthService) -> None:
    hashed = auth_service.hash_password("correct")
    assert auth_service.verify_password("wrong", hashed) is False


def test_hash_is_not_plaintext(auth_service: AuthService) -> None:
    plain = "x"
    assert auth_service.hash_password(plain) != plain


def test_dummy_password_hash_is_valid_bcrypt(auth_service: AuthService) -> None:
    """Regression: the constant-time decoy hash used by ``login()`` for
    unknown emails must be a real bcrypt hash that ``verify_password``
    can compare against without raising. If the constant ever drifts
    (e.g. someone replaces it with a placeholder string), login would
    crash for every unknown-email request — much worse than the timing
    leak it was meant to prevent.
    """
    # verify_password must return False (not raise) for any plain text
    assert auth_service.verify_password("anything", _DUMMY_PASSWORD_HASH) is False


# -- Refresh token ------------------------------------------------------------


def test_hash_token_is_deterministic(auth_service: AuthService) -> None:
    token_bytes = b"\x00" * 32
    assert auth_service._hash_token(token_bytes) == auth_service._hash_token(
        token_bytes
    )


def test_hash_token_matches_sha256(auth_service: AuthService) -> None:
    token_bytes = b"\x00" * 32
    expected = hashlib.sha256(token_bytes).hexdigest()
    assert auth_service._hash_token(token_bytes) == expected


def test_hash_token_different_inputs_differ(auth_service: AuthService) -> None:
    a = auth_service._hash_token(b"\x00" * 32)
    b = auth_service._hash_token(b"\x01" * 32)
    assert a != b


def test_hash_via_hex_roundtrip_matches_direct(auth_service: AuthService) -> None:
    """Guards the create→verify invariant: hashing raw bytes on create
    must produce the same digest as hashing ``bytes.fromhex(raw_token)``
    on verify. If this ever diverges, existing refresh tokens become
    unredeemable.
    """
    token_bytes = bytes(range(32))
    raw_token = token_bytes.hex()

    hash_on_create = auth_service._hash_token(token_bytes)
    hash_on_verify = auth_service._hash_token(bytes.fromhex(raw_token))

    assert hash_on_create == hash_on_verify

"""Integration tests for MonobankRepo pgcrypto methods.

Requires a live Postgres connection with pgcrypto enabled (provided by
the session-scoped db_pool fixture in conftest.py).
"""

import asyncpg
import pytest

from grosh_ingestion.sources.monobank.repo import MonobankRepo


async def test_encrypt_then_decrypt_roundtrip(conn: asyncpg.Connection) -> None:
    """encrypt_token followed by decrypt_token_value recovers the original plaintext."""
    repo = MonobankRepo()
    plaintext = "test-token-value-12345"
    key = "test-encryption-key"

    ciphertext_bytes = await repo.encrypt_token(conn, plaintext, key)
    assert isinstance(ciphertext_bytes, bytes | bytearray | memoryview)

    ciphertext_hex = bytes(ciphertext_bytes).hex()
    recovered = await repo.decrypt_token_value(conn, ciphertext_hex, key)
    assert recovered == plaintext


async def test_encrypt_token_produces_different_ciphertext_each_call(
    conn: asyncpg.Connection,
) -> None:
    """pgp_sym_encrypt uses random IV — same plaintext+key yields distinct ct."""
    repo = MonobankRepo()
    plaintext = "test-token"
    key = "test-key"

    a = await repo.encrypt_token(conn, plaintext, key)
    b = await repo.encrypt_token(conn, plaintext, key)
    assert a != b

    # Both still decrypt to the same plaintext
    assert await repo.decrypt_token_value(conn, bytes(a).hex(), key) == plaintext
    assert await repo.decrypt_token_value(conn, bytes(b).hex(), key) == plaintext


async def test_decrypt_token_value_with_wrong_key_raises(
    conn: asyncpg.Connection,
) -> None:
    """Wrong key must not silently return garbage — pgcrypto raises a PostgresError."""
    repo = MonobankRepo()
    plaintext = "test-token"

    ciphertext_bytes = await repo.encrypt_token(conn, plaintext, "right-key")
    ciphertext_hex = bytes(ciphertext_bytes).hex()

    with pytest.raises(asyncpg.PostgresError):
        await repo.decrypt_token_value(conn, ciphertext_hex, "wrong-key")

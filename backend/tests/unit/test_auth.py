import re

from backend.model.auth import (
    API_KEY_PREFIX_LENGTH,
    generate_api_key,
    get_api_key_prefix,
    hash_api_key,
    verify_api_key_hash,
)


def test_generate_api_key_uses_live_prefix():
    """Generated API keys should have the expected public format."""
    api_key = generate_api_key()

    assert api_key.startswith("ak_live_")
    assert len(api_key) == 51
    assert re.fullmatch(r"ak_live_[A-Za-z0-9_-]{43}", api_key)


def test_hash_api_key_is_stable_for_same_secret():
    """Hashing the same API key with the same secret should be deterministic."""
    api_key = "ak_live_test_key"
    secret = "test-secret"

    assert hash_api_key(api_key, secret) == hash_api_key(api_key, secret)


def test_verify_api_key_hash_rejects_wrong_secret():
    """Verification should accept the right secret and reject a wrong one."""
    api_key = "ak_live_test_key"
    stored_hash = hash_api_key(api_key, "correct-secret")

    assert verify_api_key_hash(api_key, "correct-secret", stored_hash)
    assert not verify_api_key_hash(api_key, "wrong-secret", stored_hash)


def test_get_api_key_prefix_returns_configured_prefix_length():
    """Stored key prefixes should use the configured prefix length."""
    api_key = "ak_live_abcdefghijklmnopqrstuvwxyz"

    assert get_api_key_prefix(api_key) == api_key[:API_KEY_PREFIX_LENGTH]

import hmac
import secrets
from hashlib import sha256

API_KEY_PREFIX_LENGTH = 16


def generate_api_key() -> str:
    return f"ak_live_{secrets.token_urlsafe(32)}"


def hash_api_key(api_key: str, secret: str) -> str:
    return hmac.new(
        secret.encode("utf-8"),
        api_key.encode("utf-8"),
        sha256,
    ).hexdigest()


def verify_api_key_hash(api_key: str, secret: str, api_key_hash: str) -> bool:
    return hmac.compare_digest(hash_api_key(api_key, secret), api_key_hash)


def get_api_key_prefix(api_key: str) -> str:
    return api_key[:API_KEY_PREFIX_LENGTH]

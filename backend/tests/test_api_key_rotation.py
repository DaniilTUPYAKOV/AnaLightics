import re

from backend.repositories.projects import build_rotated_api_key_name


def test_build_rotated_api_key_name_uses_provided_name():
    """Explicit rotate names should be used unchanged."""
    assert build_rotated_api_key_name("frontend-prod", "frontend-prod-v2") == "frontend-prod-v2"


def test_build_rotated_api_key_name_generates_default_name():
    """Missing rotate names should fall back to a dated name."""
    rotated_name = build_rotated_api_key_name("frontend-prod", None)

    assert re.fullmatch(r"frontend-prod-rotated-\d{8}", rotated_name)

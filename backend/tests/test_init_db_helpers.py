import datetime
from typing import Union

import pytest

from backend.db.init_db import get_clickhouse_type, unwrap_optional


def test_unwrap_optional_returns_inner_type_and_nullable_flag():
    """PEP 604 optional types should unwrap to their inner type."""
    inner_type, is_nullable = unwrap_optional(str | None)

    assert inner_type is str
    assert is_nullable is True


def test_unwrap_optional_supports_typing_union_optional():
    """typing.Union optional types should unwrap to their inner type."""
    inner_type, is_nullable = unwrap_optional(Union[int, None])

    assert inner_type is int
    assert is_nullable is True


def test_unwrap_optional_rejects_non_optional_union():
    """Non-optional unions should fail fast instead of mapping incorrectly."""
    with pytest.raises(ValueError, match="Unsupported union type"):
        unwrap_optional(str | int)


def test_unwrap_optional_rejects_multi_type_optional_union():
    """Optional unions with multiple real types should fail fast."""
    with pytest.raises(ValueError, match="Unsupported optional union type"):
        unwrap_optional(str | int | None)


@pytest.mark.parametrize(
    ("python_type", "clickhouse_type"),
    [
        (str, "String"),
        (int, "Int32"),
        (float, "Float64"),
        (bool, "UInt8"),
        (datetime.datetime, "DateTime"),
        (str | None, "Nullable(String)"),
    ],
)
def test_get_clickhouse_type_maps_supported_types(python_type: type, clickhouse_type: str):
    """Supported Python annotations should map to ClickHouse column types."""
    assert get_clickhouse_type(python_type) == clickhouse_type

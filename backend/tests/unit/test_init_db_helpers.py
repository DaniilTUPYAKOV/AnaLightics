import datetime
import uuid
from typing import Union

import pytest

from backend.db.init_db import create_table_sql, get_clickhouse_type, unwrap_optional
from backend.model.schemas import Event


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
        (uuid.UUID, "UUID"),
        (str | None, "Nullable(String)"),
    ],
)
def test_get_clickhouse_type_maps_supported_types(python_type: type, clickhouse_type: str):
    """Supported Python annotations should map to ClickHouse column types."""
    assert get_clickhouse_type(python_type) == clickhouse_type


def test_create_table_sql_adds_event_id_as_uuid_column():
    """ClickHouse event tables should store client-generated ids as UUID."""
    sql = create_table_sql(Event, "events")

    assert "event_id UUID" in sql


def test_create_table_sql_uses_event_id_as_deduplication_key():
    """ClickHouse event tables should be keyed by project and client event id."""
    sql = create_table_sql(Event, "events")

    assert "ENGINE = ReplacingMergeTree()" in sql
    assert "ORDER BY (project_id, event_id)" in sql

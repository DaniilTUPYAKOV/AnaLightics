import clickhouse_connect
from backend.model.schemas import Event
from typing import Type, get_origin, get_args, Union
import datetime
from pydantic import BaseModel

TYPE_MAPPING = {
    str: "String",
    int: "Int32",
    float: "Float64",
    bool: "UInt8",
    datetime.datetime: "DateTime",
}


def get_clickhouse_type(python_type):
    if get_origin(python_type) is Union and type(None) in get_args(python_type):
        inner_type = [t for t in get_args(
            python_type) if t is not type(None)][0]
        return f"Nullable({TYPE_MAPPING.get(inner_type, 'String')})"
    return TYPE_MAPPING.get(python_type, "String")


def create_table_sql(model: Type[BaseModel], table_name: str) -> str:
    columns = []

    for name, field in model.model_fields.items():
        ch_type = get_clickhouse_type(field.annotation)
        columns.append(f"{name} {ch_type}")

    if "project_id" not in model.model_fields:
        columns.append("project_id String DEFAULT 'unknown'")

    if "received_at" not in model.model_fields:
        columns.append("received_at DateTime DEFAULT now()")

    columns_sql = ",\n    ".join(columns)

    sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
    {columns_sql}
    ) ENGINE = MergeTree()
    PARTITION BY toYYYYMM(received_at)
    ORDER BY (event_type, timestamp)
    """
    return sql


def migrate_table(client, model: Type[BaseModel], table_name: str):
    """
    Сравнивает схему Pydantic с реальной таблицей в БД и добавляет новые колонки.
    """
    try:
        result = client.query(f"DESCRIBE TABLE {table_name}")
        existing_columns = {row[0]: row[1] for row in result.result_rows}
    except clickhouse_connect.driver.exceptions.DatabaseError:
        client.command(create_table_sql(model, table_name))
        return

    for field_name, field_info in model.model_fields.items():
        ch_type = get_clickhouse_type(field_info.annotation)

        if field_name not in existing_columns:
            alter_query = f"ALTER TABLE {table_name} ADD COLUMN {field_name} {ch_type}"
            client.command(alter_query)


def main():
    client = clickhouse_connect.get_client(host='localhost', port=8123)
    client.command("CREATE DATABASE IF NOT EXISTS analightics")
    migrate_table(client, Event, "analightics.events")


if __name__ == "__main__":
    main()

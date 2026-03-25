import clickhouse_connect
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import DatabaseError
from backend.model.schemas import Event
from backend.model.config import (
    CLICKHOUSE_HOST,
    CLICKHOUSE_PORT,
    CLICKHOUSE_TABLE,
    CLICKHOUSE_DB,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_USER
)
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


def get_clickhouse_type(python_type: type) -> str:
    """
    Returns the ClickHouse type that corresponds to the given Python type.

    If the given type is a Union and contains None, it returns a Nullable type
    with the inner type being the first non-None type in the Union.

    Args:
        python_type (type): The Python type to convert

    Returns:
        str: The ClickHouse type that corresponds to the given Python type
    """
    if get_origin(python_type) is Union and type(None) in get_args(python_type):
        inner_type = [t for t in get_args(
            python_type) if t is not type(None)][0]
        return f"Nullable({TYPE_MAPPING.get(inner_type, 'String')})"
    return TYPE_MAPPING.get(python_type, "String")


def create_table_sql(model: Type[BaseModel], table_name: str) -> str:
    """
    Creates a ClickHouse SQL query to create a table based on the given BaseModel.

    The query will include all fields from the BaseModel, as well as two additional fields:
    "project_id" with a default value of "unknown", and "received_at" with a default value of now().

    The table will use the MergeTree engine, and will be partitioned by the year and month of the "received_at" field.
    The table will be ordered by the "event_type" and "timestamp" fields.

    Args:
        model (Type[BaseModel]): The BaseModel to create a table for
        table_name (str): The name of the table to create

    Returns:
        str: The ClickHouse SQL query to create the table
    """
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


def migrate_table(client: Client, model: Type[BaseModel], table_name: str) -> None:
    """
    Migrates a ClickHouse table to match the given BaseModel.

    First, attempts to describe the table. If the table does not exist, creates it with the given BaseModel.

    Then, checks each field in the BaseModel to see if it exists in the table. If a field does not exist, adds it to the table with the corresponding ClickHouse type.

    Args:
        client (Client): The ClickHouse client to use
        model (Type[BaseModel]): The BaseModel to migrate the table to
        table_name (str): The name of the table to migrate
    """
    try:
        result = client.query(f"DESCRIBE TABLE {table_name}")
        existing_columns = {row[0]: row[1] for row in result.result_rows}
    except DatabaseError:
        client.command(create_table_sql(model, table_name))
        return

    for field_name, field_info in model.model_fields.items():
        ch_type = get_clickhouse_type(field_info.annotation)

        if field_name not in existing_columns:
            alter_query = f"ALTER TABLE {table_name} ADD COLUMN {field_name} {ch_type}"
            client.command(alter_query)


def main():
    """
    Main entry point for the script.

    Creates the ClickHouse database if it does not exist, then migrates the given table to match the Event model.

    Args:
        None

    Returns:
        None
    """
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )
    client.command(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB}")
    migrate_table(client, Event, CLICKHOUSE_TABLE)


if __name__ == "__main__":
    main()

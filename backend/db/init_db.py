import asyncio
import clickhouse_connect
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import DatabaseError
from typing import Type, get_origin, get_args, Union
import datetime
from pydantic import BaseModel
from sqlalchemy import select

from backend.model.schemas import Event
from backend.model.config import (
    CLICKHOUSE_HOST,
    CLICKHOUSE_PORT,
    CLICKHOUSE_TABLE,
    CLICKHOUSE_DB,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_USER
)

from backend.db.postgres import engine, Base, Project, AsyncSessionLocal

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
    """
    if get_origin(python_type) is Union and type(None) in get_args(python_type):
        inner_type = [t for t in get_args(python_type) if t is not type(None)][0]
        return f"Nullable({TYPE_MAPPING.get(inner_type, 'String')})"
    return TYPE_MAPPING.get(python_type, "String")


def create_table_sql(model: Type[BaseModel], table_name: str) -> str:
    """
    Creates a ClickHouse SQL query to create a table based on the given BaseModel.
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


async def init_postgres():
    """
    Инициализирует PostgreSQL: создает таблицы и добавляет демо-проект.
    """
    print("Initializing PostgreSQL...")

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with AsyncSessionLocal() as session:
        stmt = select(Project).where(Project.id == "demo-project")
        result = await session.execute(stmt)
        
        if not result.scalar_one_or_none():
            demo_project = Project(
                id="demo-project",
                name="Demo Website",
                api_key="secret-demo-key-123"
            )
            session.add(demo_project)
            await session.commit()
            print("Demo project 'demo-project' created in PostgreSQL.")
        else:
            print("Demo project already exists in PostgreSQL.")


def init_clickhouse():
    """
    Инициализирует ClickHouse: создает БД и накатывает миграции.
    """
    print("Initializing ClickHouse...")
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )
    client.command(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB}")
    migrate_table(client, Event, CLICKHOUSE_TABLE)
    print("ClickHouse initialized successfully.")


async def main():
    """
    Main entry point for the script.
    """
    print("Starting database initialization...")

    try:
        await init_postgres()
    except Exception as e:
        print(f"Error initializing PostgreSQL: {e}")
        raise e

    try:
        init_clickhouse()
    except Exception as e:
        print(f"Error initializing ClickHouse: {e}")
        raise e

    print("All databases initialized successfully!")


if __name__ == "__main__":
    asyncio.run(main())

import os
from pathlib import Path

import pytest
from dotenv import load_dotenv

from backend.db.postgres import (
    Base,
    build_database_url,
    create_postgres_engine,
    create_sessionmaker,
)
from backend.model.config import Settings


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]

load_dotenv(REPOSITORY_ROOT / ".env.test", override=False)


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
def repository_test_database_url() -> str:
    required_env_vars = [
        "POSTGRES_TEST_USER",
        "POSTGRES_TEST_PASSWORD",
        "POSTGRES_TEST_DB",
        "POSTGRES_TEST_HOST",
        "POSTGRES_TEST_PORT_EXTERNAL",
    ]
    missing_env_vars = [name for name in required_env_vars if name not in os.environ]

    if missing_env_vars:
        raise RuntimeError(
            "Repository tests require test database environment variables: "
            + ", ".join(missing_env_vars),
        )

    database = os.environ["POSTGRES_TEST_DB"]

    if not database.endswith("_test"):
        raise RuntimeError("Refusing to run repository tests against a non-test database")

    return build_database_url(
        user=os.environ["POSTGRES_TEST_USER"],
        password=os.environ["POSTGRES_TEST_PASSWORD"],
        host=os.environ["POSTGRES_TEST_HOST"],
        port=os.environ["POSTGRES_TEST_PORT_EXTERNAL"],
        database=database,
    )


@pytest.fixture
def test_settings() -> Settings:
    return Settings(api_key_hash_secret="test-api-key-hash-secret")


@pytest.fixture
async def db_session(repository_test_database_url: str):
    engine = create_postgres_engine(repository_test_database_url)
    sessionmaker = create_sessionmaker(engine)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    async with sessionmaker() as session:
        yield session

    await engine.dispose()

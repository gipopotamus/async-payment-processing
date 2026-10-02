"""Isolated PostgreSQL schemas for opt-in database integration checks."""

import os
from collections.abc import AsyncIterator
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import Connection, make_url
from sqlalchemy.ext.asyncio import create_async_engine

from payments.infrastructure.database import Database


def migrate(connection: Connection, revision: str = "head") -> None:
    """Upgrade using an injected connection, including its isolated search path."""
    config = Config("alembic.ini")
    config.attributes["connection"] = connection
    command.upgrade(config, revision)


@pytest.fixture
async def database() -> AsyncIterator[Database]:
    """Create and remove only a uniquely named schema in an explicit test database."""
    url = os.environ.get("PAYMENTS_TEST_DATABASE_URL")
    if url is None:
        pytest.skip("Set PAYMENTS_TEST_DATABASE_URL to run PostgreSQL integration checks")
    parsed_url = make_url(url)
    if parsed_url.database is None or not parsed_url.database.endswith("_test"):
        pytest.fail("Integration checks require a database name ending in _test")

    schema = f"test_{uuid4().hex}"
    admin_engine = create_async_engine(url, hide_parameters=True)
    async with admin_engine.begin() as connection:
        await connection.exec_driver_sql(f'CREATE SCHEMA "{schema}"')

    engine = create_async_engine(
        url,
        hide_parameters=True,
        connect_args={"server_settings": {"search_path": schema}},
    )
    resource = Database(engine)
    try:
        async with engine.begin() as connection:
            await connection.run_sync(migrate)
        yield resource
    finally:
        await resource.close()
        try:
            async with admin_engine.begin() as connection:
                await connection.exec_driver_sql(f'DROP SCHEMA "{schema}" CASCADE')
        finally:
            await admin_engine.dispose()

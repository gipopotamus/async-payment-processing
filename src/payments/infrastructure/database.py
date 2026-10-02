"""Async database resource composition with explicit session ownership."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

# asyncpg exposes runtime exceptions but does not ship typing metadata.
from asyncpg import PostgresError  # type: ignore[import-untyped]
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from payments.core.settings import DatabaseSettings


class DatabaseUnavailable(Exception):
    """Report a raw network/driver failure without exposing credentials."""


class Database:
    """Own an injected engine and create a separate session for each unit of work.

    Callers explicitly open transactions with session.begin(); sessions do not
    commit implicitly. Close the database when its owning process shuts down.
    """

    def __init__(self, engine: AsyncEngine) -> None:
        """Bind session creation to the supplied async engine."""
        self.engine = engine
        self.sessions = async_sessionmaker(engine, expire_on_commit=False)

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        """Close one session and normalize connection failures after transaction rollback.

        SQLAlchemy errors retain their existing type, including integrity violations.
        Cancellation and programming errors propagate without being reclassified.
        """
        try:
            async with self.sessions() as session:
                yield session
        except (OSError, TimeoutError, PostgresError) as error:
            raise DatabaseUnavailable(type(error).__name__) from error

    async def close(self) -> None:
        """Release pooled connections owned by this database resource."""
        await self.engine.dispose()


def create_database(settings: DatabaseSettings) -> Database:
    """Compose a PostgreSQL engine without logging bound query parameters."""
    engine = create_async_engine(
        settings.url,
        pool_pre_ping=True,
        hide_parameters=True,
        isolation_level="READ COMMITTED",
        connect_args={"timeout": 5},
    )
    return Database(engine)

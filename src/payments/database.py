"""Async database resource composition with explicit session ownership."""

from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from payments.settings import DatabaseSettings


class Database:
    """Own an injected engine and create a separate session for each unit of work.

    Callers explicitly open transactions with session.begin(); sessions do not
    commit implicitly. Close the database when its owning process shuts down.
    """

    def __init__(self, engine: AsyncEngine) -> None:
        """Bind session creation to the supplied async engine."""
        self.engine = engine
        self.sessions = async_sessionmaker(engine, expire_on_commit=False)

    async def close(self) -> None:
        """Release pooled connections owned by this database resource."""
        await self.engine.dispose()


def create_database(settings: DatabaseSettings) -> Database:
    """Compose a PostgreSQL engine without logging bound query parameters."""
    engine = create_async_engine(
        settings.url,
        pool_pre_ping=True,
        hide_parameters=True,
        connect_args={"timeout": 5},
    )
    return Database(engine)

"""Separate outbox process: compose resources, poll, and shut down cleanly."""

import asyncio
import logging
import signal
from contextlib import suppress

from sqlalchemy.exc import SQLAlchemyError

from payments.broker import RabbitEventPublisher, create_broker
from payments.database import DatabaseUnavailable, create_database
from payments.logging_config import configure_logging
from payments.outbox import OutboxRelay
from payments.settings import BrokerSettings, DatabaseSettings, RelaySettings

logger = logging.getLogger(__name__)


async def run_relay(relay: OutboxRelay, stop: asyncio.Event, poll_interval: float) -> None:
    """Drain due events and back off when idle or when the database is unavailable."""
    while not stop.is_set():
        try:
            worked = await relay.publish_next()
        except (DatabaseUnavailable, SQLAlchemyError, OSError, TimeoutError) as error:
            logger.error("Outbox storage unavailable: %s", type(error).__name__)
            worked = False
        if not worked:
            with suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=poll_interval)


async def main() -> None:
    """Own resources and honor SIGTERM on Unix and Ctrl+C on Windows."""
    settings = RelaySettings()
    database_settings = DatabaseSettings()
    broker_settings = BrokerSettings()
    database = create_database(database_settings)
    broker = create_broker(broker_settings)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    with suppress(NotImplementedError):
        loop.add_signal_handler(signal.SIGTERM, stop.set)
    try:
        relay = OutboxRelay(database, RabbitEventPublisher(broker), settings.relay_publish_timeout)
        await run_relay(relay, stop, settings.relay_poll_interval)
    finally:
        try:
            await broker.stop()
        finally:
            await database.close()
        with suppress(NotImplementedError):
            loop.remove_signal_handler(signal.SIGTERM)


if __name__ == "__main__":
    configure_logging()
    with suppress(KeyboardInterrupt):
        asyncio.run(main())

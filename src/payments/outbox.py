"""Durable outbox relay using one bounded publication per locked transaction."""

import asyncio
import logging
from datetime import timedelta
from typing import Protocol

from sqlalchemy import func, select

from payments.database import Database
from payments.domain import Publication, PublicationError
from payments.models import OutboxEvent

logger = logging.getLogger(__name__)


def publication_backoff(attempt: int) -> timedelta:
    """Schedule unbounded retries at 2, 4, 8, 16, 32, then at most 60 seconds."""
    return timedelta(seconds=min(2 ** min(attempt, 6), 60))


class EventPublisher(Protocol):
    """Report success only after routing and positive broker confirmation."""

    async def publish(self, event: Publication) -> None:
        """Confirm delivery, or raise PublicationError; cancellation must propagate."""
        ...


class OutboxRelay:
    """Publish one due event with injected database and broker dependencies.

    A row lock prevents simultaneous relay instances publishing the same event.
    The transaction stays open during a bounded network call. A process crash
    after confirmation but before commit can repeat publication with the same ID.
    """

    def __init__(
        self, database: Database, publisher: EventPublisher, publish_timeout: float = 5
    ) -> None:
        """Borrow resources and bound the time spent waiting while holding a lock."""
        self._database = database
        self._publisher = publisher
        self._publish_timeout = publish_timeout

    async def publish_next(self) -> bool:
        """Commit a confirmed publication or durable retry; return False if idle.

        Database errors and cancellation propagate after transaction rollback.
        Publication failures are retained with sanitized error types and a due date.
        """
        # ponytail: one bounded network-held row lock; leased claims if throughput requires it.
        async with self._database.sessions() as session, session.begin():
            event = await session.scalar(
                select(OutboxEvent)
                .where(
                    OutboxEvent.published_at.is_(None),
                    OutboxEvent.available_at <= func.clock_timestamp(),
                )
                .order_by(OutboxEvent.available_at, OutboxEvent.id)
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            if event is None:
                return False
            publication = Publication(
                event_id=event.id,
                payment_id=event.payment_id,
                event_type=event.event_type,
                destination=event.destination,
                payload=event.payload,
            )
            event.publish_attempts += 1
            try:
                async with asyncio.timeout(self._publish_timeout):
                    await self._publisher.publish(publication)
            except (PublicationError, TimeoutError) as error:
                event.last_error = type(error).__name__
                event.available_at = func.clock_timestamp() + publication_backoff(
                    event.publish_attempts
                )
                logger.warning("Outbox event %s deferred: %s", event.id, event.last_error)
            else:
                event.published_at = func.clock_timestamp()
                event.last_error = None
            return True

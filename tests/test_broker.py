"""AMQP adapter contract checks; mocks do not prove real broker durability."""

from typing import cast
from unittest.mock import AsyncMock, patch
from uuid import uuid7

import pytest
from faststream.rabbit import RabbitBroker
from pamqp.commands import Basic
from pydantic import SecretStr

from payments.broker import DLQ_EXCHANGE, RabbitEventPublisher, create_broker
from payments.domain import DEAD_LETTER_QUEUE, NEW_PAYMENTS_QUEUE, Publication, PublicationError
from payments.settings import BrokerSettings


def publication(destination: str = NEW_PAYMENTS_QUEUE) -> Publication:
    """Build an event with separate stable AMQP and payment identities."""
    event_id = uuid7()
    return Publication(
        event_id, uuid7(), "payment.created", destination, {"event_id": str(event_id)}
    )


@pytest.mark.parametrize("destination", [NEW_PAYMENTS_QUEUE, DEAD_LETTER_QUEUE])
async def test_persistent_mandatory_confirmed_publication(destination: str) -> None:
    """Set durability/routing flags, preserve IDs, and use a separate DLQ exchange."""
    broker = AsyncMock(spec=RabbitBroker)
    broker.publish.return_value = Basic.Ack()
    event = publication(destination)
    await RabbitEventPublisher(cast(RabbitBroker, broker)).publish(event)
    broker.publish.assert_awaited_once_with(
        event.payload,
        queue=destination,
        exchange=DLQ_EXCHANGE if destination == DEAD_LETTER_QUEUE else None,
        mandatory=True,
        persist=True,
        timeout=5,
        message_id=str(event.event_id),
        correlation_id=str(event.payment_id),
        message_type=event.event_type,
    )
    queues = [call.args[0] for call in broker.declare_queue.await_args_list]
    assert {queue.name for queue in queues} == {NEW_PAYMENTS_QUEUE, DEAD_LETTER_QUEUE}
    assert all(queue.durable for queue in queues)


@pytest.mark.parametrize("confirmation", [None, Basic.Nack(), Basic.Reject()])
async def test_missing_positive_confirmation_is_failure(confirmation: object) -> None:
    """Reject absent/negative confirmations instead of marking an outbox event published."""
    broker = AsyncMock(spec=RabbitBroker)
    broker.publish.return_value = confirmation
    with pytest.raises(PublicationError, match="Positive publisher confirmation"):
        await RabbitEventPublisher(cast(RabbitBroker, broker)).publish(publication())


async def test_connection_failure_is_sanitized() -> None:
    """Hide connection details from the adapter's public failure contract."""
    broker = AsyncMock(spec=RabbitBroker)
    broker.connect.side_effect = ConnectionError("amqp://user:secret@host")
    with pytest.raises(PublicationError, match="^ConnectionError$"):
        await RabbitEventPublisher(cast(RabbitBroker, broker)).publish(publication())
    broker.publish.assert_not_awaited()


def test_broker_composition_requires_confirms_and_return_errors() -> None:
    """Prevent silently dropping an unroutable message even when RabbitMQ ACKs it."""
    with patch("payments.broker.RabbitBroker") as constructor:
        create_broker(
            BrokerSettings(
                broker_user="test",
                broker_password=SecretStr("test-only-broker-password"),
                _env_file=None,
            )
        )
    channel = constructor.call_args.kwargs["default_channel"]
    assert channel.publisher_confirms
    assert channel.on_return_raises

"""Transport acknowledgement contracts and malformed JSON through FastStream decoding."""

import json
from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock
from uuid import uuid7

import pytest
from faststream.rabbit import TestRabbitBroker
from faststream.rabbit.message import RabbitMessage
from pydantic import SecretStr

from payments.application.processing import PaymentProcessor
from payments.core.domain import InvalidWorkflow
from payments.core.settings import BrokerSettings
from payments.infrastructure.broker import create_broker
from payments.workers.consumer import configure_consumer, handle_message


def message(body: bytes | None = None) -> AsyncMock:
    """Build an acknowledgement spy with a real raw body and optional AMQP identity."""
    resource = AsyncMock(spec=RabbitMessage)
    resource.body = (
        body
        if body is not None
        else json.dumps(
            {
                "event_id": str(uuid7()),
                "payment_id": str(uuid7()),
                "stage": "processing",
                "attempt": 1,
            }
        ).encode()
    )
    resource.raw_message = SimpleNamespace(message_id=None)
    return resource


async def test_ack_only_after_durable_handler_returns() -> None:
    """Assert transport order instead of treating merely invoked business logic as committed."""
    resource = message()
    processor = AsyncMock(spec=PaymentProcessor)

    async def persisted(*args: object) -> None:
        """Verify acknowledgement has not occurred before the transaction finishes."""
        resource.ack.assert_not_awaited()

    processor.handle.side_effect = persisted
    await handle_message(cast(RabbitMessage, resource), cast(PaymentProcessor, processor), 0.001)
    processor.handle.assert_awaited_once()
    resource.ack.assert_awaited_once()
    resource.nack.assert_not_awaited()


@pytest.mark.parametrize("failure", [OSError("DB unreachable"), RuntimeError("unknown bug")])
async def test_database_or_unknown_error_nacks(failure: Exception) -> None:
    """Keep the message recoverable instead of consuming a business retry or losing it."""
    resource, processor = message(), AsyncMock(spec=PaymentProcessor)
    processor.handle.side_effect = failure
    await handle_message(cast(RabbitMessage, resource), cast(PaymentProcessor, processor), 0.001)
    resource.nack.assert_awaited_once_with(requeue=True)
    resource.ack.assert_not_awaited()


async def test_invalid_source_commits_dlq_before_ack() -> None:
    """Sanitize a validated message with an unknown persisted workflow identity."""
    resource, processor = message(), AsyncMock(spec=PaymentProcessor)
    processor.handle.side_effect = InvalidWorkflow("untrusted details")
    await handle_message(cast(RabbitMessage, resource), cast(PaymentProcessor, processor), 0.001)
    processor.record_invalid.assert_awaited_once_with(
        resource.body, "", "invalid_workflow", processor.handle.call_args.args[0].payment_id
    )
    resource.ack.assert_awaited_once()


@pytest.mark.parametrize("body", [b"invalid JSON", b"{}", b"x" * 4097])
async def test_malformed_dlq_storage_failure_keeps_message(body: bytes) -> None:
    """Do not acknowledge malformed messages until their dead-letter intent is durable."""
    resource, processor = message(body), AsyncMock(spec=PaymentProcessor)
    processor.record_invalid.side_effect = OSError("DB unreachable")
    await handle_message(cast(RabbitMessage, resource), cast(PaymentProcessor, processor), 0.001)
    processor.handle.assert_not_awaited()
    resource.ack.assert_not_awaited()
    resource.nack.assert_awaited_once_with(requeue=True)


async def test_faststream_raw_decoder_reaches_handler_with_bad_json() -> None:
    """Prevent SDK JSON decoding from discarding malformed payloads before DLQ persistence."""
    broker = create_broker(
        BrokerSettings(
            broker_user="test",
            broker_password=SecretStr("test-only-password"),
            _env_file=None,
        )
    )
    processor = AsyncMock(spec=PaymentProcessor)
    configure_consumer(broker, cast(PaymentProcessor, processor), 0.001)
    async with TestRabbitBroker(broker):
        await broker.publish(
            b'{"password":"private",broken-json',
            queue="payments.new",
            content_type="application/json",
            message_id="invalid-test",
        )
    processor.handle.assert_not_awaited()
    processor.record_invalid.assert_awaited_once_with(
        b'{"password":"private",broken-json', "invalid-test", "malformed_message"
    )

"""Single RabbitMQ consumer with manual acknowledgement after durable workflow handling."""

import asyncio
import logging
from contextlib import suppress

import httpx
from faststream import AckPolicy, FastStream
from faststream.rabbit import Channel, RabbitBroker, RabbitMessage
from faststream.rabbit.message import RabbitMessage as RawRabbitMessage
from pydantic import ValidationError

from payments.adapters import EmulatedGateway, HttpWebhookSender
from payments.broker import PAYMENTS_QUEUE, create_broker
from payments.database import create_database
from payments.domain import InvalidWorkflow
from payments.processing import PaymentProcessor
from payments.schemas import WorkflowEnvelope
from payments.services import WebhookPolicy
from payments.settings import BrokerSettings, ConsumerSettings, DatabaseSettings, WebhookSettings
from payments.workflow_repository import WorkflowRepository

logger = logging.getLogger(__name__)


def raw_decoder(message: RawRabbitMessage) -> bytes:
    """Preserve invalid JSON bytes until the handler can durably record a DLQ intent."""
    return message.body


async def handle_message(
    message: RawRabbitMessage, processor: PaymentProcessor, requeue_delay: float
) -> None:
    """ACK only durable outcomes; on storage/unknown errors back off and requeue.

    Cancellation leaves acknowledgement to connection recovery. Untrusted bodies
    and validation messages are excluded from logs and dead-letter payloads.
    """
    try:
        await _dispatch_body(message, processor)
    except Exception as error:
        # At the transport boundary, unknown errors must leave work recoverable.
        logger.error("Payment message deferred: %s", type(error).__name__)
        await asyncio.sleep(requeue_delay)
        await message.nack(requeue=True)
    else:
        await message.ack()


async def _dispatch_body(message: RawRabbitMessage, processor: PaymentProcessor) -> None:
    """Distinguish malformed envelopes from validated but unknown workflow sources."""
    source_id = message.raw_message.message_id or ""
    if len(message.body) > 4096:
        await processor.record_invalid(message.body, source_id, "malformed_message")
        return
    try:
        event = WorkflowEnvelope.model_validate_json(message.body).to_event()
    except ValidationError:
        await processor.record_invalid(message.body, source_id, "malformed_message")
        return
    try:
        await processor.handle(event)
    except InvalidWorkflow:
        await processor.record_invalid(
            message.body, source_id, "invalid_workflow", event.payment_id
        )


def configure_consumer(
    broker: RabbitBroker, processor: PaymentProcessor, requeue_delay: float
) -> None:
    """Register one subscriber with prefetch one and explicit raw decoding/ACKs."""

    @broker.subscriber(
        PAYMENTS_QUEUE,
        channel=Channel(prefetch_count=1),
        ack_policy=AckPolicy.MANUAL,
        decoder=raw_decoder,
        no_reply=True,
    )
    async def consume(message: RabbitMessage) -> None:
        """Invoke the injected use case through the recoverable transport boundary."""
        await handle_message(message, processor, requeue_delay)


async def main() -> None:
    """Compose one gateway, HTTP client, store, and consumer; close owned resources."""
    settings = ConsumerSettings()
    database_settings = DatabaseSettings()
    broker_settings = BrokerSettings()
    policy = WebhookPolicy(WebhookSettings().webhook_allowed_origins)
    database = create_database(database_settings)
    broker = create_broker(broker_settings)
    try:
        async with httpx.AsyncClient(timeout=settings.webhook_timeout, trust_env=False) as client:
            processor = PaymentProcessor(
                WorkflowRepository(database),
                EmulatedGateway(),
                HttpWebhookSender(client, policy),
                settings.processing_timeout,
                settings.webhook_timeout,
            )
            configure_consumer(broker, processor, settings.consumer_requeue_delay)
            await FastStream(broker).run()
    finally:
        try:
            await broker.stop()
        finally:
            await database.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    with suppress(KeyboardInterrupt):
        asyncio.run(main())

"""RabbitMQ publication adapter with durable topology and explicit confirmations."""

from aio_pika.exceptions import CONNECTION_EXCEPTIONS
from faststream.rabbit import Channel, RabbitBroker, RabbitExchange, RabbitQueue
from faststream.security import SASLPlaintext
from pamqp.commands import Basic

from payments.domain import (
    DEAD_LETTER_EXCHANGE,
    DEAD_LETTER_QUEUE,
    NEW_PAYMENTS_QUEUE,
    Publication,
    PublicationError,
)
from payments.settings import BrokerSettings

PAYMENTS_QUEUE = RabbitQueue(NEW_PAYMENTS_QUEUE, durable=True, timeout=5)
DLQ_QUEUE = RabbitQueue(DEAD_LETTER_QUEUE, durable=True, timeout=5)
DLQ_EXCHANGE = RabbitExchange(DEAD_LETTER_EXCHANGE, durable=True, timeout=5)


def create_broker(settings: BrokerSettings) -> RabbitBroker:
    """Compose a reconnecting broker without constructing credential-bearing URLs."""
    return RabbitBroker(
        host=settings.broker_host,
        port=settings.broker_port,
        virtualhost=settings.broker_vhost,
        security=SASLPlaintext(
            username=settings.broker_user,
            password=settings.broker_password.get_secret_value(),
        ),
        timeout=5,
        fail_fast=True,
        default_channel=Channel(publisher_confirms=True, on_return_raises=True),
        logger=None,
    )


class RabbitEventPublisher:
    """Send persistent mandatory messages and require a positive broker ACK.

    Failed routing raises through on_return_raises even if the broker ACKs the
    unroutable publication. Connection setup is lazy so downtime can be recorded
    by the relay instead of preventing the worker from starting.
    """

    def __init__(self, broker: RabbitBroker) -> None:
        """Borrow a broker whose process composition root owns shutdown."""
        self._broker = broker

    async def publish(self, event: Publication) -> None:
        """Declare durable routes and positively confirm a stable event identity.

        Raises:
            PublicationError: Connection, routing, or confirmation failed.
        """
        if event.destination not in {NEW_PAYMENTS_QUEUE, DEAD_LETTER_QUEUE}:
            raise PublicationError("Unsupported outbox destination")
        try:
            await self._broker.connect()
            await self._broker.declare_queue(PAYMENTS_QUEUE)
            exchange = await self._broker.declare_exchange(DLQ_EXCHANGE)
            queue = await self._broker.declare_queue(DLQ_QUEUE)
            await queue.bind(exchange, routing_key=DEAD_LETTER_QUEUE, timeout=5)
            confirmation = await self._broker.publish(
                event.payload,
                queue=event.destination,
                exchange=DLQ_EXCHANGE if event.destination == DEAD_LETTER_QUEUE else None,
                mandatory=True,
                persist=True,
                timeout=5,
                message_id=str(event.event_id),
                correlation_id=str(event.payment_id or event.event_id),
                message_type=event.event_type,
            )
        except (*CONNECTION_EXCEPTIONS, TimeoutError) as error:
            raise PublicationError(type(error).__name__) from error
        if not isinstance(confirmation, Basic.Ack):
            raise PublicationError("Positive publisher confirmation was not received")

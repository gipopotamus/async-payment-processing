"""Opt-in acceptance with real RabbitMQ, PostgreSQL, HTTP, and isolated virtual hosts."""

import asyncio
import json
import os
from collections.abc import AsyncIterator
from urllib.parse import quote
from uuid import uuid4, uuid7

import httpx
import pytest
import uvicorn
from aio_pika import DeliveryMode
from aio_pika.exceptions import DeliveryError
from pydantic import SecretStr
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from examples.receiver import create_receiver
from payments.api.app import create_app
from payments.application.processing import PaymentProcessor
from payments.application.services import WebhookPolicy
from payments.core.domain import DEAD_LETTER_QUEUE, NEW_PAYMENTS_QUEUE, Publication, WorkflowStage
from payments.core.settings import BrokerSettings, Settings
from payments.infrastructure.adapters import EmulatedGateway, HttpWebhookSender
from payments.infrastructure.broker import RabbitEventPublisher, create_broker
from payments.infrastructure.database import Database
from payments.infrastructure.models import OutboxEvent, Payment
from payments.infrastructure.outbox import OutboxRelay
from payments.infrastructure.workflow_repository import WorkflowRepository
from payments.workers.consumer import configure_consumer
from payments.workers.outbox import run_relay
from payments.workers.schemas import WorkflowEnvelope

pytestmark = [pytest.mark.integration, pytest.mark.broker_integration]


@pytest.fixture
async def live_broker() -> AsyncIterator[BrokerSettings]:
    """Create and remove only a unique vhost when explicit management access is supplied."""
    management = os.environ.get("PAYMENTS_TEST_BROKER_MANAGEMENT_URL")
    if management is None:
        pytest.skip("Set PAYMENTS_TEST_BROKER_MANAGEMENT_URL for live RabbitMQ acceptance")
    settings = BrokerSettings()
    name = f"payments_{uuid4().hex}_test"
    path = quote(name, safe="")
    async with httpx.AsyncClient(
        base_url=management,
        trust_env=False,
        timeout=5,
        auth=(settings.broker_user, settings.broker_password.get_secret_value()),
    ) as admin:
        (await admin.put(f"/api/vhosts/{path}")).raise_for_status()
        try:
            (
                await admin.put(
                    f"/api/permissions/{path}/{quote(settings.broker_user, safe='')}",
                    json={"configure": ".*", "write": ".*", "read": ".*"},
                )
            ).raise_for_status()
            yield settings.model_copy(update={"broker_vhost": name})
        finally:
            (await admin.delete(f"/api/vhosts/{path}")).raise_for_status()


async def test_real_durable_routing_and_unroutable_return(live_broker: BrokerSettings) -> None:
    """Receive durable confirmed messages on both routes and reject an unroutable publish."""
    broker = create_broker(live_broker)
    try:
        for destination in [NEW_PAYMENTS_QUEUE, DEAD_LETTER_QUEUE]:
            event_id = uuid7()
            publication = Publication(
                event_id, None, "acceptance.test", destination, {"event_id": str(event_id)}
            )
            await RabbitEventPublisher(broker).publish(publication)
            connection = await broker.connect()
            async with await connection.channel() as channel:
                queue = await channel.declare_queue(destination, durable=True)
                message = await queue.get(timeout=3)
                assert message is not None
                assert message.delivery_mode == DeliveryMode.PERSISTENT
                assert message.message_id == str(event_id)
                assert json.loads(message.body) == publication.payload
                await message.ack()
        with pytest.raises(DeliveryError):
            await broker.publish(
                {"test": True}, queue="payments.missing", mandatory=True, timeout=3
            )
    finally:
        await broker.stop()


async def test_real_confirmation_before_failed_commit(
    live_broker: BrokerSettings, database: Database
) -> None:
    """Receive the same event ID twice after real confirmation precedes a rolled-back update."""
    event_id = uuid7()
    async with database.sessions() as session, session.begin():
        session.add(
            OutboxEvent(
                id=event_id,
                event_type="acceptance.test",
                destination=NEW_PAYMENTS_QUEUE,
                payload={"event_id": str(event_id)},
            )
        )
    broker = create_broker(live_broker)
    try:
        relay = OutboxRelay(database, RabbitEventPublisher(broker))
        async with database.engine.begin() as database_connection:
            await database_connection.exec_driver_sql(
                "ALTER TABLE outbox ADD CONSTRAINT fail_live_commit CHECK (published_at IS NULL)"
            )
        with pytest.raises(IntegrityError):
            await relay.publish_next()
        async with database.engine.begin() as database_connection:
            await database_connection.exec_driver_sql(
                "ALTER TABLE outbox DROP CONSTRAINT fail_live_commit"
            )
        assert await relay.publish_next()
        connection = await broker.connect()
        async with await connection.channel() as channel:
            queue = await channel.declare_queue(NEW_PAYMENTS_QUEUE, durable=True)
            for _ in range(2):
                message = await queue.get(timeout=3)
                assert message is not None
                assert message.message_id == str(event_id)
                await message.ack()
        async with database.sessions() as session:
            stored = await session.get(OutboxEvent, event_id)
            assert stored is not None and stored.published_at is not None
            assert stored.publish_attempts == 1
    finally:
        await broker.stop()


async def test_live_api_relay_consumer_http_and_malformed_dlq(
    live_broker: BrokerSettings, database: Database
) -> None:
    """Exercise the whole pipeline with two HTTP failures, stable retries, and invalid JSON."""
    import socket

    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
        origin = f"http://127.0.0.1:{port}"
        server = uvicorn.Server(
            uvicorn.Config(create_receiver(2), log_level="error", lifespan="off")
        )
        server_task = asyncio.create_task(server.serve(sockets=[listener]))
        publisher = create_broker(live_broker)
        consumer = create_broker(live_broker)
        stop = asyncio.Event()
        relay_task: asyncio.Task[None] | None = None
        try:
            async with asyncio.timeout(3):
                while not server.started:
                    if server_task.done():
                        await server_task
                    await asyncio.sleep(0.01)
            async with httpx.AsyncClient(timeout=2, trust_env=False) as http:
                processor = PaymentProcessor(
                    WorkflowRepository(database),
                    EmulatedGateway(),
                    HttpWebhookSender(http, WebhookPolicy(frozenset({origin}))),
                )
                configure_consumer(consumer, processor, 0.01)
                await consumer.start()
                relay_task = asyncio.create_task(
                    run_relay(OutboxRelay(database, RabbitEventPublisher(publisher)), stop, 0.02)
                )
                app = create_app(
                    Settings(
                        api_key=SecretStr("acceptance-only-api-key"),
                        webhook_allowed_origins=frozenset({origin}),
                        _env_file=None,
                    ),
                    database,
                )
                async with (
                    app.router.lifespan_context(app),
                    httpx.AsyncClient(
                        transport=httpx.ASGITransport(app=app),
                        base_url="http://api",
                        headers={
                            "X-API-Key": "acceptance-only-api-key",
                            "Idempotency-Key": "live-order",
                        },
                    ) as api,
                ):
                    created = await api.post(
                        "/api/v1/payments",
                        json={
                            "amount": "125.50",
                            "currency": "RUB",
                            "webhook_url": f"{origin}/callback",
                        },
                    )
                    assert created.status_code == 202
                    payment_id = created.json()["payment_id"]
                    async with asyncio.timeout(25):
                        while True:
                            details = (await api.get(f"/api/v1/payments/{payment_id}")).json()
                            if details["webhook_status"] == "delivered":
                                break
                            await asyncio.sleep(0.05)
                    assert details["status"] in {"succeeded", "failed"}
                    assert details["webhook_attempts"] == 3
                    receipts = (await http.get(f"{origin}/receipts")).json()
                    assert len(receipts) == 1 and receipts[0]["attempts"] == 3
                    assert receipts[0]["accepted"]
                    async with database.sessions() as session:
                        source = await session.scalar(
                            select(OutboxEvent).where(
                                OutboxEvent.payload["stage"].astext
                                == WorkflowStage.PROCESSING.value,
                            )
                        )
                        assert source is not None
                        duplicate = WorkflowEnvelope.model_validate(source.payload).to_event()
                    await publisher.publish(
                        duplicate.payload(),
                        queue=NEW_PAYMENTS_QUEUE,
                        message_id=str(duplicate.event_id),
                        persist=True,
                    )
                    await publisher.publish(
                        b'{"private":"secret",invalid',
                        queue=NEW_PAYMENTS_QUEUE,
                        content_type="application/json",
                        message_id="invalid-test",
                    )
                    async with asyncio.timeout(5):
                        while True:
                            async with database.sessions() as session:
                                dead = await session.scalar(
                                    select(OutboxEvent).where(
                                        OutboxEvent.destination == DEAD_LETTER_QUEUE,
                                        OutboxEvent.published_at.is_not(None),
                                    )
                                )
                            if dead is not None:
                                break
                            await asyncio.sleep(0.02)
                    assert dead.payload["reason"] == "malformed_message"
                    assert "secret" not in str(dead.payload)
                    connection = await publisher.connect()
                    async with await connection.channel() as channel:
                        queue = await channel.declare_queue(DEAD_LETTER_QUEUE, durable=True)
                        message = await queue.get(timeout=3)
                        assert message is not None
                        assert json.loads(message.body) == dead.payload
                        await message.ack()
                    async with database.sessions() as session:
                        payment = await session.scalar(select(Payment))
                        assert payment is not None and payment.processing_attempts == 1
                        assert await session.scalar(select(func.count()).select_from(Payment)) == 1
                    assert (await http.get(f"{origin}/receipts")).json()[0]["attempts"] == 3
        finally:
            stop.set()
            if relay_task is not None:
                await asyncio.wait_for(relay_task, timeout=6)
            await consumer.stop()
            await publisher.stop()
            server.should_exit = True
            await asyncio.wait_for(server_task, timeout=3)

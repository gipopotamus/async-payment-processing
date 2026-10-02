"""Stable emulator behavior and HTTP trust-boundary contracts without external services."""

import json
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import httpx
import pytest

from payments.adapters import EmulatedGateway, HttpWebhookSender, gateway_outcome
from payments.domain import Currency, PaymentSnapshot, PaymentStatus, WebhookError, WebhookStatus
from payments.processing import webhook_event_id
from payments.services import WebhookPolicy


def terminal_payment() -> PaymentSnapshot:
    """Build a committed terminal snapshot with fixed identity and timestamps."""
    return PaymentSnapshot(
        payment_id=UUID(int=42),
        amount=Decimal("125.50"),
        currency=Currency.RUB,
        description="Order",
        metadata={},
        status=PaymentStatus.SUCCEEDED,
        idempotency_key="order-42",
        webhook_url="http://receiver.test/callback",
        created_at=datetime(2026, 10, 2, tzinfo=UTC),
        processed_at=datetime(2026, 10, 2, tzinfo=UTC),
        webhook_status=WebhookStatus.PENDING,
        webhook_attempts=0,
        webhook_delivered_at=None,
        request_hash="a" * 64,
    )


async def test_gateway_stable_result_and_emulated_delay() -> None:
    """Keep the 90/10 hash partition stable and preserve 2-5-second waits across instances."""
    delays: list[float] = []

    async def recorded_sleep(delay: float) -> None:
        """Record requested production latency while letting the test finish immediately."""
        delays.append(delay)

    outcomes = [gateway_outcome(UUID(int=index)) for index in range(1, 1001)]
    assert all(2 <= delay <= 5 for _, delay in outcomes)
    assert 50 <= sum(status == PaymentStatus.FAILED for status, _ in outcomes) <= 150
    payment = terminal_payment()
    first = await EmulatedGateway(recorded_sleep).process(payment)
    second = await EmulatedGateway(recorded_sleep).process(payment)
    assert first == second == gateway_outcome(payment.payment_id)[0]
    assert delays[0] == delays[1]


async def test_webhook_payload_identity_and_no_api_key() -> None:
    """Send precise money and a stable deduplication ID without incoming credentials."""
    requests: list[httpx.Request] = []

    def receiver(request: httpx.Request) -> httpx.Response:
        """Accept the delivery without requiring a response body."""
        requests.append(request)
        return httpx.Response(204)

    payment = terminal_payment()
    identity = webhook_event_id(payment.payment_id)
    policy = WebhookPolicy(frozenset({"http://receiver.test"}))
    async with httpx.AsyncClient(transport=httpx.MockTransport(receiver)) as client:
        sender = HttpWebhookSender(client, policy)
        await sender.send(payment, identity)
        await sender.send(payment, webhook_event_id(payment.payment_id))
    assert len(requests) == 2
    assert requests[0].content == requests[1].content
    assert requests[0].headers["Idempotency-Key"] == str(identity)
    assert "X-API-Key" not in requests[0].headers
    body = json.loads(requests[0].content)
    assert body["event_id"] == str(identity)
    assert body["amount"] == "125.50"
    assert body["status"] == "succeeded"
    assert payment.processed_at is not None
    assert body["processed_at"] == payment.processed_at.isoformat()


@pytest.mark.parametrize("status", [301, 302, 400, 500])
async def test_webhook_non_success_and_redirects(status: int) -> None:
    """Fail non-2xx statuses and override even an injected redirect-following client."""
    requests: list[httpx.Request] = []

    def receiver(request: httpx.Request) -> httpx.Response:
        """Point redirects outside the allowlist to detect an accidental follow-up request."""
        requests.append(request)
        return httpx.Response(status, headers={"Location": "http://untrusted.test/secret"})

    policy = WebhookPolicy(frozenset({"http://receiver.test"}))
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(receiver), follow_redirects=True
    ) as client:
        with pytest.raises(WebhookError, match=f"^HTTP {status}$"):
            await HttpWebhookSender(client, policy).send(terminal_payment(), UUID(int=1))
    assert len(requests) == 1


async def test_blocked_callback_and_sanitized_network_error() -> None:
    """Reject disallowed destinations before I/O and avoid propagating client error URLs."""
    requests: list[httpx.Request] = []

    def unavailable(request: httpx.Request) -> httpx.Response:
        """Raise a client failure containing sensitive text to test sanitization."""
        requests.append(request)
        raise httpx.ConnectError("sensitive URL and credentials", request=request)

    policy = WebhookPolicy(frozenset({"http://receiver.test"}))
    async with httpx.AsyncClient(transport=httpx.MockTransport(unavailable)) as client:
        sender = HttpWebhookSender(client, policy)
        with pytest.raises(WebhookError, match="^Blocked callback origin$"):
            await sender.send(
                replace(terminal_payment(), webhook_url="http://untrusted.test/"), UUID(int=1)
            )
        assert requests == []
        with pytest.raises(WebhookError, match="^ConnectError$"):
            await sender.send(terminal_payment(), UUID(int=1))
    assert len(requests) == 1

"""Idempotent gateway emulator and bounded, allowlisted HTTP webhook delivery."""

import asyncio
from collections.abc import Awaitable, Callable
from hashlib import sha256
from uuid import UUID

import httpx

from payments.domain import (
    AMOUNT_SCALE,
    InvalidWebhook,
    PaymentSnapshot,
    PaymentStatus,
    WebhookError,
)
from payments.services import WebhookPolicy


def gateway_outcome(payment_id: UUID) -> tuple[PaymentStatus, float]:
    """Derive a stable 90/10 outcome and 2-5s delay from a uniformly hashed identity."""
    digest = sha256(payment_id.bytes).digest()
    status = (
        PaymentStatus.FAILED if int.from_bytes(digest[:8]) % 10 == 0 else PaymentStatus.SUCCEEDED
    )
    delay = 2 + 3 * int.from_bytes(digest[8:16]) / (2**64 - 1)
    return status, delay


class EmulatedGateway:
    """Return the same business outcome for an operation ID across retries/restarts."""

    def __init__(self, sleep: Callable[[float], Awaitable[None]] = asyncio.sleep) -> None:
        """Inject waiting for fast tests without changing production delay semantics."""
        self._sleep = sleep

    async def process(self, payment: PaymentSnapshot) -> PaymentStatus:
        """Emulate gateway latency and a terminal business success/decline."""
        status, delay = gateway_outcome(payment.payment_id)
        await self._sleep(delay)
        return status


class HttpWebhookSender:
    """Send only to allowed origins and accept 2xx responses without following redirects."""

    def __init__(self, client: httpx.AsyncClient, policy: WebhookPolicy) -> None:
        """Borrow one process-scoped client and revalidate destinations on every attempt."""
        self._client = client
        self._policy = policy

    async def send(self, payment: PaymentSnapshot, event_id: UUID) -> None:
        """Deliver a terminal result without forwarding API credentials or reading bodies.

        Raises:
            WebhookError: The URL is blocked or the HTTP operation did not succeed.
        """
        try:
            self._policy.validate(payment.webhook_url)
        except InvalidWebhook as error:
            raise WebhookError("Blocked callback origin") from error
        payload = {
            "event_id": str(event_id),
            "payment_id": str(payment.payment_id),
            "status": payment.status.value,
            "amount": format(payment.amount, f".{AMOUNT_SCALE}f"),
            "currency": payment.currency.value,
            "created_at": payment.created_at.isoformat(),
            "processed_at": payment.processed_at.isoformat() if payment.processed_at else None,
        }
        try:
            async with self._client.stream(
                "POST",
                payment.webhook_url,
                json=payload,
                follow_redirects=False,
                headers={"Idempotency-Key": str(event_id)},
            ) as response:
                if not 200 <= response.status_code < 300:
                    raise WebhookError(f"HTTP {response.status_code}")
        except httpx.HTTPError as error:
            raise WebhookError(type(error).__name__) from error

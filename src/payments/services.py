"""Payment use cases and narrow persistence contracts, independent of HTTP and ORM."""

import json
from dataclasses import asdict
from hashlib import sha256
from typing import Protocol
from urllib.parse import urlsplit
from uuid import UUID

from payments.domain import (
    AMOUNT_SCALE,
    IdempotencyConflict,
    InvalidWebhook,
    NewPayment,
    PaymentNotFound,
    PaymentSnapshot,
)

type Origin = tuple[str, str, int]


def webhook_origin(url: str) -> Origin:
    """Extract an HTTP origin while rejecting embedded credentials and fragments."""
    parsed = urlsplit(url)
    if (
        parsed.scheme not in {"http", "https"}
        or parsed.hostname is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        raise InvalidWebhook("Webhook requires an HTTP(S) URL without credentials or a fragment")
    try:
        port = parsed.port
    except ValueError as error:
        raise InvalidWebhook("Webhook has an invalid port") from error
    if port == 0:
        raise InvalidWebhook("Webhook has an invalid port")
    return (
        parsed.scheme,
        parsed.hostname.lower(),
        port if port is not None else (443 if parsed.scheme == "https" else 80),
    )


class WebhookPolicy:
    """Allow callbacks only to explicitly configured scheme/host/port origins.

    This validates admission. The delivery adapter must also disable redirects,
    and deployment must restrict egress to address DNS-based SSRF bypasses.
    """

    def __init__(self, allowed_origins: frozenset[str]) -> None:
        """Validate configuration and precompute origins once at composition time."""
        origins: set[Origin] = set()
        for value in allowed_origins:
            parsed = urlsplit(value)
            if parsed.path not in {"", "/"} or parsed.query:
                raise InvalidWebhook("Allowed webhook origins must not include paths or queries")
            origins.add(webhook_origin(value))
        self._origins = frozenset(origins)

    def validate(self, url: str) -> None:
        """Reject callbacks to any origin absent from the configured allowlist."""
        if webhook_origin(url) not in self._origins:
            raise InvalidWebhook("Webhook origin is not allowed")


class PaymentStore(Protocol):
    """Define atomic creation and lookup without leaking persistence objects."""

    async def create_or_get(
        self, command: NewPayment, idempotency_key: str, request_hash: str
    ) -> PaymentSnapshot:
        """Commit one payment/event pair or return the existing payment for this key."""
        ...

    async def get(self, payment_id: UUID) -> PaymentSnapshot | None:
        """Return a fully loaded snapshot, or None if the payment does not exist."""
        ...


def request_fingerprint(command: NewPayment) -> str:
    """Hash normalized amount, defaults, URL, and recursively sorted JSON object keys."""
    payload = asdict(command)
    payload["amount"] = format(command.amount, f".{AMOUNT_SCALE}f")
    payload["currency"] = command.currency.value
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    ).encode("ascii")
    return sha256(encoded).hexdigest()


class PaymentService:
    """Coordinate creation/replay and lookup through injected persistence and policy."""

    def __init__(self, store: PaymentStore, webhook_policy: WebhookPolicy) -> None:
        """Inject the transaction-owning store and callback admission policy."""
        self._store = store
        self._webhook_policy = webhook_policy

    async def create(self, command: NewPayment, idempotency_key: str) -> PaymentSnapshot:
        """Create or replay a payment after validating its callback destination.

        Raises:
            InvalidWebhook: The callback destination is not permitted.
            IdempotencyConflict: The key belongs to a different normalized payload.
        """
        self._webhook_policy.validate(command.webhook_url)
        fingerprint = request_fingerprint(command)
        payment = await self._store.create_or_get(command, idempotency_key, fingerprint)
        if payment.request_hash != fingerprint:
            raise IdempotencyConflict("Idempotency key is already used with another request")
        return payment

    async def get(self, payment_id: UUID) -> PaymentSnapshot:
        """Return a fully loaded payment.

        Raises:
            PaymentNotFound: The identifier has no stored payment.
        """
        payment = await self._store.get(payment_id)
        if payment is None:
            raise PaymentNotFound("Payment not found")
        return payment

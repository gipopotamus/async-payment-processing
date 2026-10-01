# Async Payment Processing

A Python assignment: accept payments, process them asynchronously through an
emulated gateway, and deliver their results via webhook.

## Current status

Milestones 1-3 provide a runnable FastAPI foundation, configuration and database DI,
API-key authentication, SQLAlchemy models, an Alembic migration, and PostgreSQL
integration checks, plus payment creation and lookup with concurrent idempotency.
Dockerfile and Compose configuration are prepared; container
build and startup have not been verified because development currently runs locally.
Event publication and the payment consumer are upcoming milestones. Accepted payments
currently stay pending, with their initial event retained in the database outbox.

## Development

Use Python 3.14 and [uv](https://docs.astral.sh/uv/). Exact dependency versions and
artifact hashes are committed in `uv.lock`.

```powershell
uv sync --locked
Copy-Item .env.example .env
```

Configure `PAYMENTS_DATABASE_*` in `.env` for a local PostgreSQL server. The example
uses port 55432; change it to 5432 if your server uses the standard port. Create the
configured database and apply migrations before starting the API:

```powershell
createdb -h 127.0.0.1 -p 55432 -U payments payments
uv run --locked alembic upgrade head
uv run --locked uvicorn payments.api:create_app --factory --host 127.0.0.1 --port 8000
```

Replace the example credentials before exposing the service. API configuration
has no default API key; startup fails when the key is missing or too short.
Database settings are independent: Alembic does not require an API key. URLs are
constructed from separate fields, so special characters in passwords are preserved.

In another terminal, using the development example credential:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/health -Headers @{
    'X-API-Key' = 'local-development-key-change-me'
}
```

The response is `{"status":"ok"}`. This checks process liveness only, not database
or broker readiness. Missing or invalid credentials return `401`. Automatic docs
and OpenAPI routes are disabled so they cannot bypass API authentication.

## Payment API

Set `PAYMENTS_WEBHOOK_ALLOWED_ORIGINS` in `.env` to a JSON array of exact
scheme/host/port origins. The example allows `["http://127.0.0.1:8081"]`; the empty
default rejects every callback. Paths and queries belong in each request's URL,
not in the configured origins. URLs containing credentials or fragments are rejected.
This is admission validation; the delivery adapter and deployment egress controls
will provide the remaining protections when webhook delivery is implemented.

Use a decimal **string** for `amount`; JSON numbers are rejected to avoid binary
float precision loss. Currency must be `RUB`, `USD`, or `EUR`, and amount must be
positive, finite, within `NUMERIC(18,2)` range, and have at most two effective
fractional digits. Unknown fields, unsupported JSON values, and text PostgreSQL
cannot store return `422`. `description` defaults to an empty string and is limited
to 2,000 characters; `metadata` defaults to an empty object.

```powershell
$headers = @{
    'X-API-Key' = 'local-development-key-change-me'
    'Idempotency-Key' = 'order-42'
}
$body = @{
    amount = '125.50'
    currency = 'RUB'
    description = 'Order 42'
    metadata = @{ order_id = 42 }
    webhook_url = 'http://127.0.0.1:8081/callback'
} | ConvertTo-Json -Depth 10

$payment = Invoke-RestMethod http://127.0.0.1:8000/api/v1/payments `
    -Method Post -Headers $headers -ContentType 'application/json' -Body $body
Invoke-RestMethod "http://127.0.0.1:8000/api/v1/payments/$($payment.payment_id)" `
    -Headers $headers
```

POST returns `202` with `payment_id`, `status`, and `created_at` after the payment
and initial event commit together. `Idempotency-Key` must contain 1-255 printable
ASCII characters without spaces. Repeating the same normalized body with the same
key returns the existing ID, original creation time, and current status. Reusing
the key with another body returns `409`. Amount representation, validated defaults,
URL normalization, and recursive JSON object key order are normalized for comparison.
Changing currency, description, metadata, or callback URL also changes the fingerprint.

GET returns payment fields, a decimal string amount, and independent webhook progress.
An absent ID returns `404`, an invalid UUID returns `422`, and missing/invalid API
credentials return `401`. Storage errors return a sanitized `503` with `Retry-After: 1`;
retry POST using the original idempotency key when the outcome is uncertain.

## Quality checks

```powershell
uv run --locked ruff check .
uv run --locked ruff format --check .
uv run --locked mypy
uv run --locked pytest
```

Public classes and functions have Google-style docstrings, checked by Ruff.
Without a test database, PostgreSQL checks are explicitly skipped. To run them:

```powershell
createdb -h 127.0.0.1 -p 55432 -U payments payments_test
$env:PAYMENTS_TEST_DATABASE_URL = 'postgresql+asyncpg://payments:local-development-db-password@127.0.0.1:55432/payments_test'
uv run --locked pytest
```

Use only a disposable database whose name ends in `_test`. Every test creates its
own random schema, applies the real migration, and removes only that schema. Checks
cover decimal roundtrips, invalid money, duplicate keys, transaction rollback,
webhook-state consistency, request-session cleanup, and migration roundtrips/drift.
API checks additionally cover concurrent equivalent/conflicting creation, normalized
replays, current-status responses, exact webhook-origin admission, invalid inputs,
and rollback/recovery when outbox insertion fails. They use actual PostgreSQL
transactions and migrations; they do not require a running RabbitMQ server.
`uv run --locked alembic check` checks an application's migrated schema for drift.

## Persistence

`payments` stores `NUMERIC(18,2)` amounts, constrained currency/status values,
unique idempotency keys, normalized request hashes, UTC timestamps, and independent
processing/webhook attempt counters. `outbox` retains publication intents and uses
a partial index on unpublished events ordered by `available_at` and ID. Foreign
keys prevent events from silently losing their linked payments. IDs use Python
3.14's UUIDv7 generator. Frozen migration definitions do not import evolving models.

The API owns its database engine through `lifespan`, while an explicitly injected
engine remains owned by its caller. Request-scoped sessions close and roll back
uncommitted work; use cases must explicitly start and commit transactions.
The payment use case receives a narrow persistence protocol and a callback policy.
The PostgreSQL adapter uses `INSERT ... ON CONFLICT DO NOTHING` under explicitly
configured `READ COMMITTED` isolation. Only the winning insert adds the initial event;
the next statement reads the competing committed payment for payload comparison.

## Docker configuration (prepared, not runtime-verified)

When Docker is available, the intended command is:

```powershell
docker compose up --build -d
```

The current file includes PostgreSQL, RabbitMQ, one-shot migrations, and the API.
Worker services will be added with their executable implementations in milestones
4-5. Application containers run without root; services publish ports on loopback
only. PostgreSQL and RabbitMQ use named volumes and healthchecks; the API waits for
successful migrations but does not depend on broker readiness. The PostgreSQL 18
volume mounts at `/var/lib/postgresql`, matching the official image's data layout.
RabbitMQ has a stable hostname so its persisted node identity survives recreation.

## Commit sequence

Each milestone is a separate local commit. Publication and the next milestone
require the user's approval after that commit.

1. Foundation: package, configuration DI, authentication, checks, architecture.
2. Persistence and environment: SQLAlchemy models, Alembic, Docker Compose.
3. Payment API: validation, transactional creation, concurrent idempotency, GET.
4. Outbox worker: confirmed publication, durable scheduling, recovery.
5. Consumer: gateway processing, webhook delivery, retries, DLQ.
6. Acceptance: failure-window integration tests, runnable examples, final README.

## Approved architecture

The final Compose environment has `postgres`, `rabbitmq`, `api`, `outbox-worker`,
and `consumer`, plus a one-shot migration service. The three application processes
share one image. There is one payment consumer; the outbox relay is a separate
worker. See [the implementation contract](docs/architecture.md) for transaction
boundaries, retry semantics, and dependency injection rules.

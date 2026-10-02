# Async Payment Processing

A Python assignment: accept payments, process them asynchronously through an
emulated gateway, and deliver their results via webhook.

## Current status

All six milestones are implemented: authenticated FastAPI, PostgreSQL persistence,
concurrent idempotency, a separate outbox relay, and a separate payment consumer
with gateway processing, webhook retries, and DLQ delivery. DI keeps application
rules independent of transports and database adapters.

Local checks pass with actual PostgreSQL, RabbitMQ, and a loopback HTTP receiver.
Four-process acceptance also verifies broker outage recovery, consumer restart,
durable messages after a full broker restart, and exhausted webhook delivery.
See [acceptance evidence and boundaries](docs/acceptance.md). GitHub Actions is
prepared; its remote execution and Docker image/Compose startup remain unverified.

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
The delivery adapter rechecks this policy on every attempt, disables redirects,
and ignores environment proxy/netrc settings. Deployment must restrict egress
to prevent DNS-based SSRF bypasses.

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

## Local outbox worker

Configure a local RabbitMQ server and its virtual host with `PAYMENTS_BROKER_*`
settings from `.env.example`. The example uses port 5673 to match Compose's host
mapping; change it to 5672 if your local server uses the standard port. Broker
credentials are required; the worker does not require an API key.

Run it in a terminal separate from the API:

```powershell
uv run --locked python -m payments.worker
```

The worker lazily connects to RabbitMQ, declares durable `payments.new` and
`payments.dlq` queues, and binds the latter to the durable direct exchange
`payments.dead-letter`. Publications are persistent and mandatory. The AMQP
channel requires publisher confirms and raises on returned, unroutable messages;
only a positive ACK lets the relay mark the event published. The outbox event ID
becomes the message ID, and the payment ID becomes the correlation ID when present.

Each due event is locked with `FOR UPDATE SKIP LOCKED` in its own transaction.
The lock remains held during a bounded publish call; the default timeout is five
seconds. This keeps recovery simple for the assignment, with one event per relay
at a time. Leased claims with fencing are the upgrade path if throughput requires
shorter transactions. Multiple relay instances skip events already locked by another.

Broker failures retain the intent and schedule retries after 2, 4, 8, 16, 32,
then 60 seconds. Publication retries have no total limit and do not consume the
payment or webhook attempt budget. Errors contain only sanitized exception types.
Due dates and counts survive restarts. Idle polling defaults to one second; adjust
`PAYMENTS_RELAY_PUBLISH_TIMEOUT` and `PAYMENTS_RELAY_POLL_INTERVAL` if needed.
Database failures back off instead of spinning. Ctrl+C closes resources on Windows;
SIGTERM requests a clean stop after the current bounded attempt on Unix.

A crash after RabbitMQ accepts a message but before the database commit may send
the same event again. Cancellation rolls back the publication transaction, leaving
the intent recoverable. Consumers must deduplicate by event/workflow identity;
the consumer implements that behavior with persisted stage counters and state.

## Local payment consumer

With PostgreSQL migrated and RabbitMQ configured, run a third terminal alongside
the API and outbox worker:

```powershell
uv run --locked python -m payments.consumer
```

The process has one `payments.new` subscriber with prefetch one and manual ACKs.
It validates raw envelopes against their persisted outbox identity and due date,
then locks the payment and processes one stage in a transaction. Application rules
depend on narrow transaction/gateway/webhook protocols, with no FastAPI, SQLAlchemy,
or RabbitMQ imports. The PostgreSQL adapter commits state and new events together.
Locks are held during bounded external operations, matching the relay's documented
throughput tradeoff. Keep outbox history while broker redelivery is possible.

The gateway hashes the payment ID into a stable 90% success / 10% business-decline
partition and a 2-5-second delay. Repeating an ID after restart keeps the same result.
A business decline is immediately terminal `failed` and still triggers a webhook.
Technical gateway faults retry up to three total attempts; on exhaustion the
payment becomes `failed`, a processing DLQ intent is saved, and its result is notified.

Webhook attempts are independent: failures schedule retries after 2 and 4 seconds,
with three attempts total. Exhaustion sets `webhook_status=failed` and saves one
delivery DLQ intent while preserving the payment's terminal status. Replaying an
already counted or completed stage creates no new attempt or retry chain.
Gateway/HTTP deadlines default to 10/5 seconds, configured with
`PAYMENTS_PROCESSING_TIMEOUT` and `PAYMENTS_WEBHOOK_TIMEOUT`.

Webhook POST sends `event_id`, `payment_id`, `status`, decimal-string `amount`,
`currency`, `created_at`, and `processed_at`. Its `Idempotency-Key` header equals
`event_id`, derived deterministically from the payment ID and `payment.result`.
This ID stays stable across retry messages and uncertain HTTP outcomes. The receiver
must deduplicate it. Only 2xx counts as delivery; redirects and other statuses fail.
The client is reused, responses are streamed without loading their bodies, and the
incoming API key is never forwarded.

The consumer ACKs after a committed result, retry, DLQ intent, or harmless duplicate.
On database errors, early messages, or unknown internal failures it waits
`PAYMENTS_CONSUMER_REQUEUE_DELAY` (default one second), then NACKs with requeue.
Cancellation leaves the message recoverable through connection shutdown.
Malformed JSON, invalid envelopes, and messages over 4 KiB produce a deduplicated
validation DLQ intent. Only hashes and sanitized reasons are retained, with a known
payment ID where available; raw bodies and credentials are excluded. Raw decoding
ensures FastStream cannot discard bad JSON before this intent is persisted.

## Runnable webhook demo

With the API, relay, and consumer running in their three terminals, start the
receiver from the repository root in a fourth terminal:

```powershell
uv run --locked python -m examples.receiver --failures 2 --port 8081
```

Send the payment from the Payment API example above, then inspect both sides:

```powershell
Invoke-RestMethod "http://127.0.0.1:8000/api/v1/payments/$($payment.payment_id)" `
    -Headers $headers
Invoke-RestMethod http://127.0.0.1:8081/receipts
```

After gateway processing and two scheduled webhook retries, expect a terminal
payment status, `webhook_status=delivered`, and `webhook_attempts=3`. The receiver
reports one accepted result with three attempts. Its gateway result can be either
`succeeded` or a business decline `failed`; both are delivered to the callback.

For DLQ delivery, stop only the demo receiver and restart it with `--failures 3`.
Submit a **new** payment with a new idempotency key. Expect `webhook_status=failed`,
three attempts, and a message in `payments.dlq` whose stage is `webhook`. The terminal
payment result stays unchanged. The receiver rejects changed bodies for the same
event ID with `409` and accepts identical replays without adding another result.
Its state is in memory and resets on restart. This unauthenticated demo binds only
to loopback; it is not a production callback service.

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
Relay checks cover due-event ordering, retries beyond the three business attempts,
restart recovery through fresh sessions, competing row locks, cancellation,
publication timeout, and the confirmation/failed-commit duplicate window. They also
exercise the actual AMQP client's connection failure against an unavailable local
port. Positive ACKs and durable routing flags are tested through mocked adapter
contracts. Optional live-broker tests additionally verify actual confirmations,
durable routing, returned publications, and duplicate delivery after commit failure.
Workflow checks cover concurrent duplicates, business declines, technical retries,
webhook retry/exhaustion, early or fabricated events, cancellation, and uncertain
HTTP acceptance. A loopback HTTP server fails twice then succeeds with the actual
gateway delay; the result payload and deduplication ID remain stable. FastStream's
in-memory test broker verifies raw invalid JSON reaches the handler, while transport
spies verify acknowledgement order. The live acceptance pipeline also connects the
API, relay, consumer, and actual loopback HTTP receiver through RabbitMQ.
`uv run --locked alembic check` checks an application's migrated schema for drift.

To include actual RabbitMQ acceptance, enable its management plugin and use an
isolated development server. Set the broker credentials/port in `.env`, then:

```powershell
$env:PAYMENTS_TEST_BROKER_MANAGEMENT_URL = 'http://127.0.0.1:15673'
uv run --locked pytest
```

This also requires the explicit PostgreSQL test URL above. Live tests create and
delete only their own random virtual hosts; the broker user must have management
permission to create virtual hosts and grant itself access. Without the management
URL these tests are explicitly skipped. CI enables both services and runs the full
suite, lint, formatting, types, migration upgrade, and schema-drift check. Actions
are pinned to commit SHAs and dependencies to `uv.lock`. Do not run disruptive broker
restart checks against shared infrastructure; those are separate local acceptance.

## Persistence

`payments` stores `NUMERIC(18,2)` amounts, constrained currency/status values,
unique idempotency keys, normalized request hashes, UTC timestamps, and independent
processing/webhook attempt counters. `outbox` retains publication intents and uses
a partial index on unpublished events ordered by `available_at` and ID. Foreign
keys prevent events from silently losing their linked payments. New payment/workflow
IDs use Python 3.14's UUIDv7 generator; stable webhook and invalid-message identities
use deterministic UUIDv5. Frozen migration definitions do not import evolving models.

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

The current file includes PostgreSQL, RabbitMQ, one-shot migrations, the API,
outbox worker, and consumer. Application containers run without root; services publish ports on loopback
only. PostgreSQL and RabbitMQ use named volumes and healthchecks; the API waits for
successful migrations but does not depend on broker readiness. The PostgreSQL 18
volume mounts at `/var/lib/postgresql`, matching the official image's data layout.
RabbitMQ has a stable hostname so its persisted node identity survives recreation.
The outbox worker also starts independently of broker readiness, retaining failed
publication intents for eventual recovery. Broker settings are shared between the
RabbitMQ service and the worker so their credentials and virtual host stay consistent.
The consumer waits for broker health and successful migrations at initial startup.
Callback URLs must be reachable from its container; a loopback development receiver
on the host requires a different destination/origin than the local example.

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

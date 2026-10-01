# Async Payment Processing

A Python assignment: accept payments, process them asynchronously through an
emulated gateway, and deliver their results via webhook.

## Current status

Milestones 1-2 provide a runnable FastAPI foundation, configuration and database DI,
API-key authentication, SQLAlchemy models, an Alembic migration, and PostgreSQL
integration checks. Dockerfile and Compose configuration are prepared; container
build and startup have not been verified because development currently runs locally.
Payment endpoints, event publication, and the payment consumer are upcoming milestones.

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

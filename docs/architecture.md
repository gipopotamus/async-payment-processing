# Architecture

The API, outbox relay, and payment consumer run as separate processes.
Integration checks cover PostgreSQL, RabbitMQ, and an HTTP webhook receiver.
Docker image build and Compose startup remain unverified.

## Source layout

- `api`: HTTP routes, authentication, and request/response schemas.
- `application`: payment creation, processing, and webhook delivery rules.
- `core`: domain types, configuration, and logging.
- `infrastructure`: PostgreSQL repositories, RabbitMQ publication, and external adapters.
- `workers`: outbox/consumer entry points and broker message validation.

## Responsibilities and dependency injection

- The API composes dependencies and translates HTTP requests and errors.
- Application use cases own payment creation, processing, and delivery rules.
- SQLAlchemy persistence owns queries, constraints, and transaction mechanics.
- The outbox worker owns event publication and scheduled publication retries.
- One RabbitMQ consumer invokes the processing use case and acknowledges messages.
- Gateway and webhook adapters perform external operations.

Compose dependencies once at each process entry point and inject them explicitly.
Use FastAPI `Depends` for request-scoped dependencies and constructors/arguments
for application dependencies. Database sessions are scoped to each request/task.
Use narrow protocols for the gateway and webhook sender, so their
test adapters share the same behavior and error contract. Do not expose Request,
RabbitMessage, or global service locators to application rules.

Apply DRY to business rules, SOLID to these concrete responsibilities, and
KISS/YAGNI to scope. Document public contracts and non-obvious failure behavior.

## Money and HTTP contracts

- Currency is RUB, USD, or EUR.
- Parse amounts as Decimal, persist as NUMERIC(18,2), and serialize as JSON strings.
- Accept amounts only as JSON strings, avoiding binary float decoding.
- Require a finite positive amount within the column range, with at most two
  effective fractional digits. Reject excess precision before database insertion.
- POST /api/v1/payments requires X-API-Key and Idempotency-Key.
- A new payment returns 202 with payment_id, status, and created_at after commit.
- The same key and normalized payload return the existing payment ID and current
  status without creating another payment or initial event.
- The same key with a different normalized payload returns 409.
- A unique database constraint resolves concurrent requests with the same key.
- The repository uses INSERT ON CONFLICT DO NOTHING under READ COMMITTED, then
  reads the existing payment in a fresh statement. Only the insertion winner
  creates the initial event; competing bodies are compared after the store returns.
- GET /api/v1/payments/{payment_id} returns payment details or 404.
- Normalize validated defaults, Decimal values, and JSON object key ordering
  before comparing idempotent payloads. Include webhook_url in the comparison.
- Validate nested metadata for non-finite numbers, NUL, and unpaired Unicode
  surrogates before hashing or PostgreSQL insertion. Do not echo request values
  in validation errors or SQL parameters in storage errors.

## Transactional outbox

Creation inserts the payment and its initial outbox event in one transaction.
An API request does not publish directly to RabbitMQ. The API can accept requests
while the broker is unavailable if the database is available.

The relay publishes durable messages with stable event IDs, checks routing and
publisher confirms, and marks events published only after confirmation. Recovery
can publish an event again if the process dies after confirmation but before the
database update. Consumers must tolerate duplicates.

An outbox record includes its event ID, event type, payment ID, destination,
payload, available_at, published_at, and publication attempt/error information.
Creation currently records `payment.created` routed to `payments.new`, with payload
`event_id`, `payment_id`, `stage: "processing"`, and `attempt: 1`. Event IDs remain
stable across publication retries.
Retry publication failures with capped backoff. Broker unavailability does not
consume the webhook's business retry budget.

The executable relay locks one due event with `FOR UPDATE SKIP LOCKED` and keeps
the transaction open through a bounded publisher call. Only positive ACK marks
`published_at`. Publication failures update `available_at` and `publish_attempts`
in the transaction, with 2/4/8/16/32/60-second backoff and no total retry limit.
Cancellation or a failed commit rolls back these changes; the same event ID may
therefore be sent again. Idle/database-failure polling is bounded and interruptible.
Holding a row lock during a network call is a documented throughput tradeoff;
leased claims with fencing can replace it if higher throughput becomes necessary.

The RabbitMQ adapter uses persistent messages, durable queues, mandatory routing,
and a confirm-enabled channel with `on_return_raises`. DLQ uses a separate durable
direct exchange `payments.dead-letter`, bound to `payments.dlq`. Broker credentials
are composed from separate settings rather than interpolated into URL strings.

## Processing, retries, and DLQ

Payment status is pending, succeeded, or failed. Webhook delivery state and
attempt counts are separate. A webhook failure never changes a succeeded payment
into a failed one or re-executes a completed gateway operation.

The gateway emulates a 2-5 second delay and a 90% success / 10% business-failure
distribution. A business failure is a terminal payment outcome, followed by a
webhook. Repeating the operation with payment_id must return the same gateway
outcome, including across process restarts; define the emulator accordingly.
The implemented emulator hashes the UUID to choose both the outcome and delay;
it has no random process state. A technical failure exhausted on attempt three
records a failed payment, processing DLQ intent, and a webhook intent atomically.

Three attempts means three total, with two exponential delays (initially 2s and
4s). Count processing technical failures separately from webhook delivery failures.
Persist counters and workflow state, and create each scheduled retry event in the
same transaction. Include the workflow stage and expected attempt in retry
messages so duplicates cannot create independent retry chains. Broker redelivery
alone is not a business-attempt counter.
The consumer compares the envelope with its persisted outbox source and due date
before applying stage rules under a payment row lock. Counted/completed stages
return without external calls or new events. The workflow use case manipulates
an ORM-independent unit of work; the repository commits its state and event intents.
Retry intents carry relative delays, anchored to PostgreSQL's clock at insertion,
so due dates do not depend on the application's wall-clock offset.

Use available_at in the existing outbox to schedule retries. A consumer ACKs only
after it has durably saved success, a scheduled retry, or a DLQ publication intent.
Use a separate direct exchange/route for payments.dlq; the relay publishes DLQ
messages with the same confirmation guarantees as other outbox records. Handle
malformed broker payloads explicitly before application validation can discard
them. If the database is unavailable, leave the message recoverable and back off.
One raw-byte subscriber with prefetch one and manual ACKs validates envelopes in
the handler. Malformed or oversized envelopes are hashed into deterministic,
deduplicated validation DLQ intents. Failed DLQ persistence causes NACK/requeue,
never an ACK that would lose the invalid message. Early or unknown internal failures
also back off and requeue without consuming business-attempt counts.

On exhausted delivery attempts, retain the payment result and record delivery
failure. DLQ payloads contain an event ID, payment ID where available, failed
stage, attempt count, and a sanitized reason. No credentials are included.

## External effects and security

Delivery is at-least-once. If a receiver accepts a webhook but the consumer dies
before persisting success, delivery may repeat. Send a stable webhook event_id so
the receiver can deduplicate it. Exactly-once effects require cooperation from the
external receiver or payment gateway.
The webhook ID is UUIDv5(payment_id, "payment.result"); every retry uses that ID
in both its body and Idempotency-Key header. HTTP accepts only 2xx, disables
redirects, and streams response headers without loading untrusted response bodies.
The process shares one client with trust_env=False and total operation deadlines.

Reuse one async HTTP client per process, set timeouts, and disable redirects.
Validate destinations against an explicit webhook host/port allowlist; use a
specific development receiver in local integration checks. Restrict outbound
network access in deployment to prevent DNS-based SSRF bypasses. Never forward
the incoming API key to webhook destinations or log secrets.

## Acceptance scenarios

- Concurrent duplicate POST requests create one payment and one initial event.
- Reusing a key with another normalized payload returns 409.
- Stopping RabbitMQ preserves pending outbox records and allows later publication.
- A relay crash after publication produces safe duplicate consumption.
- A consumer restart resumes persisted state without changing the gateway outcome.
- Two webhook failures followed by success finish on attempt three.
- Three delivery failures produce a DLQ message while preserving payment status.
- Restarting between attempts preserves scheduled retries and attempt counts.
- A crash after webhook acceptance demonstrates the documented deduplication need.
- A clean Compose start waits for dependency health and successful migrations.

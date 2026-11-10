## How to run

From this directory:

```
docker compose up --build
```

That starts NATS with JetStream, Postgres 16, and the mailer. Postgres creates the `mailer` database (user `mailer`, password `mailer`) and, on a fresh volume, loads the schema and seed data. The mailer then listens for `email.send`.

- Health: http://localhost:8080/healthz
- Metrics: http://localhost:8080/metrics
- NATS: `localhost:4222`
- Postgres: `localhost:5432`

### Environment variables

Compose sets the database and NATS values for the mailer container. Running `go run .` on the host uses the defaults below, with `localhost` instead of the Compose hostnames. `MAILER_HOST` has no default. Until it is set, the process still starts and failed sends are dead-lettered.

| Variable | Default | Compose value |
| --- | --- | --- |
| `NATS_URL` | `nats://127.0.0.1:4222` | `nats://nats:4222` |
| `POSTGRES_HOST` | `localhost` | `postgres` |
| `POSTGRES_PORT` | `5432` | `5432` |
| `POSTGRES_USER` | empty | `mailer` |
| `POSTGRES_PASSWORD` | empty | `mailer` |
| `POSTGRES_DB_NAME` | empty | `mailer` |
| `POSTGRES_SSL_MODE` | `disable` | `disable` |
| `METRICS_ADDR` | `:8080` | `:8080` |
| `MAILER_HOST` | empty | unset |
| `MAILER_PORT` | `587` | unset |
| `MAILER_USERNAME` | empty | unset |
| `MAILER_PASSWORD` | empty | unset |
| `MAX_SEND_ATTEMPTS` | `3` | unset |
| `MAX_IN_FLIGHT` | `8` | unset |
| `MAX_DELIVER` | `5` | unset |
| `RETRY_BACKOFF` | `200ms` | unset |
| `REDELIVERY_DELAY` | `2s` | unset |
| `PROCESSING_TIMEOUT` | `45s` | unset |
| `ACK_WAIT` | `90s` | unset |

## This mailer service will handle emails being sent out to users of the band builder (or potentially other) app.

Basic architecture:

                                  ┌─────────────────────┐
                                  │      Some service   │
                                  └──────────┬──────────┘
                                             │
                                             │ publish
                                             │ email.send
                                             ▼
                                  ┌─────────────────────┐
                                  │    NATS / JetStream │
                                  │                     │
                                  │   email.send        │
                                  └──────────┬──────────┘
                                             │
                              queue group / consumer group
                                             │
                         ┌───────────────────┼───────────────────┐
                         │                   │                   │
                         ▼                   ▼                   ▼
                  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
                  │ Mailer Pod  │     │ Mailer Pod  │     │ Mailer Pod  │
                  │     A       │     │     B       │     │     C       │
                  └──────┬──────┘     └──────┬──────┘     └──────┬──────┘
                         │                   │                   │
                         │                   │                   │
                         └───────────────────┼───────────────────┘
                                             │
                                             ▼
                                      ┌─────────────┐
                                      │    Worker   │
                                      │             │
                                      │ async       │
                                      │ processing  │
                                      └──────┬──────┘
                                             │
                                             ▼
                                    ┌─────────────────┐
                                    │ Validate email  │
                                    └────────┬────────┘
                                             │
                                             ▼
                                  ┌────────────────────┐
                                  │ Idempotency check  │
                                  │                    │
                                  │ request ID         │
                                  └─────────┬──────────┘
                                            │
                                  already sent?
                                   ┌────────┴────────┐
                                  YES                NO
                                   │                  │
                                   │                  ▼
                                   │          ┌─────────────────┐
                                   │          │ Email Provider  │
                                   │          └────────┬────────┘
                                   │                   │
                                   │             success/failure
                                   │                   │
                                   │                   ▼
                                   │          ┌─────────────────┐
                                   │          │ Retry handling  │
                                   │          │                 │
                                   │          │ attempt 1 → 2   │
                                   │          │ → 3 → ...       │
                                   │          └────────┬────────┘
                                   │                   │
                                   │             exhausted?
                                   │             ┌─────┴─────┐
                                   │            NO          YES
                                   │             │            │
                                   │             │            ▼
                                   │             │      ┌─────────────┐
                                   │             │      │     DLQ     │
                                   │             │      └─────────────┘
                                   │             │
                                   │             ▼
                                   │       processing succeeds
                                   │             │
                                   └──────┬──────┘
                                          │
                                          ▼
                                  ┌─────────────────┐
                                  │ Publish outcome │
                                  │                 │
                                  │ email.sent      │
                                  │ email.failed    │
                                  └────────┬────────┘
                                           │
                                           ▼
                                  ┌─────────────────┐
                                  │    Users API    │
                                  │ / originating   │
                                  │    service      │
                                  └─────────────────┘


                         ACK TO NATS
                              ▲
                              │
                    after intended processing
                         completion

the failure / recovery path:

NATS
│
▼
Mailer Pod
│
▼
Worker
│
▼
Email Provider
│
├── success ───────────────► publish email.sent
│ │
│ ▼
│ ACK
│
└── failure
│
▼
retry locally
│
├── succeeds ─────────► publish email.sent
│ │
│ ▼
│ ACK
│
└── retries exhausted
│
├──────────────► publish email.failed
│
└──────────────► DLQ

I'd start with the smallest end-to-end path:

1. Publisher publishes an email request to NATS.
2. Mailer consumes it via a queue group.
3. Mailer processes it asynchronously.
4. Mailer sends the email through a provider/mock provider.
5. Mailer publishes email.sent.
6. Originator consumes that result.
7. Add the ACK at the appropriate point and test what happens when the consumer dies.
   Then layer on:

- application-level retries
- NATS redelivery behaviour
- idempotency
- DLQ
- Kubernetes replicas
- Prometheus metrics
- failure/recovery testing
  That ordering is useful because you'll actually see each architectural property emerge rather than writing 2,000 lines before discovering that one assumption was wrong.
  And I would deliberately leave these as explicit TODO/design decisions initially:
- exact ACK semantics
- idempotency store
- retry policy/backoff
- DLQ implementation
- provider failure classification
- concurrency limits

/\*
Implement NATS consumption and event publishing.

Implement the database methods (duplicate checking, DLQ storage, sent records).

Wire everything together in main.

Test the end-to-end flow.
\*/

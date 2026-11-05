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
Fix the retry recursion and failure-path handling.

Implement NATS consumption and event publishing.

Implement the database methods (duplicate checking, DLQ storage, sent records).

Wire everything together in main.

Test the end-to-end flow.
\*/

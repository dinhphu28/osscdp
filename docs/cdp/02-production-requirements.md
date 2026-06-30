# Production Requirements

## Purpose

This document defines the minimum production-grade requirements for the CDP.

The system is production-grade only when events are reliable, traceable, isolated by tenant, protected from data leakage, and recoverable after failures.

## Core production requirement

For every event, the system must be able to answer:

1. When did we receive it?
2. Was it valid?
3. Which source sent it?
4. Which tenant owns it?
5. Which customer did it resolve to?
6. Did it update a profile?
7. Which segments did it match?
8. Which destinations received it?
9. Did activation succeed or fail?
10. Can it be retried or replayed?

## Multi-tenancy

Required:

- Every business table must include `tenant_id`.
- Every event envelope must include `tenant_id`.
- Every API key must be scoped to one tenant and one source.
- Identity Resolution must never merge identities across tenants.
- Segment evaluation must only use data from the same tenant.
- Activation must only send data to destinations configured under the same tenant.

Forbidden:

- Global identity lookup without tenant scope.
- Shared destination credentials across tenants unless explicitly modeled.
- Cross-tenant analytics queries without authorization.

## Reliability

Required:

- Event ingestion must be idempotent by `event_id`.
- Event publishing must use retry.
- Workers must be idempotent.
- Activation must use idempotency keys.
- Failed events must go to DLQ after retry exhaustion.
- Processing lag must be monitored.
- Replay must be possible from raw events.

Important rule:

```text
At-least-once processing is acceptable only if every side effect is idempotent.
```

## Security

Required:

- API key authentication for ingestion sources.
- Admin authentication and RBAC.
- Destination secrets encrypted at rest.
- PII masked in admin UI where necessary.
- PII never logged in plain text.
- Audit log for config changes and sensitive operations.
- API key rotation support.
- Rate limiting per tenant/source.

## Privacy and consent

Required:

- Consent state per customer/channel/purpose.
- Activation must check consent before sending.
- Ability to delete customer data by tenant/customer.
- Ability to export customer data.
- Retention policy for raw events and profiles.

Minimum consent dimensions:

```text
channel: email, sms, push, ads, webhook
purpose: marketing, analytics, personalization, transactional
status: granted, denied, unknown
```

## Observability

Required metrics:

```text
events_received_total
events_validated_total
events_rejected_total
kafka_publish_failed_total
identity_resolved_total
identity_merge_total
profile_updated_total
segment_evaluated_total
segment_matched_total
activation_sent_total
activation_failed_total
dlq_total
processing_lag_seconds
```

Required logs:

- Structured JSON logs.
- Include `tenant_id`, `event_id`, `source_id`, and component name when available.
- Do not include raw email, phone, tokens, API keys, or destination secrets.

Required dashboards:

- Ingress volume.
- Validation failure rate.
- Kafka consumer lag.
- Worker error rate.
- DLQ count.
- Identity merge count.
- Profile update latency.
- Segment match count.
- Activation success/failure.

## Performance

Initial expected behavior:

- Ingress returns quickly after validation and event bus publish.
- Heavy work happens asynchronously.
- Batch ingestion is supported.
- Workers can scale horizontally by consumer group.
- Event partition key should preserve per-customer ordering where possible.

Recommended partition key:

```text
tenant_id + stable_user_identifier
```

Fallback order for stable identifier:

```text
user_id -> anonymous_id -> email_hash -> phone_hash -> event_id
```

## Data quality

Required:

- Canonical event envelope.
- Schema validation for known event types.
- Unknown properties allowed only if configured.
- Invalid events stored or sent to DLQ with reason.
- Timestamp normalization.
- Source attribution.

## Operational features

Admin/operator must be able to:

- View source configuration.
- Rotate API keys.
- View recent events.
- View a customer profile.
- View identity cluster members.
- View segment membership.
- View destination delivery logs.
- View DLQ events.
- Retry DLQ events.
- Disable a destination.
- Disable a source.

## Backup and recovery

Required:

- PostgreSQL backup and restore procedure.
- Kafka/Redpanda topic retention policy.
- Raw event archive strategy.
- Config export/import strategy.
- Disaster recovery test before production.

## Production readiness checklist

- [ ] Tenant isolation implemented and tested.
- [ ] API keys are hashed/encrypted and rotatable.
- [ ] Event idempotency implemented.
- [ ] Kafka/Redpanda retry implemented.
- [ ] DLQ implemented.
- [ ] Raw event replay supported.
- [ ] Identity merge is transactional and idempotent.
- [ ] Profile update uses optimistic locking or safe concurrency.
- [ ] Segment evaluator has tests for all operators.
- [ ] Activation has retry, timeout, and idempotency key.
- [ ] Consent check enforced before activation.
- [ ] PII protection implemented.
- [ ] Audit log implemented.
- [ ] Metrics and dashboards exist.
- [ ] Load test completed.
- [ ] Failure test completed.
- [ ] Backup/restore tested.

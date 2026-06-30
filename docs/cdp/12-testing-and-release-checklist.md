# Testing and Release Checklist

## Purpose

This document defines the testing and release checklist for the CDP.

The CDP processes customer data and activates it to external destinations, so correctness and recovery matter more than feature speed.

## Testing layers

```text
Unit tests
Integration tests
Contract tests
End-to-end tests
Load tests
Failure tests
Security tests
Migration tests
```

## Unit tests

Required for:

- Event validation.
- Event normalization.
- Identifier normalization.
- Identity matching rules.
- Cluster merge logic.
- Profile merge policy.
- Segment rule evaluator.
- Activation idempotency key generation.
- Retry policy.
- Consent check.
- PII masking.

## Integration tests

Run with real dependencies:

```text
PostgreSQL
Kafka/Redpanda
Redis if used
```

Required flows:

- Ingest event -> publish to event bus.
- Worker consumes event -> raw event stored.
- Event -> identity resolved.
- Event -> profile updated.
- Profile updated -> segment evaluated.
- Segment matched -> activation task created.
- Activation task -> webhook sent.
- Retryable webhook failure -> retry.
- Permanent webhook failure -> failed/DLQ.

## Contract tests

Test public API contracts:

```http
POST /v1/events/track
POST /v1/events/batch
POST /v1/identify
POST /v1/alias
```

Validate:

- Required fields.
- Invalid payloads.
- Duplicate event behavior.
- Authentication failures.
- Rate limit behavior.
- Error response format.

## End-to-end test scenarios

### Scenario 1 — anonymous event

```text
Given source API key is valid
When an anonymous product_viewed event is sent
Then event is accepted
And raw event is stored
And identity cluster is created
And customer profile is created
```

### Scenario 2 — identify anonymous user

```text
Given anonymous customer exists
When identify links anonymous_id to user_id/email
Then identity cluster is merged
And profile is updated
And merge history is recorded
```

### Scenario 3 — segment match

```text
Given segment rule matches country VN and event product_viewed
When matching event is processed
Then customer enters segment
And segment_membership_changed event is emitted
```

### Scenario 4 — activation success

```text
Given segment is subscribed to webhook destination
When customer enters segment
Then activation task is created
And webhook is sent
And delivery log is succeeded
```

### Scenario 5 — activation retry

```text
Given webhook destination returns HTTP 503
When activation worker sends payload
Then task is retried with backoff
And delivery attempts are logged
```

### Scenario 6 — consent denied

```text
Given customer consent is denied for marketing push
When customer enters marketing segment
Then activation is skipped
And skip reason is recorded
```

## Tenant isolation tests

Required tests:

- Same email in different tenants does not merge.
- Same user ID in different tenants does not merge.
- Segment from tenant A does not evaluate profile from tenant B.
- Destination from tenant A does not receive activation from tenant B.
- Admin user from tenant A cannot view tenant B data.

## Idempotency tests

Required tests:

- Same event ID ingested twice creates one raw event.
- Same event ID ingested twice updates profile once.
- Same identity merge event processed twice creates one merge history entry.
- Same segment membership change processed twice creates one activation task.
- Same activation task retried uses the same idempotency key.

## Failure tests

Test these failures:

- PostgreSQL unavailable.
- Kafka/Redpanda unavailable.
- Worker crashes mid-processing.
- Destination timeout.
- Destination returns HTTP 429.
- Destination returns HTTP 500.
- Malformed event payload.
- Invalid segment rule.
- Poison event repeatedly fails.

Expected behavior:

- Retry where safe.
- DLQ after retry exhaustion.
- No duplicate irreversible side effects.
- Metrics and logs show the failure.

## Load tests

Measure:

```text
ingress requests per second
events per second
worker throughput
profile update latency
segment evaluation latency
activation throughput
Kafka consumer lag
PostgreSQL CPU/IO
```

Minimum load test questions:

- How many events/sec can ingress accept?
- How many events/sec can workers process?
- What is p95 end-to-end latency from ingress to profile update?
- What is p95 latency from segment match to activation send?
- What happens when destination is slow?

## Migration tests

Required:

- Migrations run from empty database.
- Migrations run from previous version.
- Rollback strategy exists or forward fix strategy is documented.
- Constraints do not break existing data.

## Security tests

Required:

- Invalid API key rejected.
- Disabled source rejected.
- API key not logged.
- Destination secret not returned in API response.
- PII masked for users without permission.
- Cross-tenant access rejected.
- Consent denied prevents activation.

## CI checklist

Each merge request should run:

- [ ] Format check.
- [ ] Lint/static analysis.
- [ ] Unit tests.
- [ ] Integration tests.
- [ ] Migration test.
- [ ] Docker image build.
- [ ] Basic security scan.

## Release checklist

Before production release:

- [ ] Version tagged.
- [ ] Migration reviewed.
- [ ] Backward compatibility checked.
- [ ] Config documented.
- [ ] Secrets configured.
- [ ] Dashboard available.
- [ ] Alerts configured.
- [ ] Backup completed.
- [ ] Restore tested.
- [ ] Smoke test passed.
- [ ] Rollback/forward-fix plan documented.

## Production smoke test

After deployment:

1. Create test source.
2. Send test event.
3. Confirm raw event stored.
4. Confirm identity resolved.
5. Confirm profile updated.
6. Confirm segment evaluated.
7. Confirm activation sent to test webhook.
8. Confirm logs and metrics appear.
9. Confirm DLQ is empty or expected.

## Definition of done for CDP features

A feature is done only when:

- Code is implemented.
- Tests are added.
- Tenant isolation is covered.
- Idempotency is covered.
- Errors are observable.
- PII/logging impact is reviewed.
- Documentation is updated.
- Migration is included if data model changed.

# Event Model and Ingress

## Purpose

Ingress receives customer data from external systems and normalizes it into a canonical CDP event envelope.

Ingress must be fast, strict, and reliable.

## Ingress rule

Correct flow:

```text
HTTP request
  -> authenticate source
  -> validate payload
  -> normalize event
  -> publish to event bus
  -> return 202 Accepted
```

Wrong flow:

```text
HTTP request
  -> identity resolution
  -> profile update
  -> segmentation
  -> activation
  -> return
```

Do not perform heavy processing in the HTTP request path.

## Initial APIs

```http
POST /v1/events/track
POST /v1/events/batch
POST /v1/identify
POST /v1/alias
```

## Authentication

Each source has an API key.

API key scope:

```text
tenant_id
source_id
allowed_event_types
status
rate_limit
```

API key requirements:

- Store only hash of API key.
- Support API key rotation.
- Support disabling a source.
- Do not log API keys.

## Canonical event envelope

```json
{
  "event_id": "evt_01H...",
  "tenant_id": "tenant_001",
  "source_id": "src_web",
  "type": "track",
  "event_name": "product_viewed",
  "anonymous_id": "anon_123",
  "user_id": "user_456",
  "timestamp": "2026-06-30T03:00:00Z",
  "received_at": "2026-06-30T03:00:01Z",
  "context": {
    "ip": "203.0.113.10",
    "user_agent": "Mozilla/5.0",
    "page": {
      "url": "https://example.com/products/p001",
      "referrer": "https://example.com"
    }
  },
  "properties": {
    "product_id": "p001",
    "category": "phone"
  }
}
```

## Event types

### `track`

Represents a customer behavior event.

Examples:

```text
product_viewed
product_added_to_cart
checkout_started
order_completed
page_viewed
button_clicked
```

Required fields:

```text
event_id
tenant_id
source_id
type = track
event_name
timestamp
received_at
```

At least one identifier should be present:

```text
user_id
anonymous_id
email
phone
external_id
device_id
```

### `identify`

Connects traits and identifiers to a customer.

Example:

```json
{
  "type": "identify",
  "user_id": "user_456",
  "anonymous_id": "anon_123",
  "traits": {
    "email": "user@example.com",
    "phone": "+8490...",
    "name": "Nguyen Van A"
  }
}
```

### `alias`

Explicitly connects two identifiers.

Example:

```json
{
  "type": "alias",
  "previous_id": "anon_123",
  "user_id": "user_456"
}
```

Use alias carefully because it can trigger identity cluster merge.

## Validation rules

Required:

- `tenant_id` must be resolved from API key, not trusted directly from request body.
- `source_id` must be resolved from API key.
- `event_id` must be unique within tenant.
- `event_name` is required for `track` events.
- `timestamp` must be parseable.
- `received_at` is set by server.
- Payload size must be limited.
- Batch size must be limited.

Recommended limits for first version:

```text
Max event payload: 64 KB
Max batch size: 500 events
Max batch payload: 5 MB
```

## Idempotency

Idempotency key:

```text
tenant_id + event_id
```

Behavior:

- If event is new, accept and publish.
- If same `tenant_id + event_id` already exists, return success but do not duplicate side effects.
- If same `tenant_id + event_id` exists with different payload hash, reject or send to conflict DLQ.

## Ingress durability: transactional outbox (Phase 2)

To make ingress durable and idempotent without a dual-write to the event bus,
Phase 2 writes each normalized event to an `event_outbox` table in one
transaction, keyed `UNIQUE (tenant_id, event_id)` (see
`09-data-model.md`). Ingress returns `202` once the row is committed. The Phase 3
relay (in `cdp-worker`) drains `status = 'pending'` rows to `cdp.events` with the
Kafka key set to `partition_key`, then marks them `published`. This keeps the
HTTP path fast (no bus round-trip) and guarantees no event is lost between accept
and publish. `cdp-worker` consumes `cdp.events` and persists each event to
`raw_event` (idempotent); messages that exhaust retries are written to
`dlq_event`. Delivery is at-least-once and every side effect is idempotent.

## Event bus topics

Simple first version:

```text
cdp.events
cdp.profile-updated
cdp.segment-membership-changed
cdp.activation
cdp.dlq
```

Expanded version:

```text
cdp.raw-events
cdp.validated-events
cdp.identity-events
cdp.profile-events
cdp.segment-events
cdp.activation-events
cdp.dlq
```

## Partition key

Use:

```text
tenant_id + stable_user_identifier
```

Identifier priority:

```text
user_id -> anonymous_id -> email_hash -> phone_hash -> external_id -> event_id
```

This preserves ordering for most customer-level processing.

## Raw event storage

Store raw normalized events before or during processing.

Purpose:

- Replay.
- Debugging.
- Audit.
- Analytics.
- Profile rebuild.

Minimum fields:

```text
id
tenant_id
event_id
source_id
type
event_name
identifier_key
payload_json
payload_hash
timestamp
received_at
processing_status
error_reason
```

## Error handling

Validation failures should be returned synchronously when possible.

Processing failures should go to DLQ.

DLQ payload must include:

```text
tenant_id
event_id
source_id
component
error_code
error_message
original_payload
failed_at
retry_count
```

## Acceptance criteria

- [ ] Ingress authenticates API key.
- [ ] Ingress resolves `tenant_id` and `source_id` from API key.
- [ ] Ingress validates event payload.
- [ ] Ingress assigns `event_id` if missing.
- [ ] Ingress sets `received_at`.
- [ ] Ingress publishes to event bus.
- [ ] Duplicate `event_id` does not create duplicate side effects.
- [ ] Invalid events are rejected or sent to DLQ with clear reason.
- [ ] Batch ingestion works.
- [ ] PII is not logged.

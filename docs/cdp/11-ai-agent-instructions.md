# AI Agent Instructions

## Purpose

This file gives AI coding agents stable context and rules for implementing this CDP repository.

Agents must follow this document before making architecture or code changes.

## Product goal

Build a production-grade Customer Data Platform with this core flow:

```text
Ingress
  -> Event Pipeline
  -> Raw Event Store
  -> Identity Resolution
  -> Customer Unification / Profile Store
  -> Segmentation
  -> Activation / Outgress
```

## Non-negotiable architecture rules

1. `tenant_id` is mandatory for all business data.
2. Do not merge identities across tenants.
3. Do not run heavy processing inside HTTP ingress request path.
4. Ingress should validate and publish, then return quickly.
5. Identity Resolution and Customer Unification are separate concepts.
6. Processing must be idempotent.
7. Activation must be idempotent.
8. DLQ must preserve enough context to debug and retry.
9. PII must not be logged.
10. Activation must check consent before sending.

## Important distinction

```text
Identity Resolution = connect identifiers into an identity cluster.
Customer Unification = build the final customer profile from resolved identity and events.
```

Do not mix these responsibilities.

## First implementation scope

Build first:

- Tenant/source/API key management.
- Event ingestion API.
- Kafka/Redpanda event pipeline.
- Raw event store.
- Deterministic identity resolution.
- Unified profile store.
- Stateless segmentation.
- Webhook/Kafka activation.
- Retry and DLQ.
- Metrics and audit logs.

Do not build yet:

- Browser SDK.
- Mobile SDK.
- Probabilistic identity matching.
- Complex journey builder.
- Drag-and-drop campaign builder.
- Ads integrations.
- Email editor.
- ML recommendation.
- Stateful behavioral segmentation.

## Suggested repository structure

Use this if the repository has no better structure yet:

```text
/docs/cdp
/src
  /api
  /worker
  /admin
  /shared
/migrations
/deploy
/scripts
/tests
```

For a Go project:

```text
/cmd
  /cdp-api
  /cdp-worker
/internal
  /tenant
  /source
  /ingress
  /events
  /identity
  /profile
  /segmentation
  /activation
  /governance
  /observability
  /admin
/pkg
/migrations
```

For a Java/Quarkus project:

```text
/app
/core
/infra
/migrations
```

Prefer hexagonal boundaries:

```text
API adapter -> application service -> domain -> port -> infrastructure adapter
```

## Event model rules

Canonical event envelope must include:

```text
event_id
tenant_id
source_id
type
timestamp
received_at
properties/context as JSON
```

For `track` events, `event_name` is required.

At least one identifier should exist where possible:

```text
user_id
anonymous_id
email
phone
external_id
device_id
```

`tenant_id` and `source_id` should be resolved from API key, not trusted from request body.

## Identity Resolution rules

Use deterministic matching first:

```text
same tenant + same user_id
same tenant + same email
same tenant + same phone
identify anonymous_id with user_id
alias previous_id with user_id
```

Do not use probabilistic matching in version 1.

Do not match by IP address, user agent, name similarity, or behavior similarity.

## Profile rules

Profile update must use explicit merge policy.

Initial merge policy:

```text
email: latest non-empty verified value wins
phone: latest non-empty verified value wins
name: latest non-empty value wins
first_seen_at: earliest wins
last_seen_at: latest wins
total_events: increment idempotently
last_event_name: latest event wins
```

Use optimistic locking or safe concurrency control.

## Segmentation rules

Start with JSON DSL.

Support these operators first:

```text
eq
neq
gt
gte
lt
lte
contains
not_contains
in
not_in
exists
not_exists
and
or
not
```

Rules may reference:

```text
profile.traits.*
profile.computed_attributes.*
event.event_name
event.properties.*
```

Do not implement stateful windows first.

## Activation rules

Activation must have:

- Idempotency key.
- Retry.
- Delivery log.
- DLQ after retry exhaustion.
- Destination-level timeout.
- Consent check.
- Secret protection.

Webhook must send an idempotency header:

```http
Idempotency-Key: <key>
```

## Testing expectations

For each component, add tests for:

- Happy path.
- Duplicate event/idempotency.
- Tenant isolation.
- Invalid input.
- Retryable failure.
- Permanent failure.
- DLQ behavior.

Identity tests must verify no cross-tenant merge.

Segmentation tests must cover every operator.

Activation tests must verify duplicate prevention.

## Documentation update rule

When changing architecture, update `/docs/cdp`.

When adding a new event type, update:

- `03-event-model-and-ingress.md`
- relevant schema/migration docs
- tests

When adding a new segment operator, update:

- `06-segmentation-engine.md`
- evaluator tests

When adding a new destination type, update:

- `07-activation-outgress.md`
- destination config docs
- delivery tests

## Agent behavior rules

Before implementing:

1. Read the relevant file in `/docs/cdp`.
2. Identify the component boundary.
3. Check idempotency requirements.
4. Check tenant isolation requirements.
5. Check PII/logging requirements.
6. Add or update tests.
7. Keep changes small and reviewable.

Do not:

- Add new infrastructure without justification.
- Create microservices prematurely.
- Add probabilistic matching.
- Add stateful segmentation before stateless segmentation is stable.
- Put business processing in HTTP ingress path.
- Log PII.
- Trust tenant ID from external payload.

## Useful implementation prompt for agents

Use this prompt when assigning a task to an AI coding agent:

```text
You are working in a production-grade CDP repository. First read /docs/cdp/00-index.md and /docs/cdp/11-ai-agent-instructions.md. Then implement the requested feature while preserving tenant isolation, idempotency, PII safety, and component boundaries. Add tests for happy path, duplicate/idempotent behavior, invalid input, and tenant isolation. Update /docs/cdp if the design or public behavior changes.
```

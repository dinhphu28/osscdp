# Initial Data Model

## Purpose

This document provides an initial PostgreSQL schema draft for the CDP core.

It is not final SQL migration code. Use it as design documentation and a starting point for implementation.

## Common conventions

Every business table should include:

```sql
tenant_id UUID NOT NULL
created_at TIMESTAMPTZ NOT NULL DEFAULT now()
updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
```

Use UUID or ULID for public IDs.

Use database constraints to enforce tenant safety where possible.

## Tenant and source

```sql
CREATE TABLE tenant (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE source (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenant(id),
    name TEXT NOT NULL,
    type TEXT NOT NULL,
    status TEXT NOT NULL,
    api_key_hash TEXT NOT NULL,
    config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, name)
);
```

## Schema definition

```sql
CREATE TABLE schema_definition (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenant(id),
    event_name TEXT NOT NULL,
    version INT NOT NULL,
    schema_json JSONB NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, event_name, version)
);
```

## Event outbox (ingress, Phase 2)

Ingress writes each normalized event here in one transaction (transactional
outbox). It is idempotent on `(tenant_id, event_id)`. A Phase 3 relay drains
`status = 'pending'` rows to the event bus and marks them `published`. Columns
mirror `raw_event` so the Phase 4 raw-event store can reuse them.

```sql
CREATE TABLE event_outbox (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenant(id),
    source_id UUID NOT NULL REFERENCES source(id),
    event_id TEXT NOT NULL,
    type TEXT NOT NULL,
    event_name TEXT,
    identifier_key TEXT,
    partition_key TEXT NOT NULL,
    payload_json JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, event_id)
);

-- Drives the Phase 3 relay drain (oldest pending first).
CREATE INDEX idx_event_outbox_status_created
    ON event_outbox (status, created_at);
```

`payload_hash` is computed over client-meaningful content only (it excludes the
server-set `received_at` and a server-generated `event_id`) so a legitimate retry
of the same event hashes identically. A second event with the same
`(tenant_id, event_id)` but a different hash is a conflict (rejected with `409`;
conflict-DLQ routing arrives in Phase 3).

## Raw event

Created in Phase 3. `cdp-worker` writes a row per consumed event, idempotent on
`(tenant_id, event_id)`, with `processing_status = 'stored'`.

```sql
CREATE TABLE raw_event (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenant(id),
    source_id UUID NOT NULL REFERENCES source(id),
    event_id TEXT NOT NULL,
    type TEXT NOT NULL,
    event_name TEXT,
    identifier_key TEXT,
    payload_json JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    processing_status TEXT NOT NULL,
    error_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, event_id)
);

CREATE INDEX idx_raw_event_tenant_time
    ON raw_event (tenant_id, received_at DESC);

CREATE INDEX idx_raw_event_tenant_event_name
    ON raw_event (tenant_id, event_name);
```

## Identity graph

```sql
CREATE TABLE identity_node (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenant(id),
    namespace TEXT NOT NULL,
    value_hash TEXT NOT NULL,
    value_encrypted TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, namespace, value_hash)
);

CREATE TABLE identity_cluster (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenant(id),
    canonical_user_id TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, canonical_user_id)
);

CREATE TABLE identity_cluster_member (
    tenant_id UUID NOT NULL REFERENCES tenant(id),
    cluster_id UUID NOT NULL REFERENCES identity_cluster(id),
    identity_node_id UUID NOT NULL REFERENCES identity_node(id),
    confidence NUMERIC(5, 4) NOT NULL DEFAULT 1.0,
    source TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, cluster_id, identity_node_id)
);

CREATE TABLE identity_merge_history (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenant(id),
    from_cluster_id UUID NOT NULL,
    to_cluster_id UUID NOT NULL,
    reason TEXT NOT NULL,
    event_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

## Customer profile

```sql
CREATE TABLE customer_profile (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenant(id),
    canonical_user_id TEXT NOT NULL,
    identity_cluster_id UUID NOT NULL REFERENCES identity_cluster(id),
    traits_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    computed_attributes_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    first_seen_at TIMESTAMPTZ,
    last_seen_at TIMESTAMPTZ,
    version BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, canonical_user_id),
    UNIQUE (tenant_id, identity_cluster_id)
);

CREATE INDEX idx_customer_profile_tenant_updated
    ON customer_profile (tenant_id, updated_at DESC);
```

## Consent

```sql
CREATE TABLE customer_consent (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenant(id),
    customer_profile_id UUID NOT NULL REFERENCES customer_profile(id),
    channel TEXT NOT NULL,
    purpose TEXT NOT NULL,
    status TEXT NOT NULL,
    source TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, customer_profile_id, channel, purpose)
);
```

## Segmentation

```sql
CREATE TABLE segment (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenant(id),
    name TEXT NOT NULL,
    description TEXT,
    status TEXT NOT NULL,
    current_version_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, name)
);

CREATE TABLE segment_version (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenant(id),
    segment_id UUID NOT NULL REFERENCES segment(id),
    version INT NOT NULL,
    rule_json JSONB NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, segment_id, version)
);

CREATE TABLE segment_membership (
    tenant_id UUID NOT NULL REFERENCES tenant(id),
    segment_id UUID NOT NULL REFERENCES segment(id),
    customer_profile_id UUID NOT NULL REFERENCES customer_profile(id),
    status TEXT NOT NULL,
    entered_at TIMESTAMPTZ,
    exited_at TIMESTAMPTZ,
    last_evaluated_at TIMESTAMPTZ NOT NULL,
    version BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant_id, segment_id, customer_profile_id)
);
```

## Destination and activation

```sql
CREATE TABLE destination (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenant(id),
    type TEXT NOT NULL,
    name TEXT NOT NULL,
    status TEXT NOT NULL,
    config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    secret_ref TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, name)
);

CREATE TABLE destination_subscription (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenant(id),
    destination_id UUID NOT NULL REFERENCES destination(id),
    trigger_type TEXT NOT NULL,
    segment_id UUID,
    event_name TEXT,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE activation_task (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenant(id),
    destination_id UUID NOT NULL REFERENCES destination(id),
    subscription_id UUID NOT NULL REFERENCES destination_subscription(id),
    customer_profile_id UUID NOT NULL REFERENCES customer_profile(id),
    source_event_id TEXT,
    idempotency_key TEXT NOT NULL,
    payload_json JSONB NOT NULL,
    status TEXT NOT NULL,
    attempt_count INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, idempotency_key)
);

CREATE TABLE activation_delivery (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenant(id),
    activation_task_id UUID NOT NULL REFERENCES activation_task(id),
    destination_id UUID NOT NULL REFERENCES destination(id),
    customer_profile_id UUID NOT NULL REFERENCES customer_profile(id),
    source_event_id TEXT,
    idempotency_key TEXT NOT NULL,
    status TEXT NOT NULL,
    http_status INT,
    response_body_hash TEXT,
    error_message TEXT,
    attempt_count INT NOT NULL,
    sent_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

## DLQ

Created in Phase 3. Implementation note: `tenant_id`/`source_id` are nullable
(not FK-constrained) because a poison message may fail to parse before its tenant
is known.

```sql
CREATE TABLE dlq_event (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES tenant(id),
    source_id UUID,
    event_id TEXT,
    component TEXT NOT NULL,
    error_code TEXT NOT NULL,
    error_message TEXT NOT NULL,
    original_payload JSONB NOT NULL,
    retry_count INT NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    failed_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

## Audit log

```sql
CREATE TABLE audit_log (
    id UUID PRIMARY KEY,
    tenant_id UUID,
    actor_id TEXT,
    actor_type TEXT NOT NULL,
    action TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id TEXT,
    before_json JSONB,
    after_json JSONB,
    ip_address TEXT,
    user_agent TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

## Notes for implementation

- Use real enum types or application-level validation for status fields.
- Add row-level security later if needed.
- Add partial indexes based on query patterns.
- Add JSONB indexes only after measuring query needs.
- Keep migration files small and reversible when possible.
- Use `tenant_id` in every unique constraint that involves business identity.

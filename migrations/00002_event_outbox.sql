-- +goose Up
-- Phase 2 ingress: transactional outbox. Ingress writes normalized events here
-- in one transaction (idempotent on tenant_id+event_id). Phase 3 adds a relay
-- that drains status='pending' rows to the event bus. Columns mirror raw_event
-- (docs/cdp/09-data-model.md) so the Phase 4 raw-event store can reuse them.

CREATE TABLE event_outbox (
    id             UUID PRIMARY KEY,
    tenant_id      UUID NOT NULL REFERENCES tenant(id),
    source_id      UUID NOT NULL REFERENCES source(id),
    event_id       TEXT NOT NULL,
    type           TEXT NOT NULL,
    event_name     TEXT,
    identifier_key TEXT,
    partition_key  TEXT NOT NULL,
    payload_json   JSONB NOT NULL,
    payload_hash   TEXT NOT NULL,
    timestamp      TIMESTAMPTZ NOT NULL,
    received_at    TIMESTAMPTZ NOT NULL,
    status         TEXT NOT NULL DEFAULT 'pending',
    published_at   TIMESTAMPTZ,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, event_id)
);

-- Drives the Phase 3 relay drain (oldest pending first).
CREATE INDEX idx_event_outbox_status_created ON event_outbox (status, created_at);

-- +goose Down
DROP TABLE IF EXISTS event_outbox;

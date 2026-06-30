-- +goose Up
-- Phase 3: raw event store. The worker consumes cdp.events and persists each
-- event here, idempotent on (tenant_id, event_id). See docs/cdp/09-data-model.md.

CREATE TABLE raw_event (
    id                UUID PRIMARY KEY,
    tenant_id         UUID NOT NULL REFERENCES tenant(id),
    source_id         UUID NOT NULL REFERENCES source(id),
    event_id          TEXT NOT NULL,
    type              TEXT NOT NULL,
    event_name        TEXT,
    identifier_key    TEXT,
    payload_json      JSONB NOT NULL,
    payload_hash      TEXT NOT NULL,
    timestamp         TIMESTAMPTZ NOT NULL,
    received_at       TIMESTAMPTZ NOT NULL,
    processing_status TEXT NOT NULL,
    error_reason      TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, event_id)
);

CREATE INDEX idx_raw_event_tenant_time ON raw_event (tenant_id, received_at DESC);
CREATE INDEX idx_raw_event_tenant_event_name ON raw_event (tenant_id, event_name);

-- +goose Down
DROP TABLE IF EXISTS raw_event;

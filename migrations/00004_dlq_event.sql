-- +goose Up
-- Phase 3: dead-letter queue. Events that exhaust retries during processing land
-- here with enough context to debug and retry. See docs/cdp/09-data-model.md.

CREATE TABLE dlq_event (
    id               UUID PRIMARY KEY,
    tenant_id        UUID,
    source_id        UUID,
    event_id         TEXT,
    component        TEXT NOT NULL,
    error_code       TEXT NOT NULL,
    error_message    TEXT NOT NULL,
    original_payload JSONB NOT NULL,
    retry_count      INT NOT NULL DEFAULT 0,
    status           TEXT NOT NULL,
    failed_at        TIMESTAMPTZ NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_dlq_event_tenant_status ON dlq_event (tenant_id, status);

-- +goose Down
DROP TABLE IF EXISTS dlq_event;

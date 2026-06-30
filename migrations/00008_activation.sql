-- +goose Up
-- Phase 8: activation/outgress. See docs/cdp/07-activation-outgress.md.

CREATE TABLE destination (
    id          UUID PRIMARY KEY,
    tenant_id   UUID NOT NULL REFERENCES tenant(id),
    type        TEXT NOT NULL,
    name        TEXT NOT NULL,
    status      TEXT NOT NULL,
    config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    secret_ref  TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, name)
);

CREATE TABLE destination_subscription (
    id             UUID PRIMARY KEY,
    tenant_id      UUID NOT NULL REFERENCES tenant(id),
    destination_id UUID NOT NULL REFERENCES destination(id),
    trigger_type   TEXT NOT NULL,
    segment_id     UUID,
    event_name     TEXT,
    status         TEXT NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_destination_subscription_segment
    ON destination_subscription (tenant_id, segment_id, status);

CREATE TABLE activation_task (
    id                  UUID PRIMARY KEY,
    tenant_id           UUID NOT NULL REFERENCES tenant(id),
    destination_id      UUID NOT NULL REFERENCES destination(id),
    subscription_id     UUID NOT NULL REFERENCES destination_subscription(id),
    customer_profile_id UUID NOT NULL REFERENCES customer_profile(id),
    source_event_id     TEXT,
    idempotency_key     TEXT NOT NULL,
    payload_json        JSONB NOT NULL,
    status              TEXT NOT NULL,
    attempt_count       INT NOT NULL DEFAULT 0,
    next_attempt_at     TIMESTAMPTZ,
    last_error          TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, idempotency_key)
);

-- Drives the sender claim (due tasks oldest first).
CREATE INDEX idx_activation_task_due ON activation_task (status, next_attempt_at);

CREATE TABLE activation_delivery (
    id                  UUID PRIMARY KEY,
    tenant_id           UUID NOT NULL REFERENCES tenant(id),
    activation_task_id  UUID NOT NULL REFERENCES activation_task(id),
    destination_id      UUID NOT NULL REFERENCES destination(id),
    customer_profile_id UUID NOT NULL REFERENCES customer_profile(id),
    source_event_id     TEXT,
    idempotency_key     TEXT NOT NULL,
    status              TEXT NOT NULL,
    http_status         INT,
    response_body_hash  TEXT,
    error_message       TEXT,
    attempt_count       INT NOT NULL,
    sent_at             TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_activation_delivery_task ON activation_delivery (tenant_id, activation_task_id);

-- +goose Down
DROP TABLE IF EXISTS activation_delivery;
DROP TABLE IF EXISTS activation_task;
DROP TABLE IF EXISTS destination_subscription;
DROP TABLE IF EXISTS destination;

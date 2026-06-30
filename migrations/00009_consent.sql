-- +goose Up
-- Phase 9a: customer consent. See docs/cdp/08-governance-security-observability.md.

CREATE TABLE customer_consent (
    id                  UUID PRIMARY KEY,
    tenant_id           UUID NOT NULL REFERENCES tenant(id),
    customer_profile_id UUID NOT NULL REFERENCES customer_profile(id),
    channel             TEXT NOT NULL,
    purpose             TEXT NOT NULL,
    status              TEXT NOT NULL,
    source              TEXT,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, customer_profile_id, channel, purpose)
);

-- +goose Down
DROP TABLE IF EXISTS customer_consent;

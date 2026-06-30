-- +goose Up
-- Phase 6: unified customer profile + change/idempotency ledger.
-- See docs/cdp/05-customer-profile-unification.md and docs/cdp/09-data-model.md.

CREATE TABLE customer_profile (
    id                       UUID PRIMARY KEY,
    tenant_id                UUID NOT NULL REFERENCES tenant(id),
    canonical_user_id        TEXT NOT NULL,
    identity_cluster_id      UUID NOT NULL REFERENCES identity_cluster(id),
    traits_json              JSONB NOT NULL DEFAULT '{}'::jsonb,
    computed_attributes_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    first_seen_at            TIMESTAMPTZ,
    last_seen_at             TIMESTAMPTZ,
    version                  BIGINT NOT NULL DEFAULT 0,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, canonical_user_id),
    UNIQUE (tenant_id, identity_cluster_id)
);

CREATE INDEX idx_customer_profile_tenant_updated ON customer_profile (tenant_id, updated_at DESC);
CREATE INDEX idx_customer_profile_email ON customer_profile (tenant_id, (traits_json ->> 'email'));
CREATE INDEX idx_customer_profile_phone ON customer_profile (tenant_id, (traits_json ->> 'phone'));

-- Doubles as the per-event idempotency ledger (UNIQUE on event_id) and change log.
CREATE TABLE customer_profile_history (
    id                  UUID PRIMARY KEY,
    tenant_id           UUID NOT NULL REFERENCES tenant(id),
    customer_profile_id UUID NOT NULL REFERENCES customer_profile(id),
    event_id            TEXT NOT NULL,
    change_type         TEXT NOT NULL,
    before_json         JSONB,
    after_json          JSONB,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, customer_profile_id, event_id)
);

-- +goose Down
DROP TABLE IF EXISTS customer_profile_history;
DROP TABLE IF EXISTS customer_profile;

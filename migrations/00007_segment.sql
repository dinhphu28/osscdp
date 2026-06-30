-- +goose Up
-- Phase 7: stateless segmentation. See docs/cdp/06-segmentation-engine.md.

CREATE TABLE segment (
    id                 UUID PRIMARY KEY,
    tenant_id          UUID NOT NULL REFERENCES tenant(id),
    name               TEXT NOT NULL,
    description        TEXT,
    status             TEXT NOT NULL,
    current_version_id UUID,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, name)
);

CREATE TABLE segment_version (
    id         UUID PRIMARY KEY,
    tenant_id  UUID NOT NULL REFERENCES tenant(id),
    segment_id UUID NOT NULL REFERENCES segment(id),
    version    INT NOT NULL,
    rule_json  JSONB NOT NULL,
    status     TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, segment_id, version)
);

CREATE TABLE segment_membership (
    tenant_id           UUID NOT NULL REFERENCES tenant(id),
    segment_id          UUID NOT NULL REFERENCES segment(id),
    customer_profile_id UUID NOT NULL REFERENCES customer_profile(id),
    status              TEXT NOT NULL,
    entered_at          TIMESTAMPTZ,
    exited_at           TIMESTAMPTZ,
    last_evaluated_at   TIMESTAMPTZ NOT NULL,
    version             BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant_id, segment_id, customer_profile_id)
);

CREATE INDEX idx_segment_membership_status ON segment_membership (tenant_id, segment_id, status);

-- +goose Down
DROP TABLE IF EXISTS segment_membership;
DROP TABLE IF EXISTS segment_version;
DROP TABLE IF EXISTS segment;

-- +goose Up
-- Phase 5: deterministic identity graph. See docs/cdp/04-identity-resolution.md.
-- Deviation from the data-model draft: identity_cluster_member uses
-- PRIMARY KEY (tenant_id, identity_node_id) so a node belongs to exactly one
-- cluster (enforces the core invariant; makes merge a single UPDATE).

CREATE TABLE identity_node (
    id              UUID PRIMARY KEY,
    tenant_id       UUID NOT NULL REFERENCES tenant(id),
    namespace       TEXT NOT NULL,
    value_hash      TEXT NOT NULL,
    value_encrypted TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, namespace, value_hash)
);

CREATE TABLE identity_cluster (
    id               UUID PRIMARY KEY,
    tenant_id        UUID NOT NULL REFERENCES tenant(id),
    canonical_user_id TEXT NOT NULL,
    status           TEXT NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, canonical_user_id)
);

CREATE TABLE identity_cluster_member (
    tenant_id        UUID NOT NULL REFERENCES tenant(id),
    identity_node_id UUID NOT NULL REFERENCES identity_node(id),
    cluster_id       UUID NOT NULL REFERENCES identity_cluster(id),
    confidence       NUMERIC(5, 4) NOT NULL DEFAULT 1.0,
    source           TEXT NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, identity_node_id)
);

CREATE INDEX idx_identity_cluster_member_cluster ON identity_cluster_member (tenant_id, cluster_id);

CREATE TABLE identity_merge_history (
    id              UUID PRIMARY KEY,
    tenant_id       UUID NOT NULL REFERENCES tenant(id),
    from_cluster_id UUID NOT NULL,
    to_cluster_id   UUID NOT NULL,
    reason          TEXT NOT NULL,
    event_id        TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE IF EXISTS identity_merge_history;
DROP TABLE IF EXISTS identity_cluster_member;
DROP TABLE IF EXISTS identity_cluster;
DROP TABLE IF EXISTS identity_node;

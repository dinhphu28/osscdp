-- +goose Up
-- Phase 9b: role-bearing admin tokens for RBAC.
-- tenant_id NULL = cross-tenant (super-admin scope).

CREATE TABLE admin_token (
    id         UUID PRIMARY KEY,
    tenant_id  UUID REFERENCES tenant(id),
    name       TEXT NOT NULL,
    role       TEXT NOT NULL,
    token_hash TEXT NOT NULL,
    status     TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX idx_admin_token_hash ON admin_token (token_hash);

-- +goose Down
DROP TABLE IF EXISTS admin_token;

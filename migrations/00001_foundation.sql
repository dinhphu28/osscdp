-- +goose Up
-- Phase 1 foundation: tenant, source, and audit_log.
-- See docs/cdp/09-data-model.md and docs/cdp/11-ai-agent-instructions.md.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE tenant (
    id         UUID PRIMARY KEY,
    name       TEXT NOT NULL,
    status     TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE source (
    id           UUID PRIMARY KEY,
    tenant_id    UUID NOT NULL REFERENCES tenant(id),
    name         TEXT NOT NULL,
    type         TEXT NOT NULL,
    status       TEXT NOT NULL,
    api_key_hash TEXT NOT NULL,
    config_json  JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, name)
);

-- API keys are resolved by hash on every ingress request; keep the lookup O(1).
CREATE UNIQUE INDEX idx_source_api_key_hash ON source (api_key_hash);

CREATE TABLE audit_log (
    id            UUID PRIMARY KEY,
    tenant_id     UUID,
    actor_id      TEXT,
    actor_type    TEXT NOT NULL,
    action        TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id   TEXT,
    before_json   JSONB,
    after_json    JSONB,
    ip_address    TEXT,
    user_agent    TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_audit_log_tenant_time ON audit_log (tenant_id, created_at DESC);

-- +goose Down
DROP TABLE IF EXISTS audit_log;
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS tenant;

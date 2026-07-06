-- +goose Up
-- Phase 1 journey orchestration (docs/cdp/19-journey-orchestration.md): a journey is a
-- versioned, ordered flow (wait -> send) a customer profile ENTERS via segment
-- membership and advances through. journey holds identity + the active version pointer;
-- journey_version is the immutable definition (an edit mints version N+1). In-flight
-- enrollments PIN their version, so a re-authored flow never disturbs a customer
-- mid-wait. Mirrors segment / segment_version (00007) + the active-name index (00016).
CREATE TABLE journey (
    id               UUID PRIMARY KEY,
    tenant_id        UUID NOT NULL REFERENCES tenant(id),
    name             TEXT NOT NULL,
    description      TEXT,
    status           TEXT NOT NULL,               -- draft|active|archived
    entry_segment_id UUID NOT NULL REFERENCES segment(id),
    current_version  INT  NOT NULL DEFAULT 1,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One live journey per name (archived names free up; mirror idx_segment_active_name).
CREATE UNIQUE INDEX idx_journey_active_name
    ON journey (tenant_id, name) WHERE status <> 'archived';
-- "Which active journeys enter on this segment?" — the enrollment consumer hot path.
CREATE INDEX idx_journey_entry_segment
    ON journey (tenant_id, entry_segment_id, status);

CREATE TABLE journey_version (
    id              UUID PRIMARY KEY,
    tenant_id       UUID NOT NULL REFERENCES tenant(id),
    journey_id      UUID NOT NULL REFERENCES journey(id),
    version         INT  NOT NULL,
    definition_json JSONB NOT NULL,               -- ordered step array (Phase 1: wait|send)
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, journey_id, version)
);

-- +goose Down
DROP TABLE IF EXISTS journey_version;
DROP TABLE IF EXISTS journey;

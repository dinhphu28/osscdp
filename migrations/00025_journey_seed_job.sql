-- +goose Up
-- Phase 4 journey orchestration: durable, resumable population backfill. Creating a
-- SEGMENT-entry journey records a seed job in the same tx; a background runner drains
-- it, paging over the entry segment's CURRENT active members and enrolling each — so a
-- journey immediately reaches the already-qualified population, not just future joiners.
-- Clone of segment_seed_job (00017): keyset cursor + claimed_at fence + reclaim. The
-- job snapshots entry_segment_id + journey_version so the drain is self-contained and
-- pins the enrollment version. (Event-entry journeys have no existing population and
-- do not seed.)
CREATE TABLE journey_seed_job (
    tenant_id        UUID NOT NULL REFERENCES tenant(id) ON DELETE CASCADE,
    journey_id       UUID NOT NULL REFERENCES journey(id) ON DELETE CASCADE,
    entry_segment_id UUID NOT NULL,       -- snapshot: whose active members to enroll
    journey_version  INT  NOT NULL,       -- snapshot: version to pin enrollments to
    reason           TEXT NOT NULL,       -- seed
    due_at           TIMESTAMPTZ NOT NULL,
    cursor           UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000', -- last profile id enrolled
    claimed_at       TIMESTAMPTZ,         -- NULL = claimable; time-boxed for crash recovery
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, journey_id)
);

CREATE INDEX idx_journey_seed_job_claimable ON journey_seed_job (claimed_at);

-- +goose Down
DROP TABLE IF EXISTS journey_seed_job;

-- +goose Up
-- Durable, resumable population seed (follow-up to Phase 5). Creating/updating a
-- sweep-safe segment records a seed job transactionally (superseding any prior one);
-- a background runner drains it, paging over customer_profile by id and enqueuing
-- segment_pending_eval, persisting a cursor per page so a crash RESUMES from the last
-- processed profile instead of restarting or silently dropping dormant profiles.
CREATE TABLE segment_seed_job (
    tenant_id  UUID NOT NULL REFERENCES tenant(id),
    segment_id UUID NOT NULL REFERENCES segment(id),
    reason     TEXT NOT NULL,                          -- seed | version_change
    due_at     TIMESTAMPTZ NOT NULL,                   -- due_at written on enqueued pending rows
    cursor     UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000', -- last profile id processed
    claimed_at TIMESTAMPTZ,                            -- NULL = claimable; time-boxed for crash recovery
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, segment_id)
);

CREATE INDEX idx_segment_seed_job_claimable ON segment_seed_job (claimed_at);

-- +goose Down
DROP TABLE IF EXISTS segment_seed_job;

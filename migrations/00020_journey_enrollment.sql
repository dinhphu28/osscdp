-- +goose Up
-- Phase 1 journey orchestration: the single per-enrollment state row. It deliberately
-- FOLDS the position (segment_membership: status/current_step_index/step_seq) and the
-- deadline work-queue (segment_pending_eval: due_at/claimed_at/attempts/parked_at) into
-- ONE row, so an identity merge moves or drops the whole enrollment atomically (no
-- cross-table desync / orphaned work item) and every advance is a single-table
-- claim-fenced UPDATE. See migrations 00007/00013/00018 for the substrate it mirrors.
CREATE TABLE journey_enrollment (
    tenant_id           UUID NOT NULL REFERENCES tenant(id),
    journey_id          UUID NOT NULL REFERENCES journey(id),
    customer_profile_id UUID NOT NULL,                  -- no FK: reparent/erasure handle lifecycle
    enrollment_seq      INT  NOT NULL DEFAULT 0,        -- per (journey,profile) run counter; re-entry namespacing (Phase 5)
    journey_version     INT  NOT NULL,                  -- PINNED at enroll; never re-touched to latest
    status              TEXT NOT NULL DEFAULT 'active', -- active|completed|exited
    current_step_index  INT  NOT NULL DEFAULT 0,        -- next step to execute
    step_seq            BIGINT NOT NULL DEFAULT 0,      -- monotonic advance guard (transition_seq analog)
    due_at              TIMESTAMPTZ NOT NULL,           -- when the current step is due
    claimed_at          TIMESTAMPTZ,                    -- claim fence; NULL = claimable; time-boxed reclaim
    attempts            INT NOT NULL DEFAULT 0,
    parked_at           TIMESTAMPTZ,                    -- NULL = live; non-NULL = dead-lettered
    last_error          TEXT,
    enrolled_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, journey_id, customer_profile_id, enrollment_seq)
);

-- One LIVE enrollment per (journey, profile): makes enroll idempotent under
-- at-least-once redelivery (ON CONFLICT ... WHERE status='active' DO NOTHING).
CREATE UNIQUE INDEX idx_journey_enrollment_active
    ON journey_enrollment (tenant_id, journey_id, customer_profile_id) WHERE status = 'active';
-- Fair-claim hot path: due, live, unclaimed, not parked.
CREATE INDEX idx_journey_enrollment_due
    ON journey_enrollment (tenant_id, due_at)
    WHERE status = 'active' AND claimed_at IS NULL AND parked_at IS NULL;
-- Time-boxed reclaim of crashed claims.
CREATE INDEX idx_journey_enrollment_claim
    ON journey_enrollment (claimed_at) WHERE claimed_at IS NOT NULL;
-- Parked (dead-lettered) rows, for the surfacing gauge + admin list.
CREATE INDEX idx_journey_enrollment_parked
    ON journey_enrollment (tenant_id, journey_id) WHERE parked_at IS NOT NULL;

-- +goose Down
DROP TABLE IF EXISTS journey_enrollment;

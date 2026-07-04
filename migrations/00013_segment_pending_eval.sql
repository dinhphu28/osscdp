-- +goose Up
-- Phase 5 (stateful segmentation, doc 17): the deadline queue that fires
-- absence/expiry transitions with no inbound event. A row means "(segment,profile)
-- should be re-evaluated at due_at because a behavioral leaf may flip by elapse."
-- The sweeper claims due rows fairly per tenant, re-evaluates at now(), and re-arms.
CREATE TABLE segment_pending_eval (
    tenant_id           UUID NOT NULL REFERENCES tenant(id),
    segment_id          UUID NOT NULL REFERENCES segment(id),
    customer_profile_id UUID NOT NULL,               -- no FK: reparent/erasure handle lifecycle
    due_at              TIMESTAMPTZ NOT NULL,
    reason              TEXT NOT NULL,                -- absence_deadline|window_expiry|version_change|seed|safety_sweep
    claimed_at          TIMESTAMPTZ,                  -- NULL = claimable; time-boxed for crash recovery
    PRIMARY KEY (tenant_id, segment_id, customer_profile_id)
);

-- Claimable rows, ordered by deadline within a tenant (fair claim CTE).
CREATE INDEX idx_segment_pending_due   ON segment_pending_eval (tenant_id, due_at) WHERE claimed_at IS NULL;
-- In-flight rows, for the time-boxed reclaim of crashed claims.
CREATE INDEX idx_segment_pending_claim ON segment_pending_eval (claimed_at)        WHERE claimed_at IS NOT NULL;

-- +goose Down
DROP TABLE IF EXISTS segment_pending_eval;

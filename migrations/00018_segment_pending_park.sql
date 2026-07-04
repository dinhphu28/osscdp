-- +goose Up
-- Phase 5 follow-up (doc 17 "Poison rows" / doc 18 §B): dead-letter a segment_pending_eval
-- row whose SweepEvaluate persistently errors. attempts drives exponential backoff;
-- parked_at marks a row past the retry ceiling so the fair claim excludes it (it stops
-- churning and stops sorting ahead of healthy rows) until an operator retries it.
ALTER TABLE segment_pending_eval ADD COLUMN attempts   INT  NOT NULL DEFAULT 0;
ALTER TABLE segment_pending_eval ADD COLUMN last_error TEXT;                 -- NULL until first failure
ALTER TABLE segment_pending_eval ADD COLUMN parked_at  TIMESTAMPTZ;          -- NULL = live; non-NULL = dead-lettered

-- The claimable index must now also skip parked rows so the sweeper's hot path never scans them.
DROP INDEX idx_segment_pending_due;
CREATE INDEX idx_segment_pending_due
    ON segment_pending_eval (tenant_id, due_at)
    WHERE claimed_at IS NULL AND parked_at IS NULL;

-- Parked rows, for the surfacing gauge + admin list (rare rows, tenant/segment-scoped).
CREATE INDEX idx_segment_pending_parked
    ON segment_pending_eval (tenant_id, segment_id)
    WHERE parked_at IS NOT NULL;

-- +goose Down
DROP INDEX IF EXISTS idx_segment_pending_parked;
DROP INDEX IF EXISTS idx_segment_pending_due;
CREATE INDEX idx_segment_pending_due ON segment_pending_eval (tenant_id, due_at) WHERE claimed_at IS NULL;
ALTER TABLE segment_pending_eval DROP COLUMN parked_at;
ALTER TABLE segment_pending_eval DROP COLUMN last_error;
ALTER TABLE segment_pending_eval DROP COLUMN attempts;

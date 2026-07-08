-- +goose Up
-- Phase 3 journey orchestration: derived metadata for condition steps, mirroring
-- segment_version (migration 00015). referenced_event_names + max_window_seconds are
-- computed from every condition step's embedded rule at create/update. max_window_seconds
-- widens the behavioral retention horizon (behavior.Retention.effectiveHorizon) so a
-- long-wait-then-behavioral-condition journey never evaluates over DROP-partitioned data.
ALTER TABLE journey_version ADD COLUMN referenced_event_names TEXT[] NOT NULL DEFAULT '{}';
ALTER TABLE journey_version ADD COLUMN max_window_seconds     BIGINT NOT NULL DEFAULT 0;

-- No backfill: pre-Phase-3 versions are linear (wait|send only) with no behavioral
-- reads, so the fail-open defaults ('{}', 0) are exactly correct; a re-save re-analyzes.

-- The retention horizon query (behavior.Retention.EffectiveHorizon) probes for live
-- enrollments by (tenant_id, journey_id, journey_version) — the Phase-1 active index
-- omits journey_version, so add one that lets that EXISTS subquery seek.
CREATE INDEX idx_journey_enrollment_version_active
    ON journey_enrollment (tenant_id, journey_id, journey_version) WHERE status = 'active';

-- +goose Down
DROP INDEX IF EXISTS idx_journey_enrollment_version_active;
ALTER TABLE journey_version DROP COLUMN IF EXISTS max_window_seconds;
ALTER TABLE journey_version DROP COLUMN IF EXISTS referenced_event_names;

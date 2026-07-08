-- +goose Up
-- Phase 2 journey orchestration: opt-in exit-on-segment-leave. When set, a profile
-- that LEAVES the journey's entry segment (segment_membership_changed 'exited') has its
-- active enrollment terminated (status='exited') — so a customer who no longer
-- qualifies stops receiving the journey's sends. Default false preserves Phase 1
-- run-to-completion behavior.
ALTER TABLE journey ADD COLUMN exit_on_segment_leave BOOLEAN NOT NULL DEFAULT false;

-- +goose Down
ALTER TABLE journey DROP COLUMN exit_on_segment_leave;

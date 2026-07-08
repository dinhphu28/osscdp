-- +goose Up
-- Phase 4 journey orchestration: event-triggered entry. A journey enters a profile
-- EITHER when the profile joins the entry segment (Phases 1-3) OR when the profile
-- emits a specific event (entry_event_name). Exactly one entry mode per journey (XOR),
-- so entry_segment_id becomes nullable. The event path rides the existing
-- TopicProfileUpdated stream (a new -journey-event consumer group matches
-- entry_event_name against the triggering event's name).
ALTER TABLE journey ALTER COLUMN entry_segment_id DROP NOT NULL;
ALTER TABLE journey ADD COLUMN entry_event_name TEXT;
ALTER TABLE journey ADD CONSTRAINT journey_entry_mode_xor
    CHECK ((entry_segment_id IS NOT NULL) <> (entry_event_name IS NOT NULL));

-- "Which active journeys enter on this event name?" — the event-entry consumer hot path.
CREATE INDEX idx_journey_entry_event
    ON journey (tenant_id, entry_event_name, status) WHERE entry_event_name IS NOT NULL;

-- +goose Down
-- Deliberately does NOT re-add NOT NULL to entry_segment_id: event-entry journeys have
-- entry_segment_id=NULL, so SET NOT NULL would fail, and forcing it (deleting those
-- rows) would cascade-delete their enrollments/seed jobs. Rolling back therefore leaves
-- entry_segment_id nullable; any event-entry journeys become inert (no entry mode) and
-- should be cleaned up by an operator. Pre-Phase-4 code always sets entry_segment_id, so
-- a nullable column is harmless to it.
DROP INDEX IF EXISTS idx_journey_entry_event;
ALTER TABLE journey DROP CONSTRAINT IF EXISTS journey_entry_mode_xor;
ALTER TABLE journey DROP COLUMN IF EXISTS entry_event_name;

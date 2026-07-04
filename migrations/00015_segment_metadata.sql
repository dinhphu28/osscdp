-- +goose Up
-- Phase 6 (stateful segmentation, doc 17): derived segment metadata, populated from
-- the parsed rule at create/update. Lets the worker (a) prefilter the per-event
-- fan-out — evaluate a segment only if it has a stateless leaf (a trait change may
-- newly match) OR this event is one it references — without ever gating a
-- stateless-leaf segment (findings #15/#30), and (b) scope the sweeper to stateful
-- segments and route behaviour leaves to buckets vs the exact log.
ALTER TABLE segment_version ADD COLUMN is_stateful            BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE segment_version ADD COLUMN has_stateless_leaves   BOOLEAN NOT NULL DEFAULT true;
ALTER TABLE segment_version ADD COLUMN referenced_event_names TEXT[] NOT NULL DEFAULT '{}';
ALTER TABLE segment_version ADD COLUMN max_window_seconds     BIGINT NOT NULL DEFAULT 0;

-- Backfill is_stateful for pre-existing versions (a behaviour leaf's JSON always
-- carries a "behavior" object). referenced_event_names / has_stateless_leaves keep
-- their fail-open defaults ('{}' + true → the segment is over-evaluated, never
-- skipped) until the rule is next saved and re-analyzed by analyzeRule, so the
-- prefilter stays correct in the meantime; is_stateful is thus safe to consume.
UPDATE segment_version SET is_stateful = true WHERE rule_json::text LIKE '%"behavior"%';

-- +goose Down
ALTER TABLE segment_version DROP COLUMN IF EXISTS is_stateful;
ALTER TABLE segment_version DROP COLUMN IF EXISTS has_stateless_leaves;
ALTER TABLE segment_version DROP COLUMN IF EXISTS referenced_event_names;
ALTER TABLE segment_version DROP COLUMN IF EXISTS max_window_seconds;

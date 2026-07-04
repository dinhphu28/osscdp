-- +goose Up
-- Phase 4 (stateful segmentation, doc 17): make segment membership transitions
-- atomic and exactly-once-emitted. transition_seq is a per-membership monotonic
-- counter bumped on every real flip (enter/exit). It is DISTINCT from
-- segment_membership.version (which stores the rule version) — do not overload.
ALTER TABLE segment_membership ADD COLUMN transition_seq BIGINT NOT NULL DEFAULT 0;

-- Transactional outbox for segment_membership_changed. The conditional flip and its
-- emit are inserted in ONE tx, so flip + emit commit atomically; a relay then drains
-- this table at-least-once (finding #28 — no lost emit on a crash before publish).
CREATE TABLE segment_membership_outbox (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id     UUID NOT NULL REFERENCES tenant(id),
    partition_key TEXT NOT NULL,
    payload_json  JSONB NOT NULL,
    status        TEXT NOT NULL DEFAULT 'pending',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at  TIMESTAMPTZ
);

CREATE INDEX idx_segment_membership_outbox_pending
    ON segment_membership_outbox (status, created_at);

-- +goose Down
DROP TABLE IF EXISTS segment_membership_outbox;
ALTER TABLE segment_membership DROP COLUMN IF EXISTS transition_seq;

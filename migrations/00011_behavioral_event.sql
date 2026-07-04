-- +goose Up
-- Phase 11 (stateful segmentation, doc 16 §00011): durable profile-keyed behavioral
-- event log. Range-partitioned by occurred_at so retention is DROP PARTITION (Phase 8).
-- occurred_at is CLAMPED at write time to LEAST(envelope.Timestamp, received_at) so a
-- spoofed/future client timestamp cannot poison windows or defeat pruning.
CREATE TABLE behavioral_event (
    tenant_id           UUID NOT NULL REFERENCES tenant(id),
    customer_profile_id UUID NOT NULL,                     -- no FK: partitioned table; erasure deletes explicitly (Phase 7)
    event_id            TEXT NOT NULL,
    event_name          TEXT NOT NULL CHECK (event_name <> ''),
    occurred_at         TIMESTAMPTZ NOT NULL,              -- clamped; partition key
    props_json          JSONB,
    schema_version      INT NOT NULL DEFAULT 1,
    inserted_at         TIMESTAMPTZ NOT NULL DEFAULT now(),-- server audit time; retention is DROP PARTITION on the occurred_at range key (occurred_at is clamped to <= received_at, so partitions align with server time)
    PRIMARY KEY (tenant_id, customer_profile_id, event_id, occurred_at)
) PARTITION BY RANGE (occurred_at);

-- Phase 2 ships a single DEFAULT partition so all inserts land somewhere. The
-- appender is idempotent by (profile, event_id) in the INSERT (occurred_at is a
-- clamped, per-delivery value and is NOT a reliable dedup key), so this does not
-- rely on the PK for dedup. Phase-8 retention (internal/behavior/retention.go)
-- creates FUTURE weekly partitions ahead of time (empty ranges, no DEFAULT overlap)
-- so new writes land in droppable partitions, DROPs whole partitions past the horizon,
-- and DELETEs only the DEFAULT partition's residue (the bootstrap week) — it does not
-- attempt the DETACH/redistribute/re-ATTACH dance on a populated DEFAULT.
CREATE TABLE behavioral_event_default PARTITION OF behavioral_event DEFAULT;

-- Workhorse index for count-in-window / absence / recency / sequence anchors.
CREATE INDEX idx_behavioral_event_window
    ON behavioral_event (tenant_id, customer_profile_id, event_name, occurred_at DESC);

-- +goose Down
DROP TABLE IF EXISTS behavioral_event;

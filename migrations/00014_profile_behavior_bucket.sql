-- +goose Up
-- Phase 6 (stateful segmentation, doc 17): hourly rollup buckets for the hot path.
-- The write appends behavioral_event (exact log) AND upserts the (profile, event,
-- hour) bucket in the same tx, so windowed counts become a bounded SUM over whole
-- in-window hours plus an exact LOG count of the two partial boundary hours (that is
-- what makes the count exact at the leading edge, finding #5 — NOT first_at/last_at,
-- which are exact landmarks reserved for the merge rebuild + future recency/absence
-- acceleration). Range-partitioned by bucket_start so retention is DROP PARTITION
-- (Phase 8), matching behavioral_event. NOTE: bucket_start is date_trunc('hour', ...)
-- under the DB session timezone; the session must stay UTC (as it is in prod/tests)
-- so write-side and read-side hour boundaries align.
CREATE TABLE profile_behavior_bucket (
    tenant_id           UUID NOT NULL REFERENCES tenant(id),
    customer_profile_id UUID NOT NULL,                 -- no FK: partitioned; erasure/reparent handle it
    event_name          TEXT NOT NULL CHECK (event_name <> ''),
    bucket_start        TIMESTAMPTZ NOT NULL,          -- date_trunc('hour', clamped occurred_at)
    count               BIGINT NOT NULL DEFAULT 0,
    first_at            TIMESTAMPTZ NOT NULL,           -- exact min occurred_at in the bucket
    last_at             TIMESTAMPTZ NOT NULL,           -- exact max occurred_at in the bucket
    sum_value           NUMERIC NOT NULL DEFAULT 0,     -- reserved for frequency-of-value (Phase 6+)
    PRIMARY KEY (tenant_id, customer_profile_id, event_name, bucket_start)
) PARTITION BY RANGE (bucket_start);

-- Phase 6 ships a single DEFAULT partition; Phase 8 retention creates future weekly
-- partitions ahead, DROPs old ones, and DELETEs DEFAULT residue (see behavioral_event).
CREATE TABLE profile_behavior_bucket_default PARTITION OF profile_behavior_bucket DEFAULT;

-- Backfill from behavioral_event rows already recorded under earlier phases, so a
-- non-exact count over historical whole-hours is not undercounted after upgrade.
-- No-op on a fresh log; idempotent on re-run.
INSERT INTO profile_behavior_bucket (tenant_id, customer_profile_id, event_name, bucket_start, count, first_at, last_at)
SELECT tenant_id, customer_profile_id, event_name, date_trunc('hour', occurred_at), count(*), min(occurred_at), max(occurred_at)
FROM behavioral_event
GROUP BY tenant_id, customer_profile_id, event_name, date_trunc('hour', occurred_at)
ON CONFLICT (tenant_id, customer_profile_id, event_name, bucket_start) DO NOTHING;

-- +goose Down
DROP TABLE IF EXISTS profile_behavior_bucket;

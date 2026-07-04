-- +goose Up
-- Phase 7 (segment lifecycle): make the segment name unique only among ACTIVE
-- segments, so retiring (deactivating) a segment frees its name for reuse instead of
-- tombstoning it forever. CreateSegment still raises 23505 → ErrDuplicateName when an
-- ACTIVE segment already holds the name (the partial index enforces it).
ALTER TABLE segment DROP CONSTRAINT IF EXISTS segment_tenant_id_name_key;
CREATE UNIQUE INDEX segment_active_name_uniq ON segment (tenant_id, name) WHERE status = 'active';

-- +goose Down
DROP INDEX IF EXISTS segment_active_name_uniq;
ALTER TABLE segment ADD CONSTRAINT segment_tenant_id_name_key UNIQUE (tenant_id, name);

-- +goose Up
-- Phase 5 journey orchestration: re-entry policy + per-journey cap. max_enrollments is
-- the maximum number of times a profile may enter a journey. 1 (the default) = once-only
-- (Phases 1-4 behaviour); N>1 = the profile may re-enter after a terminal (completed/
-- exited) enrollment up to N total, each new run getting a fresh enrollment_seq. There
-- is never more than one ACTIVE enrollment at a time (the partial-unique-active index).
ALTER TABLE journey ADD COLUMN max_enrollments INT NOT NULL DEFAULT 1;
ALTER TABLE journey ADD CONSTRAINT journey_max_enrollments_positive CHECK (max_enrollments >= 1);

-- +goose Down
ALTER TABLE journey DROP CONSTRAINT IF EXISTS journey_max_enrollments_positive;
ALTER TABLE journey DROP COLUMN IF EXISTS max_enrollments;

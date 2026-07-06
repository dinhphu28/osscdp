-- +goose Up
-- Phase 1 journey orchestration: a journey "send" step delivers through the existing
-- activation stack (task/sender/breaker/consent). Each destination gets a single,
-- reusable trigger_type='journey' subscription (get-or-create), so the NOT-NULL
-- activation_task.subscription_id FK is satisfied without a per-send-step row. This
-- partial unique index makes that get-or-create race-safe (00008 defines the table).
CREATE UNIQUE INDEX idx_destination_subscription_journey
    ON destination_subscription (tenant_id, destination_id) WHERE trigger_type = 'journey';

-- +goose Down
DROP INDEX IF EXISTS idx_destination_subscription_journey;

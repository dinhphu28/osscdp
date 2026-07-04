package activation

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// A Phase-4 membership emit carries a per-flip token "<event_id>:<seq>" in
// SourceEventID. Distinct transitions must yield distinct idempotency keys (so an
// enter/exit/re-enter cycle creates three tasks), while a replay of the same flip
// reuses its key (dedup).
func TestIdempotencyKey_DistinctPerTransitionSeq(t *testing.T) {
	tid, dest, sub, prof := uuid.New(), uuid.New(), uuid.New(), uuid.New()

	enter1 := IdempotencyKey(tid, dest, sub, prof, "evt:1", "entered")
	exit2 := IdempotencyKey(tid, dest, sub, prof, "evt:2", "exited")
	enter3 := IdempotencyKey(tid, dest, sub, prof, "evt:3", "entered")

	require.NotEqual(t, enter1, exit2)
	require.NotEqual(t, enter1, enter3, "same change but a later transition_seq is a distinct key")
	require.NotEqual(t, exit2, enter3)

	// A replay of the same flip reuses the key (dedups at CreateTask).
	require.Equal(t, enter1, IdempotencyKey(tid, dest, sub, prof, "evt:1", "entered"))
}

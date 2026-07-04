package foundationtest

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dinhphu28/osscdp/internal/profile"
	"github.com/dinhphu28/osscdp/internal/segment"
)

// TestMembership_ConcurrentEnterFlipsOnce: two concurrent Evaluate calls on the
// same matching profile+segment produce exactly ONE flip — one outbox row and
// transition_seq=1 — proving the conditional flip removes the read-then-write
// double-emit race (finding #2).
func TestMembership_ConcurrentEnterFlipsOnce(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	repo := segment.NewRepo(f.pool)
	svc := segment.NewService(f.pool, profile.NewRepo(f.pool), nil)
	seg, err := repo.CreateSegment(ctx, tid, "s1", "", vnPhoneRule())
	require.NoError(t, err)

	pu := seedProfile(t, f, tid, sid, "e1", "product_viewed", "u1", `{"country":"VN"}`)

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) { defer wg.Done(); errs[i] = svc.Evaluate(ctx, pu) }(i)
	}
	wg.Wait()
	require.NoError(t, errs[0])
	require.NoError(t, errs[1])

	require.Equal(t, 1, countMembershipOutbox(t, f, tid), "concurrent enters must flip exactly once")
	var seq int64
	require.NoError(t, f.pool.QueryRow(ctx,
		`SELECT transition_seq FROM segment_membership WHERE tenant_id=$1 AND segment_id=$2 AND customer_profile_id=$3`,
		tid, seg.ID, pu.CustomerProfileID).Scan(&seq))
	require.EqualValues(t, 1, seq)
}

// TestMembership_EnterExitEnterDistinctTokens: an enter → exit → enter cycle bumps
// transition_seq each time and yields three distinct per-flip ReasonEventID tokens
// (finding #27 — no idempotency collision across re-entry). The rows are staged as
// pending in the outbox even though nothing published them (finding #28 — the emit
// survives a crash before publish; the relay drains it later).
func TestMembership_EnterExitEnterDistinctTokens(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	repo := segment.NewRepo(f.pool)
	svc := segment.NewService(f.pool, profile.NewRepo(f.pool), nil)
	_, err := repo.CreateSegment(ctx, tid, "s1", "", vnPhoneRule())
	require.NoError(t, err)

	require.NoError(t, svc.Evaluate(ctx, seedProfile(t, f, tid, sid, "e1", "product_viewed", "u1", `{"country":"VN"}`))) // enter
	require.NoError(t, svc.Evaluate(ctx, seedProfile(t, f, tid, sid, "e2", "page_viewed", "u1", `{"country":"VN"}`)))    // exit
	require.NoError(t, svc.Evaluate(ctx, seedProfile(t, f, tid, sid, "e3", "product_viewed", "u1", `{"country":"VN"}`))) // re-enter

	rows, err := f.pool.Query(ctx,
		`SELECT payload_json FROM segment_membership_outbox WHERE tenant_id=$1 AND status='pending' ORDER BY created_at`, tid)
	require.NoError(t, err)
	defer rows.Close()
	var tokens, changes []string
	var seqs []int64
	for rows.Next() {
		var payload []byte
		require.NoError(t, rows.Scan(&payload))
		var mc segment.MembershipChanged
		require.NoError(t, json.Unmarshal(payload, &mc))
		tokens = append(tokens, mc.ReasonEventID)
		changes = append(changes, mc.Change)
		seqs = append(seqs, mc.TransitionSeq)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []string{"e1:1", "e2:2", "e3:3"}, tokens, "three distinct per-flip idempotency tokens")
	require.Equal(t, []string{segment.ChangeEntered, segment.ChangeExited, segment.ChangeEntered}, changes)
	require.Equal(t, []int64{1, 2, 3}, seqs, "transition_seq is monotonic per membership")
}

// TestMembership_FlipRollsBackAtomically proves contract (b): the flip and its
// outbox insert are one tx, so a failure before commit undoes BOTH — no membership
// without its emit, and no emit without the membership.
func TestMembership_FlipRollsBackAtomically(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	repo := segment.NewRepo(f.pool)
	seg, err := repo.CreateSegment(ctx, tid, "s1", "", vnPhoneRule())
	require.NoError(t, err)
	pu := seedProfile(t, f, tid, sid, "e1", "product_viewed", "u1", `{"country":"VN"}`)

	tx, err := repo.Begin(ctx)
	require.NoError(t, err)
	seq, flipped, err := repo.EnterTx(ctx, tx, tid, seg.ID, pu.CustomerProfileID, seg.CurrentVersion)
	require.NoError(t, err)
	require.True(t, flipped)
	require.EqualValues(t, 1, seq)
	require.NoError(t, repo.InsertMembershipOutbox(ctx, tx, tid, "k", []byte(`{}`)))
	require.NoError(t, tx.Rollback(ctx)) // simulate a crash/failure before commit

	status, err := repo.MembershipStatus(ctx, tid, seg.ID, pu.CustomerProfileID)
	require.NoError(t, err)
	require.Equal(t, "", status, "a rolled-back flip leaves no membership row")
	require.Equal(t, 0, countMembershipOutbox(t, f, tid), "a rolled-back emit leaves no outbox row")
}

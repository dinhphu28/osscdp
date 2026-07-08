package foundationtest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/dinhphu28/osscdp/internal/activation"
	"github.com/dinhphu28/osscdp/internal/audit"
	"github.com/dinhphu28/osscdp/internal/behavior"
	"github.com/dinhphu28/osscdp/internal/bus"
	"github.com/dinhphu28/osscdp/internal/consent"
	"github.com/dinhphu28/osscdp/internal/events"
	"github.com/dinhphu28/osscdp/internal/governance"
	"github.com/dinhphu28/osscdp/internal/identity"
	"github.com/dinhphu28/osscdp/internal/journey"
	"github.com/dinhphu28/osscdp/internal/profile"
	"github.com/dinhphu28/osscdp/internal/segment"
)

// --- journey test helpers ---

func mkJourneyDest(t *testing.T, f fixture, tid uuid.UUID, name string) uuid.UUID {
	t.Helper()
	dest, err := activation.NewRepo(f.pool).CreateDestination(context.Background(), tid,
		activation.TypeWebhook, name, webhookDestConfig("http://example.invalid"), "")
	require.NoError(t, err)
	return dest.ID
}

func mkEntrySegment(t *testing.T, f fixture, tid uuid.UUID, name string) uuid.UUID {
	t.Helper()
	seg, err := segment.NewRepo(f.pool).CreateSegment(context.Background(), tid, name, "", vnPhoneRule())
	require.NoError(t, err)
	return seg.ID
}

func activeEnrollmentCount(t *testing.T, f fixture, tid uuid.UUID) int {
	t.Helper()
	var n int
	require.NoError(t, f.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM journey_enrollment WHERE tenant_id=$1 AND status='active'`, tid).Scan(&n))
	return n
}

func enrollmentCountForProfile(t *testing.T, f fixture, tid, profileID uuid.UUID) int {
	t.Helper()
	var n int
	require.NoError(t, f.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM journey_enrollment WHERE tenant_id=$1 AND customer_profile_id=$2`, tid, profileID).Scan(&n))
	return n
}

// enrollmentState returns an enrollment's current step index and status.
func enrollmentState(t *testing.T, f fixture, tid, journeyID, profileID uuid.UUID) (stepIndex int, status string) {
	t.Helper()
	require.NoError(t, f.pool.QueryRow(context.Background(),
		`SELECT current_step_index, status FROM journey_enrollment
		 WHERE tenant_id=$1 AND journey_id=$2 AND customer_profile_id=$3`,
		tid, journeyID, profileID).Scan(&stepIndex, &status))
	return
}

func taskDestination(t *testing.T, f fixture, tid uuid.UUID) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	require.NoError(t, f.pool.QueryRow(context.Background(),
		`SELECT destination_id FROM activation_task WHERE tenant_id=$1`, tid).Scan(&id))
	return id
}

func taskDestForProfile(t *testing.T, f fixture, tid, profileID uuid.UUID) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	require.NoError(t, f.pool.QueryRow(context.Background(),
		`SELECT destination_id FROM activation_task WHERE tenant_id=$1 AND customer_profile_id=$2`, tid, profileID).Scan(&id))
	return id
}

// drainJourneyRunner ticks the runner until no enrollment is claimable (condition/split
// steps are due=now, so a branch takes several ticks to reach its resting step).
func drainJourneyRunner(t *testing.T, r *journey.Runner) {
	t.Helper()
	for i := 0; i < 25; i++ {
		n, err := r.RunOnce(context.Background())
		require.NoError(t, err)
		if n == 0 {
			return
		}
	}
	t.Fatal("journey runner did not drain in 25 ticks")
}

func float64Ptr(v float64) *float64 { return &v }

func newJourneySvc(f fixture, consentReader activation.ConsentReader, now func() time.Time) *journey.Service {
	return newJourneySvcStore(f, consentReader, nil, now)
}

// newJourneySvcStore builds a journey service with an explicit behavior store (for
// condition steps). Pass nil store for wait/send-only journeys.
func newJourneySvcStore(f fixture, consentReader activation.ConsentReader, store segment.BehaviorStore, now func() time.Time) *journey.Service {
	act := activation.NewService(f.pool, profile.NewRepo(f.pool), consentReader)
	svc := journey.NewService(f.pool, profile.NewRepo(f.pool), act, store)
	if now != nil {
		svc.WithClock(now)
	}
	return svc
}

func enteredMC(tid, segID, profileID uuid.UUID, at time.Time) segment.MembershipChanged {
	return segment.MembershipChanged{
		TenantID: tid, SegmentID: segID, CustomerProfileID: profileID,
		Change: segment.ChangeEntered, ReasonEventID: "e1", ChangedAt: at,
	}
}

func exitedMC(tid, segID, profileID uuid.UUID, at time.Time) segment.MembershipChanged {
	return segment.MembershipChanged{
		TenantID: tid, SegmentID: segID, CustomerProfileID: profileID,
		Change: segment.ChangeExited, ReasonEventID: "x1", ChangedAt: at,
	}
}

// TestJourney_LinearWaitThenSend is the Phase-1 end-to-end slice: enter segment ->
// wait -> send. It also asserts enroll idempotency and send exactly-once.
func TestJourney_LinearWaitThenSend(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	pu := seedProfile(t, f, tid, sid, "ev1", "product_viewed", "u1", `{"country":"VN"}`)
	pid := pu.CustomerProfileID

	destID := mkJourneyDest(t, f, tid, "wh")
	segID := mkEntrySegment(t, f, tid, "vn")
	def := journey.Definition{Steps: []journey.Step{
		{Type: journey.StepWait, Duration: "1h"},
		{Type: journey.StepSend, DestinationID: destID},
	}}
	require.NoError(t, journey.Validate(def))
	j, err := journey.NewRepo(f.pool).CreateJourney(ctx, tid, "welcome", "", segID, false, 1, def)
	require.NoError(t, err)

	clk := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	now := func() time.Time { return clk }
	svc := newJourneySvc(f, nil, now)

	// Entry enrolls the profile; a redelivered entry is a no-op (partial-unique-active).
	require.NoError(t, svc.EnrollOnMembership(ctx, enteredMC(tid, segID, pid, clk)))
	require.NoError(t, svc.EnrollOnMembership(ctx, enteredMC(tid, segID, pid, clk)))
	require.Equal(t, 1, activeEnrollmentCount(t, f, tid))

	runner := journey.NewRunner(svc, 50, 50, time.Minute, time.Second, testLogger()).WithClock(now)

	// Tick 1 executes the wait step: advance to the send step, due 1h later, no task yet.
	n, err := runner.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, 0, taskCount(t, f, tid))
	idx, status := enrollmentState(t, f, tid, j.ID, pid)
	require.Equal(t, 1, idx)
	require.Equal(t, journey.EnrollmentActive, status)

	// The wait has not elapsed: a tick before due_at claims nothing.
	n, err = runner.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, n)

	// Advance the clock past the wait, then tick: the send fires and the journey completes.
	clk = clk.Add(2 * time.Hour)
	n, err = runner.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, 1, taskCount(t, f, tid))
	_, status = enrollmentState(t, f, tid, j.ID, pid)
	require.Equal(t, journey.EnrollmentCompleted, status)
	require.Equal(t, destID, taskDestination(t, f, tid))

	// Exactly-once: a completed enrollment is not re-claimed and no second task appears.
	n, err = runner.RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, n)
	require.Equal(t, 1, taskCount(t, f, tid))
}

// TestJourney_ConsentDeniedSkipsSend guards the extracted activation.EnqueueSend: a
// send to a destination the customer has opted out of is recorded as a skipped task,
// never delivered — mirroring the segment-activation consent gate.
func TestJourney_ConsentDeniedSkipsSend(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	pu := seedProfile(t, f, tid, sid, "ev1", "product_viewed", "u1", `{"country":"VN"}`)
	pid := pu.CustomerProfileID

	destID := mkJourneyDest(t, f, tid, "wh")
	// Deny consent for the webhook channel / marketing purpose (the destination default).
	require.NoError(t, consent.NewRepo(f.pool).Set(ctx, tid, pid, "webhook", "marketing", "denied", "test"))

	segID := mkEntrySegment(t, f, tid, "vn")
	def := journey.Definition{Steps: []journey.Step{{Type: journey.StepSend, DestinationID: destID}}}
	j, err := journey.NewRepo(f.pool).CreateJourney(ctx, tid, "welcome", "", segID, false, 1, def)
	require.NoError(t, err)

	clk := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	now := func() time.Time { return clk }
	svc := newJourneySvc(f, consent.NewRepo(f.pool), now)

	require.NoError(t, svc.EnrollOnMembership(ctx, enteredMC(tid, segID, pid, clk)))
	n, err := journey.NewRunner(svc, 50, 50, time.Minute, time.Second, testLogger()).WithClock(now).RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	require.Equal(t, 1, taskCount(t, f, tid))
	status, _, _ := taskStatus(t, f, tid)
	require.Equal(t, activation.TaskSkipped, status)
	_, estatus := enrollmentState(t, f, tid, j.ID, pid)
	require.Equal(t, journey.EnrollmentCompleted, estatus)
}

// TestJourney_StaleRunnerNoDoubleAdvance verifies the single-table claim fence: an
// advance written with a stale (reclaimed) fence/step_seq touches zero rows — no
// rewind, no double-advance.
func TestJourney_StaleRunnerNoDoubleAdvance(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	pu := seedProfile(t, f, tid, sid, "ev1", "product_viewed", "u1", `{"country":"VN"}`)
	pid := pu.CustomerProfileID

	destID := mkJourneyDest(t, f, tid, "wh")
	segID := mkEntrySegment(t, f, tid, "vn")
	def := journey.Definition{Steps: []journey.Step{
		{Type: journey.StepWait, Duration: "1h"},
		{Type: journey.StepSend, DestinationID: destID},
	}}
	j, err := journey.NewRepo(f.pool).CreateJourney(ctx, tid, "welcome", "", segID, false, 1, def)
	require.NoError(t, err)

	repo := journey.NewRepo(f.pool)
	clk := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	require.NoError(t, newJourneySvc(f, nil, func() time.Time { return clk }).EnrollOnMembership(ctx, enteredMC(tid, segID, pid, clk)))

	// Claim the enrollment (fence = clk), advance it once (wait -> step 1, step_seq -> 1).
	rows, err := repo.ClaimDueEnrollments(ctx, clk, 50, 50, time.Minute)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	e := rows[0]
	ok, err := repo.Advance(ctx, e, clk, 1, clk.Add(time.Hour), journey.EnrollmentActive)
	require.NoError(t, err)
	require.True(t, ok)

	// A stale runner replays the SAME claimed row (old fence + old step_seq): no-op.
	ok, err = repo.Advance(ctx, e, clk, 1, clk.Add(time.Hour), journey.EnrollmentActive)
	require.NoError(t, err)
	require.False(t, ok, "stale fenced advance must touch zero rows")

	idx, status := enrollmentState(t, f, tid, j.ID, pid)
	require.Equal(t, 1, idx)
	require.Equal(t, journey.EnrollmentActive, status)
}

// TestJourney_ErasureRemovesEnrollment verifies governance.Delete purges a profile's
// journey enrollments in the erasure transaction.
func TestJourney_ErasureRemovesEnrollment(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	pu := seedProfile(t, f, tid, sid, "ev1", "product_viewed", "u1", `{"country":"VN"}`)
	pid := pu.CustomerProfileID

	destID := mkJourneyDest(t, f, tid, "wh")
	segID := mkEntrySegment(t, f, tid, "vn")
	def := journey.Definition{Steps: []journey.Step{{Type: journey.StepSend, DestinationID: destID}}}
	_, err := journey.NewRepo(f.pool).CreateJourney(ctx, tid, "welcome", "", segID, false, 1, def)
	require.NoError(t, err)

	clk := time.Now().UTC()
	require.NoError(t, newJourneySvc(f, nil, func() time.Time { return clk }).EnrollOnMembership(ctx, enteredMC(tid, segID, pid, clk)))
	require.Equal(t, 1, enrollmentCountForProfile(t, f, tid, pid))

	gov := governance.NewService(f.pool, audit.NewRecorder(f.pool), nil)
	counts, err := gov.Delete(ctx, tid, pu.CanonicalUserID)
	require.NoError(t, err)
	require.Equal(t, int64(1), counts.JourneyEnrollments)
	require.Equal(t, 0, enrollmentCountForProfile(t, f, tid, pid))
}

// TestJourney_VersionPinning verifies an in-flight enrollment advances on the version
// pinned at enroll, not a later re-authored version.
func TestJourney_VersionPinning(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	pu := seedProfile(t, f, tid, sid, "ev1", "product_viewed", "u1", `{"country":"VN"}`)
	pid := pu.CustomerProfileID

	destV1 := mkJourneyDest(t, f, tid, "wh-v1")
	destV2 := mkJourneyDest(t, f, tid, "wh-v2")
	segID := mkEntrySegment(t, f, tid, "vn")
	jrepo := journey.NewRepo(f.pool)
	j, err := jrepo.CreateJourney(ctx, tid, "welcome", "", segID, false, 1, journey.Definition{Steps: []journey.Step{
		{Type: journey.StepWait, Duration: "1h"},
		{Type: journey.StepSend, DestinationID: destV1},
	}})
	require.NoError(t, err)

	clk := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	now := func() time.Time { return clk }
	svc := newJourneySvc(f, nil, now)
	require.NoError(t, svc.EnrollOnMembership(ctx, enteredMC(tid, segID, pid, clk)))

	// Re-author the journey to send to a DIFFERENT destination (mints version 2).
	_, err = jrepo.UpdateJourney(ctx, tid, j.ID, "", false, 1, journey.Definition{Steps: []journey.Step{
		{Type: journey.StepWait, Duration: "1h"},
		{Type: journey.StepSend, DestinationID: destV2},
	}})
	require.NoError(t, err)
	updated, err := jrepo.GetJourney(ctx, tid, j.ID)
	require.NoError(t, err)
	require.Equal(t, 2, updated.CurrentVersion)

	// Drive the pinned (v1) enrollment to its send.
	runner := journey.NewRunner(svc, 50, 50, time.Minute, time.Second, testLogger()).WithClock(now)
	_, err = runner.RunOnce(ctx) // wait
	require.NoError(t, err)
	clk = clk.Add(2 * time.Hour)
	_, err = runner.RunOnce(ctx) // send
	require.NoError(t, err)

	require.Equal(t, 1, taskCount(t, f, tid))
	require.Equal(t, destV1, taskDestination(t, f, tid), "send must use the pinned v1 destination")
}

// TestJourney_MergeMovesEnrollmentAtomically verifies an identity merge moves the
// loser's single-row enrollment onto the survivor (no orphan, no duplicate).
func TestJourney_MergeMovesEnrollmentAtomically(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")

	destID := mkJourneyDest(t, f, tid, "wh")
	segID := mkEntrySegment(t, f, tid, "vn")
	j, err := journey.NewRepo(f.pool).CreateJourney(ctx, tid, "welcome", "", segID, false, 1,
		journey.Definition{Steps: []journey.Step{{Type: journey.StepSend, DestinationID: destID}}})
	require.NoError(t, err)

	pub := &reparentPub{}
	idSvc := identity.NewService(f.pool, pub, bus.TopicIdentityResolved)
	profSvc := profile.NewService(f.pool, noopPub{}, bus.TopicProfileUpdated)
	prepo := profile.NewRepo(f.pool)
	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	// Two distinct clusters; the older (a1) will survive the merge.
	resolveAndUpdate(t, idSvc, pub, profSvc, mergeEnv(t, tid, sid, "e1", events.Identifiers{AnonymousID: "a1"}, "", base))
	ir2 := resolveAndUpdate(t, idSvc, pub, profSvc, mergeEnv(t, tid, sid, "e2", events.Identifiers{UserID: "u1"}, "", base.Add(time.Hour)))
	loser, err := prepo.GetByCanonical(ctx, tid, ir2.CanonicalUserID)
	require.NoError(t, err)

	// Enroll the (soon-to-be) loser profile directly into the journey.
	created, err := journey.NewRepo(f.pool).Enroll(ctx, tid, j.ID, loser.ID, j.CurrentVersion, 1, base.Add(time.Hour))
	require.NoError(t, err)
	require.True(t, created)

	// One event carrying both identifiers merges the clusters (survivor = older a1).
	ir3 := resolveAndUpdate(t, idSvc, pub, profSvc, mergeEnv(t, tid, sid, "e3", events.Identifiers{AnonymousID: "a1", UserID: "u1"}, "", base.Add(2*time.Hour)))
	require.True(t, ir3.MergeOccurred)
	survivor, err := prepo.GetByCanonical(ctx, tid, ir3.CanonicalUserID)
	require.NoError(t, err)
	require.NotEqual(t, survivor.ID, loser.ID)

	// The enrollment moved wholesale to the survivor; the loser has none.
	require.Equal(t, 1, enrollmentCountForProfile(t, f, tid, survivor.ID))
	require.Equal(t, 0, enrollmentCountForProfile(t, f, tid, loser.ID))
}

// --- Phase 2: exit-on-segment-leave ---

// TestJourney_ExitOnSegmentLeave verifies that when a profile leaves the entry segment
// of an exit-on-leave journey, its active enrollment is terminated and the runner stops
// advancing it. Exit is idempotent.
func TestJourney_ExitOnSegmentLeave(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	pu := seedProfile(t, f, tid, sid, "ev1", "product_viewed", "u1", `{"country":"VN"}`)
	pid := pu.CustomerProfileID

	destID := mkJourneyDest(t, f, tid, "wh")
	segID := mkEntrySegment(t, f, tid, "vn")
	def := journey.Definition{Steps: []journey.Step{
		{Type: journey.StepWait, Duration: "1h"},
		{Type: journey.StepSend, DestinationID: destID},
	}}
	// exit_on_segment_leave = true.
	j, err := journey.NewRepo(f.pool).CreateJourney(ctx, tid, "welcome", "", segID, true, 1, def)
	require.NoError(t, err)
	require.True(t, j.ExitOnSegmentLeave)

	clk := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	now := func() time.Time { return clk }
	svc := newJourneySvc(f, nil, now)

	require.NoError(t, svc.OnMembershipChanged(ctx, enteredMC(tid, segID, pid, clk)))
	require.Equal(t, 1, activeEnrollmentCount(t, f, tid))

	// The profile leaves the entry segment → active enrollment is exited.
	require.NoError(t, svc.OnMembershipChanged(ctx, exitedMC(tid, segID, pid, clk)))
	_, status := enrollmentState(t, f, tid, j.ID, pid)
	require.Equal(t, journey.EnrollmentExited, status)
	require.Equal(t, 0, activeEnrollmentCount(t, f, tid))

	// Idempotent: a redelivered exit is a no-op (no active enrollment left).
	require.NoError(t, svc.OnMembershipChanged(ctx, exitedMC(tid, segID, pid, clk)))

	// The runner never advances an exited enrollment, so no send is enqueued even after
	// the wait would have elapsed.
	clk = clk.Add(2 * time.Hour)
	n, err := journey.NewRunner(svc, 50, 50, time.Minute, time.Second, testLogger()).WithClock(now).RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, n)
	require.Equal(t, 0, taskCount(t, f, tid))
}

// TestJourney_NoExitWhenFlagOff verifies a journey WITHOUT exit_on_segment_leave keeps
// running when the profile leaves the entry segment (default run-to-completion).
func TestJourney_NoExitWhenFlagOff(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	pu := seedProfile(t, f, tid, sid, "ev1", "product_viewed", "u1", `{"country":"VN"}`)
	pid := pu.CustomerProfileID

	destID := mkJourneyDest(t, f, tid, "wh")
	segID := mkEntrySegment(t, f, tid, "vn")
	def := journey.Definition{Steps: []journey.Step{{Type: journey.StepSend, DestinationID: destID}}}
	j, err := journey.NewRepo(f.pool).CreateJourney(ctx, tid, "welcome", "", segID, false, 1, def)
	require.NoError(t, err)

	clk := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	now := func() time.Time { return clk }
	svc := newJourneySvc(f, nil, now)

	require.NoError(t, svc.OnMembershipChanged(ctx, enteredMC(tid, segID, pid, clk)))
	// Leaving the segment does NOT exit (flag off).
	require.NoError(t, svc.OnMembershipChanged(ctx, exitedMC(tid, segID, pid, clk)))
	_, status := enrollmentState(t, f, tid, j.ID, pid)
	require.Equal(t, journey.EnrollmentActive, status)

	// The runner still advances it to a send.
	n, err := journey.NewRunner(svc, 50, 50, time.Minute, time.Second, testLogger()).WithClock(now).RunOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, 1, taskCount(t, f, tid))
}

// TestJourney_OnceOnlyReEntryAfterTerminal verifies re-entering a segment after the
// enrollment has terminated is a clean no-op (once-only) — not a primary-key error.
func TestJourney_OnceOnlyReEntryAfterTerminal(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	pu := seedProfile(t, f, tid, sid, "ev1", "product_viewed", "u1", `{"country":"VN"}`)
	pid := pu.CustomerProfileID

	destID := mkJourneyDest(t, f, tid, "wh")
	segID := mkEntrySegment(t, f, tid, "vn")
	def := journey.Definition{Steps: []journey.Step{{Type: journey.StepSend, DestinationID: destID}}}
	j, err := journey.NewRepo(f.pool).CreateJourney(ctx, tid, "welcome", "", segID, false, 1, def)
	require.NoError(t, err)

	clk := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	now := func() time.Time { return clk }
	svc := newJourneySvc(f, nil, now)

	// Enroll then run to completion (single send step).
	require.NoError(t, svc.OnMembershipChanged(ctx, enteredMC(tid, segID, pid, clk)))
	_, err = journey.NewRunner(svc, 50, 50, time.Minute, time.Second, testLogger()).WithClock(now).RunOnce(ctx)
	require.NoError(t, err)
	_, status := enrollmentState(t, f, tid, j.ID, pid)
	require.Equal(t, journey.EnrollmentCompleted, status)

	// Re-entering the segment must NOT error and must NOT create a second enrollment.
	require.NoError(t, svc.OnMembershipChanged(ctx, enteredMC(tid, segID, pid, clk)))
	require.Equal(t, 1, enrollmentCountForProfile(t, f, tid, pid))
	require.Equal(t, 1, taskCount(t, f, tid))
}

// TestJourney_ExitWinsRaceWithAdvance verifies the status='active' guard on Advance:
// an exit that fires while an enrollment is claimed wins — the in-flight advance no-ops.
func TestJourney_ExitWinsRaceWithAdvance(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	pu := seedProfile(t, f, tid, sid, "ev1", "product_viewed", "u1", `{"country":"VN"}`)
	pid := pu.CustomerProfileID

	destID := mkJourneyDest(t, f, tid, "wh")
	segID := mkEntrySegment(t, f, tid, "vn")
	def := journey.Definition{Steps: []journey.Step{
		{Type: journey.StepWait, Duration: "1h"},
		{Type: journey.StepSend, DestinationID: destID},
	}}
	j, err := journey.NewRepo(f.pool).CreateJourney(ctx, tid, "welcome", "", segID, true, 1, def)
	require.NoError(t, err)

	repo := journey.NewRepo(f.pool)
	clk := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	require.NoError(t, newJourneySvc(f, nil, func() time.Time { return clk }).OnMembershipChanged(ctx, enteredMC(tid, segID, pid, clk)))

	// A runner claims the enrollment (fence = clk).
	rows, err := repo.ClaimDueEnrollments(ctx, clk, 50, 50, time.Minute)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	e := rows[0]

	// Concurrently, the profile leaves the segment: the enrollment is exited while claimed.
	exited, err := repo.ExitActiveEnrollmentsForSegment(ctx, tid, segID, pid)
	require.NoError(t, err)
	require.Equal(t, int64(1), exited)

	// The in-flight advance (captured fence) now writes zero rows — exit wins.
	ok, err := repo.Advance(ctx, e, clk, 1, clk.Add(time.Hour), journey.EnrollmentActive)
	require.NoError(t, err)
	require.False(t, ok, "advance must no-op once the enrollment is exited")

	_, status := enrollmentState(t, f, tid, j.ID, pid)
	require.Equal(t, journey.EnrollmentExited, status)
}

// TestJourney_FailDoesNotParkExited verifies FailEnrollment cannot (re-)park an
// enrollment that a concurrent exit already terminated — the status='active' guard
// makes the fail a no-op (adversarial-review finding).
func TestJourney_FailDoesNotParkExited(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	pu := seedProfile(t, f, tid, sid, "ev1", "product_viewed", "u1", `{"country":"VN"}`)
	pid := pu.CustomerProfileID

	destID := mkJourneyDest(t, f, tid, "wh")
	segID := mkEntrySegment(t, f, tid, "vn")
	def := journey.Definition{Steps: []journey.Step{
		{Type: journey.StepWait, Duration: "1h"},
		{Type: journey.StepSend, DestinationID: destID},
	}}
	j, err := journey.NewRepo(f.pool).CreateJourney(ctx, tid, "welcome", "", segID, true, 1, def)
	require.NoError(t, err)

	repo := journey.NewRepo(f.pool)
	clk := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	require.NoError(t, newJourneySvc(f, nil, func() time.Time { return clk }).OnMembershipChanged(ctx, enteredMC(tid, segID, pid, clk)))

	// Claim, then the customer leaves the segment (enrollment exited while claimed).
	rows, err := repo.ClaimDueEnrollments(ctx, clk, 50, 50, time.Minute)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	exited, err := repo.ExitActiveEnrollmentsForSegment(ctx, tid, segID, pid)
	require.NoError(t, err)
	require.Equal(t, int64(1), exited)

	// The runner's failure path fires (maxAttempts=1 so it would park an active row):
	// on the exited row it must no-op — no park, status stays exited.
	attempts, parked, err := repo.FailEnrollment(ctx, tid, j.ID, pid, 0, clk, "boom", time.Second, time.Minute, 1)
	require.NoError(t, err)
	require.False(t, parked, "an exited enrollment must never be parked")
	require.Equal(t, 0, attempts)

	_, status := enrollmentState(t, f, tid, j.ID, pid)
	require.Equal(t, journey.EnrollmentExited, status)
	n, err := repo.ParkedCount(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(0), n)
}

// TestJourney_ArchivedJourneyNotExitedOnLeave verifies an archived journey's in-flight
// enrollments are NOT exited on segment leave — they drain to completion, matching the
// entry path (active-only) and the DeactivateJourney contract (adversarial-review finding).
func TestJourney_ArchivedJourneyNotExitedOnLeave(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	pu := seedProfile(t, f, tid, sid, "ev1", "product_viewed", "u1", `{"country":"VN"}`)
	pid := pu.CustomerProfileID

	destID := mkJourneyDest(t, f, tid, "wh")
	segID := mkEntrySegment(t, f, tid, "vn")
	def := journey.Definition{Steps: []journey.Step{
		{Type: journey.StepWait, Duration: "1h"},
		{Type: journey.StepSend, DestinationID: destID},
	}}
	jrepo := journey.NewRepo(f.pool)
	j, err := jrepo.CreateJourney(ctx, tid, "welcome", "", segID, true, 1, def)
	require.NoError(t, err)

	clk := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	svc := newJourneySvc(f, nil, func() time.Time { return clk })
	require.NoError(t, svc.OnMembershipChanged(ctx, enteredMC(tid, segID, pid, clk)))

	// Archive the journey, THEN the customer leaves the segment.
	require.NoError(t, jrepo.DeactivateJourney(ctx, tid, j.ID))
	require.NoError(t, svc.OnMembershipChanged(ctx, exitedMC(tid, segID, pid, clk)))

	// The enrollment is untouched (archived journeys drain, not exit).
	_, status := enrollmentState(t, f, tid, j.ID, pid)
	require.Equal(t, journey.EnrollmentActive, status)
}

// --- Phase 3: branching (condition / split) + retention horizon ---

// TestJourney_ConditionRoutes verifies a condition step routes to different sends based
// on a (stateless) profile-trait rule, and the forward Next lets the two arms stay
// disjoint (each profile gets exactly one send, to the branch its rule selected).
func TestJourney_ConditionRoutes(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	vn := seedProfile(t, f, tid, sid, "ev1", "product_viewed", "u1", `{"country":"VN"}`)
	us := seedProfile(t, f, tid, sid, "ev2", "product_viewed", "u2", `{"country":"US"}`)

	destA := mkJourneyDest(t, f, tid, "whA")
	destB := mkJourneyDest(t, f, tid, "whB")
	segID := mkEntrySegment(t, f, tid, "vn")
	rule := segment.Rule{Field: "profile.traits.country", Op: segment.OpEq, Value: "VN"}
	def := journey.Definition{Steps: []journey.Step{
		{Type: journey.StepCondition, Condition: &rule, IfTrue: 1, IfFalse: 2},
		{Type: journey.StepSend, DestinationID: destA, Next: 3}, // true arm jumps past the false arm
		{Type: journey.StepSend, DestinationID: destB},          // false arm -> 3 (complete)
	}}
	require.NoError(t, journey.Validate(def))
	_, err := journey.NewRepo(f.pool).CreateJourney(ctx, tid, "welcome", "", segID, false, 1, def)
	require.NoError(t, err)

	clk := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	now := func() time.Time { return clk }
	svc := newJourneySvc(f, nil, now)
	require.NoError(t, svc.OnMembershipChanged(ctx, enteredMC(tid, segID, vn.CustomerProfileID, clk)))
	require.NoError(t, svc.OnMembershipChanged(ctx, enteredMC(tid, segID, us.CustomerProfileID, clk)))

	drainJourneyRunner(t, journey.NewRunner(svc, 50, 50, time.Minute, time.Second, testLogger()).WithClock(now))

	require.Equal(t, 2, taskCount(t, f, tid))
	require.Equal(t, destA, taskDestForProfile(t, f, tid, vn.CustomerProfileID), "VN routes to the true arm (destA)")
	require.Equal(t, destB, taskDestForProfile(t, f, tid, us.CustomerProfileID), "US routes to the false arm (destB)")
}

// TestJourney_ConditionalSkipCompletes verifies a condition whose false arm targets
// len(steps) completes the enrollment with no send (a conditional-send / goal shape).
func TestJourney_ConditionalSkipCompletes(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	us := seedProfile(t, f, tid, sid, "ev1", "product_viewed", "u1", `{"country":"US"}`)

	destA := mkJourneyDest(t, f, tid, "whA")
	segID := mkEntrySegment(t, f, tid, "vn")
	rule := segment.Rule{Field: "profile.traits.country", Op: segment.OpEq, Value: "VN"}
	def := journey.Definition{Steps: []journey.Step{
		{Type: journey.StepCondition, Condition: &rule, IfTrue: 1, IfFalse: 2}, // false -> 2 == len => complete
		{Type: journey.StepSend, DestinationID: destA},
	}}
	require.NoError(t, journey.Validate(def))
	j, err := journey.NewRepo(f.pool).CreateJourney(ctx, tid, "welcome", "", segID, false, 1, def)
	require.NoError(t, err)

	clk := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	now := func() time.Time { return clk }
	svc := newJourneySvc(f, nil, now)
	require.NoError(t, svc.OnMembershipChanged(ctx, enteredMC(tid, segID, us.CustomerProfileID, clk)))
	drainJourneyRunner(t, journey.NewRunner(svc, 50, 50, time.Minute, time.Second, testLogger()).WithClock(now))

	require.Equal(t, 0, taskCount(t, f, tid), "US fails the condition and completes with no send")
	_, status := enrollmentState(t, f, tid, j.ID, us.CustomerProfileID)
	require.Equal(t, journey.EnrollmentCompleted, status)
}

// TestJourney_VersionMetadataFromCondition verifies a behavioral condition's window +
// event name are derived into journey_version metadata (mirrors segment_version).
func TestJourney_VersionMetadataFromCondition(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, _ := mkTenant(t, f, "acme")
	destA := mkJourneyDest(t, f, tid, "whA")
	segID := mkEntrySegment(t, f, tid, "vn")

	rule := segment.Rule{Behavior: &segment.BehaviorSpec{
		Kind: segment.BehaviorCount, EventName: "order_completed", Window: "30d", Op: segment.OpGte, Value: float64Ptr(1),
	}}
	def := journey.Definition{Steps: []journey.Step{
		{Type: journey.StepCondition, Condition: &rule, IfTrue: 1, IfFalse: 2},
		{Type: journey.StepSend, DestinationID: destA, Next: 3},
		{Type: journey.StepSend, DestinationID: destA},
	}}
	require.NoError(t, journey.Validate(def))
	j, err := journey.NewRepo(f.pool).CreateJourney(ctx, tid, "welcome", "", segID, false, 1, def)
	require.NoError(t, err)

	var maxWin int64
	var names []string
	require.NoError(t, f.pool.QueryRow(ctx,
		`SELECT max_window_seconds, referenced_event_names FROM journey_version WHERE tenant_id=$1 AND journey_id=$2 AND version=1`,
		tid, j.ID).Scan(&maxWin, &names))
	require.Equal(t, int64(30*24*3600), maxWin)
	require.Contains(t, names, "order_completed")
}

// TestJourney_RetentionHorizonWidened verifies a journey with a behavioral condition
// widens the behavioral retention horizon, so its window's data is never pruned early.
func TestJourney_RetentionHorizonWidened(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, _ := mkTenant(t, f, "acme")
	destA := mkJourneyDest(t, f, tid, "whA")
	segID := mkEntrySegment(t, f, tid, "vn")

	ret := behavior.NewRetention(f.pool, time.Hour, time.Hour, testLogger())
	base, err := ret.EffectiveHorizon(ctx)
	require.NoError(t, err)
	require.Less(t, base, 30*24*time.Hour, "baseline horizon should be small before the journey exists")

	rule := segment.Rule{Behavior: &segment.BehaviorSpec{
		Kind: segment.BehaviorCount, EventName: "order_completed", Window: "30d", Op: segment.OpGte, Value: float64Ptr(1),
	}}
	def := journey.Definition{Steps: []journey.Step{
		{Type: journey.StepCondition, Condition: &rule, IfTrue: 1, IfFalse: 2},
		{Type: journey.StepSend, DestinationID: destA, Next: 3},
		{Type: journey.StepSend, DestinationID: destA},
	}}
	_, err = journey.NewRepo(f.pool).CreateJourney(ctx, tid, "welcome", "", segID, false, 1, def)
	require.NoError(t, err)

	widened, err := ret.EffectiveHorizon(ctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, widened, 30*24*time.Hour, "journey condition window must widen the retention horizon")
}

// TestJourney_RetentionHorizonWidened_ArchivedWithEnrollment verifies the retention
// horizon protects a journey_version pinned by a LIVE enrollment even after the journey
// is archived (the first EXISTS branch of EffectiveHorizon) — an in-flight enrollment
// on an archived journey must still be able to read its condition window.
func TestJourney_RetentionHorizonWidened_ArchivedWithEnrollment(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	pu := seedProfile(t, f, tid, sid, "ev1", "product_viewed", "u1", `{"country":"VN"}`)
	destA := mkJourneyDest(t, f, tid, "whA")
	segID := mkEntrySegment(t, f, tid, "vn")

	rule := segment.Rule{Behavior: &segment.BehaviorSpec{
		Kind: segment.BehaviorCount, EventName: "order_completed", Window: "30d", Op: segment.OpGte, Value: float64Ptr(1),
	}}
	def := journey.Definition{Steps: []journey.Step{
		{Type: journey.StepWait, Duration: "1h"},
		{Type: journey.StepCondition, Condition: &rule, IfTrue: 2, IfFalse: 3},
		{Type: journey.StepSend, DestinationID: destA, Next: 4},
		{Type: journey.StepSend, DestinationID: destA},
	}}
	jrepo := journey.NewRepo(f.pool)
	j, err := jrepo.CreateJourney(ctx, tid, "welcome", "", segID, false, 1, def)
	require.NoError(t, err)

	// Enroll a profile (pins version 1), then ARCHIVE the journey.
	created, err := jrepo.Enroll(ctx, tid, j.ID, pu.CustomerProfileID, j.CurrentVersion, 1, time.Now().UTC())
	require.NoError(t, err)
	require.True(t, created)
	require.NoError(t, jrepo.DeactivateJourney(ctx, tid, j.ID))

	// Even though the journey is archived, the pinned version's window must still widen
	// the horizon (the live enrollment could still evaluate the condition).
	ret := behavior.NewRetention(f.pool, time.Hour, time.Hour, testLogger())
	h, err := ret.EffectiveHorizon(ctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, h, 30*24*time.Hour, "archived journey with a live enrollment must still protect its condition window")
}

// --- Phase 4: event-triggered entry + population backfill ---

func insertActiveMembership(t *testing.T, f fixture, tid, segID, profileID uuid.UUID) {
	t.Helper()
	_, err := f.pool.Exec(context.Background(),
		`INSERT INTO segment_membership (tenant_id, segment_id, customer_profile_id, status, entered_at, last_evaluated_at, version)
		 VALUES ($1,$2,$3,'active', now(), now(), 1)`, tid, segID, profileID)
	require.NoError(t, err)
}

func journeySeedJobCount(t *testing.T, f fixture, tid uuid.UUID) int {
	t.Helper()
	var n int
	require.NoError(t, f.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM journey_seed_job WHERE tenant_id=$1`, tid).Scan(&n))
	return n
}

// TestJourney_EventEntry verifies a profile is enrolled into an event-entry journey when
// it emits the matching event, not otherwise, and idempotently under redelivery.
func TestJourney_EventEntry(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	pu := seedProfile(t, f, tid, sid, "ev1", "signup_completed", "u1", `{"country":"VN"}`)

	destID := mkJourneyDest(t, f, tid, "wh")
	def := journey.Definition{Steps: []journey.Step{{Type: journey.StepSend, DestinationID: destID}}}
	require.NoError(t, journey.Validate(def))
	j, err := journey.NewRepo(f.pool).CreateEventJourney(ctx, tid, "welcome", "", "signup_completed", false, 1, def)
	require.NoError(t, err)
	require.Equal(t, "signup_completed", j.EntryEventName)
	require.Nil(t, j.EntrySegmentID)

	svc := newJourneySvc(f, nil, nil)

	// A profile_updated for a DIFFERENT event does not enroll.
	other := seedProfile(t, f, tid, sid, "ev2", "product_viewed", "u2", `{}`)
	require.NoError(t, svc.EnrollOnEvent(ctx, other))
	require.Equal(t, 0, activeEnrollmentCount(t, f, tid))

	// The matching event enrolls; a redelivery is idempotent.
	require.NoError(t, svc.EnrollOnEvent(ctx, pu))
	require.NoError(t, svc.EnrollOnEvent(ctx, pu))
	require.Equal(t, 1, activeEnrollmentCount(t, f, tid))
	require.Equal(t, 1, enrollmentCountForProfile(t, f, tid, pu.CustomerProfileID))
}

// TestJourney_SeedBackfillEnrollsEntrySegmentMembers verifies creating a segment-entry
// journey enqueues a seed job, and the journey SeedRunner backfills the entry segment's
// CURRENT active members (paged, resumable), completing the job.
func TestJourney_SeedBackfillEnrollsEntrySegmentMembers(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	segID := mkEntrySegment(t, f, tid, "vn")
	destID := mkJourneyDest(t, f, tid, "wh")

	// Three existing active members of the entry segment.
	var members []uuid.UUID
	for i := 1; i <= 3; i++ {
		pu := seedProfile(t, f, tid, sid, "ev"+string(rune('0'+i)), "product_viewed", "u"+string(rune('0'+i)), `{"country":"VN"}`)
		insertActiveMembership(t, f, tid, segID, pu.CustomerProfileID)
		members = append(members, pu.CustomerProfileID)
	}

	def := journey.Definition{Steps: []journey.Step{{Type: journey.StepSend, DestinationID: destID}}}
	j, err := journey.NewRepo(f.pool).CreateJourney(ctx, tid, "welcome", "", segID, false, 1, def)
	require.NoError(t, err)
	require.Equal(t, 1, journeySeedJobCount(t, f, tid), "creating a segment-entry journey enqueues a backfill job")

	// Drain with a small page size to exercise multi-page resumption.
	runner := journey.NewSeedRunner(journey.NewRepo(f.pool), 10, time.Minute, time.Second, testLogger()).WithPageSize(2)
	idle, err := runner.RunOnce(ctx)
	require.NoError(t, err)
	require.False(t, idle)

	// All current members are now enrolled, and the job is complete (removed).
	require.Equal(t, 3, activeEnrollmentCount(t, f, tid))
	for _, pid := range members {
		require.Equal(t, 1, enrollmentCountForProfile(t, f, tid, pid))
	}
	require.Equal(t, 0, journeySeedJobCount(t, f, tid), "the seed job is removed once fully drained")

	// Idempotent: a re-enqueued+re-drained seed does not double-enroll.
	require.Equal(t, 1, enrollmentCountForProfile(t, f, tid, members[0]))
	_ = j
}

// TestJourney_DeactivateDropsSeedJob verifies archiving a journey removes its pending
// backfill seed job so the seed runner stops re-claiming an inert job.
func TestJourney_DeactivateDropsSeedJob(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, _ := mkTenant(t, f, "acme")
	segID := mkEntrySegment(t, f, tid, "vn")
	destID := mkJourneyDest(t, f, tid, "wh")

	def := journey.Definition{Steps: []journey.Step{{Type: journey.StepSend, DestinationID: destID}}}
	jrepo := journey.NewRepo(f.pool)
	j, err := jrepo.CreateJourney(ctx, tid, "welcome", "", segID, false, 1, def)
	require.NoError(t, err)
	require.Equal(t, 1, journeySeedJobCount(t, f, tid))

	require.NoError(t, jrepo.DeactivateJourney(ctx, tid, j.ID))
	require.Equal(t, 0, journeySeedJobCount(t, f, tid), "archiving drops the pending seed job")

	// The seed runner finds nothing to do.
	idle, err := journey.NewSeedRunner(jrepo, 10, time.Minute, time.Second, testLogger()).RunOnce(ctx)
	require.NoError(t, err)
	require.True(t, idle)
}

// TestJourney_EventEntrySkipsErasedProfile verifies a profile_updated whose profile no
// longer exists (erased/merged before a redelivery) does NOT create an orphan enrollment
// (journey_enrollment has no FK to customer_profile).
func TestJourney_EventEntrySkipsErasedProfile(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, _ := mkTenant(t, f, "acme")

	destID := mkJourneyDest(t, f, tid, "wh")
	def := journey.Definition{Steps: []journey.Step{{Type: journey.StepSend, DestinationID: destID}}}
	_, err := journey.NewRepo(f.pool).CreateEventJourney(ctx, tid, "welcome", "", "signup_completed", false, 1, def)
	require.NoError(t, err)

	// A stale profile_updated for a profile id that does not exist.
	pu := profile.ProfileUpdated{
		TenantID:          tid,
		CustomerProfileID: uuid.New(),
		Event:             events.Envelope{EventName: "signup_completed"},
	}
	require.NoError(t, newJourneySvc(f, nil, nil).EnrollOnEvent(ctx, pu))
	require.Equal(t, 0, activeEnrollmentCount(t, f, tid), "vanished profile must not be enrolled")
}

// --- Phase 5: re-entry (max_enrollments) + terminal-row retention ---

// TestJourney_ReEntryAfterTerminal verifies max_enrollments>1 lets a profile re-enter
// after a terminal enrollment (a fresh enrollment_seq per run), capped at the max.
func TestJourney_ReEntryAfterTerminal(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	pu := seedProfile(t, f, tid, sid, "ev1", "product_viewed", "u1", `{"country":"VN"}`)
	pid := pu.CustomerProfileID

	destID := mkJourneyDest(t, f, tid, "wh")
	segID := mkEntrySegment(t, f, tid, "vn")
	def := journey.Definition{Steps: []journey.Step{{Type: journey.StepSend, DestinationID: destID}}}
	// max_enrollments = 2: at most two runs per profile.
	j, err := journey.NewRepo(f.pool).CreateJourney(ctx, tid, "welcome", "", segID, false, 2, def)
	require.NoError(t, err)

	clk := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	now := func() time.Time { return clk }
	svc := newJourneySvc(f, nil, now)
	runner := journey.NewRunner(svc, 50, 50, time.Minute, time.Second, testLogger()).WithClock(now)

	// Run 1: enter -> send -> complete.
	require.NoError(t, svc.OnMembershipChanged(ctx, enteredMC(tid, segID, pid, clk)))
	require.Equal(t, 1, activeEnrollmentCount(t, f, tid))
	drainJourneyRunner(t, runner)
	require.Equal(t, 0, activeEnrollmentCount(t, f, tid))
	require.Equal(t, 1, taskCount(t, f, tid))

	// Re-enter (a new run, seq 1) after the terminal one.
	require.NoError(t, svc.OnMembershipChanged(ctx, enteredMC(tid, segID, pid, clk)))
	require.Equal(t, 1, activeEnrollmentCount(t, f, tid))
	require.Equal(t, 2, enrollmentCountForProfile(t, f, tid, pid), "two total runs")
	drainJourneyRunner(t, runner)
	require.Equal(t, 2, taskCount(t, f, tid))

	// Third entry is blocked by the cap (2 total already).
	require.NoError(t, svc.OnMembershipChanged(ctx, enteredMC(tid, segID, pid, clk)))
	require.Equal(t, 0, activeEnrollmentCount(t, f, tid))
	require.Equal(t, 2, enrollmentCountForProfile(t, f, tid, pid), "capped at max_enrollments=2")
	_ = j
}

// TestJourney_ReEntryBlockedWhileActive verifies re-entry is a no-op while an active
// enrollment exists (never two active runs at once).
func TestJourney_ReEntryBlockedWhileActive(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	pu := seedProfile(t, f, tid, sid, "ev1", "product_viewed", "u1", `{"country":"VN"}`)
	pid := pu.CustomerProfileID

	destID := mkJourneyDest(t, f, tid, "wh")
	segID := mkEntrySegment(t, f, tid, "vn")
	def := journey.Definition{Steps: []journey.Step{
		{Type: journey.StepWait, Duration: "1h"},
		{Type: journey.StepSend, DestinationID: destID},
	}}
	_, err := journey.NewRepo(f.pool).CreateJourney(ctx, tid, "welcome", "", segID, false, 5, def)
	require.NoError(t, err)

	clk := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	svc := newJourneySvc(f, nil, func() time.Time { return clk })

	require.NoError(t, svc.OnMembershipChanged(ctx, enteredMC(tid, segID, pid, clk)))
	// A second entry while still active (mid-wait) does not create a parallel run.
	require.NoError(t, svc.OnMembershipChanged(ctx, enteredMC(tid, segID, pid, clk)))
	require.Equal(t, 1, activeEnrollmentCount(t, f, tid))
	require.Equal(t, 1, enrollmentCountForProfile(t, f, tid, pid))
}

// TestJourney_RetentionPrunesTerminal verifies the retention sweeper deletes only
// terminal (completed/exited) enrollments older than the retention age.
func TestJourney_RetentionPrunesTerminal(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, _ := mkTenant(t, f, "acme")
	destID := mkJourneyDest(t, f, tid, "wh")
	segID := mkEntrySegment(t, f, tid, "vn")
	def := journey.Definition{Steps: []journey.Step{{Type: journey.StepSend, DestinationID: destID}}}
	jrepo := journey.NewRepo(f.pool)
	j, err := jrepo.CreateJourney(ctx, tid, "welcome", "", segID, false, 1, def)
	require.NoError(t, err)

	now := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	old := now.Add(-100 * 24 * time.Hour) // well past a 24h retention
	mkEnrollment := func(status string, updatedAt time.Time) uuid.UUID {
		pid := uuid.New()
		_, err := f.pool.Exec(ctx, `
			INSERT INTO journey_enrollment
				(tenant_id, journey_id, customer_profile_id, enrollment_seq, journey_version, status, current_step_index, step_seq, due_at, updated_at)
			VALUES ($1,$2,$3,0,1,$4,0,0,$5,$6)`,
			tid, j.ID, pid, status, now, updatedAt)
		require.NoError(t, err)
		return pid
	}
	agedDone := mkEnrollment(journey.EnrollmentCompleted, old)   // pruned
	agedExited := mkEnrollment(journey.EnrollmentExited, old)    // pruned
	recentDone := mkEnrollment(journey.EnrollmentCompleted, now) // within retention: kept
	agedActive := mkEnrollment(journey.EnrollmentActive, old)    // active: never pruned

	sweeper := journey.NewRetentionSweeper(jrepo, 24*time.Hour, time.Hour, testLogger()).
		WithClock(func() time.Time { return now })
	n, err := sweeper.PruneOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, n, "only the two aged terminal rows are pruned")

	require.Equal(t, 0, enrollmentCountForProfile(t, f, tid, agedDone))
	require.Equal(t, 0, enrollmentCountForProfile(t, f, tid, agedExited))
	require.Equal(t, 1, enrollmentCountForProfile(t, f, tid, recentDone), "recent terminal row kept")
	require.Equal(t, 1, enrollmentCountForProfile(t, f, tid, agedActive), "active row never pruned")
}

// TestJourney_ConcurrentReEntry verifies concurrent re-entry attempts after a terminal
// enrollment collapse to EXACTLY ONE new active run (the bare ON CONFLICT DO NOTHING
// absorbs both the PK collision and the partial-unique-active index) — never two active,
// never zero.
func TestJourney_ConcurrentReEntry(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	pu := seedProfile(t, f, tid, sid, "ev1", "product_viewed", "u1", `{"country":"VN"}`)
	pid := pu.CustomerProfileID

	destID := mkJourneyDest(t, f, tid, "wh")
	segID := mkEntrySegment(t, f, tid, "vn")
	def := journey.Definition{Steps: []journey.Step{{Type: journey.StepSend, DestinationID: destID}}}
	jrepo := journey.NewRepo(f.pool)
	j, err := jrepo.CreateJourney(ctx, tid, "welcome", "", segID, false, 5, def) // re-entry up to 5
	require.NoError(t, err)

	// First run, then drive it to terminal so a re-entry is eligible.
	clk := time.Now().UTC()
	svc := newJourneySvc(f, nil, func() time.Time { return clk })
	require.NoError(t, svc.OnMembershipChanged(ctx, enteredMC(tid, segID, pid, clk)))
	drainJourneyRunner(t, journey.NewRunner(svc, 50, 50, time.Minute, time.Second, testLogger()).WithClock(func() time.Time { return clk }))
	require.Equal(t, 0, activeEnrollmentCount(t, f, tid))

	// Fire many concurrent re-entry enrolls for the same profile.
	const n = 8
	var wg sync.WaitGroup
	created := make([]bool, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, e := jrepo.Enroll(ctx, tid, j.ID, pid, j.CurrentVersion, 5, time.Now().UTC())
			require.NoError(t, e)
			created[i] = c
		}(i)
	}
	wg.Wait()

	wins := 0
	for _, c := range created {
		if c {
			wins++
		}
	}
	require.Equal(t, 1, wins, "exactly one concurrent re-entry creates a row")
	require.Equal(t, 1, activeEnrollmentCount(t, f, tid), "exactly one active run")
	require.Equal(t, 2, enrollmentCountForProfile(t, f, tid, pid), "one terminal + one new active")
}

// TestJourney_MergeWithReEntryRows verifies an identity merge does not crash when the
// loser has MULTIPLE enrollment_seq rows for a journey (Phase 5 re-entry), and holds the
// documented survivor-wins semantics (loser runs move only when the survivor has none).
func TestJourney_MergeWithReEntryRows(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")

	destID := mkJourneyDest(t, f, tid, "wh")
	segID := mkEntrySegment(t, f, tid, "vn")
	def := journey.Definition{Steps: []journey.Step{{Type: journey.StepSend, DestinationID: destID}}}
	j, err := journey.NewRepo(f.pool).CreateJourney(ctx, tid, "welcome", "", segID, false, 3, def)
	require.NoError(t, err)

	pub := &reparentPub{}
	idSvc := identity.NewService(f.pool, pub, bus.TopicIdentityResolved)
	profSvc := profile.NewService(f.pool, noopPub{}, bus.TopicProfileUpdated)
	prepo := profile.NewRepo(f.pool)
	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	resolveAndUpdate(t, idSvc, pub, profSvc, mergeEnv(t, tid, sid, "e1", events.Identifiers{AnonymousID: "a1"}, "", base))
	ir2 := resolveAndUpdate(t, idSvc, pub, profSvc, mergeEnv(t, tid, sid, "e2", events.Identifiers{UserID: "u1"}, "", base.Add(time.Hour)))
	loser, err := prepo.GetByCanonical(ctx, tid, ir2.CanonicalUserID)
	require.NoError(t, err)

	// The loser has TWO runs for this journey: a completed seq 0 and an active seq 1.
	for _, run := range []struct {
		seq    int
		status string
	}{{0, journey.EnrollmentCompleted}, {1, journey.EnrollmentActive}} {
		_, err := f.pool.Exec(ctx, `
			INSERT INTO journey_enrollment
				(tenant_id, journey_id, customer_profile_id, enrollment_seq, journey_version, status, current_step_index, step_seq, due_at)
			VALUES ($1,$2,$3,$4,1,$5,0,0,$6)`,
			tid, j.ID, loser.ID, run.seq, run.status, base)
		require.NoError(t, err)
	}

	// Merge (survivor = older a1, which has NO enrollment for this journey).
	ir3 := resolveAndUpdate(t, idSvc, pub, profSvc, mergeEnv(t, tid, sid, "e3", events.Identifiers{AnonymousID: "a1", UserID: "u1"}, "", base.Add(2*time.Hour)))
	require.True(t, ir3.MergeOccurred)
	survivor, err := prepo.GetByCanonical(ctx, tid, ir3.CanonicalUserID)
	require.NoError(t, err)

	// Both runs moved to the survivor (survivor had none); no crash, one active.
	require.Equal(t, 2, enrollmentCountForProfile(t, f, tid, survivor.ID), "both loser runs moved")
	require.Equal(t, 0, enrollmentCountForProfile(t, f, tid, loser.ID), "loser has none")
	var active int
	require.NoError(t, f.pool.QueryRow(ctx,
		`SELECT count(*) FROM journey_enrollment WHERE tenant_id=$1 AND customer_profile_id=$2 AND status='active'`,
		tid, survivor.ID).Scan(&active))
	require.Equal(t, 1, active, "exactly one active run after merge")
}

// TestJourney_BehavioralConditionCountInWindow drives a journey condition whose rule has
// a BEHAVIORAL leaf (count of an event in a window) end-to-end at runtime, evaluated via
// the real behavior.Store against seeded behavioral_event data. A profile above the
// threshold takes the send arm; one below completes without a send.
func TestJourney_BehavioralConditionCountInWindow(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	tid, sid := mkTenant(t, f, "acme")
	destID := mkJourneyDest(t, f, tid, "wh")
	segID := mkEntrySegment(t, f, tid, "vn")

	// condition: count('order_completed') >= 2 in 7d. True arm sends; false arm (target
	// == len(steps)) completes with no send.
	rule := segment.Rule{Behavior: &segment.BehaviorSpec{
		Kind: segment.BehaviorCount, EventName: "order_completed", Window: "7d", Op: segment.OpGte, Value: float64Ptr(2),
	}}
	def := journey.Definition{Steps: []journey.Step{
		{Type: journey.StepCondition, Condition: &rule, IfTrue: 1, IfFalse: 2},
		{Type: journey.StepSend, DestinationID: destID},
	}}
	require.NoError(t, journey.Validate(def))
	j, err := journey.NewRepo(f.pool).CreateJourney(ctx, tid, "welcome", "", segID, false, 1, def)
	require.NoError(t, err)

	clk := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	now := func() time.Time { return clk }
	// A real behavior store so behavioral leaves actually evaluate (nil would be inert).
	svc := newJourneySvcStore(f, nil, behavior.NewStore(f.pool), now)

	// seedOrders inserts n 'order_completed' events (+ hourly buckets, which Count reads)
	// for a profile, anchored inside [due_at-7d, due_at] where due_at == clk (enroll time).
	seedOrders := func(profileID uuid.UUID, n int) {
		for i := 0; i < n; i++ {
			occ := clk.Add(-time.Duration(i+1) * time.Hour)
			_, err := f.pool.Exec(ctx,
				`INSERT INTO behavioral_event (tenant_id, customer_profile_id, event_id, event_name, occurred_at)
				 VALUES ($1,$2,$3,'order_completed',$4)`,
				tid, profileID, uuid.New().String(), occ)
			require.NoError(t, err)
			_, err = f.pool.Exec(ctx,
				`INSERT INTO profile_behavior_bucket (tenant_id, customer_profile_id, event_name, bucket_start, count, first_at, last_at)
				 VALUES ($1,$2,'order_completed', date_trunc('hour', $3::timestamptz), 1, $3, $3)
				 ON CONFLICT (tenant_id, customer_profile_id, event_name, bucket_start)
				 DO UPDATE SET count = profile_behavior_bucket.count + 1`,
				tid, profileID, occ)
			require.NoError(t, err)
		}
	}

	puA := seedProfile(t, f, tid, sid, "eA", "product_viewed", "u1", `{"country":"VN"}`)
	seedOrders(puA.CustomerProfileID, 2) // meets the >=2 threshold -> send arm
	puB := seedProfile(t, f, tid, sid, "eB", "product_viewed", "u2", `{"country":"VN"}`)
	seedOrders(puB.CustomerProfileID, 1) // below threshold -> no send

	require.NoError(t, svc.OnMembershipChanged(ctx, enteredMC(tid, segID, puA.CustomerProfileID, clk)))
	require.NoError(t, svc.OnMembershipChanged(ctx, enteredMC(tid, segID, puB.CustomerProfileID, clk)))
	drainJourneyRunner(t, journey.NewRunner(svc, 50, 50, time.Minute, time.Second, testLogger()).WithClock(now))

	// Only profile A (>=2 orders) took the send arm; both enrollments completed.
	require.Equal(t, 1, taskCount(t, f, tid))
	require.Equal(t, destID, taskDestForProfile(t, f, tid, puA.CustomerProfileID), "A (2 orders) sends")
	_, sA := enrollmentState(t, f, tid, j.ID, puA.CustomerProfileID)
	require.Equal(t, journey.EnrollmentCompleted, sA)
	_, sB := enrollmentState(t, f, tid, j.ID, puB.CustomerProfileID)
	require.Equal(t, journey.EnrollmentCompleted, sB)
}

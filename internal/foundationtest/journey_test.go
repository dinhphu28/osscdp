package foundationtest

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/dinhphu28/osscdp/internal/activation"
	"github.com/dinhphu28/osscdp/internal/audit"
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

func newJourneySvc(f fixture, consentReader activation.ConsentReader, now func() time.Time) *journey.Service {
	act := activation.NewService(f.pool, profile.NewRepo(f.pool), consentReader)
	svc := journey.NewService(f.pool, profile.NewRepo(f.pool), act)
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
	j, err := journey.NewRepo(f.pool).CreateJourney(ctx, tid, "welcome", "", segID, def)
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
	j, err := journey.NewRepo(f.pool).CreateJourney(ctx, tid, "welcome", "", segID, def)
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
	j, err := journey.NewRepo(f.pool).CreateJourney(ctx, tid, "welcome", "", segID, def)
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
	_, err := journey.NewRepo(f.pool).CreateJourney(ctx, tid, "welcome", "", segID, def)
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
	j, err := jrepo.CreateJourney(ctx, tid, "welcome", "", segID, journey.Definition{Steps: []journey.Step{
		{Type: journey.StepWait, Duration: "1h"},
		{Type: journey.StepSend, DestinationID: destV1},
	}})
	require.NoError(t, err)

	clk := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	now := func() time.Time { return clk }
	svc := newJourneySvc(f, nil, now)
	require.NoError(t, svc.EnrollOnMembership(ctx, enteredMC(tid, segID, pid, clk)))

	// Re-author the journey to send to a DIFFERENT destination (mints version 2).
	_, err = jrepo.UpdateJourney(ctx, tid, j.ID, "", journey.Definition{Steps: []journey.Step{
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
	j, err := journey.NewRepo(f.pool).CreateJourney(ctx, tid, "welcome", "", segID,
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
	created, err := journey.NewRepo(f.pool).Enroll(ctx, tid, j.ID, loser.ID, j.CurrentVersion, base.Add(time.Hour))
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

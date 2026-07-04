package behavior

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/dinhphu28/osscdp/internal/platform/migrate"
)

func TestMain(m *testing.M) {
	if _, set := os.LookupEnv("TESTCONTAINERS_RYUK_DISABLED"); !set {
		_ = os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
	}
	os.Exit(m.Run())
}

func newPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	c, err := tcpostgres.Run(ctx, "postgres:16",
		tcpostgres.WithDatabase("cdp"), tcpostgres.WithUsername("cdp"), tcpostgres.WithPassword("cdp"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(60*time.Second)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Terminate(ctx) })
	dsn, err := c.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	require.NoError(t, migrate.Up(dsn))
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}

func seedTenant(t *testing.T, ctx context.Context, pool *pgxpool.Pool) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := pool.Exec(ctx, `INSERT INTO tenant (id, name, status) VALUES ($1,'test','active')`, id)
	require.NoError(t, err)
	return id
}

func seedEvent(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tid, pid uuid.UUID, evID, name string, at time.Time, props string) {
	t.Helper()
	var p any
	if props != "" {
		p = []byte(props)
	}
	_, err := pool.Exec(ctx, `INSERT INTO behavioral_event (tenant_id, customer_profile_id, event_id, event_name, occurred_at, props_json) VALUES ($1,$2,$3,$4,$5,$6)`,
		tid, pid, evID, name, at, p)
	require.NoError(t, err)
}

func TestStore(t *testing.T) {
	pool := newPool(t)
	ctx := context.Background()
	s := NewStore(pool)
	at := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	day := 24 * time.Hour

	t.Run("CountInWindowBoundary", func(t *testing.T) {
		tid := seedTenant(t, ctx, pool)
		pid := uuid.New()
		seedEvent(t, ctx, pool, tid, pid, "v1", "view", at.Add(-6*day), "") // in
		seedEvent(t, ctx, pool, tid, pid, "v2", "view", at.Add(-8*day), "") // out (older than 7d)
		seedEvent(t, ctx, pool, tid, pid, "v3", "view", at, "")             // in (== at)
		seedEvent(t, ctx, pool, tid, pid, "v4", "view", at.Add(-7*day), "") // in (inclusive lower bound)

		// Exact: seed only the log (no buckets); exercises the exact-count boundary.
		n, err := s.Count(ctx, tid, pid, Spec{EventName: "view", Window: 7 * day, Exact: true}, at)
		require.NoError(t, err)
		require.EqualValues(t, 3, n, "occurred_at >= at-7d and <= at counts v1,v3,v4; v2 excluded")
	})

	t.Run("RecencyAbsenceNeverEmitted", func(t *testing.T) {
		tid := seedTenant(t, ctx, pool)
		pid := uuid.New()
		spec := Spec{EventName: "login", Window: day}

		recent, err := s.Recent(ctx, tid, pid, spec, at)
		require.NoError(t, err)
		require.False(t, recent, "never-emitted -> not recent")
		absent, err := s.Absent(ctx, tid, pid, spec, at)
		require.NoError(t, err)
		require.True(t, absent, "never-emitted -> absent")

		seedEvent(t, ctx, pool, tid, pid, "l1", "login", at.Add(-1*time.Hour), "")
		recent, _ = s.Recent(ctx, tid, pid, spec, at)
		require.True(t, recent, "login 1h ago -> recent")
		absent, _ = s.Absent(ctx, tid, pid, spec, at)
		require.False(t, absent, "login 1h ago -> not absent")
	})

	t.Run("CorrelatedAbsence", func(t *testing.T) {
		spec := Spec{EventName: "order", Window: day, Anchor: &Spec{EventName: "view", Window: 7 * day, Op: "gte", Value: 3}}
		t0 := at.Add(-3 * day)
		ta := t0.Add(2 * time.Hour) // latest of the 3 qualifying views

		seedViews := func(tid, pid uuid.UUID) {
			seedEvent(t, ctx, pool, tid, pid, "a1", "view", t0, "")
			seedEvent(t, ctx, pool, tid, pid, "a2", "view", t0.Add(time.Hour), "")
			seedEvent(t, ctx, pool, tid, pid, "a3", "view", ta, "")
		}
		// A: order 20h after the anchor -> within 24h -> not absent.
		tidA := seedTenant(t, ctx, pool)
		pidA := uuid.New()
		seedViews(tidA, pidA)
		seedEvent(t, ctx, pool, tidA, pidA, "o1", "order", ta.Add(20*time.Hour), "")
		got, err := s.CorrelatedAbsent(ctx, tidA, pidA, spec, at)
		require.NoError(t, err)
		require.False(t, got, "order within 24h of the anchor is not absent")

		// B: order 26h after the anchor -> outside 24h -> absent.
		tidB := seedTenant(t, ctx, pool)
		pidB := uuid.New()
		seedViews(tidB, pidB)
		seedEvent(t, ctx, pool, tidB, pidB, "o2", "order", ta.Add(26*time.Hour), "")
		got, err = s.CorrelatedAbsent(ctx, tidB, pidB, spec, at)
		require.NoError(t, err)
		require.True(t, got, "order outside 24h of the anchor is absent")

		// C: anchor not met (only 2 views) -> false.
		tidC := seedTenant(t, ctx, pool)
		pidC := uuid.New()
		seedEvent(t, ctx, pool, tidC, pidC, "c1", "view", t0, "")
		seedEvent(t, ctx, pool, tidC, pidC, "c2", "view", t0.Add(time.Hour), "")
		got, err = s.CorrelatedAbsent(ctx, tidC, pidC, spec, at)
		require.NoError(t, err)
		require.False(t, got, "anchor not satisfied -> false")
	})

	t.Run("SequenceWithin", func(t *testing.T) {
		spec := Spec{Steps: []string{"add_to_cart", "checkout_started"}, Within: time.Hour}
		t0 := at.Add(-2 * day)

		tid1 := seedTenant(t, ctx, pool)
		pid1 := uuid.New()
		seedEvent(t, ctx, pool, tid1, pid1, "s1", "add_to_cart", t0, "")
		seedEvent(t, ctx, pool, tid1, pid1, "s2", "checkout_started", t0.Add(5*time.Minute), "")
		ok, err := s.Sequence(ctx, tid1, pid1, spec, at)
		require.NoError(t, err)
		require.True(t, ok, "B 5m after A within 1h matches")

		tid2 := seedTenant(t, ctx, pool)
		pid2 := uuid.New()
		seedEvent(t, ctx, pool, tid2, pid2, "s3", "add_to_cart", t0, "")
		seedEvent(t, ctx, pool, tid2, pid2, "s4", "checkout_started", t0.Add(5*day), "")
		ok, err = s.Sequence(ctx, tid2, pid2, spec, at)
		require.NoError(t, err)
		require.False(t, ok, "B 5 days after A exceeds within:1h")
	})

	t.Run("WhereFilteredCount", func(t *testing.T) {
		tid := seedTenant(t, ctx, pool)
		pid := uuid.New()
		seedEvent(t, ctx, pool, tid, pid, "w1", "add_to_cart", at.Add(-1*day), `{"price":150}`)
		seedEvent(t, ctx, pool, tid, pid, "w2", "add_to_cart", at.Add(-1*day), `{"price":50}`)
		spec := Spec{EventName: "add_to_cart", Window: 3 * day, WhereMatch: func(props json.RawMessage) bool {
			var m map[string]any
			_ = json.Unmarshal(props, &m)
			p, _ := m["price"].(float64)
			return p >= 100
		}}
		n, err := s.Count(ctx, tid, pid, spec, at)
		require.NoError(t, err)
		require.EqualValues(t, 1, n, "only the price>=100 row matches the where filter")
	})

	t.Run("SumValue", func(t *testing.T) {
		tid := seedTenant(t, ctx, pool)
		pid := uuid.New()
		seedEvent(t, ctx, pool, tid, pid, "o1", "order", at.Add(-1*day), `{"revenue":300}`)
		seedEvent(t, ctx, pool, tid, pid, "o2", "order", at.Add(-2*day), `{"revenue":250}`)
		seedEvent(t, ctx, pool, tid, pid, "o3", "order", at.Add(-40*day), `{"revenue":999}`) // outside 30d
		seedEvent(t, ctx, pool, tid, pid, "o4", "order", at.Add(-3*day), `{"revenue":"free"}`) // non-numeric: guarded, ignored
		spec := Spec{EventName: "order", ValueProp: "revenue", Window: 30 * day}
		sum, err := s.SumValue(ctx, tid, pid, spec, at)
		require.NoError(t, err, "a dirty non-numeric value_prop must not abort the aggregate")
		require.EqualValues(t, 550, sum, "300+250 within 30d; 999 excluded; 'free' ignored")

		// where-scan sum path: only paid orders.
		seedEvent(t, ctx, pool, tid, pid, "o5", "order", at.Add(-1*day), `{"revenue":100,"status":"pending"}`)
		spec.WhereMatch = func(props json.RawMessage) bool {
			var m map[string]any
			_ = json.Unmarshal(props, &m)
			return m["status"] != "pending"
		}
		sum, err = s.SumValue(ctx, tid, pid, spec, at)
		require.NoError(t, err)
		require.EqualValues(t, 550, sum, "pending order excluded by the where filter")
	})

	t.Run("Sequence3Step", func(t *testing.T) {
		spec := Spec{Steps: []string{"a", "b", "c"}, Within: time.Hour}
		t0 := at.Add(-1 * day)

		tid := seedTenant(t, ctx, pool)
		pid1 := uuid.New()
		seedEvent(t, ctx, pool, tid, pid1, "x1", "a", t0, "")
		seedEvent(t, ctx, pool, tid, pid1, "x2", "b", t0.Add(10*time.Minute), "")
		seedEvent(t, ctx, pool, tid, pid1, "x3", "c", t0.Add(20*time.Minute), "")
		ok, err := s.Sequence(ctx, tid, pid1, spec, at)
		require.NoError(t, err)
		require.True(t, ok, "A->B->C each within 1h matches")

		pid2 := uuid.New()
		seedEvent(t, ctx, pool, tid, pid2, "y1", "a", t0, "")
		seedEvent(t, ctx, pool, tid, pid2, "y2", "b", t0.Add(10*time.Minute), "")
		seedEvent(t, ctx, pool, tid, pid2, "y3", "c", t0.Add(2*time.Hour), "") // B->C gap 1h50m > 1h
		ok, err = s.Sequence(ctx, tid, pid2, spec, at)
		require.NoError(t, err)
		require.False(t, ok, "B->C gap exceeding within:1h must not match")
	})

	t.Run("BoundaryInclusive", func(t *testing.T) {
		tid := seedTenant(t, ctx, pool)
		// recency/absence: event exactly at at-Window is recent (>=) and not absent (<).
		pid := uuid.New()
		seedEvent(t, ctx, pool, tid, pid, "b1", "login", at.Add(-day), "")
		spec := Spec{EventName: "login", Window: day}
		recent, err := s.Recent(ctx, tid, pid, spec, at)
		require.NoError(t, err)
		require.True(t, recent, "event exactly at at-Window is recent (inclusive)")
		absent, err := s.Absent(ctx, tid, pid, spec, at)
		require.NoError(t, err)
		require.False(t, absent)

		// sequence: B exactly at A+Within matches; A+Within+1ns does not.
		seq := Spec{Steps: []string{"a", "b"}, Within: time.Hour}
		t0 := at.Add(-2 * day)
		pOK := uuid.New()
		seedEvent(t, ctx, pool, tid, pOK, "s1", "a", t0, "")
		seedEvent(t, ctx, pool, tid, pOK, "s2", "b", t0.Add(time.Hour), "") // exactly at bound
		ok, err := s.Sequence(ctx, tid, pOK, seq, at)
		require.NoError(t, err)
		require.True(t, ok, "B exactly at A+Within matches (inclusive upper)")
		pNo := uuid.New()
		seedEvent(t, ctx, pool, tid, pNo, "s3", "a", t0, "")
		seedEvent(t, ctx, pool, tid, pNo, "s4", "b", t0.Add(time.Hour+time.Second), "") // just past (timestamptz is µs-precision)
		ok, err = s.Sequence(ctx, tid, pNo, seq, at)
		require.NoError(t, err)
		require.False(t, ok, "B just past A+Within does not match")
	})
}

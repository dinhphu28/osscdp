package behavior

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// maxWhereScan caps the rows a props_json (where) filter scans; above it the
// result would be unreliable, so we error rather than silently under-count.
const maxWhereScan = 50000

// Store answers windowed behavioral questions over the behavioral_event log in
// exact mode (direct row scans; bucket acceleration arrives in Phase 6). Every
// query is tenant+profile scoped and takes an explicit evaluation instant `at`
// (never SQL now()) so edge evaluation is deterministic across redelivery.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore constructs a Store.
func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Count returns how many of spec.EventName occurred in [at-Window, at] (inclusive
// lower bound, matching Recent/Absent complementarity and doc 16). When
// spec.WhereMatch is set the count is over rows whose props_json match (scanned
// in Go, capped by maxWhereScan).
func (s *Store) Count(ctx context.Context, tenantID, profileID uuid.UUID, spec Spec, at time.Time) (int64, error) {
	from := at.Add(-spec.Window)
	if spec.WhereMatch == nil {
		var n int64
		err := s.pool.QueryRow(ctx, `
			SELECT count(*) FROM behavioral_event
			WHERE tenant_id=$1 AND customer_profile_id=$2 AND event_name=$3 AND occurred_at >= $4 AND occurred_at <= $5`,
			tenantID, profileID, spec.EventName, from, at).Scan(&n)
		return n, err
	}
	rows, err := s.pool.Query(ctx, `
		SELECT props_json FROM behavioral_event
		WHERE tenant_id=$1 AND customer_profile_id=$2 AND event_name=$3 AND occurred_at >= $4 AND occurred_at <= $5
		LIMIT $6`,
		tenantID, profileID, spec.EventName, from, at, maxWhereScan+1)
	if err != nil {
		return 0, fmt.Errorf("count where scan: %w", err)
	}
	defer rows.Close()
	var scanned, matched int64
	for rows.Next() {
		scanned++
		if scanned > maxWhereScan {
			return 0, fmt.Errorf("behavior where-scan exceeded %d rows for %q", maxWhereScan, spec.EventName)
		}
		var props json.RawMessage
		if err := rows.Scan(&props); err != nil {
			return 0, err
		}
		if spec.WhereMatch(props) {
			matched++
		}
	}
	return matched, rows.Err()
}

// Recent reports whether spec.EventName occurred within the trailing Window. A
// never-emitted event coalesces to -infinity and is correctly not-recent.
func (s *Store) Recent(ctx context.Context, tenantID, profileID uuid.UUID, spec Spec, at time.Time) (bool, error) {
	var recent bool
	err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(MAX(occurred_at), '-infinity'::timestamptz) >= $4
		FROM behavioral_event
		WHERE tenant_id=$1 AND customer_profile_id=$2 AND event_name=$3 AND occurred_at <= $5`,
		tenantID, profileID, spec.EventName, at.Add(-spec.Window), at).Scan(&recent)
	return recent, err
}

// Absent reports whether spec.EventName did NOT occur within the trailing Window.
// The COALESCE is load-bearing: a never-emitted event is correctly absent=true.
func (s *Store) Absent(ctx context.Context, tenantID, profileID uuid.UUID, spec Spec, at time.Time) (bool, error) {
	var absent bool
	err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(MAX(occurred_at), '-infinity'::timestamptz) < $4
		FROM behavioral_event
		WHERE tenant_id=$1 AND customer_profile_id=$2 AND event_name=$3 AND occurred_at <= $5`,
		tenantID, profileID, spec.EventName, at.Add(-spec.Window), at).Scan(&absent)
	return absent, err
}

// CorrelatedAbsent reports whether the anchor behaviour is satisfied and then
// spec.EventName did NOT occur within Window after the anchor's latest occurrence.
func (s *Store) CorrelatedAbsent(ctx context.Context, tenantID, profileID uuid.UUID, spec Spec, at time.Time) (bool, error) {
	if spec.Anchor == nil {
		return false, fmt.Errorf("correlated absence requires an anchor")
	}
	a := spec.Anchor
	var count int64
	var ta *time.Time
	if err := s.pool.QueryRow(ctx, `
		SELECT count(*), MAX(occurred_at) FROM behavioral_event
		WHERE tenant_id=$1 AND customer_profile_id=$2 AND event_name=$3 AND occurred_at >= $4 AND occurred_at <= $5`,
		tenantID, profileID, a.EventName, at.Add(-a.Window), at).Scan(&count, &ta); err != nil {
		return false, fmt.Errorf("correlated anchor: %w", err)
	}
	if ta == nil || !compareCount(a.Op, float64(count), a.Value) {
		return false, nil // anchor not satisfied — no anchor time to correlate against
	}
	if ta.Add(spec.Window).After(at) {
		// The correlation window has not fully elapsed at the evaluation instant, so
		// absence cannot be confirmed yet (the Phase 5 sweep re-evaluates once it has).
		// This also keeps the read deterministic under redelivery (bounded by at).
		return false, nil
	}
	var exists bool
	if err := s.pool.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM behavioral_event
			WHERE tenant_id=$1 AND customer_profile_id=$2 AND event_name=$3 AND occurred_at >= $4 AND occurred_at <= $5)`,
		tenantID, profileID, spec.EventName, *ta, ta.Add(spec.Window)).Scan(&exists); err != nil {
		return false, fmt.Errorf("correlated absence: %w", err)
	}
	return !exists, nil
}

// Sequence reports whether an ordered chain of spec.Steps exists where each step
// occurs strictly after the previous within `Within` (exact ordered self-join).
func (s *Store) Sequence(ctx context.Context, tenantID, profileID uuid.UUID, spec Spec, at time.Time) (bool, error) {
	if len(spec.Steps) < 2 {
		return false, fmt.Errorf("sequence requires >= 2 steps")
	}
	// Build nested EXISTS: e0 (step0), then e1 in (e0, e0+Within], then e2 in (e1, e1+Within], ...
	// Args: $1 tenant, $2 profile, $3 within, $4 at, $5.. step names.
	args := []any{tenantID, profileID, spec.Within, at}
	var b strings.Builder
	closeParens := 0
	for i, step := range spec.Steps {
		args = append(args, step)
		p := len(args) // param index of this step name
		if i == 0 {
			fmt.Fprintf(&b, `SELECT EXISTS (SELECT 1 FROM behavioral_event e0 WHERE e0.tenant_id=$1 AND e0.customer_profile_id=$2 AND e0.event_name=$%d AND e0.occurred_at <= $4`, p)
		} else {
			fmt.Fprintf(&b, ` AND EXISTS (SELECT 1 FROM behavioral_event e%d WHERE e%d.tenant_id=$1 AND e%d.customer_profile_id=$2 AND e%d.event_name=$%d AND e%d.occurred_at > e%d.occurred_at AND e%d.occurred_at <= e%d.occurred_at + $3 AND e%d.occurred_at <= $4`,
				i, i, i, i, p, i, i-1, i, i-1, i)
			closeParens++
		}
	}
	b.WriteString(strings.Repeat(")", closeParens+1)) // close inner EXISTS + outer EXISTS(
	var ok bool
	if err := s.pool.QueryRow(ctx, b.String(), args...).Scan(&ok); err != nil {
		return false, fmt.Errorf("sequence: %w", err)
	}
	return ok, nil
}

// SumValue sums the numeric spec.ValueProp property over in-window events (for
// frequency-of-value). With a WhereMatch it scans and sums matching rows.
func (s *Store) SumValue(ctx context.Context, tenantID, profileID uuid.UUID, spec Spec, at time.Time) (float64, error) {
	from := at.Add(-spec.Window)
	if spec.WhereMatch == nil {
		var sum float64
		// Guard the cast by JSONB type so a single non-numeric value_prop (dirty
		// external data) cannot abort the whole aggregate; mirrors the lenient Go path.
		err := s.pool.QueryRow(ctx, `
			SELECT COALESCE(SUM(CASE WHEN jsonb_typeof(props_json->$3)='number' THEN (props_json->>$3)::numeric ELSE 0 END), 0)::float8 FROM behavioral_event
			WHERE tenant_id=$1 AND customer_profile_id=$2 AND event_name=$4 AND occurred_at >= $5 AND occurred_at <= $6`,
			tenantID, profileID, spec.ValueProp, spec.EventName, from, at).Scan(&sum)
		return sum, err
	}
	rows, err := s.pool.Query(ctx, `
		SELECT props_json FROM behavioral_event
		WHERE tenant_id=$1 AND customer_profile_id=$2 AND event_name=$3 AND occurred_at >= $4 AND occurred_at <= $5
		LIMIT $6`,
		tenantID, profileID, spec.EventName, from, at, maxWhereScan+1)
	if err != nil {
		return 0, fmt.Errorf("sumvalue where scan: %w", err)
	}
	defer rows.Close()
	var scanned int
	var sum float64
	for rows.Next() {
		scanned++
		if scanned > maxWhereScan {
			return 0, fmt.Errorf("behavior where-scan exceeded %d rows for %q", maxWhereScan, spec.EventName)
		}
		var props json.RawMessage
		if err := rows.Scan(&props); err != nil {
			return 0, err
		}
		if !spec.WhereMatch(props) {
			continue
		}
		var m map[string]any
		if json.Unmarshal(props, &m) == nil {
			if f, ok := toFloat(m[spec.ValueProp]); ok {
				sum += f
			}
		}
	}
	return sum, rows.Err()
}

func compareCount(op string, got, want float64) bool {
	switch op {
	case "gte":
		return got >= want
	case "gt":
		return got > want
	case "lte":
		return got <= want
	case "lt":
		return got < want
	case "eq":
		return got == want
	}
	return false
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}

package metrics

import (
	"strings"
	"testing"
)

// TestNew_RegistersWithoutPanic ensures every collector has a unique name (a duplicate
// would panic in reg.MustRegister) and that the journey_* series are all gathered.
func TestNew_RegistersWithoutPanic(t *testing.T) {
	m := New() // panics on a duplicate metric name

	mfs, err := m.reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	got := map[string]bool{}
	for _, mf := range mfs {
		got[mf.GetName()] = true
	}
	for _, name := range []string{
		"journey_enrolled_total", "journey_exited_total",
		"journey_runner_claimed_total", "journey_runner_advanced_total",
		"journey_runner_error_total", "journey_runner_parked_total",
		"journey_runner_lag_seconds", "journey_runner_parked_backlog",
		"journey_seed_pages_total", "journey_seed_jobs_done_total",
		"journey_enrollment_pruned_total",
	} {
		if !got[name] {
			t.Errorf("metric %q not registered (have: %s)", name, strings.Join(keys(got), ", "))
		}
	}
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

package bus

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
)

func testConsumer(maxRetries int) *Consumer {
	return &Consumer{
		maxRetries: maxRetries,
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func TestProcess_SuccessNoDLQ(t *testing.T) {
	c := testConsumer(3)
	calls := 0
	handler := func(context.Context, Record) error { calls++; return nil }
	dlqCalled := false
	dlq := func(context.Context, Record, int, error) { dlqCalled = true }

	c.process(context.Background(), Record{}, handler, dlq)
	if calls != 1 {
		t.Fatalf("handler called %d times, want 1", calls)
	}
	if dlqCalled {
		t.Fatal("dlq must not be called on success")
	}
}

func TestProcess_RetriesThenDeadLetters(t *testing.T) {
	c := testConsumer(3)
	retries := 0
	c.OnRetry = func() { retries++ }
	calls := 0
	handler := func(context.Context, Record) error { calls++; return errors.New("boom") }
	var gotRetries int
	var gotCause error
	dlqCalled := false
	dlq := func(_ context.Context, _ Record, r int, cause error) {
		dlqCalled = true
		gotRetries = r
		gotCause = cause
	}

	c.process(context.Background(), Record{Topic: "t"}, handler, dlq)

	if calls != 4 { // 1 initial + 3 retries
		t.Fatalf("handler called %d times, want 4", calls)
	}
	if retries != 3 {
		t.Fatalf("OnRetry called %d times, want 3", retries)
	}
	if !dlqCalled {
		t.Fatal("dlq should be called after exhausting retries")
	}
	if gotRetries != 3 {
		t.Fatalf("dlq retries = %d, want 3", gotRetries)
	}
	if gotCause == nil || gotCause.Error() != "boom" {
		t.Fatalf("dlq cause = %v, want boom", gotCause)
	}
}

func TestProcess_RecoversBeforeExhaustion(t *testing.T) {
	c := testConsumer(5)
	calls := 0
	handler := func(context.Context, Record) error {
		calls++
		if calls < 3 {
			return errors.New("transient")
		}
		return nil
	}
	dlqCalled := false
	dlq := func(context.Context, Record, int, error) { dlqCalled = true }

	c.process(context.Background(), Record{}, handler, dlq)
	if calls != 3 {
		t.Fatalf("handler called %d times, want 3", calls)
	}
	if dlqCalled {
		t.Fatal("dlq must not be called when handler recovers")
	}
}

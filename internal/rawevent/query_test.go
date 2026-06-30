package rawevent

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestCursor_RoundTrip(t *testing.T) {
	now := time.Date(2026, 6, 30, 12, 0, 0, 123456789, time.UTC)
	id := uuid.New()
	c := encodeCursor(now, id)

	gotTime, gotID, err := decodeCursor(c)
	if err != nil {
		t.Fatalf("decodeCursor: %v", err)
	}
	if !gotTime.Equal(now) {
		t.Fatalf("time = %v, want %v", gotTime, now)
	}
	if gotID != id {
		t.Fatalf("id = %v, want %v", gotID, id)
	}
}

func TestDecodeCursor_Invalid(t *testing.T) {
	for _, c := range []string{"!!!not base64!!!", "bm90LXBpcGU", "Zm9vfGJhcg"} { // garbage, no pipe, bad uuid
		if _, _, err := decodeCursor(c); !errors.Is(err, ErrInvalidCursor) {
			t.Fatalf("decodeCursor(%q) err = %v, want ErrInvalidCursor", c, err)
		}
	}
}

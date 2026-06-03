package store

import (
	"regexp"
	"testing"
	"time"
)

func TestNewJobID_Sortable(t *testing.T) {
	// IDs created at different times sort lexicographically by time (the 16-hex
	// unix-nanosecond prefix). A 1ms gap guarantees distinct prefixes.
	a := newJobID()
	time.Sleep(1 * time.Millisecond)
	b := newJobID()
	if !(a < b) {
		t.Fatalf("ids not time-sortable: %q !< %q", a, b)
	}
}

func TestNewJobID_FormatAndUnique(t *testing.T) {
	// The tight loop mostly shares a nanosecond prefix, so this primarily
	// exercises the random-suffix uniqueness path; cross-time ordering is
	// covered by TestNewJobID_Sortable.
	re := regexp.MustCompile(`^[0-9a-f]{16}-[0-9a-f]{12}$`)
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		id := newJobID()
		if !re.MatchString(id) {
			t.Fatalf("id %q does not match expected format", id)
		}
		if seen[id] {
			t.Fatalf("duplicate id %q", id)
		}
		seen[id] = true
	}
}

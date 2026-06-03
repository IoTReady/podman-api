package store

import (
	"regexp"
	"testing"
)

func TestNewJobID_FormatAndUnique(t *testing.T) {
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

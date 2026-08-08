package server

import (
	"reflect"
	"sort"
	"testing"

	"github.com/iotready/podman-api/internal/config"
)

func hostsOf(ids ...string) []config.Host {
	hs := make([]config.Host, len(ids))
	for i, id := range ids {
		hs[i] = config.Host{ID: id}
	}
	return hs
}

func sortedStrings(ss []string) []string {
	out := append([]string(nil), ss...)
	sort.Strings(out)
	return out
}

// #231 review finding #3: a host present in a reload but never tracked
// before (this process's very first SIGHUP, or a host added to hosts.d after
// startup) must be reported as newly seen so server.go can boot-converge it —
// the poller's own first observation of a host is deliberately baseline-only.
func TestDiffNewlySeenHosts_TrulyNewHost(t *testing.T) {
	seen := map[string]bool{"a": true, "b": true}
	newlySeen, next := diffNewlySeenHosts(seen, hostsOf("a", "b", "c"))

	if got := sortedStrings(newlySeen); !reflect.DeepEqual(got, []string{"c"}) {
		t.Fatalf("newlySeen = %v, want [c]", got)
	}
	want := map[string]bool{"a": true, "b": true, "c": true}
	if !reflect.DeepEqual(next, want) {
		t.Fatalf("nextSeen = %v, want %v", next, want)
	}
}

// A host present in both the previous and current set is not newly seen —
// the ordinary steady-state reload, which must not spam a reconcile on every
// SIGHUP.
func TestDiffNewlySeenHosts_UnchangedHostsNotReported(t *testing.T) {
	seen := map[string]bool{"a": true, "b": true}
	newlySeen, _ := diffNewlySeenHosts(seen, hostsOf("a", "b"))
	if len(newlySeen) != 0 {
		t.Fatalf("newlySeen = %v, want none", newlySeen)
	}
}

// A host removed from the config drops out of nextSeen entirely — it is not
// carried forward merged with the old set, it is replaced.
func TestDiffNewlySeenHosts_RemovedHostDropsOut(t *testing.T) {
	seen := map[string]bool{"a": true, "b": true}
	_, next := diffNewlySeenHosts(seen, hostsOf("a"))
	if next["b"] {
		t.Fatalf("nextSeen still has removed host 'b': %v", next)
	}
	if !next["a"] {
		t.Fatalf("nextSeen dropped host 'a' that is still present: %v", next)
	}
}

// #231 review finding #4, closed by the same mechanism as finding #3: a host
// removed by one reload (e.g. it rebooted while temporarily absent) and
// re-added by a later one must be reported as newly seen again, exactly like
// a never-before-seen host — because a removal already dropped it from
// nextSeen, its re-addition looks identical to "new" on the next diff. This
// is what closes the gap where a reboot during the absence window would
// otherwise never trigger a reconcile.
func TestDiffNewlySeenHosts_RemovedThenReaddedHostIsNewlySeenAgain(t *testing.T) {
	seen := map[string]bool{"a": true, "b": true}

	// First reload: "b" is removed.
	_, seen = diffNewlySeenHosts(seen, hostsOf("a"))
	if seen["b"] {
		t.Fatalf("precondition failed: 'b' should have dropped out, got %v", seen)
	}

	// Second reload: "b" comes back (e.g. it rebooted while it was gone).
	newlySeen, next := diffNewlySeenHosts(seen, hostsOf("a", "b"))
	if got := sortedStrings(newlySeen); !reflect.DeepEqual(got, []string{"b"}) {
		t.Fatalf("newlySeen on re-add = %v, want [b]", got)
	}
	if !next["a"] || !next["b"] {
		t.Fatalf("nextSeen after re-add = %v, want both a and b", next)
	}
}

// An empty starting set (the very first reload of a fresh daemon run) reports
// every host as newly seen. In production this is harmless double-coverage
// with the one-shot startup converge (both will reconcile the same hosts once
// each), not a correctness problem — the reconcile itself is idempotent and
// tolerant.
func TestDiffNewlySeenHosts_EmptyPriorSetReportsEveryHost(t *testing.T) {
	newlySeen, next := diffNewlySeenHosts(map[string]bool{}, hostsOf("a", "b"))
	if got := sortedStrings(newlySeen); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("newlySeen = %v, want [a b]", got)
	}
	if len(next) != 2 {
		t.Fatalf("nextSeen = %v, want 2 entries", next)
	}
}

// An empty current host list (hosts.d wiped or LoadHosts returning zero
// entries) must not panic and must report no newly-seen hosts.
func TestDiffNewlySeenHosts_EmptyCurrentHostList(t *testing.T) {
	newlySeen, next := diffNewlySeenHosts(map[string]bool{"a": true}, nil)
	if len(newlySeen) != 0 {
		t.Fatalf("newlySeen = %v, want none", newlySeen)
	}
	if len(next) != 0 {
		t.Fatalf("nextSeen = %v, want empty", next)
	}
}

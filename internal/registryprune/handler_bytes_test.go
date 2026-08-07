package registryprune

import (
	"errors"
	"testing"

	"github.com/iotready/podman-api/internal/imgregistry"
	"github.com/iotready/podman-api/internal/podman"
)

var errUnmeasurable = errors.New("du: exec failed")

// bytesMetrics is a metrics double that also records the bytes-reclaimed seam.
// Kept separate from fakeMetrics so this file adds no field to it.
type bytesMetrics struct {
	results []string
	bytes   []int64
}

func (m *bytesMetrics) RunDone(result string)        { m.results = append(m.results, result) }
func (m *bytesMetrics) ManifestsDeleted(string, int) {}
func (m *bytesMetrics) RepoSkipped(string, int)      {}
func (m *bytesMetrics) BytesReclaimed(bytes int64)   { m.bytes = append(m.bytes, bytes) }

// fakeMetrics (handler_test.go) must satisfy the widened interface too. Its
// method lives here so handler_test.go keeps no knowledge of this seam.
func (m *fakeMetrics) BytesReclaimed(int64) {}

func bytesHandler(t *testing.T, r *fakeRunner) (*Handler, *bytesMetrics) {
	t.Helper()
	reg := &fakeClient{
		catalogs: [][]string{{"engine"}},
		tags: map[string][]imgregistry.TagGroup{
			"engine": {orphanGroup(digD, "abc1234")},
		},
	}
	m := &bytesMetrics{}
	h := newHandler(t, reg)
	h.Metrics = m
	h.BlobGC = testBlobGC(r)
	return h, m
}

// The whole point of Result: BlobGC.Run discards it, so wiring the call site to
// Run leaves the bytes-reclaimed metric with NO data path at all. This test is
// the one that fails if someone switches it back.
func TestRun_ReportsBytesReclaimedAsAMetric(t *testing.T) {
	r := &fakeRunner{execOut: []podman.ExecResult{
		{Output: "3000\t/var/lib/registry"},
		{Output: "1000\t/var/lib/registry"},
	}}
	h, m := bytesHandler(t, r)

	if _, _, err := runJob(t, h, Payload{Policy: testPolicy()}); err != nil {
		t.Fatal(err)
	}
	if len(m.bytes) != 1 {
		t.Fatalf("want exactly one bytes-reclaimed sample, got %v", m.bytes)
	}
	if want := int64(2000 * 1024); m.bytes[0] != want {
		t.Errorf("reclaimed = %d, want %d", m.bytes[0], want)
	}
}

// An unmeasured run must record NOTHING rather than zero: "we could not size
// the registry" and "the GC freed nothing" are different facts, and a zero
// sample would make a broken measurement look like a useless GC forever.
func TestRun_UnmeasuredBlobGCRecordsNoBytes(t *testing.T) {
	r := &fakeRunner{execErr: errUnmeasurable}
	h, m := bytesHandler(t, r)

	if _, _, err := runJob(t, h, Payload{Policy: testPolicy()}); err != nil {
		t.Fatal(err)
	}
	if len(m.bytes) != 0 {
		t.Fatalf("want no bytes-reclaimed sample, got %v", m.bytes)
	}
}

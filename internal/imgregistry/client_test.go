package imgregistry

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestHTTPClient_Catalog_SinglePage(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v2/_catalog" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"repositories":["engine","otp","qabazaar"]}`))
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL, Auth{Mode: "none"})
	repos, err := c.Catalog(context.Background())
	if err != nil {
		t.Fatalf("Catalog: %v", err)
	}
	want := []string{"engine", "otp", "qabazaar"}
	if len(repos) != len(want) {
		t.Fatalf("got %v, want %v", repos, want)
	}
	for i := range want {
		if repos[i] != want[i] {
			t.Fatalf("got %v, want %v", repos, want)
		}
	}
}

func TestHTTPClient_Catalog_FollowsLinkHeader(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("last") == "" {
			w.Header().Set("Link", `</v2/_catalog?n=100&last=engine>; rel="next"`)
			w.Write([]byte(`{"repositories":["engine"]}`))
			return
		}
		w.Write([]byte(`{"repositories":["otp"]}`))
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL, Auth{Mode: "none"})
	repos, err := c.Catalog(context.Background())
	if err != nil {
		t.Fatalf("Catalog: %v", err)
	}
	if calls != 2 {
		t.Fatalf("expected 2 requests (page 1 + next), got %d", calls)
	}
	if len(repos) != 2 || repos[0] != "engine" || repos[1] != "otp" {
		t.Fatalf("got %v", repos)
	}
}

// TestHTTPClient_Catalog_NoLinkMeansOnePage is a regression test for the
// known trap on this registry: a request above ~n=1000 returns an empty list
// with HTTP 200, indistinguishable from "no repos", unless pagination stops
// only when the server stops sending Link — never by inferring "done" from
// an empty page in isolation.
func TestHTTPClient_Catalog_NoLinkMeansOnePage(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"repositories":[]}`))
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL, Auth{Mode: "none"})
	repos, err := c.Catalog(context.Background())
	if err != nil {
		t.Fatalf("Catalog: %v", err)
	}
	if len(repos) != 0 {
		t.Fatalf("expected empty catalog, got %v", repos)
	}
}

func TestHTTPClient_Manifest(t *testing.T) {
	const manifestBody = `{
		"schemaVersion": 2,
		"config": {"digest": "sha256:cfgcfgcfgcfg", "size": 1500},
		"layers": [
			{"digest": "sha256:layer1", "size": 1000},
			{"digest": "sha256:layer2", "size": 2000}
		]
	}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v2/engine/manifests/latest" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		w.Header().Set("Docker-Content-Digest", "sha256:manifestdigest")
		w.Header().Set("Content-Type", "application/vnd.docker.distribution.manifest.v2+json")
		w.Write([]byte(manifestBody))
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL, Auth{Mode: "none"})
	m, err := c.Manifest(context.Background(), "engine", "latest")
	if err != nil {
		t.Fatalf("Manifest: %v", err)
	}
	if m.Digest != "sha256:manifestdigest" {
		t.Fatalf("got digest %q", m.Digest)
	}
	if m.ConfigDigest != "sha256:cfgcfgcfgcfg" {
		t.Fatalf("got config digest %q", m.ConfigDigest)
	}
	wantSize := int64(1500 + 1000 + 2000)
	if m.Size != wantSize {
		t.Fatalf("got size %d, want %d", m.Size, wantSize)
	}
	if len(m.Layers) != 2 || m.Layers[0].Digest != "sha256:layer1" || m.Layers[1].Size != 2000 {
		t.Fatalf("got layers %+v", m.Layers)
	}
}

func TestHTTPClient_Tags_GroupsByDigestAndCreated(t *testing.T) {
	blobCreated := map[string]string{
		"sha256:cfgA": "2026-01-01T00:00:00Z",
		"sha256:cfgB": "2026-03-01T00:00:00Z",
	}
	manifestDigest := map[string]string{"latest": "sha256:digestA", "8d5f281": "sha256:digestA", "v2": "sha256:digestB"}
	manifestConfig := map[string]string{"sha256:digestA": "sha256:cfgA", "sha256:digestB": "sha256:cfgB"}

	mux := http.NewServeMux()
	mux.HandleFunc("/v2/engine/tags/list", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"name":"engine","tags":["latest","8d5f281","v2"]}`))
	})
	mux.HandleFunc("/v2/engine/manifests/", func(w http.ResponseWriter, r *http.Request) {
		tag := strings.TrimPrefix(r.URL.Path, "/v2/engine/manifests/")
		digest := manifestDigest[tag]
		w.Header().Set("Docker-Content-Digest", digest)
		w.Write([]byte(`{"config":{"digest":"` + manifestConfig[digest] + `","size":10},"layers":[]}`))
	})
	mux.HandleFunc("/v2/engine/blobs/", func(w http.ResponseWriter, r *http.Request) {
		digest := strings.TrimPrefix(r.URL.Path, "/v2/engine/blobs/")
		w.Write([]byte(`{"created":"` + blobCreated[digest] + `"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewHTTPClient(srv.URL, Auth{Mode: "none"})
	groups, err := c.Tags(context.Background(), "engine")
	if err != nil {
		t.Fatalf("Tags: %v", err)
	}
	if len(groups) != 2 {
		t.Fatalf("expected 2 digest groups, got %d: %+v", len(groups), groups)
	}
	// Newest (digestB / "v2", created 2026-03-01) first.
	if groups[0].Digest != "sha256:digestB" {
		t.Fatalf("expected newest group first, got %+v", groups[0])
	}
	if len(groups[0].Tags) != 1 || groups[0].Tags[0] != "v2" {
		t.Fatalf("got tags %v for newest group", groups[0].Tags)
	}
	if groups[1].Digest != "sha256:digestA" {
		t.Fatalf("expected oldest group second, got %+v", groups[1])
	}
	gotTags := append([]string{}, groups[1].Tags...)
	sort.Strings(gotTags)
	if len(gotTags) != 2 || gotTags[0] != "8d5f281" || gotTags[1] != "latest" {
		t.Fatalf("expected [8d5f281 latest], got %v", gotTags)
	}
}

// TestHTTPClient_Manifest_NotFoundIsErrNotFound is the Important-3 fix: a
// real registry 404 must be distinguishable from every other failure via
// errors.Is(err, ErrNotFound), so callers can tell "doesn't exist" apart from
// "couldn't reach the registry."
func TestHTTPClient_Manifest_NotFoundIsErrNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL, Auth{Mode: "none"})
	_, err := c.Manifest(context.Background(), "engine", "nope")
	if err == nil {
		t.Fatal("expected an error")
	}
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected errors.Is(err, ErrNotFound), got %v", err)
	}
	if errors.Is(err, ErrUnreachable) {
		t.Fatalf("a genuine 404 must not also match ErrUnreachable: %v", err)
	}
}

// TestHTTPClient_Manifest_ServerErrorIsErrUnreachable is the other half of
// Important-3: a non-404 non-2xx (registry reachable but broken, e.g. 500)
// must be distinguishable as ErrUnreachable, not ErrNotFound — the two must
// never collapse into the same signal an operator sees as "doesn't exist."
func TestHTTPClient_Manifest_ServerErrorIsErrUnreachable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL, Auth{Mode: "none"})
	_, err := c.Manifest(context.Background(), "engine", "latest")
	if err == nil {
		t.Fatal("expected an error")
	}
	if !errors.Is(err, ErrUnreachable) {
		t.Fatalf("expected errors.Is(err, ErrUnreachable), got %v", err)
	}
	if errors.Is(err, ErrNotFound) {
		t.Fatalf("a 500 must not also match ErrNotFound: %v", err)
	}
}

// TestHTTPClient_Manifest_TransportFailureIsErrUnreachable covers the
// transport-error (not even an HTTP response) branch of the same fix.
func TestHTTPClient_Manifest_TransportFailureIsErrUnreachable(t *testing.T) {
	// A server that immediately closes the connection without responding.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hj, ok := w.(http.Hijacker)
		if !ok {
			t.Fatal("ResponseWriter does not support hijacking")
		}
		conn, _, err := hj.Hijack()
		if err != nil {
			t.Fatal(err)
		}
		conn.Close()
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL, Auth{Mode: "none"})
	_, err := c.Manifest(context.Background(), "engine", "latest")
	if err == nil {
		t.Fatal("expected an error")
	}
	if !errors.Is(err, ErrUnreachable) {
		t.Fatalf("expected errors.Is(err, ErrUnreachable), got %v", err)
	}
}

// TestHTTPClient_Catalog_UnreachableIsDistinctFromNotFound mirrors the above
// two for Catalog's own do()-based error path.
func TestHTTPClient_Catalog_UnreachableIsDistinctFromNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL, Auth{Mode: "none"})
	_, err := c.Catalog(context.Background())
	if !errors.Is(err, ErrUnreachable) {
		t.Fatalf("expected errors.Is(err, ErrUnreachable), got %v", err)
	}
}

// TestHTTPClient_Tags_SkipsUnresolvableTagRatherThanAborting is the
// Important-4 fix: one tag this client cannot resolve (simulated here with a
// manifest request that always 500s) must not hide every other good tag in
// the repo behind a single error.
func TestHTTPClient_Tags_SkipsUnresolvableTagRatherThanAborting(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v2/engine/tags/list", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"name":"engine","tags":["good","bad"]}`))
	})
	mux.HandleFunc("/v2/engine/manifests/good", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Docker-Content-Digest", "sha256:gooddigest")
		w.Write([]byte(`{"config":{"digest":"sha256:goodcfg","size":10},"layers":[]}`))
	})
	mux.HandleFunc("/v2/engine/manifests/bad", func(w http.ResponseWriter, r *http.Request) {
		// An exotic media type / broken manifest this client can't resolve.
		http.Error(w, "unsupported media type", http.StatusUnsupportedMediaType)
	})
	mux.HandleFunc("/v2/engine/blobs/sha256:goodcfg", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"created":"2026-01-01T00:00:00Z"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewHTTPClient(srv.URL, Auth{Mode: "none"})
	groups, err := c.Tags(context.Background(), "engine")
	if err != nil {
		t.Fatalf("Tags: %v (a single bad tag must not abort the whole call)", err)
	}
	if len(groups) != 1 {
		t.Fatalf("expected the one resolvable digest group, got %+v", groups)
	}
	if len(groups[0].Tags) != 1 || groups[0].Tags[0] != "good" {
		t.Fatalf("expected only the good tag, got %v", groups[0].Tags)
	}
}

// TestHTTPClient_Tags_MultiArchManifestListResolvesWithZeroCreated is the
// other half of Important-4: a multi-arch tag (manifest list / OCI index)
// must at least resolve — recorded with zero Created/Size — rather than
// failing to decode a "manifests" array as config+layers.
func TestHTTPClient_Tags_MultiArchManifestListResolvesWithZeroCreated(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v2/engine/tags/list", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"name":"engine","tags":["multiarch"]}`))
	})
	mux.HandleFunc("/v2/engine/manifests/multiarch", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Docker-Content-Digest", "sha256:indexdigest")
		w.Header().Set("Content-Type", "application/vnd.oci.image.index.v1+json")
		w.Write([]byte(`{"manifests":[{"digest":"sha256:platform1","mediaType":"application/vnd.oci.image.manifest.v1+json"}]}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewHTTPClient(srv.URL, Auth{Mode: "none"})
	groups, err := c.Tags(context.Background(), "engine")
	if err != nil {
		t.Fatalf("Tags: %v", err)
	}
	if len(groups) != 1 {
		t.Fatalf("expected 1 group for the multi-arch tag, got %+v", groups)
	}
	if groups[0].Digest != "sha256:indexdigest" {
		t.Fatalf("got digest %q", groups[0].Digest)
	}
	if !groups[0].Created.IsZero() {
		t.Fatalf("expected zero Created for an unresolvable-config multi-arch tag, got %v", groups[0].Created)
	}
}

// TestNextLinkPath_IgnoresNonNextRel is the Important-5(a) fix: the Link
// header can carry multiple comma-separated relation entries, so matching
// the first "<...>" regardless of rel would misfire on a server that lists
// rel="prev" before rel="next".
func TestNextLinkPath_IgnoresNonNextRel(t *testing.T) {
	link := `</v2/_catalog?n=100&last=aaa>; rel="prev", </v2/_catalog?n=100&last=bbb>; rel="next"`
	got := nextLinkPath(link)
	want := "/v2/_catalog?n=100&last=bbb"
	if got != want {
		t.Fatalf("nextLinkPath(%q) = %q, want %q", link, got, want)
	}
}

func TestNextLinkPath_NoNextRelMeansDone(t *testing.T) {
	link := `</v2/_catalog?n=100&last=aaa>; rel="prev"`
	if got := nextLinkPath(link); got != "" {
		t.Fatalf("nextLinkPath(%q) = %q, want \"\"", link, got)
	}
}

// TestHTTPClient_Catalog_PaginationCap is the Important-5(b) fix: a
// misbehaving server that always sends a rel="next" Link header must not
// loop forever — Catalog must give up and return an error after
// maxPaginationPages.
func TestHTTPClient_Catalog_PaginationCap(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Link", fmt.Sprintf(`</v2/_catalog?n=100&last=%d>; rel="next"`, calls))
		w.Write([]byte(`{"repositories":["r"]}`))
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL, Auth{Mode: "none"})
	_, err := c.Catalog(context.Background())
	if err == nil {
		t.Fatal("expected an error once the pagination cap is exceeded")
	}
	if calls > maxPaginationPages+1 {
		t.Fatalf("expected the loop to stop at the cap, made %d requests", calls)
	}
}

// TestHTTPClient_Tags_ResolvesManyTagsConcurrently is the fix for a real
// production hang: the fleet's "engine" repo carries ~1000 tags accumulated
// over months of CI builds, and resolving them one manifest/blob fetch at a
// time made a single Tags call take minutes. This drives 200 tags across 40
// unique digests through a server that sleeps on every manifest/blob
// request, and asserts both correctness (every tag lands in the right
// group) and that resolution is actually concurrent — bounded by
// tagResolveConcurrency, not serialized.
func TestHTTPClient_Tags_ResolvesManyTagsConcurrently(t *testing.T) {
	const numTags = 200
	const numDigests = 40
	const perRequestDelay = 10 * time.Millisecond

	tagToDigest := make(map[string]string, numTags)
	tagNames := make([]string, 0, numTags)
	for i := 0; i < numTags; i++ {
		tag := "t" + strconv.Itoa(i)
		digest := "sha256:d" + strconv.Itoa(i%numDigests)
		tagToDigest[tag] = digest
		tagNames = append(tagNames, tag)
	}

	var inFlight, maxInFlight int64
	track := func() func() {
		n := atomic.AddInt64(&inFlight, 1)
		for {
			cur := atomic.LoadInt64(&maxInFlight)
			if n <= cur || atomic.CompareAndSwapInt64(&maxInFlight, cur, n) {
				break
			}
		}
		return func() { atomic.AddInt64(&inFlight, -1) }
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/v2/engine/tags/list", func(w http.ResponseWriter, r *http.Request) {
		body := `{"name":"engine","tags":[`
		for i, tag := range tagNames {
			if i > 0 {
				body += ","
			}
			body += `"` + tag + `"`
		}
		body += `]}`
		w.Write([]byte(body))
	})
	mux.HandleFunc("/v2/engine/manifests/", func(w http.ResponseWriter, r *http.Request) {
		done := track()
		defer done()
		time.Sleep(perRequestDelay)
		tag := strings.TrimPrefix(r.URL.Path, "/v2/engine/manifests/")
		digest := tagToDigest[tag]
		w.Header().Set("Docker-Content-Digest", digest)
		w.Write([]byte(`{"config":{"digest":"` + digest + `-cfg","size":1},"layers":[]}`))
	})
	mux.HandleFunc("/v2/engine/blobs/", func(w http.ResponseWriter, r *http.Request) {
		done := track()
		defer done()
		time.Sleep(perRequestDelay)
		w.Write([]byte(`{"created":"2026-01-01T00:00:00Z"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewHTTPClient(srv.URL, Auth{Mode: "none"})
	groups, err := c.Tags(context.Background(), "engine")
	if err != nil {
		t.Fatalf("Tags: %v", err)
	}

	// Correctness: every digest present, every tag in its right group.
	if len(groups) != numDigests {
		t.Fatalf("expected %d digest groups, got %d", numDigests, len(groups))
	}
	gotTags := 0
	for _, g := range groups {
		gotTags += len(g.Tags)
		for _, tag := range g.Tags {
			if tagToDigest[tag] != g.Digest {
				t.Fatalf("tag %q grouped under %q, want %q", tag, g.Digest, tagToDigest[tag])
			}
		}
	}
	if gotTags != numTags {
		t.Fatalf("expected %d total tags across groups, got %d", numTags, gotTags)
	}

	// Concurrency actually happened: a fully serial implementation could
	// never exceed inFlight==1. tagResolveConcurrency is 16; require at
	// least a modest fraction of that to rule out a regression back to
	// one-at-a-time without being flaky about the exact scheduling.
	if got := atomic.LoadInt64(&maxInFlight); got < 4 {
		t.Fatalf("expected concurrent requests (max in-flight >= 4), got %d — Tags may have regressed to serial resolution", got)
	}
}

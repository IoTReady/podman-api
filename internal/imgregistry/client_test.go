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

// TestHTTPClient_TagCount_DoesNotResolveManifests is the load-bearing
// property of TagCount: the repo list calls it once per repo on a single
// page load, so it must cost exactly one tags-list call. A version that
// resolved manifests (as Tags does) would reintroduce the ~21s-per-repo
// cost this method exists to avoid.
func TestHTTPClient_TagCount_DoesNotResolveManifests(t *testing.T) {
	var manifestCalls, blobCalls int64
	mux := http.NewServeMux()
	mux.HandleFunc("/v2/engine/tags/list", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"name":"engine","tags":["latest","v1","v2"]}`))
	})
	mux.HandleFunc("/v2/engine/manifests/", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&manifestCalls, 1)
		w.Write([]byte(`{}`))
	})
	mux.HandleFunc("/v2/engine/blobs/", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&blobCalls, 1)
		w.Write([]byte(`{}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewHTTPClient(srv.URL, Auth{Mode: "none"})
	n, err := c.TagCount(context.Background(), "engine")
	if err != nil {
		t.Fatalf("TagCount: %v", err)
	}
	if n != 3 {
		t.Fatalf("TagCount = %d, want 3", n)
	}
	if got := atomic.LoadInt64(&manifestCalls); got != 0 {
		t.Fatalf("TagCount made %d manifest requests, want 0 — it must not resolve manifests", got)
	}
	if got := atomic.LoadInt64(&blobCalls); got != 0 {
		t.Fatalf("TagCount made %d blob requests, want 0 — it must not resolve config blobs", got)
	}
}

// TestHTTPClient_TagCount_PaginatesLikeListTags confirms TagCount inherits
// listTags' Link-header pagination rather than counting only the first page.
func TestHTTPClient_TagCount_PaginatesLikeListTags(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("last") == "" {
			w.Header().Set("Link", `</v2/engine/tags/list?n=100&last=a>; rel="next"`)
			w.Write([]byte(`{"name":"engine","tags":["a","b"]}`))
			return
		}
		w.Write([]byte(`{"name":"engine","tags":["c"]}`))
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL, Auth{Mode: "none"})
	n, err := c.TagCount(context.Background(), "engine")
	if err != nil {
		t.Fatalf("TagCount: %v", err)
	}
	if n != 3 {
		t.Fatalf("TagCount = %d, want 3 (2 on page 1 + 1 on page 2)", n)
	}
}

// TestHTTPClient_Delete_Success asserts the DELETE method, the exact
// manifest path, and that a 202 (the v2 spec's documented success status) is
// treated as success.
func TestHTTPClient_Delete_Success(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	var gotMethod, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL, Auth{Mode: "none"})
	if err := c.Delete(context.Background(), "engine", digest); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if gotMethod != http.MethodDelete {
		t.Fatalf("method = %q, want DELETE", gotMethod)
	}
	wantPath := "/v2/engine/manifests/" + digest
	if gotPath != wantPath {
		t.Fatalf("path = %q, want %q", gotPath, wantPath)
	}
}

// TestHTTPClient_Delete_NotFoundIsErrNotFound mirrors the Manifest/Catalog
// ErrNotFound-vs-ErrUnreachable split for Delete's own error path.
func TestHTTPClient_Delete_NotFoundIsErrNotFound(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL, Auth{Mode: "none"})
	err := c.Delete(context.Background(), "engine", digest)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected errors.Is(err, ErrNotFound), got %v", err)
	}
	if errors.Is(err, ErrUnreachable) {
		t.Fatalf("a genuine 404 must not also match ErrUnreachable: %v", err)
	}
}

// TestHTTPClient_Delete_ServerErrorIsErrUnreachable is the other half: a
// non-404 non-2xx must read as ErrUnreachable, explicitly not ErrNotFound —
// a prune job must never treat "registry unreachable" as "already gone."
func TestHTTPClient_Delete_ServerErrorIsErrUnreachable(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL, Auth{Mode: "none"})
	err := c.Delete(context.Background(), "engine", digest)
	if !errors.Is(err, ErrUnreachable) {
		t.Fatalf("expected errors.Is(err, ErrUnreachable), got %v", err)
	}
	if errors.Is(err, ErrNotFound) {
		t.Fatalf("a 500 must not also match ErrNotFound: %v", err)
	}
}

// TestHTTPClient_Delete_TagFormRejectedWithoutRequest is the load-bearing
// safety check: deleting by tag would remove the manifest for every tag
// sharing that digest, so a tag-form ref must be rejected before any HTTP
// request is even constructed.
func TestHTTPClient_Delete_TagFormRejectedWithoutRequest(t *testing.T) {
	var requests int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL, Auth{Mode: "none"})
	err := c.Delete(context.Background(), "engine", "v1.2.3")
	if err == nil {
		t.Fatal("expected an error for a tag-form ref")
	}
	if n := atomic.LoadInt32(&requests); n != 0 {
		t.Fatalf("server received %d requests, want 0 — a tag-form ref must never reach the wire", n)
	}
}

// TestHTTPClient_Delete_PathTraversalRejectedWithoutRequest covers a
// path-traversal repo name, which is neither a valid repo component nor
// digest-form and must be rejected the same way.
func TestHTTPClient_Delete_PathTraversalRejectedWithoutRequest(t *testing.T) {
	var requests int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL, Auth{Mode: "none"})
	err := c.Delete(context.Background(), "../../foo", "sha256:"+strings.Repeat("a", 64))
	if err == nil {
		t.Fatal("expected an error for a path-traversal repo name")
	}
	if n := atomic.LoadInt32(&requests); n != 0 {
		t.Fatalf("server received %d requests, want 0 — an invalid repo name must never reach the wire", n)
	}
}

// A dropped tag must be REPORTED, not merely skipped. Tags() returning only
// the survivors with a nil error is indistinguishable, to any caller, from a
// repo that genuinely has fewer tags — and a prune job reasoning from that
// partial view deletes manifests that a dropped protected tag would have
// saved. ResolveTags is the same call plus the evidence.
func TestHTTPClient_ResolveTags_ReportsDroppedTags(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v2/engine/tags/list", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"name":"engine","tags":["good","bad"]}`))
	})
	mux.HandleFunc("/v2/engine/manifests/good", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Docker-Content-Digest", "sha256:gooddigest")
		w.Write([]byte(`{"config":{"digest":"sha256:goodcfg","size":10},"layers":[]}`))
	})
	mux.HandleFunc("/v2/engine/manifests/bad", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	})
	mux.HandleFunc("/v2/engine/blobs/sha256:goodcfg", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"created":"2026-01-01T00:00:00Z"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewHTTPClient(srv.URL, Auth{Mode: "none"})
	listing, err := c.ResolveTags(context.Background(), "engine")
	if err != nil {
		t.Fatalf("ResolveTags: %v (one bad tag must not abort the call)", err)
	}
	if len(listing.Groups) != 1 || listing.Groups[0].Tags[0] != "good" {
		t.Fatalf("expected the one resolvable group, got %+v", listing.Groups)
	}
	if len(listing.Dropped) != 1 || listing.Dropped[0].Tag != "bad" {
		t.Fatalf("expected the dropped tag reported, got %+v", listing.Dropped)
	}
	if listing.Dropped[0].Err == nil {
		t.Fatal("a dropped tag must carry why it was dropped")
	}
	// And the old surface still behaves exactly as before for callers that
	// do not care.
	groups, err := c.Tags(context.Background(), "engine")
	if err != nil || len(groups) != 1 {
		t.Fatalf("Tags must stay the groups-only view: %v %+v", err, groups)
	}
}

// The complete case: nothing dropped, so nothing reported. Without this, an
// implementation that reported every tag as dropped would pass the test above.
func TestHTTPClient_ResolveTags_ReportsNoDropsWhenComplete(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v2/engine/tags/list", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"name":"engine","tags":["good"]}`))
	})
	mux.HandleFunc("/v2/engine/manifests/good", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Docker-Content-Digest", "sha256:gooddigest")
		w.Write([]byte(`{"config":{"digest":"sha256:goodcfg","size":10},"layers":[]}`))
	})
	mux.HandleFunc("/v2/engine/blobs/sha256:goodcfg", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"created":"2026-01-01T00:00:00Z"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	listing, err := NewHTTPClient(srv.URL, Auth{Mode: "none"}).ResolveTags(context.Background(), "engine")
	if err != nil {
		t.Fatal(err)
	}
	if len(listing.Dropped) != 0 {
		t.Fatalf("a complete listing must report no drops, got %+v", listing.Dropped)
	}
}

// ---------------------------------------------------------------------------
// Review round 4: the HEAD fallback.
// ---------------------------------------------------------------------------

// A manifest whose stored media type is outside manifestAccept is answered
// 404 MANIFEST_UNKNOWN by distribution — byte-identical to "this tag does not
// exist". The fleet's registry mirrors third-party images whose older tags are
// still schema1, and a caller that DELETES treats an unresolvable tag as a
// reason to abort the whole fleet-wide run: one such tag would wedge pruning
// silently, behind an hour of backoff. Protection needs only the digest, so
// the last resort is HEAD with an Accept set that cannot be unsatisfied.
func TestHTTPClient_ResolveTags_LegacyManifestResolvedByHEAD(t *testing.T) {
	var headAccept string
	mux := http.NewServeMux()
	mux.HandleFunc("/v2/engine/tags/list", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"name":"engine","tags":["legacy"]}`))
	})
	mux.HandleFunc("/v2/engine/manifests/legacy", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			headAccept = r.Header.Get("Accept")
			w.Header().Set("Docker-Content-Digest", "sha256:legacydigest")
			w.WriteHeader(http.StatusOK)
			return
		}
		// GET with a negotiated Accept set: the registry cannot satisfy it.
		http.Error(w, `{"errors":[{"code":"MANIFEST_UNKNOWN"}]}`, http.StatusNotFound)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	listing, err := NewHTTPClient(srv.URL, Auth{Mode: "none"}).ResolveTags(context.Background(), "engine")
	if err != nil {
		t.Fatal(err)
	}
	if len(listing.Dropped) != 0 {
		t.Fatalf("a tag resolvable by HEAD must not be reported as dropped, got %+v", listing.Dropped)
	}
	if len(listing.Groups) != 1 || listing.Groups[0].Digest != "sha256:legacydigest" ||
		listing.Groups[0].Tags[0] != "legacy" {
		t.Fatalf("expected the tag grouped under its HEAD digest, got %+v", listing.Groups)
	}
	// Created is deliberately zero: no config blob was fetched. Callers that
	// act on age treat zero as unknown and decline to act.
	if !listing.Groups[0].Created.IsZero() {
		t.Fatalf("a HEAD-resolved group must carry no Created, got %v", listing.Groups[0].Created)
	}
	if headAccept != manifestAccept {
		t.Fatalf("the fallback must send the same explicit Accept as the GET path; Accept was %q", headAccept)
	}
}

// Measured, not reasoned. `Accept: */*` is the obvious way to write "whatever
// you have" and it is WRONG for distribution: its manifest handler parses
// Accept by exact media-type string, so `*/*` matches nothing and is NARROWER
// than the explicit list. Against real registries (see manifestDigest's doc
// comment for the transcript), `*/*` 404s an ordinary OCI image on both 2.8.3
// and 3.1.1, and on 2.8.x it makes the registry down-convert a schema2
// manifest and answer with the CONVERTED manifest's digest — a digest that
// exists nowhere in storage, which regroups the tag away from its real digest
// and strips that digest's protection.
func TestHTTPClient_ManifestDigest_SendsExplicitAcceptNeverWildcard(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Errorf("want HEAD, got %s", r.Method)
		}
		got = r.Header.Get("Accept")
		w.Header().Set("Docker-Content-Digest", "sha256:d")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	if _, err := NewHTTPClient(srv.URL, Auth{Mode: "none"}).manifestDigest(context.Background(), "engine", "v1"); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(got, "*/*") {
		t.Fatalf("HEAD must never offer a wildcard Accept; got %q", got)
	}
	if got != manifestAccept {
		t.Fatalf("HEAD Accept must equal the GET path's, so HEAD is never narrower than the GET it backstops;\n got  %q\n want %q", got, manifestAccept)
	}
}

// Minor 4, the GET path's counterpart to manifestDigest's empty-digest guard.
// A 200 with no Docker-Content-Digest would otherwise yield Digest: "", which
// groups under the empty string and is eligible to reach Delete(repo, "").
func TestHTTPClient_Manifest_MissingDigestHeaderIsErrUnreachable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"config":{"digest":"sha256:c","size":1},"layers":[]}`)) // no digest header
	}))
	defer srv.Close()

	m, err := NewHTTPClient(srv.URL, Auth{Mode: "none"}).Manifest(context.Background(), "engine", "v1")
	if !errors.Is(err, ErrUnreachable) {
		t.Fatalf("want ErrUnreachable, got %v", err)
	}
	if errors.Is(err, ErrNotFound) {
		t.Fatalf("a missing header is not evidence of absence: %v", err)
	}
	if m.Digest != "" {
		t.Fatalf("want the zero Manifest, got %+v", m)
	}
}

// The fail-closed half. A registry that cannot answer at all must still
// produce a drop, and one classified ErrUnreachable — the caller aborts on
// that, and must NOT read it as "already gone".
func TestHTTPClient_ResolveTags_HEADFailureStillDrops(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v2/engine/tags/list", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"name":"engine","tags":["bad"]}`))
	})
	mux.HandleFunc("/v2/engine/manifests/bad", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	listing, err := NewHTTPClient(srv.URL, Auth{Mode: "none"}).ResolveTags(context.Background(), "engine")
	if err != nil {
		t.Fatal(err)
	}
	if len(listing.Dropped) != 1 || listing.Dropped[0].Tag != "bad" {
		t.Fatalf("an unreadable tag must still be dropped, got %+v", listing.Dropped)
	}
	if !errors.Is(listing.Dropped[0].Err, ErrUnreachable) {
		t.Fatalf("want ErrUnreachable, got %v", listing.Dropped[0].Err)
	}
	if errors.Is(listing.Dropped[0].Err, ErrNotFound) {
		t.Fatalf("a 500 must never read as not-found: %v", listing.Dropped[0].Err)
	}
	if listing.Dropped[0].Vanished {
		t.Fatalf("an unreadable tag is not a deleted one: %+v", listing.Dropped[0])
	}
}

// A 2xx HEAD with no Docker-Content-Digest is not an answer. Recording it as a
// resolution would put an empty digest into a group; treating it as "gone"
// would be the unsafe direction. It is ErrUnreachable.
func TestHTTPClient_ResolveTags_HEADWithoutDigestHeaderDrops(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v2/engine/tags/list", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"name":"engine","tags":["odd"]}`))
	})
	mux.HandleFunc("/v2/engine/manifests/odd", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK) // no digest header
			return
		}
		http.Error(w, "nope", http.StatusNotFound)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	listing, err := NewHTTPClient(srv.URL, Auth{Mode: "none"}).ResolveTags(context.Background(), "engine")
	if err != nil {
		t.Fatal(err)
	}
	if len(listing.Dropped) != 1 || !errors.Is(listing.Dropped[0].Err, ErrUnreachable) {
		t.Fatalf("want one ErrUnreachable drop, got %+v", listing.Dropped)
	}
}

// ---------------------------------------------------------------------------
// Review round 5: absence comes from the tag list, never from a manifest 404.
// ---------------------------------------------------------------------------

// A tag genuinely deleted between the tags/list call and its manifest fetch
// must be marked Vanished — the prune handler treats exactly this case as
// benign and proceeds, rather than aborting a fleet-wide run every time CI
// deletes a tag mid-run.
//
// The evidence is the SECOND tags/list read, not the manifest's 404: the
// registry stops listing the tag. Note the first read still lists it, which is
// what puts the tag into this run's resolution set at all.
func TestHTTPClient_ResolveTags_DeletedTagIsVanishedPerTheTagList(t *testing.T) {
	var listCalls int
	mux := http.NewServeMux()
	mux.HandleFunc("/v2/engine/tags/list", func(w http.ResponseWriter, r *http.Request) {
		listCalls++
		if listCalls == 1 {
			w.Write([]byte(`{"name":"engine","tags":["gone"]}`))
			return
		}
		w.Write([]byte(`{"name":"engine","tags":[]}`)) // deleted in between
	})
	mux.HandleFunc("/v2/engine/manifests/gone", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"errors":[{"code":"MANIFEST_UNKNOWN"}]}`, http.StatusNotFound)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	listing, err := NewHTTPClient(srv.URL, Auth{Mode: "none"}).ResolveTags(context.Background(), "engine")
	if err != nil {
		t.Fatal(err)
	}
	if len(listing.Dropped) != 1 || listing.Dropped[0].Tag != "gone" {
		t.Fatalf("want the deleted tag reported, got %+v", listing.Dropped)
	}
	if !listing.Dropped[0].Vanished {
		t.Fatalf("a tag the registry no longer lists must be Vanished, got %+v", listing.Dropped[0])
	}
	if listCalls < 2 {
		t.Fatalf("absence must be established by re-reading tags/list, got %d list calls", listCalls)
	}
}

// The load-bearing converse, and the reason a manifest 404 is not evidence.
// distribution answers 404 MANIFEST_UNKNOWN both for a tag that does not exist
// and for a manifest whose stored media type matches nothing in the request's
// Accept set — measured on 2.8.3 and 3.1.1 against an ordinary OCI image with
// `Accept: */*`. Here BOTH the GET and the HEAD 404 while the tag is plainly
// still in the registry's own tag list. Marking it Vanished would tell the
// prune handler to proceed with the tag absent from Groups: it loses its name
// protection and its cross-repo shares-protected seeding, and the image it
// points at becomes a delete candidate.
func TestHTTPClient_ResolveTags_StillListedTagIsNeverVanished(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v2/engine/tags/list", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"name":"engine","tags":["latest"]}`))
	})
	mux.HandleFunc("/v2/engine/manifests/latest", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"errors":[{"code":"MANIFEST_UNKNOWN"}]}`, http.StatusNotFound)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	listing, err := NewHTTPClient(srv.URL, Auth{Mode: "none"}).ResolveTags(context.Background(), "engine")
	if err != nil {
		t.Fatal(err)
	}
	if len(listing.Dropped) != 1 {
		t.Fatalf("want one drop, got %+v", listing.Dropped)
	}
	if listing.Dropped[0].Vanished {
		t.Fatalf("a tag STILL in the registry's tag list must never be Vanished, whatever the manifest endpoint said: %+v", listing.Dropped[0])
	}
	if !errors.Is(listing.Dropped[0].Err, ErrUnreachable) {
		t.Fatalf("want ErrUnreachable, got %v", listing.Dropped[0].Err)
	}
}

// The IMPORTANT-3 shape: GET says not-found, HEAD says unreachable. Under the
// previous design the benign/hard split was read out of the drop's error, so
// this input's classification hung on client.go formatting the GET's cause
// with %v rather than %w — an invisible, untested, one-character distance from
// a live delete. It no longer hangs on anything of the sort: absence is a
// separate field set only from the tag list, and the tag is still listed here.
func TestHTTPClient_ResolveTags_GETNotFoundWithHEADUnreachableIsNotVanished(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v2/engine/tags/list", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"name":"engine","tags":["latest"]}`))
	})
	mux.HandleFunc("/v2/engine/manifests/latest", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		http.Error(w, `{"errors":[{"code":"MANIFEST_UNKNOWN"}]}`, http.StatusNotFound)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	listing, err := NewHTTPClient(srv.URL, Auth{Mode: "none"}).ResolveTags(context.Background(), "engine")
	if err != nil {
		t.Fatal(err)
	}
	if len(listing.Dropped) != 1 || listing.Dropped[0].Vanished {
		t.Fatalf("want one non-Vanished drop, got %+v", listing.Dropped)
	}
	if errors.Is(listing.Dropped[0].Err, ErrNotFound) {
		t.Fatalf("the drop must not read as not-found either: %v", listing.Dropped[0].Err)
	}
}

// Fail closed on the evidence itself. If the tag list cannot be re-read, we do
// not know whether the tag exists — and "we could not check" must never
// collapse into "it is gone".
func TestHTTPClient_ResolveTags_UnreadableTagListIsNotVanished(t *testing.T) {
	var listCalls int
	mux := http.NewServeMux()
	mux.HandleFunc("/v2/engine/tags/list", func(w http.ResponseWriter, r *http.Request) {
		listCalls++
		if listCalls == 1 {
			w.Write([]byte(`{"name":"engine","tags":["latest"]}`))
			return
		}
		http.Error(w, "boom", http.StatusInternalServerError)
	})
	mux.HandleFunc("/v2/engine/manifests/latest", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"errors":[{"code":"MANIFEST_UNKNOWN"}]}`, http.StatusNotFound)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	listing, err := NewHTTPClient(srv.URL, Auth{Mode: "none"}).ResolveTags(context.Background(), "engine")
	if err != nil {
		t.Fatal(err)
	}
	if len(listing.Dropped) != 1 || listing.Dropped[0].Vanished {
		t.Fatalf("an unreadable tag list must not produce a Vanished drop, got %+v", listing.Dropped)
	}
	if !errors.Is(listing.Dropped[0].Err, ErrUnreachable) {
		t.Fatalf("want ErrUnreachable, got %v", listing.Dropped[0].Err)
	}
}

// manifestAccept must name the legacy schema1 and OCI artifact types. Without
// them the GET path 404s on manifests that exist, and every such tag falls
// through to the HEAD fallback — correct, but it means the primary path never
// yields a Created timestamp for them.
func TestManifestAcceptCoversLegacyAndArtifactMediaTypes(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("Accept")
		w.Header().Set("Docker-Content-Digest", "sha256:x")
		w.Write([]byte(`{"config":{"digest":"sha256:c","size":1},"layers":[]}`))
	}))
	defer srv.Close()

	if _, err := NewHTTPClient(srv.URL, Auth{Mode: "none"}).Manifest(context.Background(), "engine", "v1"); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"application/vnd.docker.distribution.manifest.v1+json",
		"application/vnd.oci.artifact.manifest.v1+json",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("Accept %q does not offer %q", got, want)
		}
	}
}

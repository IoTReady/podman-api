package imgregistry

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
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

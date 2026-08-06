package imgregistry

import (
	"context"
	"net/http"
	"net/http/httptest"
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

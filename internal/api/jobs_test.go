package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/iotready/podman-api/internal/auth"
	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/store"
)

// newSrvWithJobs builds a test server whose key has jobs:read scope and whose
// router is wired to the given JobStore (nil = store disabled). It uses the
// same token/hash mechanism as newSrvFull.
func newSrvWithJobs(t *testing.T, js store.JobStore) (*httptest.Server, string) {
	t.Helper()
	tok := "t"
	hash, err := config.HashToken(tok)
	if err != nil {
		t.Fatal(err)
	}
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"jobs:read"}}}
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc := instance.NewService(fake.New(), hosts, nil)
	srv := httptest.NewServer(NewRouter(svc, js, auth.NewKeyStore(keys), nil, nil))
	t.Cleanup(srv.Close)
	return srv, tok
}

func TestJobs_ListAndGet(t *testing.T) {
	js := store.NewMemory()
	j, err := js.Enqueue(context.Background(), "migrate", json.RawMessage(`{"from":"h1"}`), "")
	if err != nil {
		t.Fatal(err)
	}
	srv, tok := newSrvWithJobs(t, js)

	resp := authedReq(t, srv, tok, "GET", "/jobs")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list status %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	var list []map[string]any
	_ = json.Unmarshal(body, &list)
	if len(list) != 1 || list[0]["kind"] != "migrate" {
		t.Fatalf("list body: %s", string(body))
	}

	resp = authedReq(t, srv, tok, "GET", "/jobs/"+j.ID)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get status %d", resp.StatusCode)
	}
	body, _ = io.ReadAll(resp.Body)
	var one map[string]any
	_ = json.Unmarshal(body, &one)
	if one["id"] != j.ID || one["state"] != "queued" {
		t.Fatalf("get body: %s", string(body))
	}
}

func TestJobs_Filter(t *testing.T) {
	js := store.NewMemory()
	_, _ = js.Enqueue(context.Background(), "migrate", json.RawMessage(`{}`), "")
	_, _ = js.Enqueue(context.Background(), "evacuate", json.RawMessage(`{}`), "")
	srv, tok := newSrvWithJobs(t, js)

	resp := authedReq(t, srv, tok, "GET", "/jobs?kind=evacuate")
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var list []map[string]any
	_ = json.Unmarshal(body, &list)
	if len(list) != 1 || list[0]["kind"] != "evacuate" {
		t.Fatalf("filter body: %s", string(body))
	}
}

func TestJobs_NotFound(t *testing.T) {
	srv, tok := newSrvWithJobs(t, store.NewMemory())
	resp := authedReq(t, srv, tok, "GET", "/jobs/missing")
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("want 404, got %d", resp.StatusCode)
	}
}

func TestJobs_DisabledReturns501(t *testing.T) {
	srv, tok := newSrvWithJobs(t, nil)

	resp := authedReq(t, srv, tok, "GET", "/jobs")
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("want 501, got %d", resp.StatusCode)
	}

	resp = authedReq(t, srv, tok, "GET", "/jobs/x")
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("want 501 for get, got %d", resp.StatusCode)
	}
}

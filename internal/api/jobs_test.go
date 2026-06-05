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
	srv := httptest.NewServer(NewRouter(svc, js, auth.NewKeyStore(keys), nil, nil, nil))
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
	if err := json.Unmarshal(body, &list); err != nil {
		t.Fatalf("decode list: %v (body: %s)", err, body)
	}
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
	if err := json.Unmarshal(body, &one); err != nil {
		t.Fatalf("decode one: %v (body: %s)", err, body)
	}
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
	if err := json.Unmarshal(body, &list); err != nil {
		t.Fatalf("decode list: %v (body: %s)", err, body)
	}
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

type fakeCanceller struct{ running map[string]bool }

func (f fakeCanceller) Cancel(id string) bool { return f.running[id] }

// newSrvWithCancel wires a router with the given store + canceller and a key
// scoped instances:write (the cancel route's required scope).
func newSrvWithCancel(t *testing.T, js store.JobStore, c JobCanceller) (*httptest.Server, string) {
	t.Helper()
	tok := "t"
	hash, err := config.HashToken(tok)
	if err != nil {
		t.Fatal(err)
	}
	keys := []config.APIKey{{ID: "k", SecretHash: hash, Scopes: []string{"instances:write", "jobs:read"}}}
	hosts := []config.Host{{ID: "h1", Addr: "unix", Socket: "/x"}}
	svc := instance.NewService(fake.New(), hosts, nil)
	srv := httptest.NewServer(NewRouter(svc, js, auth.NewKeyStore(keys), nil, nil, c))
	t.Cleanup(srv.Close)
	return srv, tok
}

func TestCancelJob(t *testing.T) {
	ctx := context.Background()

	t.Run("disabled returns 501", func(t *testing.T) {
		srv, tok := newSrvWithCancel(t, nil, fakeCanceller{})
		resp := authedReq(t, srv, tok, "POST", "/jobs/x/cancel")
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNotImplemented {
			t.Fatalf("status %d", resp.StatusCode)
		}
	})

	t.Run("absent returns 404", func(t *testing.T) {
		srv, tok := newSrvWithCancel(t, store.NewMemory(), fakeCanceller{})
		resp := authedReq(t, srv, tok, "POST", "/jobs/nope/cancel")
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("status %d", resp.StatusCode)
		}
	})

	t.Run("terminal returns 409", func(t *testing.T) {
		js := store.NewMemory()
		j, _ := js.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
		_, _, _ = js.ClaimNext(ctx)
		_ = js.Finish(ctx, j.ID, store.JobSucceeded, "")
		srv, tok := newSrvWithCancel(t, js, fakeCanceller{})
		resp := authedReq(t, srv, tok, "POST", "/jobs/"+j.ID+"/cancel")
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusConflict {
			t.Fatalf("status %d", resp.StatusCode)
		}
	})

	t.Run("queued returns 202 and becomes canceled", func(t *testing.T) {
		js := store.NewMemory()
		j, _ := js.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
		srv, tok := newSrvWithCancel(t, js, fakeCanceller{})
		resp := authedReq(t, srv, tok, "POST", "/jobs/"+j.ID+"/cancel")
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("status %d", resp.StatusCode)
		}
		got, _ := js.GetJob(ctx, j.ID)
		if got.State != store.JobCanceled {
			t.Fatalf("state %q", got.State)
		}
	})

	t.Run("running returns 202 via canceller", func(t *testing.T) {
		js := store.NewMemory()
		j, _ := js.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
		_, _, _ = js.ClaimNext(ctx) // now running
		srv, tok := newSrvWithCancel(t, js, fakeCanceller{running: map[string]bool{j.ID: true}})
		resp := authedReq(t, srv, tok, "POST", "/jobs/"+j.ID+"/cancel")
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("status %d", resp.StatusCode)
		}
	})

	t.Run("running but not yet in canceller returns 409 not-cancelable", func(t *testing.T) {
		js := store.NewMemory()
		j, _ := js.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
		_, _, _ = js.ClaimNext(ctx) // running on disk, but not registered in the runner yet
		srv, tok := newSrvWithCancel(t, js, fakeCanceller{})
		resp := authedReq(t, srv, tok, "POST", "/jobs/"+j.ID+"/cancel")
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusConflict {
			t.Fatalf("status %d", resp.StatusCode)
		}
		// Still running (not terminal), so the reason must be the transient,
		// retryable one — not "already terminal".
		var body struct {
			Code string `json:"code"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if body.Code != "job_not_cancelable" {
			t.Fatalf("code = %q, want job_not_cancelable", body.Code)
		}
	})
}

func TestCancelJob_Reconciling(t *testing.T) {
	js := store.NewMemory()
	ctx := context.Background()
	j, _ := js.Enqueue(ctx, "migrate", json.RawMessage(`{}`), "")
	_, _, _ = js.ClaimNext(ctx)
	_, _ = js.MarkReconciling(ctx, []string{"migrate"})

	srv, tok := newSrvWithCancel(t, js, fakeCanceller{running: map[string]bool{}})

	resp := authedReq(t, srv, tok, "POST", "/jobs/"+j.ID+"/cancel")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}
	if jb, _ := js.GetJob(ctx, j.ID); jb.State != store.JobCanceled {
		t.Fatalf("state = %q, want canceled", jb.State)
	}
}

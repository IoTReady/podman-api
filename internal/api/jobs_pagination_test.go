package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/iotready/podman-api/internal/store"
)

func TestJobs_Pagination(t *testing.T) {
	js := store.NewMemory()
	ctx := context.Background()
	var ids []string // oldest..newest
	for i := 0; i < 3; i++ {
		j, err := js.Enqueue(ctx, "test", json.RawMessage(`{}`), "")
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, j.ID)
	}
	srv, tok := newSrvWithJobs(t, js)

	decode := func(resp *http.Response) []map[string]any {
		t.Helper()
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(resp.Body)
			t.Fatalf("status %d: %s", resp.StatusCode, b)
		}
		var list []map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
			t.Fatal(err)
		}
		return list
	}

	// limit caps the page; newest first.
	p1 := decode(authedReq(t, srv, tok, "GET", "/jobs?limit=1"))
	if len(p1) != 1 || p1[0]["id"] != ids[2] {
		t.Fatalf("page1: %+v", p1)
	}

	// before cursor returns the next page.
	p2 := decode(authedReq(t, srv, tok, "GET", "/jobs?limit=1&before="+ids[2]))
	if len(p2) != 1 || p2[0]["id"] != ids[1] {
		t.Fatalf("page2: %+v", p2)
	}

	// no params → bare array bounded by the default limit (3 here).
	all := decode(authedReq(t, srv, tok, "GET", "/jobs"))
	if len(all) != 3 {
		t.Fatalf("all: %+v", all)
	}

	// non-integer limit → 400 invalid_query.
	resp := authedReq(t, srv, tok, "GET", "/jobs?limit=abc")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad limit status = %d, want 400", resp.StatusCode)
	}
	var eb map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&eb); err != nil {
		t.Fatal(err)
	}
	if eb["code"] != "invalid_query" {
		t.Fatalf("error code = %v, want invalid_query", eb["code"])
	}
}

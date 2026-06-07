package ui

import (
	"net/http"
	"strings"
	"testing"

	"github.com/iotready/podman-api/internal/podman"
)

func TestResolveContainerSuffix(t *testing.T) {
	containers := []podman.Container{
		{Name: "postgres-main-db"},
		{Name: "postgres-main-sidecar"},
	}
	if got := resolveContainerSuffix("postgres", "main", containers); got != "db" {
		t.Fatalf("got %q, want %q", got, "db")
	}
}

func TestResolveContainerSuffixSkipsNonPrefixed(t *testing.T) {
	containers := []podman.Container{
		{Name: "pause"},
		{Name: "infra"},
		{Name: "postgres-main-app"},
	}
	if got := resolveContainerSuffix("postgres", "main", containers); got != "app" {
		t.Fatalf("got %q, want %q", got, "app")
	}
}

func TestResolveContainerSuffixNoMatch(t *testing.T) {
	containers := []podman.Container{
		{Name: "pause"},
	}
	if got := resolveContainerSuffix("postgres", "main", containers); got != "" {
		t.Fatalf("got %q, want empty string", got)
	}
}

func TestLogsPageRedirectsWhenContainerAbsent(t *testing.T) {
	u := uiWithSeededInstance(t)
	// uiWithSeededInstance seeds container "postgres-main-db"; suffix = "db"
	w := authedGet(t, u, "/ui/hosts/edge-1/instances/postgres/main/logs")
	if w.Code != http.StatusFound {
		t.Fatalf("status = %d, want 302 redirect", w.Code)
	}
	loc := w.Header().Get("Location")
	if !strings.Contains(loc, "container=db") {
		t.Errorf("Location %q should contain container=db", loc)
	}
	if !strings.Contains(loc, "follow=true") {
		t.Errorf("Location %q should contain follow=true", loc)
	}
}

func TestLogsPageStaticRenderShowsLines(t *testing.T) {
	u := uiWithSeededInstance(t)
	// ?container=db&follow=false — static 200-line snapshot
	w := authedGet(t, u, "/ui/hosts/edge-1/instances/postgres/main/logs?container=db&follow=false")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if !strings.Contains(w.Body.String(), "database system is ready") {
		t.Error("static logs page should contain the seeded log line")
	}
}

func TestLogsPageFollowRenderHasSSEConnect(t *testing.T) {
	u := uiWithSeededInstance(t)
	// ?container=db&follow=true — SSE streaming mode
	w := authedGet(t, u, "/ui/hosts/edge-1/instances/postgres/main/logs?container=db&follow=true")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	if !strings.Contains(w.Body.String(), "sse-connect") {
		t.Error("follow mode page should contain sse-connect attribute")
	}
}

func TestLogsPageNotFoundReturns404(t *testing.T) {
	u := uiWithSeededInstance(t)
	w := authedGet(t, u, "/ui/hosts/edge-1/instances/postgres/ghost/logs?container=db")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 for unknown slug", w.Code)
	}
}

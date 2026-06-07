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

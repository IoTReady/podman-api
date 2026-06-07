package ui

import (
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

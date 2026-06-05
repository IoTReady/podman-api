package ui

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/podman/fake"
	"github.com/iotready/podman-api/internal/store"
)

// uiWithJobs builds a UI with an in-memory job store (optionally seeded) and an
// authenticated operator.
func uiWithJobs(t *testing.T, js store.JobStore) *UI {
	t.Helper()
	svc := instance.NewService(fake.New(), []config.Host{{ID: "edge-1"}}, nil)
	hash, _ := config.HashToken("pw")
	u, err := New(Config{
		Svc:  svc,
		Jobs: js,
		Auth: NewOperatorAuthenticator(config.Operator{Username: "op", PasswordHash: hash}),
	})
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func TestJobsListDisabledWhenStoreNil(t *testing.T) {
	u := uiWithService(t) // Jobs nil
	w := authedGet(t, u, "/ui/jobs")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (disabled notice, not 500)", w.Code)
	}
}

func TestJobsListRendersJobs(t *testing.T) {
	mem := store.NewMemory()
	j, err := mem.Enqueue(context.Background(), "migrate", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	u := uiWithJobs(t, mem)
	w := authedGet(t, u, "/ui/jobs")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "migrate") {
		t.Error("jobs list should show the job kind")
	}
	if !strings.Contains(body, j.ID) {
		t.Error("jobs list should show the job id")
	}
}

func TestJobDetailNotFoundIs404(t *testing.T) {
	u := uiWithJobs(t, store.NewMemory())
	w := authedGet(t, u, "/ui/jobs/nonexistent")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

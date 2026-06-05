package migrate

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/jobs"
	"github.com/iotready/podman-api/internal/store"
)

func TestReconciler_BadArgs_Fails(t *testing.T) {
	r := &Reconciler{Svc: &instance.Service{}}
	state, resolved, err := r.Reconcile(context.Background(),
		store.Job{ID: "j1", Kind: "migrate", Args: json.RawMessage(`not json`)},
		jobs.NewJobContext(store.NewMemory(), "j1"))
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !resolved || state != store.JobFailed {
		t.Fatalf("got state=%q resolved=%v, want failed/true", state, resolved)
	}
}

func TestReconciler_SatisfiesInterface(t *testing.T) {
	var _ jobs.Reconciler = (*Reconciler)(nil)
}

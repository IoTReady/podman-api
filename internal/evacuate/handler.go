// Package evacuate adapts host-evacuation orchestration to the jobs runner.
package evacuate

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/jobs"
	"github.com/iotready/podman-api/internal/store"
)

// evacuateConcurrency bounds how many child migrations run at once. migrate is
// heavy (stop + volume cold-copy + apply + verify), so keep it low. var (not
// const) so same-package tests can change it.
var evacuateConcurrency = 2

// Handler runs "evacuate" jobs: resolve the host's instances into a migrate
// plan, run the migrations with bounded concurrency (each as a child migrate
// job), and aggregate onto the parent.
//
// A parent evacuate occupies one runner pool worker for the whole fan-out (its
// children run as goroutines inside it and need no worker). That keeps a single
// evacuate deadlock-free, but with the default pool of 4, four concurrent
// evacuates hold all workers for the duration and starve plain migrate/other
// jobs of a slot. Acceptable at current scale; if many concurrent evacuates
// become common, give the runner headroom or a separate orchestration pool.
type Handler struct {
	Svc  *instance.Service
	Jobs store.JobStore
	// migrate moves one instance; defaults to Svc.Migrate. Overridable in tests.
	migrate func(ctx context.Context, req instance.MigrateRequest, step func(step, detail string)) error
}

var _ jobs.Handler = (*Handler)(nil)

func (h *Handler) Run(ctx context.Context, job store.Job, jc *jobs.JobContext) error {
	var req instance.EvacuateRequest
	if err := json.Unmarshal(job.Args, &req); err != nil {
		return fmt.Errorf("decode evacuate args: %w", err)
	}
	moves, err := h.Svc.ResolveEvacuation(ctx, req)
	if err != nil {
		return err
	}

	mig := h.migrate
	if mig == nil {
		mig = h.Svc.Migrate
	}

	sem := make(chan struct{}, evacuateConcurrency)
	failures := make([]string, len(moves)) // index-addressed; "" == success
	var wg sync.WaitGroup
	for i, m := range moves {
		// Acquire before spawning so at most evacuateConcurrency goroutines exist
		// at once (matters for a host with many instances). On a cancelled ctx the
		// in-flight children fail fast and free slots, so the loop still drains and
		// records a result for every move rather than leaving any counted as ok.
		sem <- struct{}{}
		wg.Add(1)
		go func(i int, m instance.MigrateRequest) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := h.runChild(ctx, job.ID, m, mig); err != nil {
				failures[i] = fmt.Sprintf("%s: %v", m.Slug, err)
			}
		}(i, m)
	}
	wg.Wait()

	var failed []string
	for _, f := range failures {
		if f != "" {
			failed = append(failed, f)
		}
	}
	// Single post-join writer: preserves AppendStep's single-writer invariant.
	jc.Step("summary", fmt.Sprintf("%d ok, %d failed", len(moves)-len(failed), len(failed)))
	if len(failed) > 0 {
		return fmt.Errorf("%d/%d migrations failed: %s", len(failed), len(moves), strings.Join(failed, "; "))
	}
	return nil
}

// runChild records a child migrate job, runs the migration (its progress steps
// land on the child), and finishes the child. A store bookkeeping error counts
// as a failure even if the migration itself succeeded — we could not record it,
// so fail loud.
func (h *Handler) runChild(ctx context.Context, parentID string, m instance.MigrateRequest,
	mig func(context.Context, instance.MigrateRequest, func(string, string)) error) error {
	args, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("marshal child args: %w", err)
	}
	child, err := h.Jobs.StartChild(ctx, "migrate", args, parentID)
	if err != nil {
		return fmt.Errorf("start child job: %w", err)
	}
	cjc := jobs.NewJobContext(h.Jobs, child.ID)
	migErr := mig(ctx, m, cjc.Step)

	state, errMsg := store.JobSucceeded, ""
	if migErr != nil {
		state, errMsg = store.JobFailed, migErr.Error()
	}
	if ferr := h.Jobs.Finish(ctx, child.ID, state, errMsg); ferr != nil {
		if migErr == nil {
			// Migration succeeded but we couldn't record it — surface as failure.
			return fmt.Errorf("finish child job: %w", ferr)
		}
		// Migration also failed: migErr is the more useful cause and is returned
		// below. Log the dropped finish error so the child left non-terminal
		// (reaped by boot recovery) is still traceable.
		log.Printf("evacuate: finish child %s failed (migration error: %v): %v", child.ID, migErr, ferr)
	}
	return migErr
}

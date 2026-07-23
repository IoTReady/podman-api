package ui

import (
	"context"
	"errors"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/iotready/podman-api/internal/instance"
)

// hostFetchTimeout bounds a per-host instance fetch that can hit live podman —
// the dashboard fan-out and the host page / fragment. Warm reads return
// instantly; this only bites on a cold host that has never succeeded (never
// cached in warm mode), so one such host can't stall the dashboard OR the host
// page up to podman's 10-minute op timeout. Mirrors the JSON /hosts path's 5s
// per-host bound.
const hostFetchTimeout = 5 * time.Second

func (u *UI) dashboard(w http.ResponseWriter, r *http.Request) {
	hosts := u.cfg.Svc.Hosts()
	type hostSummary struct {
		ID        string
		Instances int
	}
	summaries := make([]hostSummary, len(hosts))
	var wg sync.WaitGroup
	for i, h := range hosts {
		wg.Add(1)
		go func(i int, id string) {
			defer wg.Done()
			hctx, cancel := context.WithTimeout(r.Context(), hostFetchTimeout)
			defer cancel()
			n := 0
			if obs, err := u.cfg.Svc.ListAllInstances(hctx, id); err == nil {
				n = len(obs)
			}
			summaries[i] = hostSummary{ID: id, Instances: n}
		}(i, h.ID)
	}
	wg.Wait()

	totalInstances := 0
	for _, s := range summaries {
		totalInstances += s.Instances
	}
	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].ID < summaries[j].ID
	})
	u.render(w, r, http.StatusOK, "dashboard", u.pageData(map[string]any{
		"HostCount":      len(summaries),
		"TotalInstances": totalInstances,
		"HostSummaries":  summaries,
	}))
}

// hostInstancesData builds the view model shared by the full host page and its
// pollable fragment. It never returns a hard error for an unreachable host —
// stale data (or an empty "unreachable" panel) is surfaced instead, per the
// warm-cache serve-last-known-good contract. A genuinely unknown host still
// errors so the caller can 404.
func (u *UI) hostInstancesData(ctx context.Context, host string) (map[string]any, error) {
	// Bound the fetch: a warm host returns instantly, but a cold host that has
	// never succeeded is not cached in warm mode, so this would otherwise be an
	// unbounded live podman sweep on every page load and every 10s fragment poll.
	ctx, cancel := context.WithTimeout(ctx, hostFetchTimeout)
	defer cancel()
	obs, fresh, err := u.cfg.Svc.ListAllInstancesWithMeta(ctx, host)
	if err != nil && errors.Is(err, instance.ErrUnknownHost) {
		return nil, err
	}
	return map[string]any{
		"Host":        host,
		"ActiveHost":  host,
		"Instances":   obs,
		"AgeSeconds":  int(fresh.Age().Seconds()),
		"Unreachable": !fresh.Reachable,
		"Cold":        !fresh.HasData,
	}, nil
}

func (u *UI) hostInstances(w http.ResponseWriter, r *http.Request) {
	data, err := u.hostInstancesData(r.Context(), r.PathValue("host"))
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	u.render(w, r, http.StatusOK, "host-instances", u.pageData(data))
}

// hostInstancesFragment renders just the instance table + freshness cue for the
// htmx poll on the host page. render() returns a bare block for HX requests.
func (u *UI) hostInstancesFragment(w http.ResponseWriter, r *http.Request) {
	data, err := u.hostInstancesData(r.Context(), r.PathValue("host"))
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	u.render(w, r, http.StatusOK, "host-instances-body", u.pageData(data))
}

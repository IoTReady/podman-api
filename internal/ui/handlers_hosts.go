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

// dashboardHostTimeout bounds each per-host instance fetch on the dashboard
// fan-out so one cold or unreachable host can't stall the whole page (warm
// reads return instantly; this only bites on a cold/dead host). Mirrors the
// JSON /hosts path's 5s per-host bound.
const dashboardHostTimeout = 5 * time.Second

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
			hctx, cancel := context.WithTimeout(r.Context(), dashboardHostTimeout)
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

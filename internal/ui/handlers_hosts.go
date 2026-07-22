package ui

import (
	"net/http"
	"sort"
	"sync"
)

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
			n := 0
			if obs, err := u.cfg.Svc.ListAllInstances(r.Context(), id); err == nil {
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

func (u *UI) hostInstances(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	obs, err := u.cfg.Svc.ListAllInstances(r.Context(), host)
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	u.render(w, r, http.StatusOK, "host-instances", u.pageData(map[string]any{
		"Host":       host,
		"ActiveHost": host,
		"Instances":  obs,
	}))
}

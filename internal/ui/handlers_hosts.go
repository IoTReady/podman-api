package ui

import (
	"net/http"
	"sort"
)

func (u *UI) dashboard(w http.ResponseWriter, r *http.Request) {
	hosts := u.cfg.Svc.Hosts()
	type hostSummary struct {
		ID        string
		Instances int
	}
	summaries := make([]hostSummary, 0, len(hosts))
	totalInstances := 0
	for _, h := range hosts {
		obs, err := u.cfg.Svc.ListAllInstances(r.Context(), h.ID)
		n := 0
		if err == nil {
			n = len(obs)
		}
		totalInstances += n
		summaries = append(summaries, hostSummary{ID: h.ID, Instances: n})
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

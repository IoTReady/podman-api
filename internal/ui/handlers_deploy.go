package ui

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/store"
)

// hostSecretRef is a per-host-referenced secret name plus whether it currently
// exists on the host (display-only; not a form input).
type hostSecretRef struct {
	Name    string
	Present bool
}

// hostExists reports whether host is a configured host.
func (u *UI) hostExists(host string) bool {
	for _, h := range u.cfg.Svc.Hosts() {
		if h.ID == host {
			return true
		}
	}
	return false
}

// sortedTemplates returns the templates ordered by id, so the deploy form's
// <select> is stable across renders (Service.Templates() iterates a map). A
// store error is propagated so the handler can surface it rather than render an
// empty catalog.
func (u *UI) sortedTemplates(ctx context.Context) ([]store.Template, error) {
	tmpls, err := u.cfg.Svc.Templates(ctx)
	if err != nil {
		return nil, err
	}
	slices.SortFunc(tmpls, func(a, b store.Template) int {
		return strings.Compare(a.Meta.ID, b.Meta.ID)
	})
	return tmpls, nil
}

// hostSecretRefs computes the present/absent status of each per-host-referenced
// secret the template declares, checked against the secrets actually on the
// host. A HostSecrets error is treated as "none present" (every ref absent),
// matching the form's display-only intent. The caller supplies the already
// resolved template, so this issues no template query of its own.
func (u *UI) hostSecretRefs(r *http.Request, host string, tmpl store.Template) []hostSecretRef {
	if len(tmpl.Meta.Secrets.PerHostReferenced) == 0 {
		return nil
	}
	present := map[string]bool{}
	if secs, err := u.cfg.Svc.HostSecrets(r.Context(), host); err == nil {
		for _, s := range secs {
			present[s.Name] = true
		}
	}
	var refs []hostSecretRef
	for _, name := range tmpl.Meta.Secrets.PerHostReferenced {
		refs = append(refs, hostSecretRef{Name: name, Present: present[name]})
	}
	return refs
}

// formValues collects param.* and secret.* fields, skipping empties so a blank
// required field surfaces as a validation error rather than an empty value.
func formValues(form map[string][]string) (params map[string]any, secrets map[string]string) {
	params = map[string]any{}
	secrets = map[string]string{}
	for k, vs := range form {
		v := vs[0]
		if v == "" {
			continue
		}
		switch {
		case strings.HasPrefix(k, "param."):
			params[strings.TrimPrefix(k, "param.")] = v
		case strings.HasPrefix(k, "secret."):
			secrets[strings.TrimPrefix(k, "secret.")] = v
		}
	}
	return params, secrets
}

// typedValues collects param.* and secret.* fields verbatim, keyed by full field
// name (e.g. "param.db", "secret.password"), for re-populating the deploy form so
// a template switch or a failed deploy does not discard what the operator typed.
// Unlike formValues (the apply path), it does NOT skip empty values: a key the
// operator submitted empty (a deliberately cleared field) is preserved as empty
// and must not be back-filled with the parameter default (see mergeParamDefaults).
func typedValues(form map[string][]string) map[string]string {
	vals := map[string]string{}
	for k, vs := range form {
		if strings.HasPrefix(k, "param.") || strings.HasPrefix(k, "secret.") {
			vals[k] = vs[0]
		}
	}
	return vals
}

// queryTypedValues collects typed values from a GET query string but drops any
// secret.* keys. The deploy form never links to a secret-bearing URL, and
// reflecting a hand-crafted ?secret.x=… back into the form would round-trip the
// secret through the request line (logs) and the response HTML — the very leak
// the POST switch exists to close (#99). Secrets only ever travel via the POST
// switch body, so the GET path keeps params only.
func queryTypedValues(form map[string][]string) map[string]string {
	vals := typedValues(form)
	for k := range vals {
		if strings.HasPrefix(k, "secret.") {
			delete(vals, k)
		}
	}
	return vals
}

// mergeParamDefaults fills each parameter's one-click default into values, but
// only for keys the request did not submit at all. This resolves the
// typed-value-vs-default precedence server-side: the template can't tell a
// missing key from a typed-empty one (index returns "" for both), so doing it
// here lets a fresh form show defaults while a field the operator cleared
// (submitted empty) stays empty rather than silently reverting to the default.
//
// This is display-only. The apply path treats empty and absent the same: a
// cleared defaulted field is dropped by formValues, then back-filled by
// render.ApplyDefaults in Service.Apply, so deploying it still applies the
// default — the parameter model has no way to express "explicitly empty". The
// form communicates that by advertising the default as the input's placeholder
// (see instance-fields.html). NB: this is the UI's copy of the same
// fill-absent-from-defaults rule implemented in render.ApplyDefaults and
// api/templates.go; keep them consistent.
func mergeParamDefaults(values map[string]string, tmpl store.Template) {
	for _, p := range tmpl.Meta.Parameters {
		if p.Default == nil {
			continue
		}
		key := "param." + p.Name
		if _, ok := values[key]; !ok {
			values[key] = fmt.Sprint(p.Default)
		}
	}
}

// paramPlaceholders builds the placeholder text for each parameter, keyed by
// param name. An explicit ParamDef.Placeholder wins; otherwise a parameter with
// a default advertises it as "default: <value>", communicating that deploying
// the field empty applies that default (see mergeParamDefaults). The default
// hint is gated on the SAME Default != nil check as mergeParamDefaults — not
// template truthiness — so a falsy non-nil default (false, 0) still gets a hint
// (#100). Computing this server-side keeps the value-fill and placeholder halves
// of the policy in one place with one nil-check semantics.
func paramPlaceholders(tmpl store.Template) map[string]string {
	ph := map[string]string{}
	for _, p := range tmpl.Meta.Parameters {
		switch {
		case p.Placeholder != "":
			ph[p.Name] = p.Placeholder
		case p.Default != nil:
			ph[p.Name] = fmt.Sprintf("default: %v", p.Default)
		}
	}
	return ph
}

// portParamFloor is the minimum host port the suggestion algorithm starts from.
const portParamFloor = 30000

// hasPortParam reports whether the template declares a parameter named "port"
// with type "int", which the deploy form treats as a publishable host port.
func hasPortParam(tmpl store.Template) bool {
	for _, p := range tmpl.Meta.Parameters {
		if p.Name == "port" && p.Type == "int" {
			return true
		}
	}
	return false
}

// suggestPortFrom returns the first free port at or above the max in-use port
// (with a floor of portParamFloor), scanning upward by up to scanLimit ports.
// Returns 0 when no free port is found in the scan window.
func suggestPortFrom(ports []podman.PortMapping) int {
	busy := map[int]bool{}
	maxPort := 0
	for _, p := range ports {
		busy[p.HostPort] = true
		if p.HostPort > maxPort {
			maxPort = p.HostPort
		}
	}
	start := maxPort + 1
	if start < portParamFloor {
		start = portParamFloor
	}
	const scanLimit = 1000
	for port := start; port < start+scanLimit; port++ {
		if !busy[port] {
			return port
		}
	}
	log.Printf("port scan exhausted: no free port in [%d, %d)", start, start+scanLimit)
	return 0
}

// portsInUseStr builds a compact display string for a slice of port mappings,
// e.g. "31000, 31002". Returns "" when the slice is empty.
func portsInUseStr(ports []podman.PortMapping) string {
	if len(ports) == 0 {
		return ""
	}
	parts := make([]string, 0, len(ports))
	for _, p := range ports {
		parts = append(parts, strconv.Itoa(p.HostPort))
	}
	return strings.Join(parts, ", ")
}

// checkPortConflict returns a user-facing error when port is already in use on
// host, or nil when the port is free. The message includes a suggestion when
// one is available.
func (u *UI) checkPortConflict(ctx context.Context, host string, port int) error {
	ports, err := u.cfg.Svc.PortsInUse(ctx, host)
	if err != nil {
		return nil // degraded: let the downstream apply surface the error
	}
	for _, p := range ports {
		if p.HostPort == port {
			msg := fmt.Sprintf("port %d is already in use by %s/%s", port, p.Pod, p.Container)
			if sug := suggestPortFrom(ports); sug > 0 {
				msg += fmt.Sprintf("; suggested free port: %d", sug)
			}
			return errors.New(msg)
		}
	}
	return nil
}

// resolveTemplate looks up a template by id. Returns the zero value when
// not found; the caller should check tmpl.Meta.ID.
func resolveTemplate(ctx context.Context, svc *instance.Service, id string) store.Template {
	tmpls, err := svc.Templates(ctx)
	if err != nil {
		return store.Template{}
	}
	for _, tmpl := range tmpls {
		if tmpl.Meta.ID == id {
			return tmpl
		}
	}
	return store.Template{}
}

// deployFormData assembles the data map the "deploy-form" template needs. vals
// are the raw typed param.*/secret.* values to re-populate; mergeParamDefaults
// is applied here so every caller shares one defaults policy. A store error
// from the template list is returned for the caller to surface via renderError.
// The caller adds an "Error" key when re-rendering after a failed deploy.
//
// defaultFirst selects the first template when selected is empty — wanted on
// the initial GET load (so a fresh form shows a real template) but NOT on the
// POST switch or the failed-deploy re-render, where an empty selection is the
// operator's actual input and must not be silently replaced with the first
// template (and its merged defaults). The template is resolved from the list
// already fetched here, so no second template query is issued.
func (u *UI) deployFormData(r *http.Request, host, selected, slug string, vals map[string]string, defaultFirst bool) (map[string]any, error) {
	tmpls, err := u.sortedTemplates(r.Context())
	if err != nil {
		return nil, err
	}
	if defaultFirst && selected == "" && len(tmpls) > 0 {
		selected = tmpls[0].Meta.ID
	}
	var tmpl store.Template
	for _, t := range tmpls {
		if t.Meta.ID == selected {
			tmpl = t
			break
		}
	}
	refs := u.hostSecretRefs(r, host, tmpl)
	mergeParamDefaults(vals, tmpl)
	var sug int
	var busy string
	if hasPortParam(tmpl) {
		if ports, err := u.cfg.Svc.PortsInUse(r.Context(), host); err == nil {
			sug = suggestPortFrom(ports)
			busy = portsInUseStr(ports)
		}
	}
	return map[string]any{
		"Host":           host,
		"ActiveHost":     host,
		"Templates":      tmpls,
		"Selected":       selected,
		"Tmpl":           tmpl,
		"HostRefs":       refs,
		"Slug":           slug,
		"Values":         vals,
		"Placeholders":   paramPlaceholders(tmpl),
		"PortSuggestion": sug,
		"PortsInUse":     busy,
	}, nil
}

func (u *UI) deployForm(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	if !u.hostExists(host) {
		u.renderError(w, r, instance.ErrUnknownHost)
		return
	}
	q := r.URL.Query()
	data, err := u.deployFormData(r, host, q.Get("template"), q.Get("slug"), queryTypedValues(q), true)
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	u.render(w, r, http.StatusOK, "deploy-form", u.pageData(data))
}

// deployFormPost re-renders the deploy form for a newly selected template. The
// template <select> POSTs here (rather than GETs the deploy route) so typed
// per-instance secrets travel in the request body, not the URL (#99). It mirrors
// deployForm but reads the selected template, slug, and typed values from the
// POST body.
func (u *UI) deployFormPost(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	if !u.hostExists(host) {
		u.renderError(w, r, instance.ErrUnknownHost)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form", http.StatusBadRequest)
		return
	}
	data, err := u.deployFormData(r, host, r.PostFormValue("template"), r.PostFormValue("slug"), typedValues(r.PostForm), false)
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	u.render(w, r, http.StatusOK, "deploy-form", u.pageData(data))
}

func (u *UI) deployCreate(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form", http.StatusBadRequest)
		return
	}
	params, secrets := formValues(r.PostForm)
	tmpl := resolveTemplate(r.Context(), u.cfg.Svc, r.FormValue("template"))

	// Port conflict pre-check: if the template has a port param and the
	// operator submitted one, verify it isn't already bound on the host.
	if hasPortParam(tmpl) {
		if portStr, ok := params["port"]; ok {
			port, atoiErr := strconv.Atoi(fmt.Sprint(portStr))
			if atoiErr == nil {
				if conflictErr := u.checkPortConflict(r.Context(), host, port); conflictErr != nil {
					data, derr := u.deployFormData(r, host, tmpl.Meta.ID, r.FormValue("slug"), typedValues(r.PostForm), false)
					if derr != nil {
						u.renderError(w, r, derr)
						return
					}
					data["Error"] = conflictErr.Error()
					u.render(w, r, http.StatusConflict, "deploy-form", u.pageData(data))
					return
				}
			}
		}
	}

	req := instance.ApplyRequest{
		Template:   r.FormValue("template"),
		Slug:       r.FormValue("slug"),
		Parameters: params,
		Secrets:    secrets,
	}
	obs, applyErr := u.cfg.Svc.ApplyAndObserve(r.Context(), host, req, instance.ApplyOptions{Replace: false})
	if applyErr != nil {
		data, derr := u.deployFormData(r, host, req.Template, req.Slug, typedValues(r.PostForm), false)
		if derr != nil {
			u.renderError(w, r, derr)
			return
		}
		data["Error"] = applyErr.Error()
		u.render(w, r, errorStatus(applyErr), "deploy-form", u.pageData(data))
		return
	}
	data := u.instanceView(r.Context(), host, obs)
	if len(obs.Warnings) > 0 {
		data["Notice"] = strings.Join(obs.Warnings, "; ")
	}
	u.render(w, r, http.StatusOK, "instance-detail", u.pageData(data))
}

// upgradeForm renders the image-only upgrade form. The upgrade reuses the
// instance's stored parameters and secrets (the operator supplies only a new
// image). The desired-state store is always present, so upgrade is always
// available.
func (u *UI) upgradeForm(w http.ResponseWriter, r *http.Request) {
	host, tmplID, slug := r.PathValue("host"), r.PathValue("template"), r.PathValue("slug")
	obs, err := u.cfg.Svc.Get(r.Context(), host, tmplID, slug)
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	u.render(w, r, http.StatusOK, "upgrade-form", u.pageData(map[string]any{
		"Host":         host,
		"ActiveHost":   host,
		"Template":     tmplID,
		"Slug":         slug,
		"CurrentImage": firstContainerImage(obs),
	}))
}

func (u *UI) upgradeApply(w http.ResponseWriter, r *http.Request) {
	host, tmplID, slug := r.PathValue("host"), r.PathValue("template"), r.PathValue("slug")
	image := strings.TrimSpace(r.FormValue("image"))
	if image == "" {
		obs, _ := u.cfg.Svc.Get(r.Context(), host, tmplID, slug)
		u.render(w, r, http.StatusBadRequest, "upgrade-form", u.pageData(map[string]any{
			"Host": host, "ActiveHost": host, "Template": tmplID, "Slug": slug,
			"CurrentImage": firstContainerImage(obs),
			"Error":        "image is required",
		}))
		return
	}
	if err := u.cfg.Svc.UpgradeImage(r.Context(), host, tmplID, slug, image); err != nil {
		obs, _ := u.cfg.Svc.Get(r.Context(), host, tmplID, slug)
		u.render(w, r, errorStatus(err), "upgrade-form", u.pageData(map[string]any{
			"Host": host, "ActiveHost": host, "Template": tmplID, "Slug": slug,
			"CurrentImage": firstContainerImage(obs),
			"Error":        err.Error(),
		}))
		return
	}
	obs, err := u.cfg.Svc.Get(r.Context(), host, tmplID, slug)
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	u.render(w, r, http.StatusOK, "instance-detail", u.pageData(u.instanceView(r.Context(), host, obs)))
}

// editForm renders the deploy form pre-populated with the instance's stored
// parameters, for editing and re-applying an existing instance. Secrets are
// shown as placeholders ("unchanged") — the actual values are never sent to the
// client. The template selector is disabled since the template is fixed.
func (u *UI) editForm(w http.ResponseWriter, r *http.Request) {
	host, tmplID, slug := r.PathValue("host"), r.PathValue("template"), r.PathValue("slug")
	if !u.hostExists(host) {
		u.renderError(w, r, instance.ErrUnknownHost)
		return
	}
	// Verify the instance exists and load its stored spec.
	obs, err := u.cfg.Svc.Get(r.Context(), host, tmplID, slug)
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	spec, err := u.cfg.Svc.StoredSpec(r.Context(), host, tmplID, slug)
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	// Build typed values from stored params. Secrets are set to empty so the
	// template shows "unchanged" placeholders (see instance-fields.html).
	vals := map[string]string{}
	for k, v := range spec.Parameters {
		vals["param."+k] = fmt.Sprint(v)
	}
	for name := range spec.Secrets {
		vals["secret."+name] = ""
	}
	data, err := u.deployFormData(r, host, tmplID, slug, vals, false)
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	data["EditMode"] = true
	// Pass the observed instance so the edit form can show current state.
	data["Inst"] = obs
	u.render(w, r, http.StatusOK, "deploy-form", u.pageData(data))
}

// editApply re-applies an existing instance with updated parameters (Replace:
// true). Secrets not touched by the operator carry over from the stored spec.
func (u *UI) editApply(w http.ResponseWriter, r *http.Request) {
	host, tmplID, slug := r.PathValue("host"), r.PathValue("template"), r.PathValue("slug")
	if !u.hostExists(host) {
		u.renderError(w, r, instance.ErrUnknownHost)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form", http.StatusBadRequest)
		return
	}
	// Load the existing spec so we can preserve secrets the form didn't touch.
	spec, err := u.cfg.Svc.StoredSpec(r.Context(), host, tmplID, slug)
	if err != nil {
		u.renderError(w, r, err)
		return
	}
	params, secrets := formValues(r.PostForm)
	// Preserve existing secrets that the form did not override. An untouched
	// secret field submits empty (the template renders no value attribute in
	// edit mode), formValues drops it, and this loop carries the stored value
	// over — no sentinel string needed.
	if spec.Secrets != nil {
		for k, v := range spec.Secrets {
			if _, touched := secrets[k]; !touched {
				secrets[k] = v
			}
		}
	}
	req := instance.ApplyRequest{
		Template:   tmplID,
		Slug:       slug,
		Parameters: params,
		Secrets:    secrets,
		Domains:    spec.Domains,
	}
	obs, applyErr := u.cfg.Svc.ApplyAndObserve(r.Context(), host, req, instance.ApplyOptions{Replace: true})
	if applyErr != nil {
		// Re-render the form with the operator's submitted values (not the
		// merged secrets), so stored secret values never reach the client.
		// typedValues reads the raw POST body, preserving cleared params and
		// untouched secrets as empty — the template shows "unchanged"
		// placeholders for those.
		edata, derr := u.deployFormData(r, host, tmplID, slug, typedValues(r.PostForm), false)
		if derr != nil {
			u.renderError(w, r, derr)
			return
		}
		edata["EditMode"] = true
		edata["Error"] = applyErr.Error()
		// Load the current observed state for the edit-mode form header.
		if obs, err := u.cfg.Svc.Get(r.Context(), host, tmplID, slug); err == nil {
			edata["Inst"] = obs
		}
		u.render(w, r, errorStatus(applyErr), "deploy-form", u.pageData(edata))
		return
	}
	data := u.instanceView(r.Context(), host, obs)
	if len(obs.Warnings) > 0 {
		data["Notice"] = strings.Join(obs.Warnings, "; ")
	}
	u.render(w, r, http.StatusOK, "instance-detail", u.pageData(data))
}

// firstContainerImage returns the first container's image, for prefilling the
// upgrade form; "" when the instance has no observed containers.
func firstContainerImage(obs instance.Observed) string {
	if len(obs.Containers) > 0 {
		return obs.Containers[0].Image
	}
	return ""
}

package api

import (
	"encoding/json"
	"net/http"

	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

// templateJSON is the wire representation of a stored template. The keys are
// lowercase and stable so the CMS/admin UI can rely on them.
func templateJSON(t store.Template) map[string]any {
	return map[string]any{
		"id":         t.Meta.ID,
		"display":    t.Meta.Display,
		"parameters": t.Meta.Parameters,
		"secrets":    t.Meta.Secrets,
		"volumes":    t.Meta.Volumes,
		"ingress":    t.Meta.Ingress,
		"body":       t.Body,
		"origin":     t.Origin,
		"created":    t.Created,
		"updated":    t.Updated,
	}
}

// templateBody is the decoded request body shared by create/update. The id is
// only honoured on create; update forces it to the path id.
type templateBody struct {
	ID         string            `json:"id"`
	Body       string            `json:"body"`
	Display    render.Display    `json:"display"`
	Parameters []render.ParamDef `json:"parameters"`
	Secrets    render.Secrets    `json:"secrets"`
	Volumes    []render.Volume   `json:"volumes"`
	Ingress    *render.Ingress   `json:"ingress"`
}

// toTemplate builds a store.Template from the decoded body, forcing Meta.ID to id.
func (b templateBody) toTemplate(id string) store.Template {
	return store.Template{
		Meta: render.Meta{
			ID:         id,
			Display:    b.Display,
			Parameters: b.Parameters,
			Secrets:    b.Secrets,
			Volumes:    b.Volumes,
			Ingress:    b.Ingress,
		},
		Body: b.Body,
	}
}

func (h *handlers) listTemplates(w http.ResponseWriter, _ *http.Request) {
	tmpls := h.svc.Templates()
	out := make([]map[string]any, 0, len(tmpls))
	for _, t := range tmpls {
		out = append(out, templateJSON(t))
	}
	WriteJSON(w, http.StatusOK, out)
}

func (h *handlers) getTemplate(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !validName(id) {
		writeInvalidName(w, "template", id)
		return
	}
	t, err := h.svc.GetTemplate(r.Context(), id)
	if err != nil {
		WriteError(w, err)
		return
	}
	WriteJSON(w, http.StatusOK, templateJSON(t))
}

func (h *handlers) createTemplate(w http.ResponseWriter, r *http.Request) {
	var b templateBody
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: err.Error()})
		return
	}
	if !validName(b.ID) {
		writeInvalidName(w, "template", b.ID)
		return
	}
	if err := h.svc.CreateTemplate(r.Context(), b.toTemplate(b.ID)); err != nil {
		WriteError(w, err)
		return
	}
	t, err := h.svc.GetTemplate(r.Context(), b.ID)
	if err != nil {
		WriteError(w, err)
		return
	}
	WriteJSON(w, http.StatusCreated, templateJSON(t))
}

func (h *handlers) updateTemplate(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !validName(id) {
		writeInvalidName(w, "template", id)
		return
	}
	var b templateBody
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: err.Error()})
		return
	}
	if err := h.svc.UpdateTemplate(r.Context(), b.toTemplate(id)); err != nil {
		WriteError(w, err)
		return
	}
	t, err := h.svc.GetTemplate(r.Context(), id)
	if err != nil {
		WriteError(w, err)
		return
	}
	WriteJSON(w, http.StatusOK, templateJSON(t))
}

func (h *handlers) cloneTemplate(w http.ResponseWriter, r *http.Request) {
	src := r.PathValue("id")
	if !validName(src) {
		writeInvalidName(w, "template", src)
		return
	}
	var b struct {
		NewID string `json:"new_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: err.Error()})
		return
	}
	if !validName(b.NewID) {
		writeInvalidName(w, "new_id", b.NewID)
		return
	}
	cl, err := h.svc.CloneTemplate(r.Context(), src, b.NewID)
	if err != nil {
		WriteError(w, err)
		return
	}
	WriteJSON(w, http.StatusCreated, templateJSON(cl))
}

func (h *handlers) deleteTemplate(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !validName(id) {
		writeInvalidName(w, "template", id)
		return
	}
	if err := h.svc.DeleteTemplate(r.Context(), id, queryBool(r, "force")); err != nil {
		WriteError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *handlers) renderTemplate(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !validName(id) {
		writeInvalidName(w, "template", id)
		return
	}
	tmpl, err := h.svc.GetTemplate(r.Context(), id)
	if err != nil {
		WriteError(w, err)
		return
	}
	params := map[string]any{}
	for k, v := range r.URL.Query() {
		if len(v) > 0 {
			params[k] = v[0]
		}
	}
	out, err := render.RenderBody(tmpl.Body, params)
	if err != nil {
		WriteError(w, err)
		return
	}
	w.Header().Set("Content-Type", "text/yaml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(out))
}

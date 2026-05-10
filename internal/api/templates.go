package api

import (
	"net/http"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/render"
)

func (h *handlers) listTemplates(w http.ResponseWriter, _ *http.Request) {
	tmpls := h.svc.Templates()
	out := make([]map[string]any, 0, len(tmpls))
	for _, t := range tmpls {
		out = append(out, templateView(t))
	}
	WriteJSON(w, http.StatusOK, out)
}

func (h *handlers) getTemplate(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !validName(id) {
		writeInvalidName(w, "template", id)
		return
	}
	for _, t := range h.svc.Templates() {
		if t.Meta.ID == id {
			WriteJSON(w, http.StatusOK, templateView(t))
			return
		}
	}
	WriteError(w, instance.ErrUnknownTemplate)
}

func (h *handlers) renderTemplate(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !validName(id) {
		writeInvalidName(w, "template", id)
		return
	}
	var tmpl *config.Template
	for _, t := range h.svc.Templates() {
		if t.Meta.ID == id {
			tt := t
			tmpl = &tt
			break
		}
	}
	if tmpl == nil {
		WriteError(w, instance.ErrUnknownTemplate)
		return
	}
	params := map[string]any{}
	for k, v := range r.URL.Query() {
		if len(v) > 0 {
			params[k] = v[0]
		}
	}
	out, err := render.Render(rebuildSource(*tmpl), params)
	if err != nil {
		WriteError(w, err)
		return
	}
	w.Header().Set("Content-Type", "text/yaml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(out))
}

// rebuildSource reconstructs a complete template source from a parsed Template.
// Render needs the full source because ParseMeta strips the meta block before
// handing the body to text/template.
func rebuildSource(t config.Template) string {
	return "# template-meta:\n#   id: " + t.Meta.ID + "\n#   parameters:\n#     required: []\n---\n" + t.Body
}

func templateView(t config.Template) map[string]any {
	return map[string]any{
		"id":         t.Meta.ID,
		"parameters": t.Meta.Parameters,
		"secrets":    t.Meta.Secrets,
		"volumes":    t.Meta.Volumes,
	}
}

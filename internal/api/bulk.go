package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/iotready/podman-api/internal/instance"
)

// maxBulkOps caps a single bulk request to keep latency bounded and to
// avoid unbounded memory use on large payloads. The CMS should chunk
// larger fleets into multiple requests.
const maxBulkOps = 100

// bulkOp is one element of POST /hosts/{h}/bulk.
type bulkOp struct {
	Action   string `json:"action"`             // start | stop | restart | delete
	Template string `json:"template"`
	Slug     string `json:"slug"`
	// PruneVolumes / PruneSecrets are honoured only by action=delete.
	PruneVolumes bool `json:"prune_volumes,omitempty"`
	PruneSecrets bool `json:"prune_secrets,omitempty"`
}

type bulkRequest struct {
	Ops []bulkOp `json:"ops"`
}

type bulkResult struct {
	// Index mirrors the position in the request body so callers can
	// correlate even when ops omit identifying fields by accident.
	Index    int    `json:"index"`
	Action   string `json:"action"`
	Template string `json:"template"`
	Slug     string `json:"slug"`
	Status   int    `json:"status"`
	Code     string `json:"code,omitempty"`
	Message  string `json:"message,omitempty"`
}

func (h *handlers) bulk(w http.ResponseWriter, r *http.Request) {
	host := r.PathValue("host")

	var req bulkRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: err.Error()})
		return
	}
	if len(req.Ops) == 0 {
		WriteJSON(w, http.StatusBadRequest, ErrorBody{Code: "invalid_body", Message: "ops is required and must be non-empty"})
		return
	}
	if len(req.Ops) > maxBulkOps {
		WriteJSON(w, http.StatusRequestEntityTooLarge, ErrorBody{
			Code:    "too_many_ops",
			Message: "bulk request exceeds per-request cap",
		})
		return
	}

	results := make([]bulkResult, len(req.Ops))
	for i, op := range req.Ops {
		results[i] = h.runBulkOp(r, host, i, op)
	}

	WriteJSON(w, http.StatusOK, map[string]any{"results": results})
}

func (h *handlers) runBulkOp(r *http.Request, host string, idx int, op bulkOp) bulkResult {
	res := bulkResult{
		Index: idx, Action: op.Action,
		Template: op.Template, Slug: op.Slug,
	}

	if !validName(op.Template) {
		res.Status, res.Code, res.Message = http.StatusBadRequest, "invalid_parameters", "invalid template name"
		return res
	}
	if !validName(op.Slug) {
		res.Status, res.Code, res.Message = http.StatusBadRequest, "invalid_parameters", "invalid slug"
		return res
	}

	var err error
	switch op.Action {
	case "start":
		err = h.svc.Start(r.Context(), host, op.Template, op.Slug)
	case "stop":
		err = h.svc.Stop(r.Context(), host, op.Template, op.Slug)
	case "restart":
		err = h.svc.Restart(r.Context(), host, op.Template, op.Slug)
	case "delete":
		err = h.svc.Delete(r.Context(), host, op.Template, op.Slug, instance.DeleteOptions{
			PruneVolumes: op.PruneVolumes,
			PruneSecrets: op.PruneSecrets,
		})
	default:
		res.Status, res.Code, res.Message = http.StatusBadRequest, "invalid_action", "action must be one of: start, stop, restart, delete"
		return res
	}

	if err == nil {
		res.Status = http.StatusNoContent
		return res
	}
	code, status, msg := bulkClassify(err)
	res.Status, res.Code, res.Message = status, code, msg
	return res
}

// bulkClassify mirrors the canonical classify() in errors.go but is local
// to this file to keep the per-op result independent of the top-level
// response classification (we never short-circuit the batch).
func bulkClassify(err error) (string, int, string) {
	switch {
	case errors.Is(err, instance.ErrUnknownHost):
		return "unknown_host", http.StatusNotFound, err.Error()
	case errors.Is(err, instance.ErrUnknownTemplate):
		return "unknown_template", http.StatusNotFound, err.Error()
	case errors.Is(err, instance.ErrInstanceNotFound):
		return "instance_not_found", http.StatusNotFound, err.Error()
	case errors.Is(err, instance.ErrHostDraining):
		return "host_draining", http.StatusLocked, err.Error()
	default:
		return "internal", http.StatusInternalServerError, err.Error()
	}
}

// Package api wires HTTP routes to the instance.Service.
package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

// ErrorBody is the JSON shape of every error response.
type ErrorBody struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

// WriteJSON writes v as JSON with the given status.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// WriteError translates an error into a JSON error response. Sentinel
// errors from the instance package map to known codes; anything else
// falls through to "internal".
func WriteError(w http.ResponseWriter, err error) {
	code, status, msg := classify(err)
	WriteJSON(w, status, ErrorBody{Code: code, Message: msg})
}

// WriteErrorWithDetails is like WriteError but lets handlers attach
// structured detail (e.g. host/template/slug).
func WriteErrorWithDetails(w http.ResponseWriter, err error, details map[string]any) {
	code, status, msg := classify(err)
	WriteJSON(w, status, ErrorBody{Code: code, Message: msg, Details: details})
}

func classify(err error) (code string, status int, msg string) {
	switch {
	case errors.Is(err, instance.ErrUnknownHost):
		return "unknown_host", http.StatusNotFound, err.Error()
	case errors.Is(err, instance.ErrUnknownTemplate):
		return "unknown_template", http.StatusNotFound, err.Error()
	case errors.Is(err, instance.ErrInstanceNotFound):
		return "instance_not_found", http.StatusNotFound, err.Error()
	case errors.Is(err, instance.ErrInstanceExists):
		return "instance_already_exists", http.StatusConflict, err.Error()
	case errors.Is(err, instance.ErrTemplateExists):
		return "template_already_exists", http.StatusConflict, err.Error()
	case errors.Is(err, instance.ErrTemplateInUse):
		return "template_in_use", http.StatusConflict, err.Error()
	case errors.Is(err, instance.ErrInvalidTemplate):
		return "invalid_template", http.StatusBadRequest, err.Error()
	case errors.Is(err, instance.ErrHostSecretMissing):
		return "host_secret_missing", http.StatusUnprocessableEntity, err.Error()
	case errors.Is(err, instance.ErrImagePull):
		return "upstream_error", http.StatusBadGateway, err.Error()
	case errors.Is(err, instance.ErrHostDraining):
		return "host_draining", http.StatusLocked, err.Error()
	case errors.Is(err, podman.ErrNotFound):
		return "instance_not_found", http.StatusNotFound, err.Error()
	case errors.Is(err, podman.ErrHostVersionUnsupported):
		return "host_version_unsupported", http.StatusUnprocessableEntity, err.Error()
	case errors.Is(err, instance.ErrBackupNotFound):
		return "backup_not_found", http.StatusNotFound, err.Error()
	case errors.Is(err, instance.ErrBackupNotRestorable):
		return "backup_not_restorable", http.StatusUnprocessableEntity, err.Error()
	case errors.Is(err, instance.ErrBackupBusy):
		return "backup_busy", http.StatusConflict, err.Error()
	case errors.Is(err, instance.ErrBackupsDisabled):
		return "not_implemented", http.StatusNotImplemented, err.Error()
	case errors.Is(err, render.ErrInvalidParameters),
		errors.Is(err, render.ErrRenderInvalid):
		return "invalid_parameters", http.StatusBadRequest, err.Error()
	case errors.Is(err, errJobsDisabled):
		return "not_implemented", http.StatusNotImplemented, err.Error()
	// Defensive: these surface for direct service callers / job-error
	// classification; the synchronous POST /migrate path can't return them
	// (port-conflict is checked inside the job; the jobs-nil guard precedes
	// the store-disabled check).
	case errors.Is(err, instance.ErrStoreDisabled):
		return "not_implemented", http.StatusNotImplemented, err.Error()
	case errors.Is(err, store.ErrNotFound):
		return "not_found", http.StatusNotFound, err.Error()
	case errors.Is(err, store.ErrSecretsNeedKey):
		return "secrets_need_key", http.StatusBadRequest, "secrets require an encryption key (-spec-key-file)"
	case errors.Is(err, store.ErrSecretsUndecryptable):
		return "secrets_undecryptable", http.StatusUnprocessableEntity, err.Error()
	case errors.Is(err, store.ErrSpecCorrupt):
		return "spec_corrupt", http.StatusUnprocessableEntity, err.Error()
	case errors.Is(err, instance.ErrPortConflict):
		return "port_conflict", http.StatusConflict, err.Error()
	case errors.Is(err, instance.ErrSameHost):
		return "invalid_request", http.StatusBadRequest, err.Error()
	case errors.Is(err, instance.ErrInvalidEvacuation):
		return "invalid_request", http.StatusBadRequest, err.Error()
	default:
		return "internal", http.StatusInternalServerError, err.Error()
	}
}

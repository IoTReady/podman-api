// Package api wires HTTP routes to the instance.Service.
package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/render"
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
	case errors.Is(err, instance.ErrHostSecretMissing):
		return "host_secret_missing", http.StatusUnprocessableEntity, err.Error()
	case errors.Is(err, podman.ErrNotFound):
		return "instance_not_found", http.StatusNotFound, err.Error()
	case errors.Is(err, render.ErrInvalidParameters):
		return "invalid_parameters", http.StatusBadRequest, err.Error()
	default:
		return "internal", http.StatusInternalServerError, err.Error()
	}
}

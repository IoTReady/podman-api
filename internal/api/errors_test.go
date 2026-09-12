package api

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

func TestWriteError_KnownSentinels(t *testing.T) {
	cases := []struct {
		err  error
		code string
		stat int
	}{
		{instance.ErrUnknownHost, "unknown_host", http.StatusNotFound},
		{instance.ErrUnknownTemplate, "unknown_template", http.StatusNotFound},
		{instance.ErrInstanceNotFound, "instance_not_found", http.StatusNotFound},
		{instance.ErrInstanceExists, "instance_already_exists", http.StatusConflict},
		{instance.ErrHostSecretMissing, "host_secret_missing", http.StatusUnprocessableEntity},
		{render.ErrInvalidParameters, "invalid_parameters", http.StatusBadRequest},
		{store.ErrSecretsNeedKey, "secrets_need_key", http.StatusBadRequest},
		{instance.ErrIngressReconcileFailed, "ingress_reconcile_failed", http.StatusBadGateway},
		{fmt.Errorf("%w: %v", instance.ErrIngressReconcileFailed, errors.New("ingress: admin PUT server: status 500: boom")), "ingress_reconcile_failed", http.StatusBadGateway},
		{errors.New("anything else"), "internal", http.StatusInternalServerError},
	}
	for _, c := range cases {
		rr := httptest.NewRecorder()
		WriteError(rr, c.err)
		assert.Equal(t, c.stat, rr.Code, c.code)
		assert.Contains(t, rr.Body.String(), `"code":"`+c.code+`"`)
		assert.Equal(t, "application/json", rr.Header().Get("Content-Type"))
	}
}

// The underlying Caddy admin-API error must reach the response body, not just
// an opaque "internal" message — the whole point of issue #301's ask 1 is
// that a caller can see the pod is fine and the problem is in Caddy.
func TestWriteError_IngressReconcileFailedSurfacesUnderlyingError(t *testing.T) {
	err := fmt.Errorf("%w: %v", instance.ErrIngressReconcileFailed,
		errors.New("ingress: admin PUT server: status 500: {\"error\":\"loading new config: listener address repeated: tcp/:443\"}"))
	rr := httptest.NewRecorder()
	WriteError(rr, err)
	assert.Equal(t, http.StatusBadGateway, rr.Code)
	assert.Contains(t, rr.Body.String(), `"code":"ingress_reconcile_failed"`)
	assert.Contains(t, rr.Body.String(), "listener address repeated")
}

func TestWriteJSON(t *testing.T) {
	rr := httptest.NewRecorder()
	WriteJSON(rr, http.StatusCreated, map[string]string{"hello": "world"})
	assert.Equal(t, http.StatusCreated, rr.Code)
	assert.Equal(t, "application/json", rr.Header().Get("Content-Type"))
	assert.Contains(t, rr.Body.String(), `"hello":"world"`)
}

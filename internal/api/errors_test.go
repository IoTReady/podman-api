package api

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/render"
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

func TestWriteJSON(t *testing.T) {
	rr := httptest.NewRecorder()
	WriteJSON(rr, http.StatusCreated, map[string]string{"hello": "world"})
	assert.Equal(t, http.StatusCreated, rr.Code)
	assert.Equal(t, "application/json", rr.Header().Get("Content-Type"))
	assert.Contains(t, rr.Body.String(), `"hello":"world"`)
}

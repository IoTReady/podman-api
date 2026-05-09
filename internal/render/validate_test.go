package render

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func meta() Meta {
	return Meta{
		ID: "x",
		Parameters: Parameters{
			Required: []string{"slug", "image"},
			Optional: []string{"port"},
		},
		Secrets: Secrets{
			PerInstance:       []string{"auth_secret"},
			PerHostReferenced: []string{"s3-access-key-id"},
		},
	}
}

func TestValidate_Happy(t *testing.T) {
	err := Validate(meta(),
		map[string]any{"slug": "a", "image": "x", "port": 1},
		map[string]string{"auth_secret": "v"},
	)
	require.NoError(t, err)
}

func TestValidate_MissingRequiredParam(t *testing.T) {
	err := Validate(meta(),
		map[string]any{"slug": "a"},
		map[string]string{"auth_secret": "v"},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "image")
}

func TestValidate_UnknownParam(t *testing.T) {
	err := Validate(meta(),
		map[string]any{"slug": "a", "image": "x", "extra": "no"},
		map[string]string{"auth_secret": "v"},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "extra")
}

func TestValidate_MissingPerInstanceSecret(t *testing.T) {
	err := Validate(meta(),
		map[string]any{"slug": "a", "image": "x"},
		map[string]string{},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "auth_secret")
}

func TestValidate_UnknownSecret(t *testing.T) {
	err := Validate(meta(),
		map[string]any{"slug": "a", "image": "x"},
		map[string]string{"auth_secret": "v", "extra": "no"},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "extra")
}

package render

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func meta() Meta {
	return Meta{
		ID: "x",
		Parameters: []ParamDef{
			{Name: "slug", Type: "string", Required: true},
			{Name: "image", Type: "string", Required: true},
			{Name: "port", Type: "int"},
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

func TestValidate_TypedRequiredAndUnknown(t *testing.T) {
	m := Meta{Parameters: []ParamDef{
		{Name: "image", Type: "string", Required: true},
		{Name: "port", Type: "int"},
	}}
	err := Validate(m, map[string]any{}, nil)
	require.ErrorIs(t, err, ErrInvalidParameters)
	require.Contains(t, err.Error(), `missing required parameter "image"`)
	err = Validate(m, map[string]any{"image": "x", "bogus": 1}, nil)
	require.Contains(t, err.Error(), `unknown parameter "bogus"`)
	require.NoError(t, Validate(m, map[string]any{"image": "x"}, nil))
}

func TestApplyDefaults_FillsOmitted(t *testing.T) {
	m := Meta{Parameters: []ParamDef{
		{Name: "image", Type: "string", Required: true},
		{Name: "port", Type: "int", Default: 8080},
	}}
	eff := ApplyDefaults(m, map[string]any{"image": "nginx:1"})
	require.Equal(t, "nginx:1", eff["image"])
	require.EqualValues(t, 8080, eff["port"])
	eff = ApplyDefaults(m, map[string]any{"image": "x", "port": 9090})
	require.EqualValues(t, 9090, eff["port"])
}

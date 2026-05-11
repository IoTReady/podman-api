// Package apispec exposes the openapi.yaml spec embedded in the binary so it
// can be served at /openapi.yaml without needing the file at runtime.
package apispec

import _ "embed"

//go:embed openapi.yaml
var Spec []byte

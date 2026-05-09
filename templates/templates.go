// Package templates exposes the YAML pod-spec templates shipped with the binary.
package templates

import "embed"

//go:embed *.yaml
var Files embed.FS

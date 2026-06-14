package server

import "github.com/iotready/podman-api/extension"

// WithSidecarInjector registers a SidecarInjector that is called to inject
// sidecar containers into rendered pod YAML before it is applied.
func WithSidecarInjector(si extension.SidecarInjector) Option {
	return func(c *cfg) { c.sidecarInjector = si }
}

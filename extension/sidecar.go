package extension

import "context"

// SidecarInjector is a commercial extension point that injects sidecar
// containers into an instance's pod YAML after the template body has been
// rendered but before it is applied.
//
// The implementation receives the rendered pod YAML, the template metadata,
// the resolved template parameters, and the instance slug. It returns the
// (possibly modified) YAML. Return the input unchanged to pass through.
type SidecarInjector interface {
	InjectSidecars(ctx context.Context, renderedYAML string, meta any, params map[string]any, slug string) (string, error)
}

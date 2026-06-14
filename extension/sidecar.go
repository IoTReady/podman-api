package extension

import "context"

// TemplateMeta is a read-only projection of a template's metadata, handed to a
// SidecarInjector. It is a public type so the commercial module can consume it:
// the internal render.Meta cannot cross the module boundary (it lives under
// internal/), so the OSS core projects the fields an injector needs into this
// stable struct. New fields are added here as injectors come to need them.
type TemplateMeta struct {
	// ID is the template identifier (render.Meta.ID).
	ID string
	// Volumes lists the template's declared volumes and their backup targets.
	// The Backup marker is meta-only — it is not present in the rendered pod
	// YAML — so a backup/PITR sidecar needs it from here.
	Volumes []TemplateVolume
}

// TemplateVolume is one volume declared by a template.
type TemplateVolume struct {
	Name   string // volume name as referenced in the pod spec
	Backup string // backup target/identifier; empty when not marked for backup
}

// InjectedSecret is a secret declared by a SidecarInjector. The core creates
// it as a podman secret (using the same SecretCreate / wrapAsKubeSecret path
// used for template-declared secrets) before PlayKube and reaps it on instance
// Delete, so the injected sidecar can reference it via secretKeyRef instead of
// inlining a plaintext value.
type InjectedSecret struct {
	// Name is the short secret name (e.g. "litestream-s3-key"). The core
	// namespaces it to the instance as it does for template-declared secrets.
	Name string
	// Key is the data key within the Kubernetes Secret. The injected sidecar's
	// secretKeyRef.key references this value.
	Key string
	// Value is the plaintext secret value. The caller MUST NOT log or retain it.
	Value string
}

// SidecarInjection is the return type of SidecarInjector.InjectSidecars.
type SidecarInjection struct {
	// YAML is the (possibly modified) pod manifest. Return the input unchanged
	// to pass through.
	YAML string
	// Secrets is an optional list of secrets the injector needs the core to
	// create as podman secrets (referenced via secretKeyRef in YAML). Nil/empty
	// means no extra secrets.
	Secrets []InjectedSecret
}

// SidecarInjector is a commercial extension point that injects sidecar
// containers into an instance's pod YAML after the template body has been
// rendered but before it is applied.
//
// The implementation receives the rendered pod YAML, the projected template
// metadata, the resolved template parameters, and the instance slug. It returns
// the (possibly modified) YAML plus any secrets the core must create before
// PlayKube and prune on delete. Return SidecarInjection{YAML: renderedYAML}
// to pass through without injection.
type SidecarInjector interface {
	InjectSidecars(ctx context.Context, renderedYAML string, meta TemplateMeta, params map[string]any, slug string) (SidecarInjection, error)
}

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
//
// Declaring a secret here is also what keeps it out of the instance API's
// env_summary: the core redacts any env value matching a secret it records
// for this instance — the values here plus any template-declared secrets in
// the stored spec — including env an injector added via secretKeyRef, which
// the template body cannot reveal (#198). That boundary is "values the core
// records for this instance", not "anything referenced via secretKeyRef":
// a secretKeyRef pointing at a pre-existing per-host secret (not part of the
// instance's stored spec) or an `envFrom: [{secretRef: …}]` bulk import (not
// parsed by the name pass at all) is NOT covered and will have its value
// returned in cleartext from GET /hosts/{host}/instances/{template}/{slug}.
// Prefer InjectedSecret for anything an injector needs redacted.
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

// InstanceSecretName returns the per-instance podman secret name the core
// creates for an InjectedSecret (and template-declared secret). Injectors
// must use this to build the secretKeyRef.name in the pod YAML they return.
func InstanceSecretName(template, slug, name string) string {
	return template + "-" + slug + "-" + name
}

// RestoreIntent expresses a one-shot point-in-time restore for an instance. It
// is supplied on a single Apply (via ApplyOptions.RestoreIntent) and is NEVER
// persisted into the stored spec — so the reconcile/boot-converge path always
// passes nil and never replays the restore. That non-persistence is what makes a
// point-in-time rollback fire exactly once instead of repeating on every pod
// restart.
//
// The core ascribes no meaning to Timestamp: it projects the value verbatim to
// the injector, which owns the interpretation (the Litestream injector uses
// RFC3339). An empty Timestamp is rejected by the restore trigger, not here.
type RestoreIntent struct {
	// Timestamp is the opaque point-in-time selector, interpreted by the injector.
	Timestamp string
	// Volumes restricts the restore to these volume names; empty means all of the
	// instance's backup-marked volumes.
	Volumes []string
}

// SidecarInjector is a commercial extension point that injects sidecar
// containers into an instance's pod YAML after the template body has been
// rendered but before it is applied.
//
// The implementation receives the rendered pod YAML, the projected template
// metadata, the resolved template parameters, the instance slug, and an optional
// one-shot RestoreIntent (nil on a normal apply and on every reconcile; non-nil
// only on an explicit point-in-time restore). It returns the (possibly modified)
// YAML plus any secrets the core must create before PlayKube and prune on delete.
// Return SidecarInjection{YAML: renderedYAML} to pass through without injection.
type SidecarInjector interface {
	InjectSidecars(ctx context.Context, renderedYAML string, meta TemplateMeta, params map[string]any, slug string, restore *RestoreIntent) (SidecarInjection, error)
}

// PortSpec names a single host-level port a sidecar needs to bind exclusively,
// for a reason the pod spec's own hostPort mappings cannot express — e.g. a
// rootless IPSEC sidecar whose IKE traffic is translated through pasta's
// host-side socket on the compiled-in port (UDP 500/4500), with no `ports:`
// entry in the rendered YAML for the core's own hostPort accounting to see.
type PortSpec struct {
	// Port is the host-level port number.
	Port int
	// Protocol is "tcp" or "udp", matching podman's PortMapping.Protocol.
	Protocol string
}

// HostPortRequirer is an optional extension a SidecarInjector may also
// implement to declare host ports its injected sidecar(s) need exclusively,
// beyond whatever the rendered pod spec's own hostPort mappings already
// express. It exists so Apply can fail fast, before PlayKube, when a required
// port is already bound on the target host — instead of the pod starting
// silently with a sidecar that can never establish (the failure mode is
// invisible to the sidecar itself: a rootless pod's traffic is translated
// through the host's own network stack one layer below the sidecar's own
// process, so a colliding bind there produces no error the sidecar can see or
// log).
//
// The core type-asserts a registered SidecarInjector against this interface;
// an injector that has no such requirement (the common case) simply does not
// implement it, and Apply's behavior is unchanged.
type HostPortRequirer interface {
	// RequiredHostPorts returns the host ports this instance's sidecar(s)
	// would need exclusively, given its resolved render parameters. Called
	// after InjectSidecars, before the pod is applied to the host. An empty
	// result means this instance's configuration needs no dedicated host
	// port (e.g. no "vpn" parameter present) — not an error.
	RequiredHostPorts(params map[string]any) ([]PortSpec, error)
}

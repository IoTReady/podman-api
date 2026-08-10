// Package store is the daemon's durable desired-state record: one encrypted
// row per instance, written on Apply and removed on Delete. It is opt-in; when
// no store is wired the daemon is a stateless proxy as before.
package store

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound is returned when no row matches a lookup (specs, jobs, or host secrets).
var ErrNotFound = errors.New("store: not found")

// ErrSecretsNeedKey is returned when a secret operation is attempted on a store
// that was opened without an encryption key (-spec-key-file).
var ErrSecretsNeedKey = errors.New("secrets require an encryption key (-spec-key-file)")

// ErrSpecCorrupt marks a permanently unreadable spec row: a JSON column is
// malformed, or the secrets blob decrypts cleanly but its plaintext is not valid
// JSON. It is distinct from transient store errors (context cancellation,
// SQLITE_BUSY) and from the definitive ErrNotFound, so callers (e.g. boot
// reconciliation) can stop retrying a row that will never become readable. A
// decrypt failure (wrong/missing key) is NOT covered here — that recoverable
// case is ErrSecretsUndecryptable.
var ErrSpecCorrupt = errors.New("store: spec row corrupt (malformed)")

// ErrSecretsUndecryptable marks a spec whose sealed secrets blob will not open
// under the loaded key: the daemon was started with the WRONG -spec-key-file
// (or, rarer, the ciphertext is corrupt — the two are indistinguishable at the
// GCM layer). Unlike ErrSpecCorrupt (permanently malformed plaintext) this is
// recoverable: a restart with the correct key file makes the row readable again,
// so callers (boot reconciliation) keep retrying rather than failing terminally.
var ErrSecretsUndecryptable = errors.New("store: secrets undecryptable (wrong or missing -spec-key-file)")

// ErrHostRenameConflict is returned by RenameHost when newID already has spec,
// host-secret or backup rows — renaming into an id that already has state
// would silently merge two hosts' instances (or, for host_secrets, overwrite
// one host's credential with another's).
var ErrHostRenameConflict = errors.New("store: rename target host id already has state")

// ErrHostRenameHasBackups is returned by RenameHost when oldID has any backup
// rows. On-demand snapshot backups are keyed by S3 blob prefixes that are NOT
// re-keyed by this call (out of scope — see docs/superpowers/specs/2026-08-10-host-rename-design.md),
// so renaming a host with backups would make those backups unreachable via the
// restore API. Refuse rather than silently orphan them.
var ErrHostRenameHasBackups = errors.New("store: host has backups; rename would orphan their blob storage")

// InjectorSecret is one secret declared by a SidecarInjector. It carries the
// data key needed to reconstruct the podman/K8s secret on boot converge.
type InjectorSecret struct {
	Name  string // short name; instanceSecretName(tmpl, slug, Name) is the podman secret name
	Key   string // data key within the K8s Secret YAML (secretKeyRef.key)
	Value string // plaintext value
}

// Spec is the desired state of one instance.
type Spec struct {
	Host     string
	Template string
	Slug     string
	// Parameters is the instance's render parameters. NOTE: SQLite-backed
	// storage round-trips this through JSON, so numbers come back as float64
	// (e.g. an int 5432 becomes float64(5432)). Callers that re-render via
	// text/template are unaffected; callers must not type-assert .(int).
	Parameters map[string]any
	Secrets    map[string]string
	// InjectorSecrets tracks secrets declared by the SidecarInjector on the
	// last Apply. Each entry preserves the (Name, Key, Value) triple so boot
	// converge re-creates them with the correct K8s data key. Pruned on Delete.
	InjectorSecrets []InjectorSecret
	// Domains are the public hostnames the ingress layer routes to this
	// instance. Empty for non-web instances. Non-secret; stored in plaintext.
	Domains []string
	Created time.Time
	Updated time.Time
}

// SpecKey identifies one stored instance without exposing its secrets. Used by
// host-wide planning (evacuate) that only needs to know what is on a host.
type SpecKey struct {
	Template string
	Slug     string
}

// Store persists instance specs. Implementations encrypt Secrets at rest and
// stamp Created (first write) and Updated (every write); the in-memory test
// double does neither.
type Store interface {
	// PutSpec inserts or replaces the spec for (s.Host, s.Template, s.Slug).
	PutSpec(ctx context.Context, s Spec) error
	GetSpec(ctx context.Context, host, template, slug string) (Spec, error)
	DeleteSpec(ctx context.Context, host, template, slug string) error
	// ListSpecKeys returns the (template, slug) of every spec on host, without
	// decrypting secrets. Empty slice (no error) when the host has none.
	ListSpecKeys(ctx context.Context, host string) ([]SpecKey, error)

	// PutHostSecret inserts or replaces the sealed value of a per-host secret,
	// keyed by (host, name). Implementations seal Value at rest.
	PutHostSecret(ctx context.Context, host, name string, value []byte) error
	// GetHostSecret returns the decrypted per-host secret value, or ErrNotFound.
	GetHostSecret(ctx context.Context, host, name string) ([]byte, error)
	// DeleteHostSecret removes a per-host secret; absent is not an error.
	DeleteHostSecret(ctx context.Context, host, name string) error

	// RenameHost atomically migrates every specs/host_secrets/backups row from
	// oldID to newID. Returns ErrHostRenameConflict if newID already has spec,
	// host-secret or backup rows, or ErrHostRenameHasBackups if oldID has any
	// backup rows.
	RenameHost(ctx context.Context, oldID, newID string) error

	// CheckHostRename runs RenameHost's refusal checks WITHOUT mutating
	// anything, returning the same sentinel errors. It exists so a caller can
	// preflight a rename before taking an irreversible step of its own (the
	// API handler rewrites hosts/*.yaml before calling RenameHost, and must
	// not do so for a rename the store is going to refuse).
	//
	// It is advisory: nothing is locked between the check and the RenameHost
	// that follows, so RenameHost still performs the checks itself inside its
	// transaction.
	CheckHostRename(ctx context.Context, oldID, newID string) error

	// SecretsEnabled reports whether this store can persist secrets — true only
	// when it was opened with an encryption key. Callers use it to reject a
	// secret-bearing operation BEFORE mutating any host, so a key-less store does
	// not leave orphaned host state when the later PutSpec fails with
	// ErrSecretsNeedKey.
	SecretsEnabled() bool
}

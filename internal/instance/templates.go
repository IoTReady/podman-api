package instance

import (
	"context"
	"errors"
	"fmt"
	"log"
	"slices"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/iotready/podman-api/internal/render"
	"github.com/iotready/podman-api/internal/store"
)

// Template-management sentinel errors, mapped by the API layer to JSON codes.
var (
	ErrTemplateInUse  = errors.New("template is in use by one or more instances")
	ErrTemplateExists = errors.New("template already exists")
	// ErrInvalidTemplate wraps validation failures (bad id, unparsable body,
	// unknown parameter type, ingress mismatch) so the API can map them to 400.
	ErrInvalidTemplate = errors.New("invalid template")
)

// GetTemplate returns a stored template by id (ErrUnknownTemplate if absent).
func (s *Service) GetTemplate(ctx context.Context, id string) (store.Template, error) {
	t, err := s.store.GetTemplate(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		return store.Template{}, ErrUnknownTemplate
	}
	if err != nil {
		return store.Template{}, fmt.Errorf("get template %q: %w", id, err)
	}
	return t, nil
}

// CreateTemplate validates t and persists it; ErrTemplateExists if the id
// already exists. Origin defaults to "user" when the caller leaves it blank.
func (s *Service) CreateTemplate(ctx context.Context, t store.Template) error {
	s.tmplMu.Lock()
	defer s.tmplMu.Unlock()
	if err := render.NormalizeParams(&t.Meta); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidTemplate, err)
	}
	if err := ValidateTemplate(t); err != nil {
		return err
	}
	if _, err := s.GetTemplate(ctx, t.Meta.ID); err == nil {
		return fmt.Errorf("%w: %s", ErrTemplateExists, t.Meta.ID)
	} else if !errors.Is(err, ErrUnknownTemplate) {
		return err
	}
	if t.Origin == "" {
		t.Origin = "user"
	}
	if w := backupMarkerNoneWriteWarning(t); w != "" {
		log.Printf("WARNING: %s", w)
	}
	return s.store.PutTemplate(ctx, t)
}

// UpdateTemplate validates t and upserts it. The template must already exist
// (ErrUnknownTemplate otherwise). The stored Origin is preserved so an edit
// cannot silently flip a "seed" template to "user".
func (s *Service) UpdateTemplate(ctx context.Context, t store.Template) error {
	s.tmplMu.Lock()
	defer s.tmplMu.Unlock()
	if err := render.NormalizeParams(&t.Meta); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidTemplate, err)
	}
	// The stored row is read BEFORE validation because validation depends on it:
	// a near-miss `none` marker the row already carries must not fail an edit
	// that does not touch it (review-4 finding 4). The error precedence a caller
	// sees is unchanged — an invalid submission is still reported as invalid,
	// even for an id that does not exist — so the lookup error is held back
	// until after ValidateTemplateUpdate has had its say.
	existing, gerr := s.GetTemplate(ctx, t.Meta.ID)
	if gerr != nil && !errors.Is(gerr, ErrUnknownTemplate) {
		return gerr
	}
	if err := ValidateTemplateUpdate(t, existing); err != nil {
		return err
	}
	if gerr != nil {
		return gerr
	}
	t.Origin = existing.Origin

	// An alias edit is not just informational: unlike ingress, it can make two
	// LIVE instances claim one DNS name, wedging both against every later apply.
	// Refuse rather than warn (#269).
	if networksChanged(existing.Meta.Networks, t.Meta.Networks) {
		if err := s.checkTemplateNetworkConflicts(ctx, t); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidTemplate, err)
		}
	}

	// Edit-time signal: if this edit removes or changes the ingress declaration
	// while instances still reference the template, those instances' routes will
	// silently drop (or move) at the next reconcile — deriveRoutes skips+warns,
	// but the operator making the EDIT otherwise gets no signal. Warn (non-
	// blocking) so the edit is informed (#61 review-2).
	//
	// SpecKey carries no domains, and decrypting every spec via GetSpec just to
	// see if Domains is set would be wasteful; we keep it cheap and warn on ANY
	// referencing instance. Worst case the warning over-counts non-ingress
	// instances, which is acceptable for an informational message.
	if ingressChanged(existing.Meta.Ingress, t.Meta.Ingress) {
		if n := s.countTemplateRefs(ctx, t.Meta.ID); n > 0 {
			log.Printf("template %q ingress changed/removed but %d instance(s) reference it; their routes may drop on next reconcile", t.Meta.ID, n)
		}
	}

	// Same edit-time signal for shared networks: a pod's network membership is
	// fixed at play time, so instances already running keep whatever set they
	// were played with until their next reconcile (#243).
	if networksChanged(existing.Meta.Networks, t.Meta.Networks) {
		if n := s.countTemplateRefs(ctx, t.Meta.ID); n > 0 {
			log.Printf("template %q networks changed but %d instance(s) reference it; their pods keep the previous network set until the next reconcile", t.Meta.ID, n)
		}
	}

	if w := backupMarkerNoneWriteWarning(t); w != "" {
		log.Printf("WARNING: %s", w)
	}

	return s.store.PutTemplate(ctx, t)
}

// BackupMarkerNoneVolumes returns the names of a template's volumes vetoed by
// a `backup: none` marker, in declaration order. It is the ONE place the veto
// is projected out of a Meta, so the startup audit (server) and the write-path
// warning below cannot drift on what counts as vetoed — the comparison folds
// case and trims, so a near-miss stored before the registration validator
// existed is reported by both.
func BackupMarkerNoneVolumes(m render.Meta) []string {
	var out []string
	for _, v := range m.Volumes {
		if IsBackupMarkerNone(v.Backup) {
			out = append(out, v.Name)
		}
	}
	return out
}

// backupMarkerNoneWriteWarning returns the line to log when a template being
// registered or edited declares `backup: none` volumes — or "" when it does
// not, which is the common case and must stay silent.
//
// The startup audit (server.backupMarkerNoneWarning) covers the catalog as it
// stands at boot, and nothing else: a template registered or edited against a
// RUNNING daemon is never re-audited, so on a long-lived control plane the
// reinterpretation of `none` stays invisible for months. The write path is
// where the operator is actually standing, so it is where the consequence is
// worth stating. Log-only, never a rejection: `none` may be exactly what the
// author meant, and refusing it would break a legitimate declaration.
//
// The all-vetoed case gets its own sentence because its consequence is
// categorically worse than a missing volume: CheckBackupable rejects EVERY
// backup of every instance of such a template outright.
func backupMarkerNoneWriteWarning(t store.Template) string {
	vetoed := BackupMarkerNoneVolumes(t.Meta)
	if len(vetoed) == 0 {
		return ""
	}
	msg := fmt.Sprintf("template %q declares volume(s) %v as `backup: none`: they are a HARD VETO and will NEVER be exported by any backup, on any path — a backup of such an instance still completes green with those volumes absent from the blob set",
		t.Meta.ID, vetoed)
	if len(vetoed) == len(t.Meta.Volumes) {
		msg += ". EVERY volume this template declares is vetoed, so every backup of every instance of it is now REJECTED (invalid_backup_scope)"
	}
	return msg
}

// ingressChanged reports whether an ingress declaration was removed or altered
// in a way that affects derived routes (a different container or port, or no
// ingress at all). Adding ingress where there was none does not drop routes.
func ingressChanged(old, new *render.Ingress) bool {
	if old == nil {
		return false // had no routes to drop
	}
	if new == nil {
		return true // ingress removed
	}
	return old.Container != new.Container || old.Port != new.Port
}

// networksChanged reports whether a template's shared-network set differs
// between two revisions. Order is not compared — neither of networks nor of a
// network's aliases: the pod joins the same set and answers to the same names
// either way, so a reordered declaration is not a change. Comparing by length
// plus membership is exact here because ValidateNetworks rejects a list that
// declares the same network (or the same alias within one) twice.
//
// Unlike ingressChanged, BOTH directions matter. A removed network leaves live
// instances still attached to it; an added one leaves them still detached. An
// edited alias set is the same kind of divergence: peers resolving the old name
// keep resolving it until the instance's next reconcile (#269).
func networksChanged(old, new []render.Network) bool {
	if len(old) != len(new) {
		return true
	}
	set := make(map[string][]string, len(old))
	for _, n := range old {
		set[n.Name] = n.Aliases
	}
	for _, n := range new {
		was, ok := set[n.Name]
		if !ok || len(was) != len(n.Aliases) {
			return true
		}
		for _, a := range n.Aliases {
			if !slices.Contains(was, a) {
				return true
			}
		}
	}
	return false
}

// countTemplateRefs counts instances on any host whose spec references template
// id, reusing the in-use scan pattern from DeleteTemplate. On a per-host list
// error it logs and skips that host rather than failing the caller — this only
// feeds an informational warning. Caller already holds tmplMu.
func (s *Service) countTemplateRefs(ctx context.Context, id string) int {
	n := 0
	for _, h := range s.hostsSnap() {
		keys, err := s.store.ListSpecKeys(ctx, h.ID)
		if err != nil {
			log.Printf("template %q ref count: list specs on %s: %v", id, h.ID, err)
			continue
		}
		for _, k := range keys {
			if k.Template == id {
				n++
			}
		}
	}
	return n
}

// CloneTemplate copies srcID to a new template with id newID and Origin "user".
// ErrUnknownTemplate if src is absent; ErrTemplateExists if newID is taken.
func (s *Service) CloneTemplate(ctx context.Context, srcID, newID string) (store.Template, error) {
	s.tmplMu.Lock()
	defer s.tmplMu.Unlock()
	src, err := s.GetTemplate(ctx, srcID)
	if err != nil {
		return store.Template{}, err
	}
	cl := src
	cl.Meta.ID = newID
	cl.Origin = "user"
	cl.Created = time.Time{}
	cl.Updated = time.Time{}
	if err := render.NormalizeParams(&cl.Meta); err != nil {
		return store.Template{}, fmt.Errorf("%w: %v", ErrInvalidTemplate, err)
	}
	// Validated as an EDIT of the source row, not strictly. A clone introduces
	// no marker the source did not already carry, so a near-miss `none` stored
	// before the registration rule existed must not make the row un-clonable —
	// the same trap ValidateTemplateUpdate removes on the edit path, and with
	// the same reasoning: there is otherwise no in-product path to a working
	// clone short of editing a source template the operator may deliberately
	// want left alone. A near-miss this clone NEWLY introduces cannot exist,
	// since every marker here came from src.
	if err := ValidateTemplateUpdate(cl, src); err != nil {
		return store.Template{}, err
	}
	if _, err := s.GetTemplate(ctx, newID); err == nil {
		return store.Template{}, fmt.Errorf("%w: %s", ErrTemplateExists, newID)
	} else if !errors.Is(err, ErrUnknownTemplate) {
		return store.Template{}, err
	}
	if err := s.store.PutTemplate(ctx, cl); err != nil {
		return store.Template{}, err
	}
	// Re-fetch so the returned value reflects what was actually stored
	// (including any timestamps set by the store).
	return s.GetTemplate(ctx, newID)
}

// DeleteTemplate removes a template. Unless force is set it is rejected with
// ErrTemplateInUse when any instance on any host references it.
func (s *Service) DeleteTemplate(ctx context.Context, id string, force bool) error {
	s.tmplMu.Lock()
	defer s.tmplMu.Unlock()
	// store.DeleteTemplate is absent-OK, but the API documents 404 for an
	// unknown id — so confirm existence first (#61).
	if _, err := s.GetTemplate(ctx, id); err != nil {
		return err
	}
	if !force {
		for _, h := range s.hostsSnap() {
			keys, err := s.store.ListSpecKeys(ctx, h.ID)
			if err != nil {
				return err
			}
			for _, k := range keys {
				if k.Template == id {
					return fmt.Errorf("%w: %s/%s on %s", ErrTemplateInUse, id, k.Slug, h.ID)
				}
			}
		}
	}
	return s.store.DeleteTemplate(ctx, id)
}

// ValidateTemplate checks an authored or seed template before it is persisted:
//
//  1. The template id must be a valid DNS-label-style name.
//  2. A dry-run render of the body (with a dummy value for every declared
//     parameter) must succeed — this catches template syntax errors and
//     references to undeclared parameters (missingkey=error).
//  3. If the template declares ingress, its container must be non-empty and its
//     port in 1..65535 (render.ValidateIngress), AND the rendered pod must
//     contain a container whose name matches Ingress.Container.
//  4. No parameter may set `secret: true` (render.ValidateParamDefs) — parameter
//     values are stored in plaintext, so authors must use secrets.per_instance.
//  5. Every volume's exclude patterns must be relative, non-empty and
//     compilable (render.ValidateVolumes).
func ValidateTemplate(t store.Template) error { return validateTemplate(t, nil) }

// ValidateTemplateUpdate is ValidateTemplate for an EDIT of stored: identical
// except that a near-miss `none` volume marker already present in the stored
// row is not rejected (render.ValidateVolumesUpdate). Without that, a row
// registered before the near-miss rule existed can never be edited again, for
// any reason, and there is no in-product path to the row to fix it. A near-miss
// the update NEWLY introduces is still rejected. Pass a zero store.Template to
// validate strictly.
func ValidateTemplateUpdate(t, stored store.Template) error {
	return validateTemplate(t, &stored)
}

func validateTemplate(t store.Template, stored *store.Template) error {
	if !render.ValidName(t.Meta.ID) {
		return fmt.Errorf("%w: id %q must match %s", ErrInvalidTemplate, t.Meta.ID, render.NameRe.String())
	}

	// API-created templates build render.Meta directly and so skip ParseMeta's
	// checks; re-run the parameter-declaration validation here so a `secret:
	// true` parameter can never be persisted, whatever the entry point. (#205)
	if err := render.ValidateParamDefs(t.Meta); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidTemplate, err)
	}

	// Validate the ingress declaration (container non-empty, port in range)
	// before rendering. API-created templates build render.Meta directly and so
	// skip ParseMeta's checks; this re-runs the same validation (#61).
	if err := render.ValidateIngress(t.Meta.Ingress); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidTemplate, err)
	}

	// Likewise for shared networks: an API-created template skips ParseMeta, so
	// without this an invalid network name is persisted and then fails
	// NetworkEnsure on every deploy of every instance of it. (#243)
	if err := render.ValidateNetworks(t.Meta); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidTemplate, err)
	}

	volErr := render.ValidateVolumes(t.Meta)
	if stored != nil {
		volErr = render.ValidateVolumesUpdate(t.Meta, stored.Meta)
	}
	if volErr != nil {
		return fmt.Errorf("%w: %v", ErrInvalidTemplate, volErr)
	}

	rendered, err := render.RenderBody(t.Body, dummyParams(t.Meta))
	if err != nil {
		return fmt.Errorf("%w: body: %v", ErrInvalidTemplate, err)
	}

	if t.Meta.Ingress != nil {
		if err := checkIngressContainer(rendered, t.Meta.Ingress.Container); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidTemplate, err)
		}
	}
	return nil
}

// dummyParams builds a render parameter map giving every declared parameter a
// value: its Default when set, else a type-appropriate placeholder. This lets
// the dry-run render exercise the body without real input.
func dummyParams(m render.Meta) map[string]any {
	out := make(map[string]any, len(m.Parameters))
	for _, p := range m.Parameters {
		if p.Default != nil {
			out[p.Name] = p.Default
			continue
		}
		switch p.Type {
		case "int":
			out[p.Name] = 0
		case "bool":
			out[p.Name] = false
		default: // string, select, or unspecified
			out[p.Name] = "x"
		}
	}
	return out
}

// checkIngressContainer confirms the rendered pod YAML declares a container
// named want. It unmarshals just the container names; an absent container (or
// unparsable YAML) is an error naming the missing container.
func checkIngressContainer(renderedYAML, want string) error {
	var pod struct {
		Spec struct {
			Containers []struct {
				Name string `yaml:"name"`
			} `yaml:"containers"`
		} `yaml:"spec"`
	}
	if err := yaml.Unmarshal([]byte(renderedYAML), &pod); err != nil {
		return fmt.Errorf("ingress container %q: cannot parse rendered pod: %w", want, err)
	}
	for _, c := range pod.Spec.Containers {
		if c.Name == want {
			return nil
		}
	}
	return fmt.Errorf("ingress container %q not found in rendered pod", want)
}

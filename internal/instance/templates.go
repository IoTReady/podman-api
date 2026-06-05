package instance

import (
	"context"
	"errors"
	"fmt"
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
	if err := validateTemplate(t); err != nil {
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
	if err := validateTemplate(t); err != nil {
		return err
	}
	existing, err := s.GetTemplate(ctx, t.Meta.ID)
	if err != nil {
		return err
	}
	t.Origin = existing.Origin
	return s.store.PutTemplate(ctx, t)
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
	if err := validateTemplate(cl); err != nil {
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

// validateTemplate checks an authored template before it is persisted:
//
//  1. The template id must be a valid DNS-label-style name.
//  2. A dry-run render of the body (with a dummy value for every declared
//     parameter) must succeed — this catches template syntax errors and
//     references to undeclared parameters (missingkey=error).
//  3. If the template declares ingress, its container must be non-empty and its
//     port in 1..65535 (render.ValidateIngress), AND the rendered pod must
//     contain a container whose name matches Ingress.Container.
func validateTemplate(t store.Template) error {
	if !render.ValidName(t.Meta.ID) {
		return fmt.Errorf("%w: id %q must match %s", ErrInvalidTemplate, t.Meta.ID, render.NameRe.String())
	}

	// Validate the ingress declaration (container non-empty, port in range)
	// before rendering. API-created templates build render.Meta directly and so
	// skip ParseMeta's checks; this re-runs the same validation (#61).
	if err := render.ValidateIngress(t.Meta.Ingress); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidTemplate, err)
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

package ingress

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/iotready/podman-api/internal/store"
)

// memStore is a minimal in-memory store.Store for route-derivation tests.
type memStore struct{ specs []store.Spec }

func (m *memStore) PutSpec(context.Context, store.Spec) error { return nil }
func (m *memStore) GetSpec(_ context.Context, host, tmpl, slug string) (store.Spec, error) {
	for _, s := range m.specs {
		if s.Host == host && s.Template == tmpl && s.Slug == slug {
			return s, nil
		}
	}
	return store.Spec{}, store.ErrNotFound
}
func (m *memStore) DeleteSpec(context.Context, string, string, string) error { return nil }
func (m *memStore) ListSpecKeys(_ context.Context, host string) ([]store.SpecKey, error) {
	var out []store.SpecKey
	for _, s := range m.specs {
		if s.Host == host {
			out = append(out, store.SpecKey{Template: s.Template, Slug: s.Slug})
		}
	}
	return out, nil
}
func (m *memStore) PutHostSecret(context.Context, string, string, []byte) error { return nil }
func (m *memStore) GetHostSecret(context.Context, string, string) ([]byte, error) {
	return nil, store.ErrNotFound
}
func (m *memStore) DeleteHostSecret(context.Context, string, string) error { return nil }

func newCtl(specs []store.Spec, tmpls map[string]TemplateIngress) *CaddyController {
	return NewCaddyController(nil, &memStore{specs: specs}, tmpls, Config{})
}

func TestDeriveRoutesHappyPath(t *testing.T) {
	specs := []store.Spec{
		{Host: "h1", Template: "web", Slug: "blog", Domains: []string{"blog.example.com"}, Updated: time.Now()},
		{Host: "h1", Template: "postgres", Slug: "db", Domains: nil}, // no domains -> skipped
	}
	c := newCtl(specs, map[string]TemplateIngress{"web": {Container: "web", Port: 8080}})
	routes, err := c.deriveRoutes(context.Background(), "h1")
	require.NoError(t, err)
	require.Equal(t, []Route{{Domain: "blog.example.com", Backend: "web-blog:8080"}}, routes)
}

func TestDeriveRoutesRejectsDomainsWithoutIngress(t *testing.T) {
	specs := []store.Spec{
		{Host: "h1", Template: "postgres", Slug: "db", Domains: []string{"db.example.com"}},
	}
	c := newCtl(specs, map[string]TemplateIngress{}) // postgres declares no ingress
	_, err := c.deriveRoutes(context.Background(), "h1")
	require.Error(t, err)
	require.Contains(t, err.Error(), "declares no ingress")
}

func TestDeriveRoutesRejectsDuplicateDomain(t *testing.T) {
	specs := []store.Spec{
		{Host: "h1", Template: "web", Slug: "a", Domains: []string{"x.example.com"}},
		{Host: "h1", Template: "web", Slug: "b", Domains: []string{"x.example.com"}},
	}
	c := newCtl(specs, map[string]TemplateIngress{"web": {Port: 8080}})
	_, err := c.deriveRoutes(context.Background(), "h1")
	require.Error(t, err)
	require.Contains(t, err.Error(), "claimed by")
}

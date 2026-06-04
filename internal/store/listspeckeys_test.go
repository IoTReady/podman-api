package store

import (
	"context"
	"testing"
)

func TestListSpecKeys(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	put := func(host, tmpl, slug string) {
		if err := m.PutSpec(ctx, Spec{Host: host, Template: tmpl, Slug: slug,
			Parameters: map[string]any{}, Secrets: map[string]string{}}); err != nil {
			t.Fatal(err)
		}
	}
	put("hostA", "web", "acme")
	put("hostA", "web", "globex")
	put("hostB", "web", "other")

	keys, err := m.ListSpecKeys(ctx, "hostA")
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 {
		t.Fatalf("want 2 keys for hostA, got %d: %v", len(keys), keys)
	}
	got := map[string]string{}
	for _, k := range keys {
		got[k.Slug] = k.Template
	}
	if got["acme"] != "web" || got["globex"] != "web" {
		t.Fatalf("unexpected keys: %v", got)
	}

	empty, err := m.ListSpecKeys(ctx, "nope")
	if err != nil {
		t.Fatal(err)
	}
	if len(empty) != 0 {
		t.Fatalf("want 0 keys for unknown host, got %d", len(empty))
	}
}

func TestSQLiteListSpecKeys(t *testing.T) {
	ctx := context.Background()
	s := openTestStore(t, NewKeyStore(testKey(0x11)))
	put := func(host, tmpl, slug string) {
		if err := s.PutSpec(ctx, Spec{Host: host, Template: tmpl, Slug: slug,
			Parameters: map[string]any{}, Secrets: map[string]string{"password": "s3cr3t"}}); err != nil {
			t.Fatal(err)
		}
	}
	put("hostA", "web", "acme")
	put("hostA", "web", "globex")
	put("hostB", "web", "other")

	keys, err := s.ListSpecKeys(ctx, "hostA")
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 {
		t.Fatalf("want 2 keys for hostA, got %d: %v", len(keys), keys)
	}
	got := map[string]string{}
	for _, k := range keys {
		got[k.Slug] = k.Template
	}
	if got["acme"] != "web" || got["globex"] != "web" {
		t.Fatalf("unexpected keys: %v", got)
	}
	if _, ok := got["other"]; ok {
		t.Fatalf("hostB slug leaked into hostA result: %v", got)
	}

	empty, err := s.ListSpecKeys(ctx, "nope")
	if err != nil {
		t.Fatal(err)
	}
	if len(empty) != 0 {
		t.Fatalf("want 0 keys for unknown host, got %d", len(empty))
	}
}

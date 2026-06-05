package ui

import "testing"

func TestNewParsesTemplates(t *testing.T) {
	u, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for _, name := range []string{"layout", "login"} {
		if u.tmpl.Lookup(name) == nil {
			t.Errorf("template %q not parsed", name)
		}
	}
}

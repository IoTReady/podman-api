package ui

import (
	"testing"

	"github.com/iotready/podman-api/internal/config"
)

func TestOperatorAuthenticator(t *testing.T) {
	hash, err := config.HashToken("hunter2")
	if err != nil {
		t.Fatal(err)
	}
	a := NewOperatorAuthenticator(config.Operator{Username: "admin", PasswordHash: hash})

	id, err := a.Authenticate("admin", "hunter2")
	if err != nil {
		t.Fatalf("good creds: %v", err)
	}
	if id.Subject != "admin" {
		t.Errorf("subject = %q, want admin", id.Subject)
	}
	if !id.HasScope("instances:write") {
		t.Errorf("operator identity should hold instances:write")
	}

	if _, err := a.Authenticate("admin", "wrong"); err == nil {
		t.Error("bad password should error")
	}
	if _, err := a.Authenticate("nope", "hunter2"); err == nil {
		t.Error("unknown user should error")
	}
}

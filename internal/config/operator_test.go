package config

import "testing"

func TestParseOperatorYAML(t *testing.T) {
	hash, err := HashToken("s3cret")
	if err != nil {
		t.Fatal(err)
	}
	raw := []byte("username: admin\npassword_hash: " + hash + "\n")
	op, err := ParseOperatorYAML(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if op.Username != "admin" {
		t.Errorf("username = %q, want admin", op.Username)
	}
	ok, err := VerifyToken("s3cret", op.PasswordHash)
	if err != nil || !ok {
		t.Errorf("verify = %v, %v; want true, nil", ok, err)
	}
}

func TestParseOperatorYAMLDefaultsUsername(t *testing.T) {
	raw := []byte("password_hash: $argon2id$v=19$m=65536,t=3,p=4$c2FsdHNhbHRzYWx0c2E$aGFzaGhhc2hoYXNoaGFzaGhhc2hoYXNoaGFzaA\n")
	op, err := ParseOperatorYAML(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if op.Username != "operator" {
		t.Errorf("username = %q, want operator (default)", op.Username)
	}
}

func TestParseOperatorYAMLRequiresHash(t *testing.T) {
	if _, err := ParseOperatorYAML([]byte("username: x\n")); err == nil {
		t.Fatal("expected error when password_hash missing")
	}
}

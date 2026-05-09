package config

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/crypto/argon2"
	"gopkg.in/yaml.v3"
)

// APIKey is one entry from auth/keys.yaml.
type APIKey struct {
	ID          string   `yaml:"id"`
	SecretHash  string   `yaml:"secret_hash"`
	Scopes      []string `yaml:"scopes"`
	Description string   `yaml:"description,omitempty"`
}

// HasScope returns true if the key holds the requested scope, supporting "*"
// wildcard suffix at the action level only ("instances:*" matches "instances:read").
func (k APIKey) HasScope(want string) bool {
	for _, s := range k.Scopes {
		if s == want {
			return true
		}
		// wildcard: "instances:*" matches "instances:read", etc.
		if strings.HasSuffix(s, ":*") {
			prefix := strings.TrimSuffix(s, "*")
			if strings.HasPrefix(want, prefix) {
				return true
			}
		}
	}
	return false
}

// ParseKeysYAML parses an `auth/keys.yaml` file body.
func ParseKeysYAML(raw []byte) ([]APIKey, error) {
	var wrapper struct {
		Keys []APIKey `yaml:"keys"`
	}
	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	dec.KnownFields(true)
	if err := dec.Decode(&wrapper); err != nil {
		return nil, fmt.Errorf("parse keys: %w", err)
	}
	for i, k := range wrapper.Keys {
		if k.ID == "" {
			return nil, fmt.Errorf("keys[%d]: id is required", i)
		}
		if k.SecretHash == "" {
			return nil, fmt.Errorf("keys[%d]: secret_hash is required", i)
		}
	}
	return wrapper.Keys, nil
}

// HashToken produces an argon2id hash of the given token, encoded in the
// PHC-style format expected by VerifyToken.
func HashToken(token string) (string, error) {
	const (
		time    = 3
		memory  = 64 * 1024
		threads = 4
		keyLen  = 32
	)
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	hash := argon2.IDKey([]byte(token), salt, time, memory, threads, keyLen)
	enc := func(b []byte) string { return base64.RawStdEncoding.EncodeToString(b) }
	return fmt.Sprintf("$argon2id$v=19$m=%d,t=%d,p=%d$%s$%s",
		memory, time, threads, enc(salt), enc(hash)), nil
}

// VerifyToken checks token against an argon2id PHC string.
func VerifyToken(token, encoded string) (bool, error) {
	parts := strings.Split(encoded, "$")
	if len(parts) != 6 || parts[1] != "argon2id" {
		return false, errors.New("not an argon2id hash")
	}
	var version int
	if _, err := fmt.Sscanf(parts[2], "v=%d", &version); err != nil {
		return false, fmt.Errorf("bad version: %w", err)
	}
	if version != argon2.Version {
		return false, fmt.Errorf("argon2 version mismatch: %d != %d", version, argon2.Version)
	}
	var memory, time uint32
	var threads uint8
	if _, err := fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d", &memory, &time, &threads); err != nil {
		return false, fmt.Errorf("bad params: %w", err)
	}
	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil {
		return false, err
	}
	want, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil {
		return false, err
	}
	got := argon2.IDKey([]byte(token), salt, time, memory, threads, uint32(len(want)))
	return subtle.ConstantTimeCompare(got, want) == 1, nil
}

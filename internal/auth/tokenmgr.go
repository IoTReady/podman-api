package auth

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"sync"
	"unicode"

	"github.com/iotready/podman-api/internal/config"
	"gopkg.in/yaml.v3"
)

var ErrTokenNotFound      = errors.New("token id not found")
var ErrTokenIDExists      = errors.New("token id already exists")
var ErrTokenScopesRequired = errors.New("at least one scope is required")

// TokenManager manages the on-disk keys.yaml and the live KeyStore atomically.
// All mutating methods hold mu, rewrite the file, then reload the store.
type TokenManager struct {
	path  string
	store *KeyStore
	mu    sync.Mutex
}

func NewTokenManager(path string, store *KeyStore) *TokenManager {
	return &TokenManager{path: path, store: store}
}

// Store returns the underlying KeyStore (for tests and UI wiring).
func (m *TokenManager) Store() *KeyStore { return m.store }

// List returns all keys with SecretHash redacted.
func (m *TokenManager) List() ([]config.APIKey, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	keys, err := m.readFile()
	if err != nil {
		return nil, err
	}
	out := make([]config.APIKey, len(keys))
	for i, k := range keys {
		out[i] = k
		out[i].SecretHash = ""
	}
	return out, nil
}

// Create generates a random token, hashes it, appends the entry to keys.yaml,
// reloads the KeyStore, and returns the plaintext token (shown once; never stored).
func (m *TokenManager) Create(id, description string, scopes []string) (string, error) {
	if id == "" {
		return "", fmt.Errorf("id is required")
	}
	for _, r := range id {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '-' && r != '_' {
			return "", fmt.Errorf("token id may only contain letters, digits, hyphens, and underscores")
		}
	}
	if len(scopes) == 0 {
		return "", ErrTokenScopesRequired
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	keys, err := m.readFile()
	if err != nil {
		return "", err
	}
	for _, k := range keys {
		if k.ID == id {
			return "", ErrTokenIDExists
		}
	}

	rawBytes := make([]byte, 32)
	if _, err := rand.Read(rawBytes); err != nil {
		return "", fmt.Errorf("generate token: %w", err)
	}
	plain := base64.RawURLEncoding.EncodeToString(rawBytes)

	hash, err := config.HashToken(plain)
	if err != nil {
		return "", fmt.Errorf("hash token: %w", err)
	}

	keys = append(keys, config.APIKey{
		ID:          id,
		SecretHash:  hash,
		Scopes:      scopes,
		Description: description,
	})
	if err := m.writeFile(keys); err != nil {
		return "", err
	}
	m.store.Store(keys)
	return plain, nil
}

// Revoke removes the key with the given id from keys.yaml and reloads the KeyStore.
func (m *TokenManager) Revoke(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	keys, err := m.readFile()
	if err != nil {
		return err
	}
	// Sync store with current file state before mutating, so the store stays
	// consistent even when Revoke returns an error (e.g. last-token guard).
	m.store.Store(keys)
	next := keys[:0]
	found := false
	for _, k := range keys {
		if k.ID == id {
			found = true
			continue
		}
		next = append(next, k)
	}
	if !found {
		return ErrTokenNotFound
	}
	if len(next) == 0 {
		return fmt.Errorf("cannot revoke the last token: all bearer authentication would be locked out")
	}
	if err := m.writeFile(next); err != nil {
		return err
	}
	m.store.Store(next)
	return nil
}

// Reload reads keys.yaml from disk and replaces the live KeyStore.
// Skipped (returns error) if the file parses to zero keys, to avoid lockout.
func (m *TokenManager) Reload() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	keys, err := m.readFile()
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		return fmt.Errorf("zero keys in file, reload skipped to avoid lockout")
	}
	m.store.Store(keys)
	return nil
}

func (m *TokenManager) readFile() ([]config.APIKey, error) {
	raw, err := os.ReadFile(m.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read keys: %w", err)
	}
	keys, err := config.ParseKeysYAML(raw)
	if err != nil {
		return nil, fmt.Errorf("parse keys: %w", err)
	}
	return keys, nil
}

// writeFile re-marshals keys to YAML and atomically replaces the keys file.
// Inline comments and field ordering in the original file are not preserved —
// once TokenManager writes the file it becomes machine-managed.
func (m *TokenManager) writeFile(keys []config.APIKey) error {
	wrapper := struct {
		Keys []config.APIKey `yaml:"keys"`
	}{Keys: keys}
	out, err := yaml.Marshal(wrapper)
	if err != nil {
		return fmt.Errorf("marshal keys: %w", err)
	}
	// Write via temp file for atomic replace.
	tmp := m.path + ".tmp"
	if err := os.WriteFile(tmp, out, 0o600); err != nil {
		return fmt.Errorf("write keys: %w", err)
	}
	return os.Rename(tmp, m.path)
}

// Package ui implements the server-rendered single-operator admin UI.
package ui

import (
	"errors"

	"github.com/iotready/podman-api/internal/config"
)

// ErrAuth is returned for any failed login (unknown user or bad password);
// callers must not distinguish the two (avoids user enumeration).
var ErrAuth = errors.New("invalid credentials")

// Identity is the authenticated subject and its scopes, carried in the request
// context. Single-operator yields one fixed subject with the full scope set;
// future RBAC yields per-user subjects and scopes.
type Identity struct {
	Subject string
	Scopes  []string
}

// HasScope reports whether the identity holds want (supporting the "*"
// wildcard the operator identity is granted).
func (i Identity) HasScope(want string) bool {
	for _, s := range i.Scopes {
		if s == "*" || s == want {
			return true
		}
	}
	return false
}

// Authenticator verifies a login and yields an Identity.
type Authenticator interface {
	Authenticate(user, password string) (Identity, error)
}

// AuthenticatorFunc adapts a function to the Authenticator interface.
type AuthenticatorFunc func(user, password string) (Identity, error)

func (f AuthenticatorFunc) Authenticate(user, password string) (Identity, error) {
	return f(user, password)
}

// OperatorAuthenticator authenticates the single configured operator against an
// argon2id password hash.
type OperatorAuthenticator struct {
	op config.Operator
}

func NewOperatorAuthenticator(op config.Operator) *OperatorAuthenticator {
	return &OperatorAuthenticator{op: op}
}

func (a *OperatorAuthenticator) Authenticate(user, password string) (Identity, error) {
	if user != a.op.Username {
		return Identity{}, ErrAuth
	}
	ok, err := config.VerifyToken(password, a.op.PasswordHash)
	if err != nil || !ok {
		return Identity{}, ErrAuth
	}
	return Identity{Subject: a.op.Username, Scopes: []string{"*"}}, nil
}

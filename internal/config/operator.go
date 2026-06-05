package config

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// Operator is the single-operator UI credential, parsed from the -operator-file
// YAML. PasswordHash is an argon2id PHC string (produce one with
// `podman-api hash-token <plaintext>`).
type Operator struct {
	Username     string `yaml:"username"`
	PasswordHash string `yaml:"password_hash"`
}

// ParseOperatorYAML parses an operator credential file. Username defaults to
// "operator" when omitted; password_hash is required.
func ParseOperatorYAML(raw []byte) (Operator, error) {
	var op Operator
	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	dec.KnownFields(true)
	if err := dec.Decode(&op); err != nil {
		return Operator{}, fmt.Errorf("parse operator: %w", err)
	}
	if op.PasswordHash == "" {
		return Operator{}, fmt.Errorf("operator: password_hash is required")
	}
	if op.Username == "" {
		op.Username = "operator"
	}
	return op, nil
}

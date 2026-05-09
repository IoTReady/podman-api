package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseKeysYAML(t *testing.T) {
	src := `keys:
  - id: cms-prod
    secret_hash: $argon2id$v=19$m=65536,t=3,p=4$abc$def
    scopes: [hosts:read, instances:*]
    description: "CMS production"
`
	keys, err := ParseKeysYAML([]byte(src))
	require.NoError(t, err)
	require.Len(t, keys, 1)
	assert.Equal(t, "cms-prod", keys[0].ID)
	assert.Equal(t, []string{"hosts:read", "instances:*"}, keys[0].Scopes)
}

func TestKey_HasScope(t *testing.T) {
	k := APIKey{Scopes: []string{"hosts:read", "instances:*"}}

	assert.True(t, k.HasScope("hosts:read"))
	assert.False(t, k.HasScope("hosts:write"))
	assert.True(t, k.HasScope("instances:read"))
	assert.True(t, k.HasScope("instances:write"))
	assert.False(t, k.HasScope("secrets:write"))
}

func TestVerifyArgon2id(t *testing.T) {
	hash, err := HashToken("hunter2")
	require.NoError(t, err)
	require.NotEmpty(t, hash)

	ok, err := VerifyToken("hunter2", hash)
	require.NoError(t, err)
	assert.True(t, ok)

	ok, err = VerifyToken("wrong", hash)
	require.NoError(t, err)
	assert.False(t, ok)
}

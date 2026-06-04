package ingress

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateDomains(t *testing.T) {
	require.NoError(t, ValidateDomains(nil))
	require.NoError(t, ValidateDomains([]string{"app.example.com", "api.example.com"}))

	require.Error(t, ValidateDomains([]string{"App.Example.com"}))                    // uppercase
	require.Error(t, ValidateDomains([]string{"not a domain"}))                       // syntax
	require.Error(t, ValidateDomains([]string{"-bad.example.com"}))                   // leading hyphen
	require.Error(t, ValidateDomains([]string{"dup.example.com", "dup.example.com"})) // duplicate

	// Total FQDN length cap (253) with otherwise-valid labels.
	tooLong := strings.Repeat("ab.", 90) + "example.com" // 281 chars, every label legal
	require.Greater(t, len(tooLong), 253)
	require.Error(t, ValidateDomains([]string{tooLong}))
}

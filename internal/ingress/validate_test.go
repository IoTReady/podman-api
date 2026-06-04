package ingress

import (
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
}

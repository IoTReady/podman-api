package ingress

import (
	"fmt"
	"regexp"
	"strings"
)

// domainRE is a pragmatic FQDN check: >=2 dot-separated labels, each 1-63 chars
// of [a-z0-9-], not starting/ending with '-', a 2-63 char alpha TLD. Lowercase
// only (ACME/Caddy treat hostnames case-insensitively; we normalize by
// rejecting non-lowercase so stored domains compare byte-for-byte).
var domainRE = regexp.MustCompile(`^([a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}$`)

// ValidateDomains checks that each domain is a syntactically valid lowercase
// FQDN and that the slice has no intra-slice duplicates. A nil/empty slice is
// valid (a non-web instance). Host-wide uniqueness across instances is enforced
// later, at route derivation.
func ValidateDomains(domains []string) error {
	seen := make(map[string]bool, len(domains))
	for _, d := range domains {
		if d != strings.ToLower(d) {
			return fmt.Errorf("ingress: domain %q must be lowercase", d)
		}
		if len(d) > 253 {
			return fmt.Errorf("ingress: domain %q exceeds the 253-character FQDN limit", d)
		}
		if !domainRE.MatchString(d) {
			return fmt.Errorf("ingress: invalid domain %q", d)
		}
		if seen[d] {
			return fmt.Errorf("ingress: duplicate domain %q", d)
		}
		seen[d] = true
	}
	return nil
}

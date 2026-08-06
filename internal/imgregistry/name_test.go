package imgregistry

import "testing"

func TestValidRepoName(t *testing.T) {
	valid := []string{
		"engine",
		"otp",
		"iotready/engine",
		"a/b/c",
		"my-repo",
		"my_repo",
		"my.repo",
		"repo123",
		"a", // single character: legal here, unlike render.ValidName's 2-char floor
	}
	for _, s := range valid {
		if !ValidRepoName(s) {
			t.Errorf("ValidRepoName(%q) = false, want true", s)
		}
	}

	invalid := []string{
		"",
		"/",
		"/engine",
		"engine/",
		"engine//app",
		"Engine",        // uppercase not allowed
		"engine:latest", // ":" is not part of a repo name
		"../etc/passwd", // path traversal
		"{{template}}",  // template delimiters
		"engine app",    // space
		"-engine",       // must not start with a separator
		"engine-",       // must not end with a separator
		"engine..app",   // doubled dot not allowed by the component grammar
	}
	for _, s := range invalid {
		if ValidRepoName(s) {
			t.Errorf("ValidRepoName(%q) = true, want false", s)
		}
	}
}

func TestValidRef(t *testing.T) {
	valid := []string{
		"latest",
		"v1.2.3",
		"8d5f281",
		"sha256:" + hexString(64),
		"sha512:" + hexString(128),
	}
	for _, s := range valid {
		if !ValidRef(s) {
			t.Errorf("ValidRef(%q) = false, want true", s)
		}
	}

	invalid := []string{
		"",
		"../../../etc/passwd",
		"../etc/passwd",
		"a/b",
		"tag/with/slash",
		"sha256:tooshort",
		"sha256:",
		":nodigest",
		"tag with space",
		"-leadingdash",
	}
	for _, s := range invalid {
		if ValidRef(s) {
			t.Errorf("ValidRef(%q) = true, want false", s)
		}
	}
}

// hexString returns a string of n hex-safe characters, used to build a
// digest-length test value without hardcoding a long literal.
func hexString(n int) string {
	out := make([]byte, n)
	for i := range out {
		out[i] = "0123456789abcdef"[i%16]
	}
	return string(out)
}

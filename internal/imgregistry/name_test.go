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

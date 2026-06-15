package ui

import (
	"fmt"
	"html/template"
	"net/http"
	"time"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/store"
)

// formatBytes renders a byte count as a human-readable string (KB, MB, GB, etc).
func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

// New wires the UI's dependencies. New defaults SessionTTL (12h) and
// Sessions (an in-process MemorySessionStore) when they are left zero/nil. Auth
// is NOT defaulted — the caller must supply it before serving authenticated
// routes (the binary wires the single-operator Authenticator). New still
// succeeds with a nil Auth so the UI can be constructed for template-only tests
// that never exercise login.
type Config struct {
	Svc        *instance.Service
	Jobs       store.JobStore
	Auth       Authenticator
	Sessions   SessionStore
	SessionTTL time.Duration
	Secure     bool // set Secure flag on the session cookie (true in production)
}

// UI holds parsed templates and dependencies and produces the /ui sub-router.
type UI struct {
	cfg  Config
	tmpl *template.Template
}

func New(cfg Config) (*UI, error) {
	funcMap := template.FuncMap{"formatBytes": formatBytes}
	t, err := template.New("").Funcs(funcMap).ParseFS(templateFS, "templates/*.html")
	if err != nil {
		return nil, err
	}
	if cfg.SessionTTL == 0 {
		cfg.SessionTTL = 12 * time.Hour
	}
	if cfg.Sessions == nil {
		cfg.Sessions = NewMemorySessionStore(cfg.SessionTTL)
	}
	return &UI{cfg: cfg, tmpl: t}, nil
}

// staticHandler serves the embedded /ui/static/* assets.
func (u *UI) staticHandler() http.Handler {
	return http.StripPrefix("/ui/", http.FileServer(http.FS(staticFS)))
}

// pageData builds the render data for an authenticated page, injecting the host
// list the layout's sidebar needs (presence of "Hosts" is also what tells the
// layout to render the persistent shell rather than a bare body). Every
// authenticated handler should pass its data through this; the login page does
// not (it renders chrome-free). On a full-page load this populates the sidebar;
// on an HTMX fragment swap the extra key is simply unused.
func (u *UI) pageData(data map[string]any) map[string]any {
	if data == nil {
		data = map[string]any{}
	}
	// Shell tells the layout to render the persistent chrome (sidebar + #main).
	// Set for every authenticated page regardless of host count, so an operator
	// with zero configured hosts still gets nav + sign-out.
	data["Shell"] = true
	// Svc is nil only in template-only construction (tests that never reach an
	// authenticated handler); guard so pageData can't panic there.
	if u.cfg.Svc != nil {
		data["Hosts"] = u.cfg.Svc.Hosts()
	}
	return data
}

// Handler returns the /ui sub-router.
func (u *UI) Handler() http.Handler {
	mux := http.NewServeMux()

	// Public. POST /ui/login is intentionally NOT CSRF-guarded: there is no
	// session yet, and it is itself the credential check (protected by
	// SameSite=Lax on the cookie it sets).
	mux.HandleFunc("GET /ui/login", u.loginForm)
	mux.Handle("POST /ui/login", u.loginThrottle(u.login))
	mux.Handle("/ui/static/", u.staticHandler())

	guard := func(h http.HandlerFunc) http.Handler { return u.requireSession(h) }
	guardW := func(h http.HandlerFunc) http.Handler { return u.requireSession(u.requireCSRF(h)) }
	mux.Handle("GET /ui", guard(u.dashboard))
	mux.Handle("GET /ui/hosts/{host}", guard(u.hostInstances))
	mux.Handle("GET /ui/hosts/{host}/instances/{template}/{slug}", guard(u.instanceDetail))
	mux.Handle("GET /ui/hosts/{host}/instances/{template}/{slug}/edit", guard(u.editForm))
	mux.Handle("POST /ui/hosts/{host}/instances/{template}/{slug}/edit", guardW(u.editApply))
	mux.Handle("GET /ui/hosts/{host}/deploy", guard(u.deployForm))
	mux.Handle("POST /ui/hosts/{host}/deploy", guardW(u.deployCreate))
	mux.Handle("POST /ui/hosts/{host}/deploy/form", guardW(u.deployFormPost))
	mux.Handle("GET /ui/hosts/{host}/instances/{template}/{slug}/upgrade", guard(u.upgradeForm))
	mux.Handle("POST /ui/hosts/{host}/instances/{template}/{slug}/upgrade", guardW(u.upgradeApply))
	mux.Handle("GET /ui/hosts/{host}/instances/{template}/{slug}/secrets", guard(u.secretsForm))
	mux.Handle("POST /ui/hosts/{host}/instances/{template}/{slug}/secrets", guardW(u.secretsRotate))
	mux.Handle("GET /ui/hosts/{host}/instances/{template}/{slug}/logs", guard(u.logsPage))
	mux.Handle("GET /ui/hosts/{host}/instances/{template}/{slug}/logs/stream", guard(u.logsStream))
	mux.Handle("POST /ui/hosts/{host}/instances/{template}/{slug}/backup", guardW(u.backupNow))
	mux.Handle("POST /ui/backups/{id}/restore", guardW(u.restoreBackup))
	mux.Handle("POST /ui/backups/{id}/delete", guardW(u.deleteBackup))
	mux.Handle("POST /ui/hosts/{host}/instances/{template}/{slug}/{action}", guardW(u.lifecycle))
	mux.Handle("POST /ui/logout", guardW(u.logout))
	mux.Handle("GET /ui/jobs", guard(u.jobsList))
	mux.Handle("GET /ui/jobs/{id}", guard(u.jobDetail))

	return mux
}

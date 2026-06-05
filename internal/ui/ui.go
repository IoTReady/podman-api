package ui

import (
	"html/template"
	"net/http"
	"time"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/store"
)

// Config wires the UI's dependencies. New defaults SessionTTL (12h) and
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
	t, err := template.ParseFS(templateFS, "templates/*.html")
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
	mux.HandleFunc("POST /ui/login", u.login)
	mux.Handle("/ui/static/", u.staticHandler())

	guard := func(h http.HandlerFunc) http.Handler { return u.requireSession(h) }
	guardW := func(h http.HandlerFunc) http.Handler { return u.requireSession(u.requireCSRF(h)) }
	mux.Handle("GET /ui", guard(u.dashboard))
	mux.Handle("GET /ui/hosts/{host}", guard(u.hostInstances))
	mux.Handle("GET /ui/hosts/{host}/instances/{template}/{slug}", guard(u.instanceDetail))
	mux.Handle("GET /ui/hosts/{host}/deploy", guard(u.deployForm))
	mux.Handle("POST /ui/hosts/{host}/deploy", guardW(u.deployCreate))
	mux.Handle("GET /ui/hosts/{host}/instances/{template}/{slug}/upgrade", guard(u.upgradeForm))
	mux.Handle("POST /ui/hosts/{host}/instances/{template}/{slug}/upgrade", guardW(u.upgradeApply))
	mux.Handle("GET /ui/hosts/{host}/instances/{template}/{slug}/logs", guard(u.logsTail))
	mux.Handle("POST /ui/hosts/{host}/instances/{template}/{slug}/{action}", guardW(u.lifecycle))
	mux.Handle("POST /ui/logout", guardW(u.logout))
	mux.Handle("GET /ui/jobs", guard(u.jobsList))
	mux.Handle("GET /ui/jobs/{id}", guard(u.jobDetail))

	return mux
}

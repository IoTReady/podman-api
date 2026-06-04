package ui

import (
	"html/template"
	"net/http"
	"time"

	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/store"
)

// Config wires the UI's dependencies. Auth and Sessions are filled with the
// single-operator defaults by New when left nil.
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

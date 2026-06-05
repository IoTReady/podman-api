// Command podman-api is the HTTP service that translates CMS REST calls
// into libpod REST calls against one or more Podman hosts.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/iotready/podman-api/internal/api"
	"github.com/iotready/podman-api/internal/auth"
	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/evacuate"
	"github.com/iotready/podman-api/internal/ingress"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/jobs"
	"github.com/iotready/podman-api/internal/migrate"
	"github.com/iotready/podman-api/internal/obs"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/prune"
	"github.com/iotready/podman-api/internal/store"
	"github.com/iotready/podman-api/internal/ui"
	"github.com/iotready/podman-api/templates"
	"github.com/prometheus/client_golang/prometheus"
)

func main() {
	var (
		addr          = flag.String("addr", "127.0.0.1:8080", "bind address for the API")
		metricsAddr   = flag.String("metrics-addr", "", "if set, expose /metrics on this address (e.g. 127.0.0.1:9090); empty means no metrics endpoint")
		hostsDir      = flag.String("hosts-dir", "hosts", "directory of hosts/*.yaml files")
		keysFile      = flag.String("keys-file", "auth/keys.yaml", "path to bearer keys file")
		tmplDir       = flag.String("templates-dir", "", "if set, load templates from this dir instead of embedded")
		auditLogFile  = flag.String("audit-log-file", "", "if set, write audit lines to this path (append) instead of stdout; operational logs still go to stderr")
		stateDB       = flag.String("state-db", "", "if set, enable the desired-state store at this SQLite path (required for migrate/evacuate)")
		specKeyFile   = flag.String("spec-key-file", "", "path to the 32-byte secret encryption key (required when -state-db is set)")
		jobsRetention = flag.Duration("jobs-retention", 0, "if >0, prune terminal jobs older than this (e.g. 168h); 0 disables")
		evacConc      = flag.Int("evacuate-concurrency", 2, "max child migrations an evacuate runs at once (1..32); a request's \"concurrency\" overrides per call")
		jobWorkers    = flag.Int("job-workers", jobs.DefaultWorkers, "size of the background job worker pool (<=0 uses the built-in default)")

		migrateVerifyTimeout = flag.Duration("migrate-verify-timeout", 60*time.Second, "max wait for a migrated instance to become ready (running + declared healthchecks healthy) before reaping the source")
		migrateVerifyVolumes = flag.Bool("migrate-verify-volumes", true, "verify each copied volume's content against the source before reaping the source (adds a re-export of source and dest per volume); false disables it")

		pruneEnabled   = flag.Bool("prune-enabled", false, "enable scheduled host-health prune/cleanup (requires -state-db)")
		pruneInterval  = flag.Duration("prune-interval", 24*time.Hour, "default interval between scheduled prunes per host")
		pruneThreshold = flag.Int("prune-disk-threshold", 85, "disk used% high-water that triggers an early prune; 0 disables the threshold trigger")
		pruneScope     = flag.String("prune-scope", "dangling", "default prune scopes, comma-separated: dangling,all-images,containers,build-cache,volumes")
		pruneDryRun    = flag.Bool("prune-dry-run", false, "default dry-run: report reclaimable space without removing anything")

		ingressEnabled  = flag.Bool("ingress-enabled", false, "enable per-host Caddy ingress + auto-TLS (requires -state-db)")
		ingressNetwork  = flag.String("ingress-network", "podman-api-ingress", "shared podman network app pods join for ingress")
		ingressImage    = flag.String("ingress-caddy-image", "docker.io/library/caddy:2", "Caddy image for the per-host ingress pod; must include /bin/sh (the pod seeds its config via a small shell wrapper), so a shell-less distroless/scratch variant won't work")
		ingressACME     = flag.String("ingress-acme-email", "", "ACME account email for Let's Encrypt (required when -ingress-enabled)")
		ingressInterval = flag.Duration("ingress-reconcile-interval", 5*time.Minute, "periodic ingress drift-correction interval per host; 0 disables the periodic loop")

		operatorFile   = flag.String("operator-file", "", "if set, enable the admin UI and authenticate the single operator against this YAML file (username, password_hash)")
		uiSecureCookie = flag.Bool("ui-secure-cookie", false, "set the Secure flag on the UI session cookie (enable when serving the UI over HTTPS / behind TLS)")
	)
	flag.Parse()

	if len(flag.Args()) > 0 && flag.Arg(0) == "hash-token" {
		if len(flag.Args()) < 2 {
			fmt.Fprintln(os.Stderr, "usage: podman-api hash-token <plaintext>")
			os.Exit(2)
		}
		h, err := config.HashToken(flag.Arg(1))
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(h)
		return
	}

	if *ingressEnabled {
		if *stateDB == "" {
			log.Fatalf("ingress: -ingress-enabled requires -state-db (routes are derived from the desired-state store)")
		}
		if *ingressACME == "" {
			log.Fatalf("ingress: -ingress-enabled requires -ingress-acme-email")
		}
	}

	hosts, err := config.LoadHosts(*hostsDir)
	if err != nil {
		log.Fatalf("hosts: %v", err)
	}
	// hostsHolder mirrors the live host set so the prune scheduler can read the
	// current policies on each tick, including after a SIGHUP reload.
	var hostsHolder atomic.Pointer[[]config.Host]
	hostsHolder.Store(&hosts)
	keys, fp, err := loadKeys(*keysFile)
	if err != nil {
		log.Fatalf("keys: %v", err)
	}
	keyStore := auth.NewKeyStore(keys)
	log.Printf("keys loaded: %d entries, fingerprint=%s", len(keys), fp)

	var tmpls []config.Template
	if *tmplDir != "" {
		tmpls, err = config.LoadTemplates(os.DirFS(*tmplDir), ".")
	} else {
		tmpls, err = config.LoadTemplates(templates.Files, ".")
	}
	if err != nil {
		log.Fatalf("templates: %v", err)
	}

	client, err := podman.NewReal(hosts)
	if err != nil {
		log.Fatalf("podman: %v", err)
	}

	svc := instance.NewService(client, hosts, tmpls)
	instance.SetVerifyTimeout(*migrateVerifyTimeout)
	svc.SetVerifyVolumes(*migrateVerifyVolumes)

	db, err := openStore(*stateDB, *specKeyFile)
	if err != nil {
		log.Fatalf("store: %v", err)
	}
	if *pruneEnabled && db == nil {
		log.Fatalf("-prune-enabled requires -state-db")
	}
	// runnerCtx is cancelled by the shutdown handler to stop the job runner.
	runnerCtx, cancelRunner := context.WithCancel(context.Background())
	defer cancelRunner()
	var jobStore store.JobStore
	var canceller api.JobCanceller
	var pruneSched *prune.Scheduler
	var ingressCtl *ingress.CaddyController
	if db != nil {
		defer db.Close()
		svc.SetStore(db)

		if *ingressEnabled {
			tmplIngress := map[string]ingress.TemplateIngress{}
			for _, t := range tmpls {
				if t.Meta.Ingress != nil {
					tmplIngress[t.Meta.ID] = ingress.TemplateIngress{
						Container: t.Meta.Ingress.Container,
						Port:      t.Meta.Ingress.Port,
					}
				}
			}
			ctl := ingress.NewCaddyController(client, db, tmplIngress, ingress.Config{
				Network:    *ingressNetwork,
				CaddyImage: *ingressImage,
				ACMEEmail:  *ingressACME,
			})
			svc.SetIngress(ctl, *ingressNetwork)
			ingressCtl = ctl
			log.Printf("ingress enabled (network %s, image %s, reconcile interval %s)", *ingressNetwork, *ingressImage, *ingressInterval)
		}
		jobStore = db
		pruneMetrics := obs.NewPruneMetrics(prometheus.DefaultRegisterer)
		registry := jobs.Registry{
			"migrate":  &migrate.Handler{Svc: svc},
			"evacuate": &evacuate.Handler{Svc: svc, Jobs: db, Concurrency: *evacConc},
			"prune":    &prune.Handler{Client: client, Jobs: db, Metrics: pruneMetrics},
		}
		workers := *jobWorkers
		if workers <= 0 {
			workers = jobs.DefaultWorkers
		}
		runner := jobs.NewRunner(db, registry, workers)
		canceller = runner
		runner.Start(runnerCtx)
		if *jobsRetention > 0 {
			runner.StartRetention(runnerCtx, *jobsRetention)
			log.Printf("jobs retention enabled: pruning terminal jobs older than %s", *jobsRetention)
		}
		log.Printf("desired-state store enabled: %s (job runner started, %d workers)", *stateDB, workers)

		if *pruneEnabled {
			def := prune.Defaults{
				Enabled:       true,
				Interval:      *pruneInterval,
				DiskThreshold: *pruneThreshold,
				Scope:         splitScopes(*pruneScope),
				DryRun:        *pruneDryRun,
			}
			// Validate the startup set once so a misconfigured policy fails loudly at boot.
			if _, err := buildHostPolicies(*hostsHolder.Load(), def); err != nil {
				log.Fatalf("prune policy: %v", err)
			}
			pruneSched = &prune.Scheduler{Store: db, Client: client, Now: time.Now}
			pruneSched.Start(runnerCtx, func() []prune.HostPolicy {
				policies, _ := buildHostPolicies(*hostsHolder.Load(), def)
				return policies
			})
			log.Printf("prune scheduler enabled (interval %s, disk threshold %d%%, scopes %v)", *pruneInterval, *pruneThreshold, def.Scope)
		}
	}

	metrics := obs.New()

	auditSink := os.Stdout
	if *auditLogFile != "" {
		f, err := os.OpenFile(*auditLogFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o640)
		if err != nil {
			log.Fatalf("audit log: %v", err)
		}
		// Lifetime is the process — we never rotate from inside the binary
		// (logrotate's copytruncate or `kill -USR1` patterns are out of
		// scope; see README's "audit log shipping" section). Close on
		// shutdown for tidy fd accounting.
		defer f.Close()
		auditSink = f
		log.Printf("audit log: writing to %s", *auditLogFile)
	}
	audit := obs.NewAuditMiddleware(auditSink)

	// Compose metrics(audit(h)) so every guarded request is both measured
	// and audit-logged. The caller (main) composes rather than adding a
	// parameter to NewRouter.
	combined := func(h http.Handler) http.Handler {
		return metrics.Middleware()(audit(h))
	}

	// /metrics is never mounted on the main listener — operators must opt in
	// with -metrics-addr to bind it on a separate (typically internal) socket.
	router := api.NewRouter(svc, jobStore, keyStore, combined, nil, canceller)

	// opHolder carries the live operator credential; visible to both the UI auth
	// closure below and the SIGHUP goroutine that follows.
	var opHolder atomic.Pointer[config.Operator]
	var uiApp *ui.UI
	if *operatorFile != "" {
		op, fp, err := loadOperator(*operatorFile)
		if err != nil {
			log.Fatalf("operator: %v", err)
		}
		opHolder.Store(&op)
		authr := ui.AuthenticatorFunc(func(user, pass string) (ui.Identity, error) {
			return ui.NewOperatorAuthenticator(*opHolder.Load()).Authenticate(user, pass)
		})
		uiApp, err = ui.New(ui.Config{Svc: svc, Jobs: jobStore, Auth: authr, Secure: *uiSecureCookie})
		if err != nil {
			log.Fatalf("ui: %v", err)
		}
		log.Printf("admin UI enabled at /ui (operator=%s, fp=%s)", op.Username, fp)
		if !*uiSecureCookie {
			log.Printf("admin UI: -ui-secure-cookie=false; the session cookie will be sent over plain HTTP — enable it when serving over HTTPS/behind TLS")
		}
	}

	srv := &http.Server{
		Addr:              *addr,
		Handler:           composeHandler(router, uiApp),
		ReadHeaderTimeout: 10 * time.Second,
	}

	var metricsSrv *http.Server
	if *metricsAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("GET /metrics", metrics.Handler())
		metricsSrv = &http.Server{
			Addr:              *metricsAddr,
			Handler:           mux,
			ReadHeaderTimeout: 10 * time.Second,
		}
	}

	// SIGHUP: re-read keys.yaml AND hosts/*.yaml and atomically swap both.
	// A bad reload of either is logged but does NOT kill the running process,
	// so a fat-fingered edit can't take the API down. Each is independent —
	// if hosts parses but keys doesn't, the hosts reload still applies.
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	go func() {
		for range hup {
			if newKeys, fp, err := loadKeys(*keysFile); err != nil {
				log.Printf("keys reload FAILED, keeping previous set: %v", err)
			} else if len(newKeys) == 0 {
				log.Printf("keys reload SKIPPED, file parsed but contained zero keys (path=%s, fp=%s)", *keysFile, fp)
			} else {
				keyStore.Store(newKeys)
				log.Printf("keys reloaded: %d entries, fingerprint=%s", len(newKeys), fp)
			}

			if newHosts, err := config.LoadHosts(*hostsDir); err != nil {
				log.Printf("hosts reload FAILED, keeping previous set: %v", err)
			} else {
				svc.SetHosts(newHosts)
				// newHosts is never reassigned after this point, so storing its
				// address publishes the freshly-loaded set to the prune scheduler's
				// hostsFn; the slice is treated as immutable after load.
				hostsHolder.Store(&newHosts)
				draining := 0
				for _, hh := range newHosts {
					if hh.Drain {
						draining++
					}
				}
				log.Printf("hosts reloaded: %d entries (%d draining)", len(newHosts), draining)
			}

			if *operatorFile != "" {
				if newOp, fp, err := loadOperator(*operatorFile); err != nil {
					log.Printf("operator reload FAILED, keeping previous: %v", err)
				} else {
					opHolder.Store(&newOp)
					log.Printf("operator reloaded: username=%s, fp=%s", newOp.Username, fp)
				}
			}

		}
	}()

	// ingressLoopDone is closed when the periodic ingress loop exits (or
	// immediately if the loop never starts), so the shutdown handler can await it.
	ingressLoopDone := make(chan struct{})

	idleClosed := make(chan struct{})
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		// Signal the job runner to stop claiming. We deliberately do NOT
		// runner.Wait() here — shutdown must not block on a long in-flight
		// handler; an interrupted job stays "running" and is reaped to "failed"
		// by boot recovery on the next start. (Revisit a bounded drain in #34
		// when real migrate/evacuate handlers are registered.)
		cancelRunner()
		if pruneSched != nil {
			pruneSched.Wait()
		}
		// Wait for the periodic ingress loop (if any) to observe the cancelled
		// runnerCtx and return before we proceed with HTTP shutdown.
		<-ingressLoopDone
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		if metricsSrv != nil {
			_ = metricsSrv.Shutdown(ctx)
		}
		close(idleClosed)
	}()

	if metricsSrv != nil {
		go func() {
			log.Printf("metrics listening on %s", *metricsAddr)
			if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Printf("metrics listener: %v", err)
			}
		}()
	}

	// Periodic ingress drift-correction: reconcile every known host on a ticker.
	// Reads the live host set from hostsHolder so a SIGHUP reload is picked up.
	if ingressCtl != nil && *ingressInterval > 0 {
		go func() {
			defer close(ingressLoopDone)
			t := time.NewTicker(*ingressInterval)
			defer t.Stop()
			for {
				select {
				case <-runnerCtx.Done():
					return
				case <-t.C:
					for _, h := range *hostsHolder.Load() {
						if err := ingressCtl.Reconcile(runnerCtx, h.ID); err != nil {
							log.Printf("ingress: periodic reconcile %s failed: %v", h.ID, err)
						}
					}
				}
			}
		}()
	} else {
		close(ingressLoopDone)
	}

	log.Printf("podman-api listening on %s with %d hosts, %d templates, %d keys",
		*addr, len(hosts), len(tmpls), len(keyStore.Load()))
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
	<-idleClosed
}

// composeHandler mounts the admin UI under /ui on top of the API router when
// uiApp is non-nil (operator file configured). A bare GET / redirects to /ui.
// When uiApp is nil the API router is returned unchanged.
func composeHandler(apiRouter http.Handler, uiApp *ui.UI) http.Handler {
	if uiApp == nil {
		return apiRouter
	}
	uiHandler := uiApp.Handler()
	top := http.NewServeMux()
	top.Handle("/", apiRouter)
	top.Handle("/ui", uiHandler)
	top.Handle("/ui/", uiHandler)
	top.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/ui", http.StatusSeeOther)
	})
	return top
}

// loadOperator reads and parses the operator credential file, returning the
// parsed Operator and a short SHA-256 fingerprint of the file contents.
func loadOperator(path string) (config.Operator, string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return config.Operator{}, "", err
	}
	op, err := config.ParseOperatorYAML(raw)
	if err != nil {
		return config.Operator{}, "", err
	}
	sum := sha256.Sum256(raw)
	return op, hex.EncodeToString(sum[:8]), nil
}

// loadKeys reads and parses the keys file, returning the parsed list and a
// short SHA-256 fingerprint of the file contents (for audit logs / operator
// confirmation that a SIGHUP picked up the intended edit).
func loadKeys(path string) ([]config.APIKey, string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, "", err
	}
	keys, err := config.ParseKeysYAML(raw)
	if err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(raw)
	return keys, hex.EncodeToString(sum[:8]), nil
}

// openStore wires the optional desired-state store from the two flags. It
// returns (nil, nil) when stateDB is empty (store disabled). When stateDB is
// set it requires a readable, valid key file; any problem is an error so the
// caller can refuse to start. The spec key is loaded ONCE at startup — there is
// deliberately no runtime hot-reload, because rotating to a different key would
// silently make existing (un-re-encrypted) rows undecryptable (#41). Real
// re-encrypting rotation is a future, separate capability.
func openStore(stateDB, keyFile string) (store.DB, error) {
	if stateDB == "" {
		return nil, nil
	}
	if keyFile == "" {
		return nil, fmt.Errorf("-state-db requires -spec-key-file")
	}
	key, err := store.LoadKeyFile(keyFile)
	if err != nil {
		return nil, fmt.Errorf("spec key: %w", err)
	}
	st, err := store.OpenSQLite(stateDB, store.NewKeyStore(key))
	if err != nil {
		return nil, fmt.Errorf("state db: %w", err)
	}
	return st, nil
}

// splitScopes parses a comma-separated scope flag, trimming spaces and dropping empties.
func splitScopes(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// buildHostPolicies resolves every host's prune policy over the defaults. It
// skips (with a log) any host whose policy fails to resolve so one bad file never
// stops the others, and also returns the first such error. The per-tick caller
// ignores the error and uses the slice; the boot-time caller treats a non-nil
// error as fatal so a misconfigured startup set fails loudly.
func buildHostPolicies(hosts []config.Host, def prune.Defaults) ([]prune.HostPolicy, error) {
	var out []prune.HostPolicy
	var firstErr error
	for _, h := range hosts {
		p, err := prune.Resolve(h.Prune, def)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("host %s: %w", h.ID, err)
			}
			log.Printf("prune: host %s policy invalid, skipping: %v", h.ID, err)
			continue
		}
		out = append(out, prune.HostPolicy{Host: h.ID, Policy: p})
	}
	return out, firstErr
}

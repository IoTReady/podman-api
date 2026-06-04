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
	"syscall"
	"time"

	"github.com/iotready/podman-api/internal/api"
	"github.com/iotready/podman-api/internal/auth"
	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/evacuate"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/jobs"
	"github.com/iotready/podman-api/internal/migrate"
	"github.com/iotready/podman-api/internal/obs"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/store"
	"github.com/iotready/podman-api/templates"
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

	hosts, err := config.LoadHosts(*hostsDir)
	if err != nil {
		log.Fatalf("hosts: %v", err)
	}
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

	db, err := openStore(*stateDB, *specKeyFile)
	if err != nil {
		log.Fatalf("store: %v", err)
	}
	// runnerCtx is cancelled by the shutdown handler to stop the job runner.
	runnerCtx, cancelRunner := context.WithCancel(context.Background())
	defer cancelRunner()
	var jobStore store.JobStore
	if db != nil {
		defer db.Close()
		svc.SetStore(db)
		jobStore = db
		registry := jobs.Registry{
			"migrate":  &migrate.Handler{Svc: svc},
			"evacuate": &evacuate.Handler{Svc: svc, Jobs: db, Concurrency: *evacConc},
		}
		runner := jobs.NewRunner(db, registry, jobs.DefaultWorkers)
		runner.Start(runnerCtx)
		if *jobsRetention > 0 {
			runner.StartRetention(runnerCtx, *jobsRetention)
			log.Printf("jobs retention enabled: pruning terminal jobs older than %s", *jobsRetention)
		}
		log.Printf("desired-state store enabled: %s (job runner started)", *stateDB)
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
	router := api.NewRouter(svc, jobStore, keyStore, combined, nil)

	srv := &http.Server{
		Addr:              *addr,
		Handler:           router,
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
				draining := 0
				for _, hh := range newHosts {
					if hh.Drain {
						draining++
					}
				}
				log.Printf("hosts reloaded: %d entries (%d draining)", len(newHosts), draining)
			}

		}
	}()

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

	log.Printf("podman-api listening on %s with %d hosts, %d templates, %d keys",
		*addr, len(hosts), len(tmpls), len(keyStore.Load()))
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
	<-idleClosed
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

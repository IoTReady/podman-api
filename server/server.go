package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/iotready/podman-api/extension"
	"github.com/iotready/podman-api/internal/api"
	"github.com/iotready/podman-api/internal/auth"
	backuppkg "github.com/iotready/podman-api/internal/backup"
	"github.com/iotready/podman-api/internal/backupctl"
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

// Version is the server's release string, set via -ldflags "-X server.Version=v1.x.y" at build time.
var Version = "dev"

type cfg struct {
	blobStore       extension.BlobStore
	sidecarInjector extension.SidecarInjector
	backupScheduler extension.BackupScheduler
}

type Option func(*cfg)

func WithBlobStore(bs extension.BlobStore) Option {
	return func(c *cfg) { c.blobStore = bs }
}

func RunWithFlags(opts ...Option) error {
	var c cfg
	for _, o := range opts {
		o(&c)
	}

	fs := flag.NewFlagSet("podman-api", flag.ContinueOnError)
	var (
		addr          = fs.String("addr", "127.0.0.1:8080", "bind address for the API")
		metricsAddr   = fs.String("metrics-addr", "", "if set, expose /metrics on this address (e.g. 127.0.0.1:9090); empty means no metrics endpoint")
		hostsDir      = fs.String("hosts-dir", "hosts", "directory of hosts/*.yaml files")
		keysFile      = fs.String("keys-file", "auth/keys.yaml", "path to bearer keys file")
		auditLogFile  = fs.String("audit-log-file", "", "if set, write audit lines to this path (append) instead of stdout; operational logs still go to stderr")
		stateDB       = fs.String("state-db", "/var/lib/podman-api/state.db", "SQLite path for the always-on template catalog + desired-state store")
		specKeyFile   = fs.String("spec-key-file", "", "path to the 32-byte secret encryption key; optional — without it the store runs key-less (templates and no-secret specs work, secret ops are refused)")
		backupDir     = fs.String("backup-dir", "", "directory for volume backup artifacts; empty derives <state-db dir>/backups")
		jobsRetention = fs.Duration("jobs-retention", 0, "if >0, prune terminal jobs older than this (e.g. 168h); 0 disables")
		evacConc      = fs.Int("evacuate-concurrency", 2, "max child migrations an evacuate runs at once (1..32); a request's \"concurrency\" overrides per call")
		jobWorkers    = fs.Int("job-workers", jobs.DefaultWorkers, "size of the background job worker pool (<=0 uses the built-in default)")

		migrateVerifyTimeout = fs.Duration("migrate-verify-timeout", 180*time.Second, "max wait for a migrated instance to become ready (running + declared healthchecks healthy) before reaping the source")
		migrateVerifyVolumes = fs.Bool("migrate-verify-volumes", true, "verify each copied volume's content against the source before reaping the source (adds a re-export of source and dest per volume); false disables it")
		migrateVerifyStable  = fs.Int("migrate-verify-stable-count", 3, "number of consecutive ready polls required before a migrated instance is considered stable; higher values reduce false positives from brief restarts but increase the minimum verify time (poll interval * count)")
		deployVerifyTimeout  = fs.Duration("deploy-verify-timeout", 30*time.Second, "how long to wait for container healthchecks to pass after deploy or start (0 = disabled)")
		deployVerifyStable   = fs.Int("deploy-verify-stable-count", 1, "same as -migrate-verify-stable-count but for the deploy/start path; defaults to 1 since apps freshly applied there are less likely to cycle than during migration")

		pruneEnabled   = fs.Bool("prune-enabled", false, "enable scheduled host-health prune/cleanup")
		pruneInterval  = fs.Duration("prune-interval", 24*time.Hour, "default interval between scheduled prunes per host")
		pruneThreshold = fs.Int("prune-disk-threshold", 85, "disk used% high-water that triggers an early prune; 0 disables the threshold trigger")
		pruneScope     = fs.String("prune-scope", "dangling", "default prune scopes, comma-separated: dangling,all-images,containers,build-cache,volumes")
		pruneDryRun    = fs.Bool("prune-dry-run", false, "default dry-run: report reclaimable space without removing anything")

		ingressEnabled   = fs.Bool("ingress-enabled", false, "enable per-host Caddy ingress + auto-TLS")
		ingressNetwork   = fs.String("ingress-network", "podman-api-ingress", "shared podman network app pods join for ingress")
		ingressAdminAddr = fs.String("ingress-caddy-admin-addr", "localhost:2019", "default Caddy admin API address (host:port); per-host caddy_admin_addr in hosts/*.yaml overrides this. The admin API is unauthenticated, so keep :2019 on a trusted/private network or firewalled to the control plane")
		ingressInterval  = fs.Duration("ingress-reconcile-interval", 5*time.Minute, "periodic ingress drift-correction interval per host; 0 disables the periodic loop")

		operatorFile   = fs.String("operator-file", "", "if set, enable the admin UI and authenticate the single operator against this YAML file (username, password_hash)")
		uiSecureCookie = fs.Bool("ui-secure-cookie", false, "set the Secure flag on the UI session cookie (enable when serving the UI over HTTPS / behind TLS)")
	)
	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}

	if len(fs.Args()) > 0 && fs.Arg(0) == "hash-token" {
		if len(fs.Args()) < 2 {
			return fmt.Errorf("usage: podman-api hash-token <plaintext>")
		}
		h, err := config.HashToken(fs.Arg(1))
		if err != nil {
			return fmt.Errorf("hash-token: %w", err)
		}
		fmt.Println(h)
		return nil
	}

	hosts, err := config.LoadHosts(*hostsDir)
	if err != nil {
		return fmt.Errorf("hosts: %w", err)
	}
	var hostsHolder atomic.Pointer[[]config.Host]
	hostsHolder.Store(&hosts)
	keys, fp, err := loadKeys(*keysFile)
	if err != nil {
		return fmt.Errorf("keys: %w", err)
	}
	keyStore := auth.NewKeyStore(keys)
	log.Printf("keys loaded: %d entries, fingerprint=%s", len(keys), fp)

	client, err := podman.NewReal(hosts)
	if err != nil {
		return fmt.Errorf("podman: %w", err)
	}
	if err := client.Preflight(context.Background()); err != nil {
		return fmt.Errorf("podman: %w", err)
	}

	svc := instance.NewService(client, hosts)
	instance.SetVerifyTimeout(*migrateVerifyTimeout)
	instance.SetDeployVerifyTimeout(*deployVerifyTimeout)
	instance.SetVerifyStableCount(*migrateVerifyStable)
	instance.SetDeployVerifyStableCount(*deployVerifyStable)
	svc.SetVerifyVolumes(*migrateVerifyVolumes)

	db, err := openStore(*stateDB, *specKeyFile)
	if err != nil {
		return fmt.Errorf("store: %w", err)
	}
	defer db.Close()
	svc.SetStore(db)

	seedCtx := context.Background()
	if n, err := seedTemplates(seedCtx, db, templates.Files); err != nil {
		return fmt.Errorf("seed templates: %w", err)
	} else if n > 0 {
		log.Printf("seeded %d templates into empty catalog", n)
	}

	tmplCount, err := db.CountTemplates(seedCtx)
	if err != nil {
		return fmt.Errorf("templates: count: %w", err)
	}

	if c.blobStore != nil {
		svc.SetBlobStore(c.blobStore)
		log.Printf("backups enabled: custom blob store")
	} else {
		bdir := *backupDir
		if bdir == "" {
			bdir = filepath.Join(filepath.Dir(*stateDB), "backups")
		}
		blobs, err := backuppkg.NewLocalDir(bdir)
		if err != nil {
			return fmt.Errorf("backup dir: %w", err)
		}
		svc.SetBlobStore(blobs)
		log.Printf("backups enabled: %s", bdir)
	}

	if c.sidecarInjector != nil {
		svc.SetSidecarInjector(c.sidecarInjector)
		log.Printf("sidecar injector enabled")
	}

	runnerCtx, cancelRunner := context.WithCancel(context.Background())
	defer cancelRunner()
	var jobStore store.JobStore
	var canceller api.JobCanceller
	var pruneSched *prune.Scheduler
	var ingressCtl *ingress.CaddyController

	if *ingressEnabled {
		hostAdmins := make(map[string]string)
		for _, h := range hosts {
			switch {
			case h.CaddyAdminAddr != "":
				hostAdmins[h.ID] = h.CaddyAdminAddr
			case h.Addr != "unix" && h.Addr != "":
				// Derive from SSH addr "user@host" → "host:2019" so operators
				// don't need to set caddy_admin_addr for standard deployments.
				// Strip "user@" prefix, then strip any SSH port, then append :2019.
				raw := h.Addr
				if at := strings.IndexByte(raw, '@'); at >= 0 {
					raw = raw[at+1:]
				}
				hostname := raw
				if parsed, _, err := net.SplitHostPort(raw); err == nil {
					hostname = parsed // e.g. "host:2222" → "host"; "[::1]:22" → "::1"
				}
				hostAdmins[h.ID] = net.JoinHostPort(hostname, "2019")
			}
		}
		ctl := ingress.NewCaddyController(db, ingress.Config{
			AdminAddr:  *ingressAdminAddr,
			HostAdmins: hostAdmins,
		})
		svc.SetIngress(ctl, *ingressNetwork)
		ingressCtl = ctl
		log.Printf("ingress enabled (network %s, caddy admin %s, reconcile interval %s)", *ingressNetwork, *ingressAdminAddr, *ingressInterval)
	}
	jobStore = db
	pruneMetrics := obs.NewPruneMetrics(prometheus.DefaultRegisterer)
	jobMetrics := obs.NewJobMetrics(prometheus.DefaultRegisterer)
	registry, reconcilers := buildJobRegistry(svc, client, db, *evacConc, pruneMetrics, jobMetrics)
	workers := *jobWorkers
	if workers <= 0 {
		workers = jobs.DefaultWorkers
	}
	runner := jobs.NewRunner(db, registry, workers)
	runner.Metrics = jobMetrics
	canceller = runner
	runner.SetReconcilers(reconcilers)
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
		if _, err := buildHostPolicies(*hostsHolder.Load(), def); err != nil {
			return fmt.Errorf("prune policy: %w", err)
		}
		pruneSched = &prune.Scheduler{Store: db, Client: client, Now: time.Now}
		pruneSched.Start(runnerCtx, func() []prune.HostPolicy {
			policies, _ := buildHostPolicies(*hostsHolder.Load(), def)
			return policies
		})
		log.Printf("prune scheduler enabled (interval %s, disk threshold %d%%, scopes %v)", *pruneInterval, *pruneThreshold, def.Scope)
	}

	if c.backupScheduler != nil {
		ctrl := &backupctl.Controller{Svc: svc, Jobs: db}
		runBackupScheduler(runnerCtx, c.backupScheduler, ctrl)
		log.Printf("backup scheduler enabled (commercial)")
	}

	go func() {
		select {
		case <-time.After(2 * time.Second):
		case <-runnerCtx.Done():
			return
		}
		var wg sync.WaitGroup
		for _, h := range *hostsHolder.Load() {
			wg.Add(1)
			go func(id string) {
				defer wg.Done()
				svc.ReconcileSpecsOnHost(runnerCtx, id)
			}(h.ID)
		}
		wg.Wait()
	}()

	metrics := obs.New()

	auditSink := os.Stdout
	if *auditLogFile != "" {
		f, err := os.OpenFile(*auditLogFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o640)
		if err != nil {
			return fmt.Errorf("audit log: %w", err)
		}
		defer f.Close()
		auditSink = f
		log.Printf("audit log: writing to %s", *auditLogFile)
	}
	audit := obs.NewAuditMiddleware(auditSink)

	combined := func(h http.Handler) http.Handler {
		return metrics.Middleware()(audit(h))
	}

	router := api.NewRouter(svc, jobStore, keyStore, combined, nil, canceller, Version)

	var opHolder atomic.Pointer[config.Operator]
	var uiApp *ui.UI
	var tokenMgr *auth.TokenManager
	if *operatorFile != "" {
		op, fp, err := loadOperator(*operatorFile)
		if err != nil {
			return fmt.Errorf("operator: %w", err)
		}
		opHolder.Store(&op)
		authr := ui.AuthenticatorFunc(func(user, pass string) (ui.Identity, error) {
			return ui.NewOperatorAuthenticator(*opHolder.Load()).Authenticate(user, pass)
		})
		tokenMgr = auth.NewTokenManager(*keysFile, keyStore)
		uiApp, err = ui.New(ui.Config{Svc: svc, Jobs: jobStore, Auth: authr, Secure: *uiSecureCookie, TokenMgr: tokenMgr, Version: Version})
		if err != nil {
			return fmt.Errorf("ui: %w", err)
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

	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	go func() {
		for range hup {
			if tokenMgr != nil {
				if err := tokenMgr.Reload(); err != nil {
					log.Printf("keys reload FAILED: %v", err)
				} else {
					log.Printf("keys reloaded via TokenManager (%d entries)", len(keyStore.Load()))
				}
			} else {
				if newKeys, fp, err := loadKeys(*keysFile); err != nil {
					log.Printf("keys reload FAILED, keeping previous set: %v", err)
				} else if len(newKeys) == 0 {
					log.Printf("keys reload SKIPPED, file parsed but contained zero keys (path=%s, fp=%s)", *keysFile, fp)
				} else {
					keyStore.Store(newKeys)
					log.Printf("keys reloaded: %d entries, fingerprint=%s", len(newKeys), fp)
				}
			}

			if newHosts, err := config.LoadHosts(*hostsDir); err != nil {
				log.Printf("hosts reload FAILED, keeping previous set: %v", err)
			} else {
				client.SetHosts(newHosts)
				svc.SetHosts(newHosts)
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

	ingressLoopDone := make(chan struct{})

	idleClosed := make(chan struct{})
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		cancelRunner()
		if pruneSched != nil {
			pruneSched.Wait()
		}
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
		*addr, len(hosts), tmplCount, len(keyStore.Load()))
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("server: %w", err)
	}
	<-idleClosed
	return nil
}

func buildJobRegistry(svc *instance.Service, client podman.Client, db store.DB, evacConc int, pruneMetrics *obs.PruneMetrics, jobMetrics *obs.JobMetrics) (jobs.Registry, jobs.Reconcilers) {
	reg := jobs.Registry{
		"migrate":      &migrate.Handler{Svc: svc, Metrics: jobMetrics},
		"evacuate":     &evacuate.Handler{Svc: svc, Jobs: db, Concurrency: evacConc, Metrics: jobMetrics},
		"prune":        &prune.Handler{Client: client, Jobs: db, Metrics: pruneMetrics},
		"backup":       &backuppkg.Handler{Svc: svc},
		"restore":      &backuppkg.RestoreHandler{Svc: svc},
		"pitr-restore": &backuppkg.PITRRestoreHandler{Svc: svc},
	}
	recs := jobs.Reconcilers{
		"migrate": &migrate.Reconciler{Svc: svc},
		"backup":  &backuppkg.Reconciler{Svc: svc},
	}
	return reg, recs
}

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

func openStore(path, keyFile string) (store.DB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("state db dir: %w", err)
	}
	var keys *store.KeyStore
	if keyFile != "" {
		key, err := store.LoadKeyFile(keyFile)
		if err != nil {
			return nil, fmt.Errorf("spec key: %w", err)
		}
		keys = store.NewKeyStore(key)
	}
	st, err := store.OpenSQLite(path, keys)
	if err != nil {
		return nil, fmt.Errorf("state db: %w", err)
	}
	return st, nil
}

func seedTemplates(ctx context.Context, db store.TemplateStore, fsys fs.FS) (int, error) {
	n, err := db.CountTemplates(ctx)
	if err != nil || n > 0 {
		return 0, err
	}
	seeds, err := store.ParseSeeds(fsys)
	if err != nil {
		return 0, err
	}
	for _, t := range seeds {
		if err := instance.ValidateTemplate(t); err != nil {
			return 0, fmt.Errorf("seed template %q invalid: %w", t.Meta.ID, err)
		}
	}
	for _, t := range seeds {
		if err := db.PutTemplate(ctx, t); err != nil {
			return 0, err
		}
	}
	return len(seeds), nil
}

func splitScopes(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

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

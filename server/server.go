package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
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
	"github.com/iotready/podman-api/internal/imgregistry"
	"github.com/iotready/podman-api/internal/ingress"
	"github.com/iotready/podman-api/internal/instance"
	"github.com/iotready/podman-api/internal/inventory"
	"github.com/iotready/podman-api/internal/jobs"
	"github.com/iotready/podman-api/internal/migrate"
	"github.com/iotready/podman-api/internal/obs"
	"github.com/iotready/podman-api/internal/podman"
	"github.com/iotready/podman-api/internal/prune"
	"github.com/iotready/podman-api/internal/registryprune"
	"github.com/iotready/podman-api/internal/render"
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
		addr             = fs.String("addr", "127.0.0.1:8080", "bind address for the API")
		metricsAddr      = fs.String("metrics-addr", "", "if set, expose /metrics on this address (e.g. 127.0.0.1:9090); empty means no metrics endpoint")
		hostsDir         = fs.String("hosts-dir", "hosts", "directory of hosts/*.yaml files")
		keysFile         = fs.String("keys-file", "auth/keys.yaml", "path to bearer keys file")
		auditLogFile     = fs.String("audit-log-file", "", "if set, write audit lines to this path (append) instead of stdout; operational logs still go to stderr")
		stateDB          = fs.String("state-db", "/var/lib/podman-api/state.db", "SQLite path for the always-on template catalog + desired-state store")
		specKeyFile      = fs.String("spec-key-file", "", "path to the 32-byte secret encryption key; optional — without it the store runs key-less (templates and no-secret specs work, secret ops are refused)")
		backupDir        = fs.String("backup-dir", "", "directory for volume backup artifacts; empty derives <state-db dir>/backups")
		jobsRetention    = fs.Duration("jobs-retention", 0, "if >0, prune terminal jobs older than this (e.g. 168h); 0 disables")
		evacConc         = fs.Int("evacuate-concurrency", 2, "max child migrations an evacuate runs at once (1..32); a request's \"concurrency\" overrides per call")
		jobWorkers       = fs.Int("job-workers", jobs.DefaultWorkers, "size of the background job worker pool (<=0 uses the built-in default)")
		jobVolumeWorkers = fs.Int("job-volume-workers", 2, "hard concurrency ceiling (not a minimum) on volume-transfer-heavy job kinds (backup, restore, pitr-restore, migrate, evacuate): these are the ONLY workers that ever claim those kinds, so at most this many can run at once no matter how large -job-workers is; the rest of -job-workers serves every other kind. If a large fleet-wide backup wave needs more volume-transfer throughput, raise this (and -job-workers correspondingly, since it must stay less than the total). Must be less than -job-workers, else those kinds could starve everything else for up to -volume-transfer-timeout (#238, following up on #54); 0 disables the reservation (single shared pool, pre-#238 behaviour)")

		migrateVerifyTimeout = fs.Duration("migrate-verify-timeout", 180*time.Second, "max wait for a migrated instance to become ready (running + declared healthchecks healthy) before reaping the source")
		migrateVerifyVolumes = fs.Bool("migrate-verify-volumes", true, "verify each copied volume's content against the source before reaping the source (adds a re-export of source and dest per volume); false disables it")
		migrateVerifyStable  = fs.Int("migrate-verify-stable-count", 3, "number of consecutive ready polls required before a migrated instance is considered stable; higher values reduce false positives from brief restarts but increase the minimum verify time (poll interval * count)")
		deployVerifyTimeout  = fs.Duration("deploy-verify-timeout", 30*time.Second, "how long to wait for container healthchecks to pass after deploy or start (0 = disabled)")
		deployVerifyStable   = fs.Int("deploy-verify-stable-count", 1, "same as -migrate-verify-stable-count but for the deploy/start path; defaults to 1 since apps freshly applied there are less likely to cycle than during migration")

		volumeTransferTimeout = fs.Duration("volume-transfer-timeout", 2*time.Hour, "max duration of a single volume export/import (backup, restore, migrate, rename): must cover the whole streamed transfer of a real volume's contents, not just issuing the request (#223 — a multi-hundred-MB volume over a tailnet link routinely exceeded the general-purpose 10-minute per-call timeout). Must be positive; zero/negative is rejected at startup rather than silently falling back to the 10-minute default")
		imagePullTimeout      = fs.Duration("image-pull-timeout", 2*time.Hour, "max duration of a single image pull (e.g. migrate preflight): must cover the whole pulled image, not just issuing the request (#238 — the same shape of large, network-bound transfer that motivated -volume-transfer-timeout for VolumeExport/VolumeImport in #223). Must be positive; zero/negative is rejected at startup rather than silently falling back to the 10-minute default")

		pruneEnabled   = fs.Bool("prune-enabled", false, "enable scheduled host-health prune/cleanup")
		pruneInterval  = fs.Duration("prune-interval", 24*time.Hour, "default interval between scheduled prunes per host")
		pruneThreshold = fs.Int("prune-disk-threshold", 85, "disk used% high-water that triggers an early prune; 0 disables the threshold trigger")
		pruneScope     = fs.String("prune-scope", "dangling", "default prune scopes, comma-separated: dangling,all-images,containers,build-cache,volumes")
		pruneDryRun    = fs.Bool("prune-dry-run", false, "default dry-run: report reclaimable space without removing anything")

		inventoryInterval = fs.Duration("inventory-refresh-interval", 30*time.Second, "background inventory refresh cadence per host; 0 disables the poller (falls back to the lazy 3s cache)")
		inventoryTimeout  = fs.Duration("inventory-refresh-timeout", 20*time.Second, "per-host timeout for one background inventory refresh")

		containerStats      = fs.Bool("container-stats", true, "sample per-container CPU/memory/network/block-IO on each inventory tick and export them as Prometheus metrics; requires the inventory poller")
		containerStatsTO    = fs.Duration("container-stats-timeout", 5*time.Second, "per-host timeout for one container stats sample, independent of -inventory-refresh-timeout; 0 or negative means the 5s default, never unbounded or instant; keep it plus -inventory-refresh-timeout under -inventory-refresh-interval")
		volumeUsageInterval = fs.Duration("volume-usage-interval", time.Hour, "cadence for the per-host volume sizing walk (podman system df); 0 disables it; requires the inventory poller")
		volumeUsageTimeout  = fs.Duration("volume-usage-timeout", 5*time.Minute, "per-host timeout for one volume sizing walk")

		ingressEnabled   = fs.Bool("ingress-enabled", false, "enable per-host Caddy ingress + auto-TLS")
		ingressNetwork   = fs.String("ingress-network", "podman-api-ingress", "shared podman network app pods join for ingress")
		ingressAdminAddr = fs.String("ingress-caddy-admin-addr", "localhost:2019", "default Caddy admin API address (host:port); per-host caddy_admin_addr in hosts/*.yaml overrides this. The admin API is unauthenticated, so keep :2019 on a trusted/private network or firewalled to the control plane")
		ingressInterval  = fs.Duration("ingress-reconcile-interval", 5*time.Minute, "periodic ingress drift-correction interval per host; 0 disables the periodic loop")

		operatorFile   = fs.String("operator-file", "", "if set, enable the admin UI and authenticate the single operator against this YAML file (username, password_hash)")
		uiSecureCookie = fs.Bool("ui-secure-cookie", false, "set the Secure flag on the UI session cookie (enable when serving the UI over HTTPS / behind TLS)")

		registryAddress  = fs.String("registry-address", "", "container registry host:port to browse (e.g. 100.64.0.23:5000); empty disables the registry browser feature entirely")
		registryAuth     = fs.String("registry-auth", "none", "registry auth mode: none or basic")
		registryUsername = fs.String("registry-username", "", "registry basic-auth username (only used when -registry-auth=basic)")
		registryPassword = fs.String("registry-password", "", "registry basic-auth password (only used when -registry-auth=basic); prefer the REGISTRY_PASSWORD env var instead — unlike every other secret input here (-operator-file, -keys-file, -spec-key-file are file-based for the same reason), a flag value is visible in /proc/<pid>/cmdline and a systemd ExecStart")
	)
	// Declared apart from the block above only because there are enough of them
	// to be worth a struct; every one of them defaults to off. See
	// server/registryprune.go.
	regPruneCfg := registryPruneFlags(fs)
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
	// seenHostIDs tracks every host id podman-api has ever known about in the
	// CURRENT run, so the SIGHUP reload path below can tell "genuinely new"
	// and "reappeared after a removal" apart from "already tracked" (#231
	// review findings #3 and #4 — one consistent mechanism for both). It is
	// mutated only inside applyHosts below, which holds hostsApplyMu — the
	// SIGHUP goroutine is no longer the only caller now that the host-rename
	// route triggers a reload from an HTTP handler.
	seenHostIDs := make(map[string]bool, len(hosts))
	for _, h := range hosts {
		seenHostIDs[h.ID] = true
	}
	keys, fp, err := loadKeys(*keysFile)
	if err != nil {
		return fmt.Errorf("keys: %w", err)
	}
	keyStore := auth.NewKeyStore(keys)
	log.Printf("keys loaded: %d entries, fingerprint=%s", len(keys), fp)

	if *volumeTransferTimeout <= 0 {
		return fmt.Errorf("-volume-transfer-timeout must be positive, got %s", *volumeTransferTimeout)
	}
	if *imagePullTimeout <= 0 {
		return fmt.Errorf("-image-pull-timeout must be positive, got %s", *imagePullTimeout)
	}
	client, err := podman.NewReal(hosts)
	if err != nil {
		return fmt.Errorf("podman: %w", err)
	}
	podman.SetVolumeTransferTimeout(*volumeTransferTimeout)
	podman.SetImagePullTimeout(*imagePullTimeout)
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

	if ids, err := migrateSeededTemplates(seedCtx, db, templates.Files); err != nil {
		return fmt.Errorf("migrate seeded templates: %w", err)
	} else if len(ids) > 0 {
		log.Printf("templates: rewrote %d untouched seed row(s) to the current shipped seed: %s", len(ids), strings.Join(ids, ", "))
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

	// `backup: none` became a hard veto (#249): before it, ANY non-empty marker
	// simply meant "marked", and this repo's own fixtures across five packages
	// carried `none` on PRIMARY DATA volumes — good evidence real templates do
	// too. Those instances keep returning 202 and completing green while the
	// blob set quietly loses a volume, and the only trace is one `skip-volume`
	// step buried in a job trail nobody reads on a success. Say it once, at
	// startup, where an upgrading operator can see the reinterpretation.
	//
	// Log-only and once per process: it is a property of the catalog, not of
	// any run, so failing startup would take a fleet down over a marker that
	// may well be exactly what the author meant, and logging it per backup
	// would bury it in noise.
	if tmpls, err := db.ListTemplates(seedCtx); err != nil {
		log.Printf("templates: listing for the `backup: none` audit failed: %v (skipping the audit)", err)
	} else if w := backupMarkerNoneWarning(tmpls); w != "" {
		log.Printf("WARNING: %s", w)
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
	var regPruneSched *registryprune.Scheduler
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

	// Built here, before the job registry, because the registry-prune handler
	// consumes it. registryBase is kept alongside the client: the prune's
	// ref-matching MUST derive from the same string the client talks to, and
	// re-deriving it anywhere else is how every tag-pinned image in the fleet
	// quietly stops being protected.
	var registryClient imgregistry.Client
	// registryPruneClient is the UNCACHED client. classifyAll builds the delete
	// plan and the cross-repo protected-digest set from ResolveTags; a listing up
	// to TagsCacheTTL old — warmed by anything that browsed the repo, including a
	// UI page load — can miss a protected tag pushed minutes ago and let its
	// manifest be deleted. See buildRegistryPrune's doc comment.
	// Typed CONCRETELY, not as imgregistry.Client: buildRegistryPrune's
	// parameter type is what makes "the prune never sees a cached listing" a
	// compile-time property rather than a convention or a runtime check.
	var registryPruneClient *imgregistry.HTTPClient
	var registryBase string
	if strings.TrimSpace(*registryAddress) != "" {
		auth := imgregistry.Auth{Mode: strings.TrimSpace(*registryAuth)}
		switch auth.Mode {
		case "none":
		case "basic":
			auth.Username = *registryUsername
			auth.Password = resolveRegistryPassword(*registryPassword, os.Getenv)
		default:
			return fmt.Errorf("registry: invalid -registry-auth %q (must be none or basic)", *registryAuth)
		}
		registryBase = *registryAddress
		if !strings.Contains(registryBase, "://") {
			registryBase = "http://" + registryBase
		}
		// Wrap in the TTL-caching decorator so both the API and UI share one
		// cache: Tags() resolves every tag's manifest (and every unique
		// digest's config blob), ~21s cold for the fleet's "engine" repo, and
		// the UI's list page fires it once per catalog repo on every single
		// page view. See imgregistry.CachingClient's doc comment.
		httpRegistry := imgregistry.NewHTTPClient(registryBase, auth)
		registryPruneClient = httpRegistry
		registryClient = imgregistry.NewCachingClient(httpRegistry, imgregistry.TagsCacheTTL)
	}

	pruneMetrics := obs.NewPruneMetrics(prometheus.DefaultRegisterer)
	jobMetrics := obs.NewJobMetrics(prometheus.DefaultRegisterer)

	// Absent unless explicitly enabled AND a registry client exists: nil
	// handler means no job kind, no scheduler, no behaviour.
	//
	// The collectors are registered only once the handler exists, not on the
	// flag alone: if they were registered on -registry-prune-enabled and the
	// build then returned nil for a missing registry client, the presence of
	// podman_api_registry_prune_* series would suggest a running feature that
	// is in fact off.
	var regPruneMetrics registryprune.Metrics
	if regPruneCfg.Enabled && registryPruneClient != nil {
		regPruneMetrics = obs.NewRegistryPruneMetrics(prometheus.DefaultRegisterer)
	}
	regPruneHandler, err := buildRegistryPrune(*regPruneCfg, registryBase, registryPruneClient, svc, db, client, regPruneMetrics, *inventoryInterval)
	if err != nil {
		return err
	}

	registry, reconcilers := buildJobRegistry(svc, client, db, *evacConc, pruneMetrics, jobMetrics, regPruneHandler)
	workers := *jobWorkers
	if workers <= 0 {
		workers = jobs.DefaultWorkers
	}
	if *jobVolumeWorkers < 0 {
		return fmt.Errorf("-job-volume-workers must be >= 0, got %d", *jobVolumeWorkers)
	}
	jobVolumeWorkersSet := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "job-volume-workers" {
			jobVolumeWorkersSet = true
		}
	})
	effectiveJobVolumeWorkers := *jobVolumeWorkers
	if effectiveJobVolumeWorkers >= workers {
		if jobVolumeWorkersSet {
			return fmt.Errorf("-job-volume-workers (%d) must be less than -job-workers (%d), else no workers are left for other job kinds", *jobVolumeWorkers, workers)
		}
		// Left at its default and it doesn't fit -job-workers. Rather than
		// dropping straight to 0 (fully disabling the reservation and
		// reproducing the exact starvation risk #238 fixes, with only a log
		// line as a signal), clamp down to the largest reservation that still
		// fits: workers-1, which guarantees the general pool always keeps at
		// least 1 worker — the same reasoning the explicit-value hard-error
		// above already uses. Only fall back to fully disabling (0) when
		// there's truly no room for any reservation (workers <= 1).
		if workers > 1 {
			log.Printf("job-volume-workers: default %d does not fit within -job-workers=%d, clamping the volume-transfer worker reservation down to %d (pass -job-volume-workers explicitly to override)", *jobVolumeWorkers, workers, workers-1)
			effectiveJobVolumeWorkers = workers - 1
		} else {
			log.Printf("job-volume-workers: default %d does not fit within -job-workers=%d, disabling the volume-transfer worker reservation entirely (no room for any reservation; pass -job-volume-workers explicitly to override)", *jobVolumeWorkers, workers)
			effectiveJobVolumeWorkers = 0
		}
	}
	runner := jobs.NewRunner(db, registry, workers)
	runner.Metrics = jobMetrics
	canceller = runner
	runner.SetReconcilers(reconcilers)
	if effectiveJobVolumeWorkers > 0 {
		runner.SetVolumeTransferPool(jobs.VolumeTransferKinds, effectiveJobVolumeWorkers)
	}
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

	// Two things enqueue a registry prune, and BOTH go through the scheduler:
	// its own ticker and POST /registry/prune (wired to regPruneSched below, not
	// to the job store). That is deliberate — the scheduler's in-flight scan plus
	// its enqueueSem are the single mechanism preventing two concurrent runs, and
	// two runs each classifying from a listing the other is mutating is exactly
	// the shape of the #64 incident. Any further trigger must enqueue through the
	// scheduler too; nothing may reach the job store directly.
	if regPruneHandler != nil {
		regPruneSched = buildRegistryPruneScheduler(*regPruneCfg, db)
		regPruneSched.Start(runnerCtx)
		mode := "DRY RUN (deletes nothing)"
		if !regPruneCfg.DryRun {
			mode = "DELETING"
		}
		blob := "blob GC off (manifests unlinked, blobs left recoverable)"
		if regPruneHandler.BlobGC != nil {
			blob = fmt.Sprintf("blob GC on %s: pod %s stops %s", regPruneCfg.Host, regPruneHandler.BlobGC.PodName, regPruneCfg.RegistryPod)
		}
		cadence := fmt.Sprintf("interval %s", regPruneCfg.Interval)
		if regPruneCfg.Interval <= 0 {
			cadence = "no scheduled runs (interval <=0); POST /registry/prune only"
		}
		log.Printf("registry prune enabled (%s, %s, max deletes/repo %d, %s; on-demand: POST /registry/prune)",
			cadence, mode, regPruneCfg.MaxDeletesPerRepo, blob)
	}

	var invPoller *inventory.Poller
	if *inventoryInterval > 0 {
		svc.EnableWarmInventory()
		hostIDs := func() []string {
			hs := *hostsHolder.Load()
			ids := make([]string, len(hs))
			for i, h := range hs {
				ids[i] = h.ID
			}
			return ids
		}
		invPoller = &inventory.Poller{
			Svc: svc, Interval: *inventoryInterval,
			Timeout: *inventoryTimeout, StatsTimeout: *containerStatsTO,
			// Detects a host reboot from its kernel uptime and re-converges
			// that host's stored specs when one is found — the runtime
			// counterpart to the one-shot startup boot converge below, which
			// only covers podman-api's own restart, not a managed host's
			// (#231).
			Boot: svc,
		}
		if *containerStats {
			invPoller.Stats = svc
		}
		// Checked here, not inside the -container-stats block below, so it is
		// evaluated on the one code path where all these budgets can ever be
		// spent back to back — a poller-enabled start — regardless of where the
		// sampler's own wiring lives. invPoller.BootTimeout is passed (not a
		// literal 0) so this stays correct if a flag for it is ever added;
		// today it is always the zero value, i.e. always the 5s default.
		if w := statsBudgetWarning(*inventoryInterval, *inventoryTimeout, *containerStatsTO, invPoller.BootTimeout, *containerStats); w != "" {
			log.Printf("WARNING: %s", w)
		}
		invPoller.Start(runnerCtx, hostIDs)
		// Volume sizing runs on its own, much slower loop: podman's system df
		// walks the whole store and must never delay an inventory refresh.
		invPoller.StartVolumeUsage(runnerCtx, hostIDs, svc, *volumeUsageInterval, *volumeUsageTimeout)
		// Same host list as the poller, so a host that has never been polled
		// still reports host_reachable 0 instead of being silently absent.
		registerInventoryMetrics(prometheus.DefaultRegisterer, svc, hostIDs, true)
		log.Printf("inventory poller enabled (interval %s, per-host timeout %s)", *inventoryInterval, *inventoryTimeout)
		// Both collectors register only inside this block: with the poller off
		// nothing samples, so an empty metric would lie rather than be absent.
		// Both also take hostIDs, so a host removed on SIGHUP stops emitting
		// instead of freezing at its last sample — neither cache is pruned on
		// host removal.
		if *containerStats {
			obs.NewStatsCollector(prometheus.DefaultRegisterer, svc, svc, hostIDs)
			log.Printf("container stats sampling enabled (per-host timeout %s)", *containerStatsTO)
		}
		if *volumeUsageInterval > 0 {
			obs.NewVolumeUsageCollector(prometheus.DefaultRegisterer, svc, svc, hostIDs)
			log.Printf("volume usage sampling enabled (interval %s, per-host timeout %s)", *volumeUsageInterval, *volumeUsageTimeout)
		}
	} else {
		setFlags := map[string]bool{}
		fs.Visit(func(f *flag.Flag) { setFlags[f.Name] = true })
		if w := pollerDisabledMetricsWarning(setFlags, *containerStats, *volumeUsageInterval); w != "" {
			log.Printf("WARNING: %s", w)
		}
	}

	if c.backupScheduler != nil {
		ctrl := &backupctl.Controller{Svc: svc, Jobs: db}
		runBackupScheduler(runnerCtx, c.backupScheduler, ctrl)
		log.Printf("backup scheduler enabled (commercial)")
	}

	// One-shot boot converge: covers podman-api's own (re)start. It does not
	// cover a managed host rebooting while podman-api keeps running — that
	// case is handled at runtime by the inventory poller's Boot field above,
	// when the poller is enabled (#231).
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

	metrics := obs.New(prometheus.DefaultRegisterer, prometheus.DefaultGatherer)

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

	// The UI and browse routes get the CACHED client; the prune got the uncached
	// one above. POST /registry/prune is wired to the scheduler, not the job
	// store, so the on-demand trigger reuses the same in-flight guard the ticker
	// does rather than walking past it.
	// applyHosts makes a freshly-loaded host list live EVERYWHERE, and is the
	// single definition of what that means: the podman client's own host map,
	// the instance service's, and hostsHolder (read by the periodic ingress
	// reconcile, the prune scheduler's policies and the boot-converge
	// goroutine). Anything that changes the host set — SIGHUP, and the host
	// rename route via the hosts reloader below — must go through here; a
	// caller that updates only one of the three leaves the others working an
	// id that no longer exists (final-review findings #1 and #2).
	//
	// Called from the SIGHUP goroutine and from HTTP handlers, so it locks:
	// seenHostIDs is plain map state.
	var hostsApplyMu sync.Mutex
	applyHosts := func(newHosts []config.Host) {
		hostsApplyMu.Lock()
		defer hostsApplyMu.Unlock()

		client.SetHosts(newHosts)
		svc.SetHosts(newHosts)
		hostsHolder.Store(&newHosts)
		draining := 0
		for _, hh := range newHosts {
			if hh.Drain {
				draining++
			}
		}
		// A host id this process has not tracked before — either genuinely
		// new, or previously removed by an earlier SIGHUP and now re-added, or
		// the target of a rename — needs a boot-converge pass of its own
		// (#231 review findings #3 and #4). Neither the one-shot startup
		// converge (only ran once, over the hosts loaded at start) nor the
		// inventory poller's own reboot detector (its first observation of any
		// host is deliberately baseline-only, see inventory.Poller.checkBoot)
		// ever reconciles such a host, so without this, a host whose pods were
		// already down when it was (re-)added stays down until it reboots a
		// SECOND time after being added.
		var newlySeen []string
		newlySeen, seenHostIDs = diffNewlySeenHosts(seenHostIDs, newHosts)
		log.Printf("hosts applied: %d entries (%d draining)", len(newHosts), draining)
		if len(newlySeen) > 0 {
			log.Printf("hosts: boot-converging %d newly-seen host(s): %v", len(newlySeen), newlySeen)
			go func(ids []string) {
				var wg sync.WaitGroup
				for _, id := range ids {
					wg.Add(1)
					go func(hostID string) {
						defer wg.Done()
						svc.ReconcileSpecsOnHost(runnerCtx, hostID)
					}(id)
				}
				wg.Wait()
			}(newlySeen)
		}
	}

	routerOpts := []api.RouterOption{}
	if regPruneSched != nil {
		routerOpts = append(routerOpts, api.WithRegistryPruner(regPruneSched))
	}
	routerOpts = append(routerOpts, api.WithHostRenamer(config.HostsDir(*hostsDir)))
	// The rename handler does not know about client/hostsHolder/pollers: it
	// just asks the server to re-read hosts/*.yaml, exactly as a SIGHUP would,
	// once the rename has been committed to the file and the store.
	routerOpts = append(routerOpts, api.WithHostsReloader(func() error {
		newHosts, err := config.LoadHosts(*hostsDir)
		if err != nil {
			return err
		}
		applyHosts(newHosts)
		return nil
	}))
	router := api.NewRouter(svc, jobStore, keyStore, combined, nil, canceller, Version, registryClient, routerOpts...)

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
		uiApp, err = ui.New(ui.Config{Svc: svc, Jobs: jobStore, Auth: authr, Secure: *uiSecureCookie, TokenMgr: tokenMgr, Version: Version, Registry: registryClient})
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
				applyHosts(newHosts)
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
		if regPruneSched != nil {
			regPruneSched.Wait()
		}
		if invPoller != nil {
			invPoller.Wait()
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

// registerInventoryMetrics registers the per-container inventory collector, but
// only when the background poller is running. With the poller disabled the warm
// cache is filled lazily by reads, so a snapshot would report never-read hosts
// as cold — a metric that lies is worse than one that is absent.
func registerInventoryMetrics(reg prometheus.Registerer, src obs.InventorySource, hosts func() []string, pollerEnabled bool) *obs.InventoryCollector {
	if !pollerEnabled {
		return nil
	}
	return obs.NewInventoryCollector(reg, src, hosts)
}

// backupMarkerNoneWarning returns the one-time startup line naming every
// template that declares a `backup: none` volume — or "" when the catalog has
// none, which is the common case and must stay silent.
//
// The comparison is instance.IsBackupMarkerNone, not string equality, so a
// near-miss spelling stored before the registration validator existed (`None`,
// `"none "`) is reported here too: it vetoes now, and that is exactly the
// reinterpretation an operator needs told.
//
// Templates and their volumes are both sorted so the line is stable across
// restarts — an operator diffing two boots should see a change only when the
// catalog changed.
//
// This covers the catalog AS IT STANDS AT BOOT and nothing more. A template
// registered or edited against a running daemon is audited on the write path
// instead (instance.CreateTemplate/UpdateTemplate), which is where the operator
// making the change can actually read it.
func backupMarkerNoneWarning(tmpls []store.Template) string {
	var lines, allVetoed []string
	for _, t := range tmpls {
		vols := instance.BackupMarkerNoneVolumes(t.Meta)
		if len(vols) == 0 {
			continue
		}
		sort.Strings(vols)
		lines = append(lines, fmt.Sprintf("%s[%s]", t.Meta.ID, strings.Join(vols, " ")))
		if len(vols) == len(t.Meta.Volumes) {
			allVetoed = append(allVetoed, t.Meta.ID)
		}
	}
	if len(lines) == 0 {
		return ""
	}
	sort.Strings(lines)
	msg := fmt.Sprintf("`backup: none` is a HARD VETO: these templates declare volume(s) that will NEVER be exported by any backup, "+
		"and a backup of such an instance still completes green with that volume absent from the blob set — %s. "+
		"If a volume there holds data you need restorable, remove its `none` marker.", strings.Join(lines, ", "))
	// A template whose volumes are ALL vetoed is a categorically stronger
	// statement than "this template loses a volume": CheckBackupable refuses
	// every backup of every instance of it, so those instances have no backup
	// path at all — manual, scheduled or otherwise — and the operator finds out
	// as a 400 at the next backup window unless it is said here. Named
	// separately, and sorted for the same restart-stability reason.
	if len(allVetoed) > 0 {
		sort.Strings(allVetoed)
		msg += fmt.Sprintf(" WORSE: EVERY volume declared by %s is vetoed, so instances of %s CANNOT BE BACKED UP AT ALL — "+
			"every backup request is rejected outright (invalid_backup_scope), including the scheduler's.",
			strings.Join(allVetoed, ", "),
			map[bool]string{true: "that template", false: "those templates"}[len(allVetoed) == 1])
	}
	return msg
}

// pollerDisabledMetricsWarning returns the line to log when the inventory
// poller is off but the operator explicitly asked for metrics only the poller
// can produce — or "" when there is nothing worth saying. Both resource-metric
// collectors register inside the poller-enabled block, so with the poller off
// they emit nothing and log nothing; without this the operator gets no metrics
// and no explanation. (#209 review)
//
// It keys on flags EXPLICITLY SET (flag.FlagSet.Visit), not on "value differs
// from its default". Both flags are ON by default, so a default-vs-actual test
// would fire on every deployment that has deliberately turned the poller off —
// noise, not a diagnosis. And the non-default values that do exist,
// -container-stats=false and -volume-usage-interval=0, are explicit *disables*:
// the operator wants no metrics, so there is nothing to warn about. That leaves
// exactly the confusing case — asked for it, won't get it.
func pollerDisabledMetricsWarning(setFlags map[string]bool, containerStats bool, volumeUsageInterval time.Duration) string {
	var asked []string
	if setFlags["container-stats"] && containerStats {
		asked = append(asked, "-container-stats")
	}
	if setFlags["volume-usage-interval"] && volumeUsageInterval > 0 {
		asked = append(asked, "-volume-usage-interval")
	}
	if len(asked) == 0 {
		return ""
	}
	return fmt.Sprintf("%s set but the inventory poller is disabled (-inventory-refresh-interval=0): "+
		"container resource and volume usage metrics require the poller and will NOT be exported",
		strings.Join(asked, " and "))
}

// diffNewlySeenHosts compares the host ids this process has tracked so far
// (seen) against the freshly-reloaded host list (current, from a SIGHUP
// config.LoadHosts), and returns:
//   - newlySeen: ids present in current but absent from seen — a host id this
//     process has never tracked before, in this run. That covers both a
//     genuinely new host and one removed by an earlier reload and now
//     re-added: the earlier removal already dropped it from seen (it is
//     replaced wholesale below, not merged), so it looks identical to "new"
//     here. That equivalence is deliberate (#231 review findings #3 and #4):
//     a host that rebooted while absent needs exactly the same boot-converge
//     pass a truly-new host does, since neither the one-shot startup converge
//     nor the poller's baseline-only first observation ever covers it.
//   - nextSeen: the tracked set to carry forward — current's ids, replacing
//     seen entirely (not merged with it), so a host that drops out of a
//     later reload is "unseen" again and gets re-converged if it returns.
func diffNewlySeenHosts(seen map[string]bool, current []config.Host) (newlySeen []string, nextSeen map[string]bool) {
	nextSeen = make(map[string]bool, len(current))
	for _, h := range current {
		nextSeen[h.ID] = true
		if !seen[h.ID] {
			newlySeen = append(newlySeen, h.ID)
		}
	}
	return newlySeen, nextSeen
}

// statsBudgetWarning returns the line to log when the per-host inventory,
// stats and boot-probe budgets together can reach or exceed the poll
// interval — or "" when there is nothing worth saying.
//
// Until #212 the stats sample shared the refresh's hctx, which held the per-host
// bound at exactly -inventory-refresh-timeout by construction; the price was
// permanent starvation of the sampler on a host whose sweep ate that budget.
// Now each of the three steps (refresh, stats sample, boot-reboot probe) gets
// its own budget, so the true per-host bound is their sum, and nothing in the
// type system stops an operator tuning past the interval. This is that missing
// enforcement — deliberately a warning, not a fatal: a long
// -inventory-refresh-timeout is a legitimate choice on a slow fleet, and the
// consequence (a stretched poll cadence, visible as
// podman_api_inventory_age_seconds) is degradation, not breakage.
//
// The boot-probe term is folded in UNCONDITIONALLY once the poller itself is
// enabled: unlike the stats sampler (gated by -container-stats), server.go
// wires Boot: svc with no flag to turn it off, so tick() always pays this
// third budget (#231 review finding #2). With the shipped defaults —
// Timeout=20s, StatsTimeout=5s, Interval=30s — the old two-term sum (25s) was
// silently fine while the poller's real per-host cost, 30s including the 5s
// boot probe, was already AT the interval; this is exactly the hole that let
// that pass unnoticed.
//
// The stats term is silent when the sampler is off: with -container-stats=false
// no stats call is ever made, so that budget cannot be spent and adding it
// would be noise. Every term is silent with the poller off, for the same
// reason — nothing ticks, and pollerDisabledMetricsWarning already explains
// that case.
//
// Both the stats and boot terms reason about their EFFECTIVE timeout, not the
// raw flag/field: the poller maps a non-positive value to its own default
// (inventory.EffectiveStatsTimeout / inventory.EffectiveBootTimeout), so e.g.
// -container-stats-timeout=0 with a 28s refresh timeout and a 30s interval
// really spends 33s (28+5) while a raw-value check would compute 28s and stay
// silent — a hole in exactly the invariant this function exists to police.
//
// -inventory-refresh-timeout gets no such normalisation because the poller
// applies none: Poller.Timeout is passed to context.WithTimeout verbatim, where
// 0 means a context that is already expired rather than a default. That is a
// pre-existing sharp edge (a refresh timeout of 0 fails every refresh
// immediately, loudly and on every host) and squarely outside #212; the honest
// sum for that case is the one computed here.
func statsBudgetWarning(interval, timeout, statsTimeout, bootTimeout time.Duration, statsEnabled bool) string {
	if interval <= 0 {
		return ""
	}
	effBoot := inventory.EffectiveBootTimeout(bootTimeout)
	sum := timeout + effBoot
	terms := fmt.Sprintf("-inventory-refresh-timeout (%s) + boot-reboot-probe timeout (%s)", timeout, effBoot)
	if statsEnabled {
		effStats := inventory.EffectiveStatsTimeout(statsTimeout)
		sum += effStats
		terms = fmt.Sprintf("%s + -container-stats-timeout (%s)", terms, effStats)
	}
	if sum < interval {
		return ""
	}
	return fmt.Sprintf("%s = %s, which is not under -inventory-refresh-interval (%s): "+
		"a slow host can spend a whole interval on one tick and stretch the poll cadence "+
		"for every host, inflating podman_api_inventory_age_seconds. Lower a timeout or "+
		"raise the interval",
		terms, sum, interval)
}

// buildJobRegistry assembles the job kind -> handler table. regPrune is nil
// unless the registry prune feature is enabled, and a nil handler must leave
// the kind ABSENT rather than registered-and-broken — a registered nil would
// be a typed-nil interface that panics on the first tick.
//
// When registering a NEW job kind here, consider whether it belongs in
// jobs.VolumeTransferKinds (internal/jobs/runner.go): any handler that
// streams a large volume (calls VolumeExport/VolumeImport/CopyVolume) or
// fans out child work in-process on its own claimed worker (like evacuate
// does) is a candidate for the reserved volume-transfer worker pool (#238).
// There's no mechanical way to detect this from the handler's shape, so it
// has to be a conscious decision every time a kind is added here.
func buildJobRegistry(svc *instance.Service, client podman.Client, db store.DB, evacConc int, pruneMetrics *obs.PruneMetrics, jobMetrics *obs.JobMetrics, regPrune *registryprune.Handler) (jobs.Registry, jobs.Reconcilers) {
	reg := jobs.Registry{
		"migrate":      &migrate.Handler{Svc: svc, Metrics: jobMetrics},
		"evacuate":     &evacuate.Handler{Svc: svc, Jobs: db, Concurrency: evacConc, Metrics: jobMetrics},
		"prune":        &prune.Handler{Client: client, Jobs: db, Metrics: pruneMetrics},
		"backup":       &backuppkg.Handler{Svc: svc},
		"restore":      &backuppkg.RestoreHandler{Svc: svc},
		"pitr-restore": &backuppkg.PITRRestoreHandler{Svc: svc},
	}
	if regPrune != nil {
		reg[registryprune.JobKind] = regPrune
	}
	// No registry-prune reconciler, deliberately: a run interrupted by a
	// restart has already persisted whatever it deleted, and re-driving it
	// from a half-finished state is strictly worse than letting the scheduler
	// start a clean run from a fresh catalog — same call as "prune".
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

// legacySeedVolumeMarkers records, per seed template id, the volume `backup:`
// markers a PREVIOUS shipped seed carried, keyed by volume name. It is the
// evidence a stored row is the untouched old seed rather than an operator's
// own declaration, and nothing else — see migrateSeededTemplates.
//
// #249: `postgres` shipped `data: backup: none` back when a marker was opaque
// ("non-empty means marked") and `none` therefore meant nothing in particular.
// `none` is now a HARD VETO, which makes that row's every volume vetoed, and
// CheckBackupable rejects such an instance outright — so on upgrade the exact
// population with working postgres backups would get a permanent 400
// `invalid_backup_scope` on every POST .../backup and every scheduler tick.
var legacySeedVolumeMarkers = map[string]map[string]string{
	"postgres": {"data": "none"},
}

// seedMigratedOrigin is the provenance stamped on a row migrateSeededTemplates
// has rewritten. It is the applied-once marker: the migration only considers
// rows whose origin is still "seed", so a row carrying this value is never
// revisited on a later boot. It still reads as "we shipped this template", not
// "a user created it" — the distinction Origin exists to record.
const seedMigratedOrigin = "seed-migrated"

// migrateSeededTemplates rewrites a stored template row to the currently
// shipped seed — but ONLY when the row is recognisably the untouched previous
// seed. seedTemplates returns early once the catalog is non-empty, so without
// this a fix to a bundled template reaches fresh installs and nothing else.
//
// The bar for "untouched" is deliberately high: the row's origin must still be
// "seed", its body must match the shipped body byte-for-byte, and its meta must
// match the previous seed's meta exactly (compared through the same JSON
// encoding the store round-trips it in). Any divergence — an edited body, an
// added parameter, a marker the operator chose themselves — leaves the row
// alone. Removing a `backup: none` the operator never wrote, from a row they
// never touched, is correcting OUR reinterpretation of OUR own default;
// anything less certain would be overwriting their intent, so it is not done.
//
// It is ONE-SHOT per row, and that is load-bearing rather than an efficiency
// nicety (review-5 finding 2). An operator who decides they genuinely do not
// want postgres `data` backed up and sets `backup: none` produces a row that is
// BYTE-IDENTICAL to the untouched legacy seed — same origin, same body, same
// meta — so the three-part guard above cannot tell their deliberate veto from
// the row this migration exists to correct. Re-running every boot would revert
// that veto, silently and in the fail-open direction, at every single restart.
// Rewriting a row therefore stamps its Origin seedMigratedOrigin, which the
// `Origin != "seed"` guard then skips forever: the correction happens at most
// once per install, and any `none` set AFTER it is the operator's own and is
// never revisited.
//
// Origin is the marker rather than a schema_migrations table because it is
// already persisted per row, already survives UpdateTemplate (which preserves
// the stored Origin), and the fact being recorded is genuinely per row — "has
// THIS row been corrected" — not per database.
//
// Returns the ids actually rewritten.
func migrateSeededTemplates(ctx context.Context, db store.TemplateStore, fsys fs.FS) ([]string, error) {
	seeds, err := store.ParseSeeds(fsys)
	if err != nil {
		return nil, err
	}
	var migrated []string
	for _, seed := range seeds {
		oldMarkers, ok := legacySeedVolumeMarkers[seed.Meta.ID]
		if !ok {
			continue
		}
		stored, err := db.GetTemplate(ctx, seed.Meta.ID)
		if errors.Is(err, store.ErrNotFound) {
			continue
		}
		if err != nil {
			return migrated, err
		}
		legacy, err := legacySeedMeta(seed.Meta, oldMarkers)
		if err != nil {
			return migrated, err
		}
		same, err := sameStoredMeta(stored.Meta, legacy)
		if err != nil {
			return migrated, err
		}
		if stored.Origin != "seed" || stored.Body != seed.Body || !same {
			continue // operator-edited (or already current): never touched
		}
		upd := stored
		upd.Meta = seed.Meta
		upd.Body = seed.Body
		upd.Origin = seedMigratedOrigin
		if err := db.PutTemplate(ctx, upd); err != nil {
			return migrated, err
		}
		migrated = append(migrated, seed.Meta.ID)
	}
	return migrated, nil
}

// legacySeedMeta returns m with the named volumes' markers set back to what the
// previous seed shipped. The volume slice is copied, so the caller's meta (the
// live seed) is never mutated.
func legacySeedMeta(m render.Meta, markers map[string]string) (render.Meta, error) {
	vols := make([]render.Volume, len(m.Volumes))
	copy(vols, m.Volumes)
	for i := range vols {
		if marker, ok := markers[vols[i].Name]; ok {
			vols[i].Backup = marker
		}
	}
	m.Volumes = vols
	return m, nil
}

// sameStoredMeta compares two metas through the JSON encoding the template
// store persists them in, so the comparison sees exactly what a round trip
// through the DB preserves and cannot be tripped by an unexported or
// non-comparable field.
func sameStoredMeta(a, b render.Meta) (bool, error) {
	ja, err := json.Marshal(a)
	if err != nil {
		return false, err
	}
	jb, err := json.Marshal(b)
	if err != nil {
		return false, err
	}
	return bytes.Equal(ja, jb), nil
}

// resolveRegistryPassword picks the registry basic-auth password, preferring
// the REGISTRY_PASSWORD env var over the -registry-password flag: unlike
// every other secret input this server takes (-operator-file, -keys-file,
// -spec-key-file are all file-based for the same reason), a flag value is
// visible in /proc/<pid>/cmdline and a systemd unit's ExecStart. The flag is
// kept, not removed, for backward-compat/simplicity — this is additive.
// getenv is injected for testability (os.Getenv in production).
func resolveRegistryPassword(flagVal string, getenv func(string) string) string {
	if envPass := getenv("REGISTRY_PASSWORD"); envPass != "" {
		return envPass
	}
	return flagVal
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

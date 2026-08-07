package registryprune

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/iotready/podman-api/internal/jobs"
	"github.com/iotready/podman-api/internal/podman"
)

// PodRunner is the slice of podman.Client Stage B needs. Narrow on purpose:
// this stage stops the fleet's only container registry, and the smaller the
// surface it holds, the smaller the set of things a bug here can do.
type PodRunner interface {
	PlayKube(ctx context.Context, hostID, yaml string, replace bool, networks ...string) error
	WaitForPodCompletion(ctx context.Context, hostID, podName string, timeout time.Duration) (int, error)
	PodStop(ctx context.Context, hostID, name string) error
	PodStart(ctx context.Context, hostID, name string) error
	PodRemove(ctx context.Context, hostID, name string, force bool) error
	ContainerExec(ctx context.Context, hostID, container string, cmd []string) (podman.ExecResult, error)
}

const (
	defaultGCImage    = "registry:2"
	defaultGCPodName  = "registry-blob-gc"
	defaultConfigPath = "/etc/docker/registry/config.yml"
	defaultMountPath  = "/var/lib/registry"
	defaultGCTimeout  = 30 * time.Minute
	// restartGrace bounds the deferred restart's own context. It is derived
	// from context.WithoutCancel so a cancelled or timed-out job still gets
	// the registry back: the one outcome this stage must never produce is a
	// fleet with no registry.
	restartGrace = 2 * time.Minute
	// restartAttempts is how many times PodStart is tried before the stage
	// gives up and fails the job. More than one because the alternative to a
	// retry is bleak: the scheduler's failureBackoff is an hour, and it is
	// only a whole prune run that would incidentally try again, so a single
	// transient start failure would mean an hour-long registry outage.
	restartAttempts = 3
	// sizeTimeout bounds one `du` exec. Sizing gets its own context rather
	// than sharing the restart grace: `du` over a hundreds-of-GB registry can
	// legitimately take minutes, and it must never be the reason the restart
	// or the GC pod teardown runs out of time.
	sizeTimeout = 3 * time.Minute
)

// restartRetryDelay is the pause between restart attempts. A var, not a const,
// so tests can collapse it.
var restartRetryDelay = 2 * time.Second

// Result is Stage B's measured outcome, returned as data rather than only as
// formatted job steps: the bytes-reclaimed metric has no other data path, and
// re-parsing a step string is not one.
//
// Reclaimed is a FLOOR, not a measurement. The "after" size is read once the
// registry is serving pushes again, so anything written in between counts
// against the reclaim; a negative difference is clamped to zero.
type Result struct {
	Before    int64
	After     int64
	Reclaimed int64
	// Measured is true only when BOTH sizes were read. A false Measured means
	// the byte fields say nothing — they must not be reported as zero bytes
	// reclaimed.
	Measured bool
}

// BlobGC is Stage B: reclaiming the blobs that Stage A's manifest deletions
// only unlinked.
//
// Deleting a manifest through the registry API unlinks it; the bytes stay on
// disk until the registry's own `garbage-collect` runs, and that must not run
// against a live registry (a push mid-GC can have its freshly-written blob
// swept). So the sequence is stop → GC → start, and the start is deferred.
type BlobGC struct {
	Podman PodRunner
	// HostID is the host carrying the registry.
	HostID string
	// RegistryPod is the podman pod name of the managed registry instance.
	// Lifecycle is native podman, not systemd: podman-api speaks libpod over
	// an SSH tunnel and has no shell on the far end, and the quadlet's
	// Restart=always would have systemd restart the registry mid-GC anyway.
	RegistryPod string
	// RegistryContainer is the container to measure storage size in. Empty
	// disables sizing; sizing never fails the job either way.
	RegistryContainer string
	// StoragePath is the registry storage directory ON THE HOST, hostPath-
	// mounted into the GC pod. Must be absolute.
	StoragePath string

	// MountPath is where StoragePath is mounted inside the GC POD. It is not
	// where the registry container mounts it — see SizePath, which is measured
	// in a different container and is deliberately a separate field. The two
	// coincide only because both default to /var/lib/registry.
	MountPath string
	// SizePath is the storage directory as seen inside the REGISTRY container
	// (its `rootdirectory`), used only for `du`. Defaults to
	// /var/lib/registry — NOT to MountPath, which would silently re-couple
	// the two and make sizing measure a path that does not exist in the
	// registry container the moment MountPath is configured.
	SizePath string

	// ConfigPath, Image, PodName and Timeout all default; see the default*
	// constants.
	ConfigPath string
	Image      string
	// PodName is the one-shot GC pod's name. It must NOT be the registry's own
	// pod name; see validate.
	PodName string
	Timeout time.Duration
}

func (g *BlobGC) mountPath() string {
	if g.MountPath != "" {
		return g.MountPath
	}
	return defaultMountPath
}

func (g *BlobGC) sizePath() string {
	if g.SizePath != "" {
		return g.SizePath
	}
	return defaultMountPath
}

func (g *BlobGC) configPath() string {
	if g.ConfigPath != "" {
		return g.ConfigPath
	}
	return defaultConfigPath
}

func (g *BlobGC) image() string {
	if g.Image != "" {
		return g.Image
	}
	return defaultGCImage
}

func (g *BlobGC) podName() string {
	if g.PodName != "" {
		return g.PodName
	}
	return defaultGCPodName
}

func (g *BlobGC) timeout() time.Duration {
	if g.Timeout > 0 {
		return g.Timeout
	}
	return defaultGCTimeout
}

func (g *BlobGC) validate() error {
	if g.Podman == nil {
		return errors.New("blob GC has no podman client")
	}
	if g.HostID == "" {
		return errors.New("blob GC has no host")
	}
	if g.RegistryPod == "" {
		return errors.New("blob GC has no registry pod name (it cannot stop what it cannot name)")
	}
	if g.StoragePath == "" || !strings.HasPrefix(g.StoragePath, "/") {
		return fmt.Errorf("blob GC storage path %q must be an absolute host path", g.StoragePath)
	}
	// The whole-registry-destroyed guard. The GC pod is played with
	// replace=true and force-removed afterwards, so pointing it at the
	// registry's own pod name would replace the registry instance and then
	// DELETE it — gone, not down, with no reconciler in the core to rebuild
	// it. A copy-paste between two Task 7 flag defaults is all it would take.
	//
	// EqualFold + TrimSpace, matching the server's own startup guard. This one
	// is the defence-in-depth backstop — it must catch everything that guard
	// does and then some, because it is what protects a BlobGC constructed
	// from anywhere else. Plain == made it the WEAKER of the two, which is the
	// opposite of what a backstop is for: RegistryPod "Registry-Main" against
	// PodName "registry-main" would sail through and destroy the registry.
	if strings.EqualFold(strings.TrimSpace(g.podName()), strings.TrimSpace(g.RegistryPod)) {
		return fmt.Errorf("blob GC pod name %q is the registry's own pod name %q: "+
			"playing it would replace and then destroy the registry instance", g.podName(), g.RegistryPod)
	}
	return nil
}

// Reclaim performs Stage B. It is a no-op (and never touches the registry) on
// a dry run or when the payload sets SkipBlobGC.
//
// The named returns are load-bearing: the deferred restart runs during a panic
// unwind, fills in the after-size, AND can turn "GC succeeded but the registry
// did not come back" into a job failure.
func (g *BlobGC) Reclaim(ctx context.Context, jc *jobs.JobContext, p Payload) (res Result, err error) {
	if p.DryRun {
		jc.Step("blob-gc", "skipped: dry run (no pod played, registry untouched)")
		return Result{}, nil
	}
	if p.SkipBlobGC {
		jc.Step("blob-gc", "skipped: skip_blob_gc set; manifests are unlinked but blobs remain recoverable")
		return Result{}, nil
	}
	if verr := g.validate(); verr != nil {
		// Before anything is stopped. A misconfigured stage must never take
		// the registry down for a GC it cannot run.
		jc.Step("blob-gc", "ABORTED: "+verr.Error())
		return Result{}, verr
	}

	before, beforeOK := g.storageBytes(ctx)
	if beforeOK {
		res.Before = before
		jc.Step("blob-gc:size-before", fmt.Sprintf("%d byte(s) on disk at %s", before, g.StoragePath))
	} else {
		jc.Step("blob-gc:size-before", "size unavailable (GC proceeds; sizing is diagnostic only)")
	}

	// The restart is deferred BEFORE the stop is attempted, deliberately. A
	// PodStop that returns an error may still have stopped the pod, so the
	// only safe assumption is that the registry might be down from here on.
	// Deferring first means every exit — error, panic, or success — passes
	// through the restart.
	played := false
	defer func() {
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), restartGrace)
		defer cancel()
		// Restarting the registry is the FIRST thing this path does — before
		// sizing, before tearing the GC pod down — so nothing slow or failing
		// can sit between an aborted GC and the registry coming back. That is
		// also why the GC pod's removal lives here rather than in its own
		// defer: a later-registered defer would run first.
		serr := g.startRegistry(rctx, jc)
		if played {
			// Best-effort teardown of the exited one-shot pod. A leftover pod
			// is not a failure (PlayKube replaces), so this never changes the
			// job's result.
			if rerr := g.Podman.PodRemove(rctx, g.HostID, g.podName(), true); rerr != nil &&
				!errors.Is(rerr, podman.ErrNotFound) {
				log.Printf("registryprune: removing blob-GC pod %s on %s: %v", g.podName(), g.HostID, rerr)
			}
		}
		if serr != nil {
			jc.Step("blob-gc:registry-start", fmt.Sprintf(
				"REGISTRY DID NOT RESTART after %d attempt(s): %v", restartAttempts, serr))
			log.Printf("registryprune: REGISTRY DID NOT RESTART on %s/%s after %d attempts: %v",
				g.HostID, g.RegistryPod, restartAttempts, serr)
			if err == nil {
				err = fmt.Errorf("restart registry %s on %s after blob GC: %w", g.RegistryPod, g.HostID, serr)
			}
			return
		}
		// PodStart returning nil means libpod ACCEPTED the start, not that the
		// registry is serving. Nothing here waits for a ready registry; the
		// claim is "the start was accepted", and an instance that then fails
		// to come up is the inventory poller's and #220's alerting's business.
		jc.Step("blob-gc:registry-start", "registry restarted")

		// The after-size gets its own context, not the remainder of the
		// restart grace: `du` over a large registry can take minutes, and it
		// must never be able to eat the time the restart and teardown need.
		sctx, scancel := context.WithTimeout(context.WithoutCancel(ctx), sizeTimeout)
		defer scancel()
		if after, ok := g.storageBytes(sctx); ok {
			res.After = after
			detail := fmt.Sprintf("%d byte(s) on disk at %s", after, g.StoragePath)
			if beforeOK {
				reclaimed := before - after
				if reclaimed < 0 {
					// The registry has been serving pushes again since the
					// restart above, so it can legitimately have grown.
					reclaimed = 0
				}
				res.Reclaimed = reclaimed
				res.Measured = true
				detail += fmt.Sprintf("; at least %d byte(s) reclaimed "+
					"(a floor: the registry is serving pushes again by the time this is read)", reclaimed)
			}
			jc.Step("blob-gc:size-after", detail)
		} else {
			jc.Step("blob-gc:size-after", "size unavailable (GC outcome above is authoritative)")
		}
	}()

	if serr := g.Podman.PodStop(ctx, g.HostID, g.RegistryPod); serr != nil {
		// Do NOT proceed: garbage-collect against a live registry is the
		// blob-corruption race this whole stage is shaped around.
		jc.Step("blob-gc:registry-stop", "ABORTED: "+serr.Error())
		return res, fmt.Errorf("stop registry %s on %s before blob GC: %w", g.RegistryPod, g.HostID, serr)
	}
	jc.Step("blob-gc:registry-stop", "registry stopped for garbage collection")

	manifest, merr := g.manifest()
	if merr != nil {
		return res, merr
	}
	// Set BEFORE the call, not after it: a PlayKube that fails partway can
	// still have created the pod, and the teardown must cover that.
	played = true
	if perr := g.Podman.PlayKube(ctx, g.HostID, manifest, true); perr != nil {
		jc.Step("blob-gc:play", "FAILED: "+perr.Error())
		return res, fmt.Errorf("play blob-GC pod %s on %s: %w", g.podName(), g.HostID, perr)
	}
	jc.Step("blob-gc:play", fmt.Sprintf("one-shot pod %s started (%s, restartPolicy Never)", g.podName(), g.image()))

	code, werr := g.Podman.WaitForPodCompletion(ctx, g.HostID, g.podName(), g.timeout())
	if werr != nil {
		// The three cases stay distinct: a timeout is "we gave up watching"
		// (the GC may still be running), a vanished container is "we never
		// observed it finish", and neither is exit 0. Collapsing any of them
		// into success would report a GC that did not happen.
		switch {
		case errors.Is(werr, podman.ErrWaitTimeout):
			jc.Step("blob-gc:wait", fmt.Sprintf("FAILED: garbage collection did not finish within %s", g.timeout()))
		default:
			jc.Step("blob-gc:wait", "FAILED: "+werr.Error())
		}
		return res, fmt.Errorf("await blob-GC pod %s on %s: %w", g.podName(), g.HostID, werr)
	}
	if code != 0 {
		jc.Step("blob-gc:wait", fmt.Sprintf("FAILED: garbage-collect exited %d", code))
		return res, fmt.Errorf("blob GC pod %s on %s exited %d", g.podName(), g.HostID, code)
	}
	jc.Step("blob-gc:wait", "garbage-collect exited 0")
	return res, nil
}

// startRegistry brings the registry back, retrying a failed PodStart.
//
// The retry is not politeness. Without it, one transient start failure leaves
// the registry down until the scheduler's next run — failureBackoff is an hour
// (scheduler.go), and even that only tries again as a side effect of a whole
// prune. Attempts are bounded and every one is logged, so a genuinely broken
// start still fails the job loudly rather than looping.
//
// ctx is the restart-grace context, already derived with WithoutCancel, so a
// cancelled job does not abort the retries. If it does expire mid-backoff the
// loop stops early and returns the last error.
func (g *BlobGC) startRegistry(ctx context.Context, jc *jobs.JobContext) error {
	var last error
	for attempt := 1; attempt <= restartAttempts; attempt++ {
		last = g.Podman.PodStart(ctx, g.HostID, g.RegistryPod)
		if last == nil {
			if attempt > 1 {
				jc.Step("blob-gc:registry-start", fmt.Sprintf("registry start succeeded on attempt %d", attempt))
			}
			return nil
		}
		log.Printf("registryprune: restarting registry %s on %s, attempt %d/%d failed: %v",
			g.RegistryPod, g.HostID, attempt, restartAttempts, last)
		if attempt == restartAttempts {
			break
		}
		select {
		case <-ctx.Done():
			return last
		case <-time.After(restartRetryDelay):
		}
	}
	return last
}

// gcPod is the manifest, built as data and marshalled, so a path with YAML
// metacharacters in it cannot rewrite the document.
type gcPod struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Metadata   struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	Spec gcPodSpec `yaml:"spec"`
}

type gcPodSpec struct {
	// RestartPolicy Never is what makes this a one-shot job. The OSS core
	// never sets or overrides restartPolicy, so what is written here is what
	// podman sees; without it the pod would re-run garbage-collect forever.
	RestartPolicy string         `yaml:"restartPolicy"`
	Containers    []gcContainer  `yaml:"containers"`
	Volumes       []gcHostVolume `yaml:"volumes"`
}

type gcContainer struct {
	Name string `yaml:"name"`
	// Image is registry:2 — the same image the registry itself runs, so the
	// GC binary is byte-identical to the one that wrote the blobs.
	Image string `yaml:"image"`
	// Args, NOT command: registry:2's entrypoint is what turns these words
	// into `registry garbage-collect …`. Overriding command would run
	// `garbage-collect` as a binary that does not exist.
	Args         []string  `yaml:"args"`
	VolumeMounts []gcMount `yaml:"volumeMounts"`
}

type gcMount struct {
	Name      string `yaml:"name"`
	MountPath string `yaml:"mountPath"`
}

type gcHostVolume struct {
	Name     string `yaml:"name"`
	HostPath struct {
		Path string `yaml:"path"`
		Type string `yaml:"type"`
	} `yaml:"hostPath"`
}

func (g *BlobGC) manifest() (string, error) {
	const volName = "registry-storage"
	var pod gcPod
	pod.APIVersion = "v1"
	pod.Kind = "Pod"
	pod.Metadata.Name = g.podName()
	pod.Spec = gcPodSpec{
		RestartPolicy: "Never",
		Containers: []gcContainer{{
			Name:  "gc",
			Image: g.image(),
			Args: []string{
				"garbage-collect",
				"--delete-untagged",
				g.configPath(),
			},
			VolumeMounts: []gcMount{{Name: volName, MountPath: g.mountPath()}},
		}},
	}
	vol := gcHostVolume{Name: volName}
	vol.HostPath.Path = g.StoragePath
	vol.HostPath.Type = "Directory"
	pod.Spec.Volumes = []gcHostVolume{vol}

	out, err := yaml.Marshal(pod)
	if err != nil {
		return "", fmt.Errorf("marshal blob-GC pod manifest: %w", err)
	}
	return string(out), nil
}

// storageBytes measures the registry storage directory from inside the running
// registry container. It is diagnostic only: every failure returns ok=false and
// is recorded as a job step, never as a job failure — a GC that reclaimed disk
// but could not be measured still reclaimed disk.
//
// `du -sk` (not GNU's -sb): the registry image is busybox-based and its du has
// no byte mode. The KiB result is scaled here so the recorded numbers are
// bytes, matching every other size in this API.
func (g *BlobGC) storageBytes(ctx context.Context) (int64, bool) {
	if g.RegistryContainer == "" {
		return 0, false
	}
	res, err := g.Podman.ContainerExec(ctx, g.HostID, g.RegistryContainer,
		[]string{"du", "-sk", g.sizePath()})
	if err != nil || res.ExitCode != 0 {
		return 0, false
	}
	fields := strings.Fields(res.Output)
	if len(fields) == 0 {
		return 0, false
	}
	kib, perr := strconv.ParseInt(fields[0], 10, 64)
	if perr != nil || kib < 0 {
		return 0, false
	}
	return kib * 1024, true
}

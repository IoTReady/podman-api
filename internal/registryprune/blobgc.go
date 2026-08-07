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
)

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

	// MountPath, ConfigPath, Image, PodName and Timeout all default; see the
	// default* constants.
	MountPath  string
	ConfigPath string
	Image      string
	PodName    string
	Timeout    time.Duration
}

func (g *BlobGC) mountPath() string {
	if g.MountPath != "" {
		return g.MountPath
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
	return nil
}

// Run performs Stage B. It is a no-op (and never touches the registry) on a
// dry run or when the payload sets SkipBlobGC.
//
// The named return is load-bearing: the deferred restart both runs during a
// panic unwind AND can turn "GC succeeded but the registry did not come back"
// into a job failure.
func (g *BlobGC) Run(ctx context.Context, jc *jobs.JobContext, p Payload) (err error) {
	if p.DryRun {
		jc.Step("blob-gc", "skipped: dry run (no pod played, registry untouched)")
		return nil
	}
	if p.SkipBlobGC {
		jc.Step("blob-gc", "skipped: skip_blob_gc set; manifests are unlinked but blobs remain recoverable")
		return nil
	}
	if verr := g.validate(); verr != nil {
		// Before anything is stopped. A misconfigured stage must never take
		// the registry down for a GC it cannot run.
		jc.Step("blob-gc", "ABORTED: "+verr.Error())
		return verr
	}

	before, beforeOK := g.storageBytes(ctx)
	if beforeOK {
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
		serr := g.Podman.PodStart(rctx, g.HostID, g.RegistryPod)
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
			jc.Step("blob-gc:registry-start", "REGISTRY DID NOT RESTART: "+serr.Error())
			log.Printf("registryprune: REGISTRY DID NOT RESTART on %s/%s: %v", g.HostID, g.RegistryPod, serr)
			if err == nil {
				err = fmt.Errorf("restart registry %s on %s after blob GC: %w", g.RegistryPod, g.HostID, serr)
			}
			return
		}
		jc.Step("blob-gc:registry-start", "registry restarted")
		if after, ok := g.storageBytes(rctx); ok {
			detail := fmt.Sprintf("%d byte(s) on disk at %s", after, g.StoragePath)
			if beforeOK {
				reclaimed := before - after
				if reclaimed < 0 {
					reclaimed = 0
				}
				detail += fmt.Sprintf("; %d byte(s) reclaimed", reclaimed)
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
		return fmt.Errorf("stop registry %s on %s before blob GC: %w", g.RegistryPod, g.HostID, serr)
	}
	jc.Step("blob-gc:registry-stop", "registry stopped for garbage collection")

	manifest, merr := g.manifest()
	if merr != nil {
		return merr
	}
	if perr := g.Podman.PlayKube(ctx, g.HostID, manifest, true); perr != nil {
		jc.Step("blob-gc:play", "FAILED: "+perr.Error())
		return fmt.Errorf("play blob-GC pod %s on %s: %w", g.podName(), g.HostID, perr)
	}
	played = true
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
		return fmt.Errorf("await blob-GC pod %s on %s: %w", g.podName(), g.HostID, werr)
	}
	if code != 0 {
		jc.Step("blob-gc:wait", fmt.Sprintf("FAILED: garbage-collect exited %d", code))
		return fmt.Errorf("blob GC pod %s on %s exited %d", g.podName(), g.HostID, code)
	}
	jc.Step("blob-gc:wait", "garbage-collect exited 0")
	return nil
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
		[]string{"du", "-sk", g.mountPath()})
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

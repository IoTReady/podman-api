// Package fake is an in-memory implementation of podman.Client used by tests.
// It models pods, secrets, and volumes as maps keyed by host ID.
package fake

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/iotready/podman-api/internal/config"
	"github.com/iotready/podman-api/internal/podman"
)

// Fake is a thread-safe in-memory podman.Client.
type Fake struct {
	mu      sync.Mutex
	pods    map[string]map[string]podman.Pod    // hostID -> name -> Pod
	secrets map[string]map[string]podman.Secret // hostID -> name -> Secret
	volumes map[string]map[string]podman.Volume // hostID -> name -> Volume
	volData map[string]map[string][]byte        // hostID -> name -> tar bytes

	// Optional hooks for tests that want to inject errors.
	PlayKubeErr error
	// PlayKubePodStatus overrides the status assigned to pods created by
	// PlayKube. Empty means "Running". Lets a test force a played pod to stay
	// un-healthy so a verify-poll times out.
	PlayKubePodStatus string
	// PlayKubeContainerStatus overrides the status of containers created by
	// PlayKube. Empty means "Running". Lets a test make a pod report Running
	// while a container is not, exercising the migrate container-level verify.
	PlayKubeContainerStatus string
	// PlayKubeContainerHealth sets the Health status of containers created by
	// PlayKube (default "" = no healthcheck declared). Lets a test drive the
	// migrate readiness gate.
	PlayKubeContainerHealth string
	// ExportErr, if non-nil, makes VolumeExport fail immediately.
	ExportErr error
	// ImportErr, if non-nil, makes VolumeImport fail immediately (without
	// reading the supplied reader) — models a destination that rejects the import.
	ImportErr error
	// ImportTransform, if non-nil, rewrites the bytes VolumeImport stores on the
	// destination — lets a test simulate a lossy/corrupting copy so the source
	// and dest manifests diverge.
	ImportTransform func(host, name string, in []byte) []byte
	// ExportReader, if non-nil, overrides VolumeExport's reader. Lets a test
	// supply a stream that errors mid-transfer.
	ExportReader func(host, name string) io.ReadCloser
	// Prune hooks. PruneReports maps a scope ("images","containers","buildcache",
	// "volumes") to the report ImagePrune/etc. should return; absent → empty report.
	// PruneErr maps a scope to an error to return. PruneCalls records every call.
	PruneReports map[string]podman.PruneReport
	PruneErr     map[string]error
	PruneCalls   []PruneCall
	// Unknown lists hosts that Knows should report as not registered; nil means
	// every host is known. Lets a test exercise the scheduler's unknown-host skip.
	Unknown map[string]bool
	// VersionStr overrides the version Version reports; empty means "fake-1.0".
	VersionStr string

	// PullErr, if non-nil, makes ImagePull return this error for matching refs.
	// Key is image ref; the empty key matches any ref.
	PullErr map[string]error
	// PullCalls records every (host, image) pair passed to ImagePull.
	PullCalls []struct{ Host, Image string }
	// PlayCalls records every PlayKube invocation (host, replace, networks).
	PlayCalls []PlayCall
	// PodListErr, if non-nil, makes PodList return this error.
	PodListErr error
	// LogLines, if set, are emitted in order by ContainerLogs before the
	// channel closes. Lets tests exercise the streaming response paths.
	LogLines []podman.LogLine
	// ContainerLogsErr, if non-nil, makes ContainerLogs return this error
	// instead of a channel.
	ContainerLogsErr error
	// UsedHostPortsErr, if non-nil, makes UsedHostPorts return this error.
	UsedHostPortsErr error
	// PodInspectErr, if non-nil, makes PodInspect return this error (use a
	// non-ErrNotFound error to exercise the unexpected-backend-error paths).
	PodInspectErr error
	// VolumeInspectErr, if non-nil, makes VolumeInspect return this error (use a
	// non-ErrNotFound error to exercise the transient-failure path).
	VolumeInspectErr error
	// SecretCreateErr, if non-nil, makes SecretCreate return this error
	// instead of recording the secret.
	SecretCreateErr error
	// LifecycleErr, if non-nil, makes PodStart/PodStop/PodRestart fail while
	// leaving the pod in place, to exercise action-failure paths.
	LifecycleErr error
	// HostInfoVal is returned by HostInfo when HostInfoErr is nil.
	HostInfoVal podman.HostInfo
	// HostInfoErr, if non-nil, makes HostInfo return this error.
	HostInfoErr error
	// HostInfoCalls counts HostInfo invocations (lets a test assert probe throttling).
	HostInfoCalls int

	// NetworkEnsureCalls records, per host, the network names ensured.
	NetworkEnsureCalls map[string][]string
	// NetworkEnsureErr, if non-nil, makes NetworkEnsure fail.
	NetworkEnsureErr error

	// ExecFunc, if set, produces the ContainerExec result for tests. Default
	// (nil) returns ExitCode 0, empty output.
	ExecFunc func(host, container string, cmd []string) (podman.ExecResult, error)
	// ExecCalls records every ContainerExec invocation.
	ExecCalls []ExecCall

	// CopyCalls records every CopyToContainer invocation.
	CopyCalls []CopyCall
	// CopyErr, if non-nil, makes CopyToContainer fail.
	CopyErr error
}

// PlayCall records one PlayKube invocation for assertions.
type PlayCall struct {
	Host     string
	Replace  bool
	Networks []string
	YAML     string
}

// ExecCall records one ContainerExec invocation for assertions.
type ExecCall struct {
	Host      string
	Container string
	Cmd       []string
}

// CopyCall records one CopyToContainer invocation for assertions.
type CopyCall struct {
	Host      string
	Container string
	DestDir   string
	Name      string
	Content   []byte
}

// AddVolume seeds a volume on a host so VolumeInspect resolves it. Test-only.
func (f *Fake) AddVolume(host string, v podman.Volume) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hostVolumes(host)[v.Name] = v
}

// AddPod seeds a pod on a host (with whatever container ports it carries), so a
// test can occupy host ports or pre-place an instance. Test-only.
func (f *Fake) AddPod(host string, p podman.Pod) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hostPods(host)[p.Name] = p
}

// SetVolumeData seeds a volume and its contents on a host. Test-only.
func (f *Fake) SetVolumeData(host, name string, data []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hostVolumes(host)[name] = podman.Volume{Name: name}
	f.hostVolData(host)[name] = data
}

// VolumeData returns the stored contents of a volume (nil if none). Test-only.
func (f *Fake) VolumeData(host, name string) []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hostVolData(host)[name]
}

// New returns a fresh fake.
func New() *Fake {
	return &Fake{
		pods:               map[string]map[string]podman.Pod{},
		secrets:            map[string]map[string]podman.Secret{},
		volumes:            map[string]map[string]podman.Volume{},
		volData:            map[string]map[string][]byte{},
		PruneReports:       map[string]podman.PruneReport{},
		PruneErr:           map[string]error{},
		NetworkEnsureCalls: map[string][]string{},
	}
}

func (f *Fake) hostPods(h string) map[string]podman.Pod {
	if _, ok := f.pods[h]; !ok {
		f.pods[h] = map[string]podman.Pod{}
	}
	return f.pods[h]
}
func (f *Fake) hostSecrets(h string) map[string]podman.Secret {
	if _, ok := f.secrets[h]; !ok {
		f.secrets[h] = map[string]podman.Secret{}
	}
	return f.secrets[h]
}
func (f *Fake) hostVolumes(h string) map[string]podman.Volume {
	if _, ok := f.volumes[h]; !ok {
		f.volumes[h] = map[string]podman.Volume{}
	}
	return f.volumes[h]
}
func (f *Fake) hostVolData(h string) map[string][]byte {
	if _, ok := f.volData[h]; !ok {
		f.volData[h] = map[string][]byte{}
	}
	return f.volData[h]
}

func (f *Fake) PlayKube(_ context.Context, hostID, raw string, replace bool, networks ...string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.PlayCalls = append(f.PlayCalls, PlayCall{Host: hostID, Replace: replace, Networks: networks, YAML: raw})
	if f.PlayKubeErr != nil {
		return f.PlayKubeErr
	}
	// Model real podman: a pod cannot join a network that does not exist, so the
	// network must have been created (NetworkEnsure) first. This lets unit tests
	// catch ordering bugs where an app pod is played before its network exists.
	for _, n := range networks {
		if !f.networkEnsuredLocked(hostID, n) {
			return fmt.Errorf("unable to find network with name or ID %s: network not found", n)
		}
	}
	pods := f.hostPods(hostID)
	for _, doc := range strings.Split(raw, "\n---\n") {
		var head struct {
			Kind     string `yaml:"kind"`
			Metadata struct {
				Name   string            `yaml:"name"`
				Labels map[string]string `yaml:"labels"`
			} `yaml:"metadata"`
			Spec struct {
				Containers []struct {
					Name  string `yaml:"name"`
					Image string `yaml:"image"`
				} `yaml:"containers"`
			} `yaml:"spec"`
		}
		_ = yaml.Unmarshal([]byte(doc), &head)
		if head.Kind != "Pod" {
			continue
		}
		if _, exists := pods[head.Metadata.Name]; exists && !replace {
			return fmt.Errorf("pod %q already exists", head.Metadata.Name)
		}
		cstatus := "Running"
		if f.PlayKubeContainerStatus != "" {
			cstatus = f.PlayKubeContainerStatus
		}
		var cs []podman.Container
		for _, c := range head.Spec.Containers {
			cs = append(cs, podman.Container{
				Name: c.Name, Image: c.Image, ImageTag: c.Image,
				Status: cstatus, Health: f.PlayKubeContainerHealth, StartedAt: time.Now(),
			})
		}
		podStatus := "Running"
		if f.PlayKubePodStatus != "" {
			podStatus = f.PlayKubePodStatus
		}
		pods[head.Metadata.Name] = podman.Pod{
			ID: head.Metadata.Name, Name: head.Metadata.Name,
			Status: podStatus, Created: time.Now(),
			Containers: cs, Labels: head.Metadata.Labels,
		}
	}
	return nil
}

func (f *Fake) PodInspect(_ context.Context, h, name string) (podman.Pod, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.PodInspectErr != nil {
		return podman.Pod{}, f.PodInspectErr
	}
	p, ok := f.hostPods(h)[name]
	if !ok {
		return podman.Pod{}, podman.ErrNotFound
	}
	return p, nil
}

func (f *Fake) PodList(_ context.Context, h string, filters map[string]string) ([]podman.Pod, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.PodListErr != nil {
		return nil, f.PodListErr
	}
	var out []podman.Pod
	for _, p := range f.hostPods(h) {
		match := true
		for k, v := range filters {
			if p.Labels[k] != v {
				match = false
				break
			}
		}
		if match {
			out = append(out, p)
		}
	}
	return out, nil
}

func (f *Fake) setStatus(h, name, status string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.LifecycleErr != nil {
		return f.LifecycleErr
	}
	p, ok := f.hostPods(h)[name]
	if !ok {
		return podman.ErrNotFound
	}
	p.Status = status
	for i := range p.Containers {
		p.Containers[i].Status = status
	}
	f.hostPods(h)[name] = p
	return nil
}

func (f *Fake) PodStart(_ context.Context, h, name string) error {
	return f.setStatus(h, name, "Running")
}
func (f *Fake) PodStop(_ context.Context, h, name string) error {
	return f.setStatus(h, name, "Exited")
}
func (f *Fake) PodRestart(_ context.Context, h, name string) error {
	return f.setStatus(h, name, "Running")
}
func (f *Fake) PodRemove(_ context.Context, h, name string, _ bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.hostPods(h)[name]; !ok {
		return podman.ErrNotFound
	}
	delete(f.hostPods(h), name)
	return nil
}

func (f *Fake) SecretCreate(_ context.Context, h, name string, _ []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.SecretCreateErr != nil {
		return f.SecretCreateErr
	}
	f.hostSecrets(h)[name] = podman.Secret{Name: name, CreatedAt: time.Now()}
	return nil
}
func (f *Fake) SecretList(_ context.Context, h string) ([]podman.Secret, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []podman.Secret
	for _, s := range f.hostSecrets(h) {
		out = append(out, s)
	}
	return out, nil
}
func (f *Fake) SecretInspect(_ context.Context, h, name string) (podman.Secret, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.hostSecrets(h)[name]
	if !ok {
		return podman.Secret{}, podman.ErrNotFound
	}
	return s, nil
}
func (f *Fake) SecretRemove(_ context.Context, h, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.hostSecrets(h)[name]; !ok {
		return podman.ErrNotFound
	}
	delete(f.hostSecrets(h), name)
	return nil
}

func (f *Fake) VolumeInspect(_ context.Context, h, name string) (podman.Volume, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.VolumeInspectErr != nil {
		return podman.Volume{}, f.VolumeInspectErr
	}
	v, ok := f.hostVolumes(h)[name]
	if !ok {
		return podman.Volume{}, podman.ErrNotFound
	}
	return v, nil
}
func (f *Fake) VolumeRemove(_ context.Context, h, name string, _ bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.hostVolumes(h)[name]; !ok {
		return podman.ErrNotFound
	}
	delete(f.hostVolumes(h), name)
	delete(f.hostVolData(h), name)
	return nil
}

func (f *Fake) VolumeExport(_ context.Context, h, name string) (io.ReadCloser, error) {
	if f.ExportReader != nil {
		return f.ExportReader(h, name), nil
	}
	if f.ExportErr != nil {
		return nil, f.ExportErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.hostVolumes(h)[name]; !ok {
		return nil, podman.ErrNotFound
	}
	src := f.hostVolData(h)[name]
	buf := make([]byte, len(src))
	copy(buf, src)
	return io.NopCloser(bytes.NewReader(buf)), nil
}

func (f *Fake) VolumeImport(_ context.Context, h, name string, r io.Reader) error {
	if f.ImportErr != nil {
		return f.ImportErr
	}
	// Read outside the lock — r may be an io.Pipe fed by another goroutine.
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if f.ImportTransform != nil {
		data = f.ImportTransform(h, name, data)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.hostVolumes(h)[name]; !ok {
		return podman.ErrNotFound
	}
	f.hostVolData(h)[name] = data
	return nil
}

func (f *Fake) VolumeCreate(_ context.Context, h, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.hostVolumes(h)[name]; !ok {
		f.hostVolumes(h)[name] = podman.Volume{Name: name}
	}
	return nil
}

func (f *Fake) NetworkEnsure(_ context.Context, host, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.NetworkEnsureErr != nil {
		return f.NetworkEnsureErr
	}
	f.NetworkEnsureCalls[host] = append(f.NetworkEnsureCalls[host], name)
	return nil
}

// networkEnsuredLocked reports whether NetworkEnsure has created name on host.
// Caller must hold f.mu.
func (f *Fake) networkEnsuredLocked(host, name string) bool {
	for _, n := range f.NetworkEnsureCalls[host] {
		if n == name {
			return true
		}
	}
	return false
}

func (f *Fake) ContainerLogs(_ context.Context, _, _ string, _ podman.LogOptions) (<-chan podman.LogLine, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.ContainerLogsErr != nil {
		return nil, f.ContainerLogsErr
	}
	ch := make(chan podman.LogLine, len(f.LogLines))
	for _, l := range f.LogLines {
		ch <- l
	}
	close(ch)
	return ch, nil
}

func (f *Fake) ImagePull(_ context.Context, host, ref string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.PullCalls = append(f.PullCalls, struct{ Host, Image string }{host, ref})
	if err, ok := f.PullErr[ref]; ok {
		return err
	}
	if err, ok := f.PullErr[""]; ok {
		return err
	}
	return nil
}

func (f *Fake) Ping(_ context.Context, _ string) error { return nil }
func (f *Fake) Version(_ context.Context, _ string) (string, error) {
	if f.VersionStr != "" {
		return f.VersionStr, nil
	}
	return "fake-1.0", nil
}

// Knows reports a host as registered unless it's listed in Unknown.
func (f *Fake) Knows(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return !f.Unknown[id]
}

// SetHosts is a no-op: the fake has no persistent host map — it services any
// host ID, so a reload does not change behaviour.
func (f *Fake) SetHosts(_ []config.Host) {}
func (f *Fake) HostInfo(_ context.Context, _ string) (podman.HostInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.HostInfoCalls++
	if f.HostInfoErr != nil {
		return podman.HostInfo{}, f.HostInfoErr
	}
	return f.HostInfoVal, nil
}
func (f *Fake) UsedHostPorts(_ context.Context, h string) ([]podman.PortMapping, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.UsedHostPortsErr != nil {
		return nil, f.UsedHostPortsErr
	}
	var out []podman.PortMapping
	for _, p := range f.hostPods(h) {
		for _, c := range p.Containers {
			out = append(out, c.Ports...)
		}
	}
	return out, nil
}

// PruneCall records one prune invocation for assertions.
type PruneCall struct {
	Host    string
	Scope   string              // "images" | "containers" | "buildcache" | "volumes"
	All     bool                // ImagePrune only
	Filters map[string][]string // VolumePrune only
}

func (f *Fake) pruneScope(host, scope string, all bool, filters map[string][]string) (podman.PruneReport, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.PruneCalls = append(f.PruneCalls, PruneCall{Host: host, Scope: scope, All: all, Filters: filters})
	if err := f.PruneErr[scope]; err != nil {
		return podman.PruneReport{}, err
	}
	if r, ok := f.PruneReports[scope]; ok {
		return r, nil
	}
	return podman.PruneReport{}, nil
}

func (f *Fake) ImagePrune(_ context.Context, host string, all bool) (podman.PruneReport, error) {
	return f.pruneScope(host, "images", all, nil)
}

func (f *Fake) ContainerPrune(_ context.Context, host string) (podman.PruneReport, error) {
	return f.pruneScope(host, "containers", false, nil)
}

func (f *Fake) BuildCachePrune(_ context.Context, host string) (podman.PruneReport, error) {
	return f.pruneScope(host, "buildcache", false, nil)
}

func (f *Fake) VolumePrune(_ context.Context, host string, filters map[string][]string) (podman.PruneReport, error) {
	return f.pruneScope(host, "volumes", false, filters)
}

func (f *Fake) ContainerExec(_ context.Context, host, container string, cmd []string) (podman.ExecResult, error) {
	f.mu.Lock()
	f.ExecCalls = append(f.ExecCalls, ExecCall{Host: host, Container: container, Cmd: cmd})
	fn := f.ExecFunc
	f.mu.Unlock()
	if fn != nil {
		return fn(host, container, cmd)
	}
	return podman.ExecResult{}, nil
}

func (f *Fake) CopyToContainer(_ context.Context, host, container, destDir, name string, content []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.CopyErr != nil {
		return f.CopyErr
	}
	cp := make([]byte, len(content))
	copy(cp, content)
	f.CopyCalls = append(f.CopyCalls, CopyCall{Host: host, Container: container, DestDir: destDir, Name: name, Content: cp})
	return nil
}

// Compile-time guarantee that Fake implements the interface.
var _ podman.Client = (*Fake)(nil)

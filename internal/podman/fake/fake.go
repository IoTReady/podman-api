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
	// ExportErr, if non-nil, makes VolumeExport fail immediately.
	ExportErr error
	// ImportErr, if non-nil, makes VolumeImport fail immediately (without
	// reading the supplied reader) — models a destination that rejects the import.
	ImportErr error
	// ExportReader, if non-nil, overrides VolumeExport's reader. Lets a test
	// supply a stream that errors mid-transfer.
	ExportReader func(host, name string) io.ReadCloser
	// PullErr, if non-nil, makes ImagePull return this error for matching refs.
	// Key is image ref; the empty key matches any ref.
	PullErr map[string]error
	// PullCalls records every (host, image) pair passed to ImagePull.
	PullCalls []struct{ Host, Image string }
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
	// HostInfoVal is returned by HostInfo when HostInfoErr is nil.
	HostInfoVal podman.HostInfo
	// HostInfoErr, if non-nil, makes HostInfo return this error.
	HostInfoErr error
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
		pods:    map[string]map[string]podman.Pod{},
		secrets: map[string]map[string]podman.Secret{},
		volumes: map[string]map[string]podman.Volume{},
		volData: map[string]map[string][]byte{},
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

func (f *Fake) PlayKube(_ context.Context, hostID, raw string, replace bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.PlayKubeErr != nil {
		return f.PlayKubeErr
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
				Status: cstatus, StartedAt: time.Now(),
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

func (f *Fake) Ping(_ context.Context, _ string) error              { return nil }
func (f *Fake) Version(_ context.Context, _ string) (string, error) { return "fake-1.0", nil }
func (f *Fake) HostInfo(_ context.Context, _ string) (podman.HostInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
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

// Compile-time guarantee that Fake implements the interface.
var _ podman.Client = (*Fake)(nil)

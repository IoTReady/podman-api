package podman

import "time"

// Pod is the libpod-shaped pod summary the rest of the API consumes.
type Pod struct {
	ID         string
	Name       string
	Status     string // "Running", "Created", "Exited", etc.
	Created    time.Time
	Containers []Container
	Labels     map[string]string
	// InfraID is the ID of the pod's infra container, empty when the pod has
	// none. It is the only reliable way to tell an infra container from an app
	// container: names collide (`podman kube play` names containers
	// <pod>-<containerName>) and an infra container reports no image at all,
	// which is indistinguishable from an app container whose inspect failed.
	InfraID string
}

type Container struct {
	ID       string
	Name     string
	// Image is InspectContainerData.ImageDigest, which is
	// image.Digest().String(): a BARE digest with no repository, e.g.
	// "sha256:42283567cae4…". It is NOT the "repo@sha256:…" shape — measured
	// across 34 non-infra containers on engine-1 (podman 5.8.2, 2026-08-07),
	// 34/34. When podman has no digest, enrichContainer falls back to
	// InspectContainerData.Image, a bare 64-hex image ID that is not a manifest
	// digest at all; a consumer that must resolve against a registry has to
	// distinguish the two.
	Image string
	// ImageTag is InspectContainerData.ImageName, the full reference:
	// "host/repo:tag" (18/34 in the same survey) or "host/repo@sha256:…" (16/34).
	ImageTag string
	Status   string
	// Health is the container's healthcheck status: "" when the container
	// declares no healthcheck, otherwise "healthy" / "unhealthy" / "starting".
	Health string
	// HealthStartPeriod and HealthInterval are the container's *declared*
	// healthcheck timings, both zero when it declares no healthcheck. They are
	// the grace the container was promised, so readiness waits can bound
	// themselves by the spec they are verifying rather than by a fixed constant
	// that may be shorter than the first check can possibly run (#196).
	HealthStartPeriod time.Duration
	HealthInterval    time.Duration
	StartedAt         time.Time
	RestartCount      int
	Ports             []PortMapping
	Env               map[string]string
	// ExitCode is the container's last exit code, as libpod reports it. It is
	// meaningful only when Exited is true; a running container has ExitCode 0
	// (never populated) alongside Exited false.
	ExitCode int
	// Exited reports whether the container has stopped running (libpod's
	// State.Running == false). Distinguishes "never ran" / "still running"
	// from "ran and produced ExitCode".
	Exited bool
}

type PortMapping struct {
	HostIP        string
	HostPort      int
	ContainerPort int
	Protocol      string // tcp/udp
	Pod           string // pod name owning this port (empty if not derivable)
	Container     string // container name owning this port (empty if not derivable)
}

type Volume struct {
	Name      string
	SizeBytes int64
}

// ContainerStats is a point-in-time resource sample for one container, mapped
// from libpod's define.ContainerStats.
//
// The cumulative fields (CPUNano, the byte counters) reset to zero when the
// container is recreated, exactly as RestartCount does — a redeploy reads as a
// counter reset, which Prometheus rate()/increase() handle.
//
// Podman's own CPU/AvgCPU/MemPerc percentages are deliberately NOT carried:
// for a non-streaming call they are averaged against container start time, so a
// container busy at boot and idle since reads as permanently hot. Export the
// counters and let PromQL derive rates.
type ContainerStats struct {
	Name            string
	CPUNano         uint64 // cumulative CPU time, nanoseconds
	MemUsageBytes   uint64
	MemLimitBytes   uint64 // 0 when the container declares no limit
	NetRxBytes      uint64 // summed across interfaces
	NetTxBytes      uint64
	BlockReadBytes  uint64
	BlockWriteBytes uint64
	PIDs            uint64
}

type Secret struct {
	Name      string
	CreatedAt time.Time
}

// HostInfo is a point-in-time resource snapshot for a host, sourced from
// libpod `info` + `system df` plus a best-effort read of /proc/loadavg.
// Pointer fields are nil when the underlying source does not report them, so
// an absent metric serializes as null rather than a misleading zero.
type HostInfo struct {
	CPUs       int         // logical CPUs
	MemTotal   int64       // bytes
	MemFree    int64       // bytes
	MemUsedPct float64     // derived: (MemTotal-MemFree)/MemTotal*100, 0 if MemTotal==0
	CPUPct     *float64    // average CPU utilization since boot (user+system %); nil when libpod omits CPUUtilization
	LoadAvg    *[3]float64 // 1/5/15-min; nil when unavailable
	Disk       DiskUsage
}

// DiskUsage describes the host's container-storage partition (graphroot).
type DiskUsage struct {
	Total       int64 // bytes (graphroot partition size)
	Used        int64 // bytes
	Free        int64 // bytes (Total-Used)
	Reclaimable int64 // bytes reclaimable from dangling volumes (system df)
}

type LogLine struct {
	Container string
	Stream    string // stdout / stderr
	Time      time.Time
	Line      string
}

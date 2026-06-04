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
}

type Container struct {
	ID       string
	Name     string
	Image    string // resolved digest, e.g. "docker.io/library/postgres@sha256:..."
	ImageTag string // human-readable tag, e.g. "docker.io/library/postgres:16"
	Status   string
	// Health is the container's healthcheck status: "" when the container
	// declares no healthcheck, otherwise "healthy" / "unhealthy" / "starting".
	Health       string
	StartedAt    time.Time
	RestartCount int
	Ports        []PortMapping
	Env          map[string]string
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

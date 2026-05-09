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
	ID           string
	Name         string
	Image        string // resolved digest, e.g. "localhost/lite-engine@sha256:..."
	ImageTag     string // human-readable tag, e.g. "localhost/lite-engine:latest"
	Status       string
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
}

type Volume struct {
	Name      string
	SizeBytes int64
}

type Secret struct {
	Name      string
	CreatedAt time.Time
}

type LogLine struct {
	Container string
	Stream    string // stdout / stderr
	Time      time.Time
	Line      string
}

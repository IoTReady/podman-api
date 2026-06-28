package api

import (
	"encoding/json"
	"net/http"

	"github.com/iotready/podman-api/internal/auth"
)

const agentDocsBody = `# podman-api Agent Guide

## Authentication

All endpoints except /healthz, /openapi.yaml, /mcp, and /agent-docs require:

    Authorization: Bearer <token>

Tokens carry scopes. Grant only the scopes a workflow needs:
  hosts:read, templates:read, templates:write,
  instances:read, instances:write,
  secrets:read, secrets:write, jobs:read

## Core Concepts

- Host     — a remote machine running rootless Podman, identified by an ID
             configured server-side (e.g. "engine-1")
- Template — a reusable pod/container definition stored in the catalog
- Instance — a live deployment of a template on a host; addressed by
             template + slug (e.g. template "webapp", slug "prod")
- Slug     — a short label for one deployment of a template (e.g. "dev",
             "prod", "acme"); unique per template per host
- Job      — a background task (migrate, backup, restore, evacuate);
             poll GET /jobs/{id} until status is "succeeded" or "failed"

## Key Workflows

### Create or update an instance (idempotent)
PUT /hosts/{host}/instances/{template}/{slug}
Authorization: Bearer <token>
Content-Type: application/json

{"parameters": {"image": "registry.example.com/app:v1.2", "port": 8080}}

### List all instances on a host
GET /hosts/{host}/instances

### Trigger a manual backup
POST /hosts/{host}/instances/{template}/{slug}/backup

### Restore from a backup
POST /backups/{backup-id}/restore

### Point-in-time restore (Litestream; commercial)
POST /hosts/{host}/instances/{template}/{slug}/restore
{"timestamp": "2026-06-01T00:00:00Z"}

### Stream live logs (SSE)
GET /hosts/{host}/instances/{template}/{slug}/logs

### Poll a background job
GET /jobs/{id}

## Full API Reference

GET /openapi.yaml  — OpenAPI 3.x specification
GET /mcp           — MCP discovery document (JSON)
`

func mcpDiscoveryHandler(version string) http.HandlerFunc {
	doc := map[string]any{
		"protocol_version": "2024-11-05",
		"server": map[string]any{
			"name":        "podman-api",
			"version":     version,
			"description": "Rootless Podman control plane — manage container instances on remote hosts via a declarative REST API.",
		},
		"capabilities": map[string]any{},
		"references": map[string]any{
			"openapi":    "/openapi.yaml",
			"agent_docs": "/agent-docs",
		},
		"auth": map[string]any{
			"type":   "bearer",
			"header": "Authorization",
			"format": "Bearer <token>",
			"scopes": auth.AllScopes,
		},
	}
	body, _ := json.MarshalIndent(doc, "", "  ")
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}
}

func agentDocsHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(agentDocsBody))
}

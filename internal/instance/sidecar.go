package instance

import (
	"github.com/iotready/podman-api/extension"
	"github.com/iotready/podman-api/internal/render"
)

type SidecarInjector = extension.SidecarInjector

// toExtMeta projects an internal render.Meta into the public
// extension.TemplateMeta handed to a SidecarInjector. render.Meta lives under
// internal/ and cannot cross the module boundary, so the injector sees only
// this stable projection.
func toExtMeta(m render.Meta) extension.TemplateMeta {
	vols := make([]extension.TemplateVolume, len(m.Volumes))
	for i, v := range m.Volumes {
		vols[i] = extension.TemplateVolume{Name: v.Name, Backup: v.Backup}
	}
	return extension.TemplateMeta{ID: m.ID, Volumes: vols}
}

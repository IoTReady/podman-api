# Instance Detail Responsive Grid

**Date:** 2026-06-09
**Status:** Approved

## Problem

The instance detail view at `/ui/hosts/{host}/instances/{template}/{slug}` is a flat collection of `<div>` elements with no grid structure. Buttons and text intermingle in backup rows, container metadata lacks alignment, and the mobile layout has no consistent grid collapse. The section headings are plain `<div class="label">` with no semantic structure.

## Solution

Apply a CSS Grid property-pattern to every section of the detail view, with a consistent card collapse on mobile (≤640px) using the existing `data-label` convention.

## Sections

### 1. Header (`.inst-head`)

**Desktop:** Two-column CSS grid — left column has the instance title (template/slug) and pod status, right column has the action buttons in a flex-wrap group right-aligned.

**Mobile:** Single column — title on top, buttons wrapped below. The existing `.inst-head .actions { flex-wrap: wrap; gap: 0.4rem; }` rules already handle this; minor spacing cleanup may be needed.

No `data-label` pseudo-elements — this is a page header, not a data row.

### 2. Containers (`.detail-section.containers .grid-row`)

**Desktop:** 4-column grid: `Name | Image | Status (restarts) | Health`
Column widths: `minmax(0, 2fr) minmax(0, 3fr) minmax(0, 1.5fr) minmax(0, 1fr)`
Image column gets the most space (long image refs). Health column is narrow (dot + word).

**Mobile:** Single-column card — each grid cell shows its `data-label` via `::before`.
Each container row has a subtle border-bottom separator, becomes a full card with border+radius on mobile.

**Replaces:** The current `.container-row` with inline `<span>` elements.

### 3. Volumes (`.detail-section.volumes .grid-row`)

**Desktop:** 2-column grid: `Name | Size`
Column widths: `1fr auto` — name fills remaining space, size is content-width.

**Mobile:** Stacked with `data-label` pseudo-elements.

**Replaces:** The current flat `<div>{{.Name}} {{.SizeBytes | formatBytes}}</div>`.

### 4. Backups (`.detail-section.backups .grid-row`)

**Desktop:** 3-column grid: `1fr auto auto`
First cell: combined info text (ID · timestamp · state · image), followed by two cells for Restore and Delete buttons. The Restore cell only renders when `.State == "complete"`.

**Mobile:** Single column card with labels. Buttons stack below the info text, same visual hierarchy.

**Fixes:** Currently text and buttons share one `<div>` which wraps messily.

### 5. Env (`.detail-section.env .grid-row`)

**Desktop:** 2-column grid: `Key | Value`
Column widths: `minmax(0, 1fr) minmax(0, 2fr)`. Both cells use `font-family: monospace`.
Long values use `word-break: break-all`.

**Mobile:** Stacked with `data-label="Key"` and `data-label="Value"`.

### 6. Logs link

Unchanged — a single `pure-button` at the bottom of the page.

## CSS Structure

### New classes

```css
/* Section wrapper — heading + grid rows */
.detail-section { margin-bottom: 1.5rem; }
.detail-section .section-title {
  font-size: .75rem; letter-spacing: .05em; color: #888;
  margin-bottom: 0.5rem;
}

/* Grid rows — applied to each data row within a section */
.detail-grid .grid-row {
  display: grid;
  grid-template-columns: var(--grid-cols);
  gap: 0.5rem;
  padding: 0.4rem 0;
  border-bottom: 1px solid #eee;
  align-items: center;
}
.detail-grid .grid-row:last-child { border-bottom: none; }
```

Per-section column templates set via CSS custom properties on the section wrapper:

```css
.containers { --grid-cols: minmax(0, 2fr) minmax(0, 3fr) minmax(0, 1.5fr) minmax(0, 1fr); }
.volumes   { --grid-cols: 1fr auto; }
.backups   { --grid-cols: 1fr auto auto; }
.env       { --grid-cols: minmax(0, 1fr) minmax(0, 2fr); }
```

### Mobile card collapse

```css
@media (max-width: 640px) {
  .detail-grid .grid-row {
    display: flex;
    flex-direction: column;
    gap: 0.2rem;
    padding: 0.5rem;
    border: 1px solid #ddd;
    border-radius: 4px;
    margin-bottom: 0.5rem;
  }
  .detail-grid .grid-row:last-child { margin-bottom: 0; }
  .detail-grid .grid-cell[data-label]::before {
    content: attr(data-label) ": ";
    font-weight: 700;
    color: #555;
  }
}
```

### Header grid

```css
.inst-head {
  display: grid;
  grid-template-columns: 1fr auto;
  gap: 1rem;
  align-items: center;
  margin-bottom: 1rem;
}
.inst-head .actions { justify-self: end; }

@media (max-width: 640px) {
  .inst-head {
    grid-template-columns: 1fr;
    gap: 0.5rem;
  }
  .inst-head .actions { justify-self: start; }
}
```

## HTML Template Changes

### Header

```html
<div class="inst-head">
  <div class="inst-title">
    <strong>{{.Inst.Template}} / {{.Inst.Slug}}</strong> · {{.Inst.Pod.Status}}
  </div>
  <span class="actions">...buttons unchanged...</span>
</div>
```

### Containers

Replace `.container-row` spans with `.detail-section.containers` + `.detail-grid` + `.grid-row` + `.grid-cell` with `data-label`:

```html
<div class="detail-section containers">
  <div class="section-title">CONTAINERS</div>
  <div class="detail-grid">
    {{range .Inst.Containers}}
    <div class="grid-row">
      <div class="grid-cell" data-label="Name">{{.Name}}</div>
      <div class="grid-cell" data-label="Image">{{.Image}}</div>
      <div class="grid-cell" data-label="Status">{{.Status}} (restarts {{.RestartCount}})</div>
      <div class="grid-cell" data-label="Health">
        {{if eq .Health "healthy"}}<span class="health-dot healthy" title="healthy">●</span> healthy
        {{else if eq .Health "starting"}}<span class="health-dot starting" title="starting">●</span> starting
        {{else if eq .Health "unhealthy"}}<span class="health-dot unhealthy" title="unhealthy">●</span> unhealthy
        {{else}}—{{end}}
      </div>
    </div>
    {{end}}
  </div>
</div>
```

### Volumes

```html
<div class="detail-section volumes">
  <div class="section-title">VOLUMES</div>
  <div class="detail-grid">
    {{range .Inst.Volumes}}
    <div class="grid-row">
      <div class="grid-cell" data-label="Name">{{.Name}}</div>
      <div class="grid-cell" data-label="Size">{{.SizeBytes | formatBytes}}</div>
    </div>
    {{end}}
  </div>
</div>
```

### Backups

```html
<div class="detail-section backups">
  <div class="section-title">BACKUPS</div>
  <button class="pure-button" ...hx attributes...>Back up now</button>
  <div class="detail-grid" style="margin-top: 0.5rem">
    {{range .Backups}}
    <div class="grid-row">
      <div class="grid-cell" data-label="Backup">
        {{.ID}} · {{.Created.UTC.Format "2006-01-02 15:04:05"}} · {{.State}}{{if .Image}} · {{.Image}}{{end}}
      </div>
      <div class="grid-cell actions">
        {{if eq .State "complete"}}
        <button class="pure-button" ...hx attributes...>Restore</button>
        {{end}}
      </div>
      <div class="grid-cell actions">
        <button class="pure-button" ...hx attributes...>Delete</button>
      </div>
    </div>
    {{end}}
  </div>
</div>
```

### Env

```html
<div class="detail-section env">
  <div class="section-title">ENV</div>
  <div class="detail-grid">
    {{range $k, $v := .Inst.EnvSummary}}
    <div class="grid-row">
      <div class="grid-cell" data-label="Key"><code>{{$k}}</code></div>
      <div class="grid-cell" data-label="Value"><code>{{$v}}</code></div>
    </div>
    {{end}}
  </div>
</div>
```

## Changes to Remove

- `.container-row` CSS rules (both desktop and mobile) — replaced by `.detail-grid .grid-row`
- `.inst-head .actions` mobile-only flex-wrap rules (now handled by the header grid collapse)
- The old `inst-head` desktop layout (currently relies on inline/browser defaults — explicit grid replaces it)

## Edge Cases

- **Zero containers/volumes/backups:** The section heading still renders; the grid has no rows. This is consistent with the current behavior.
- **Long container image names:** `word-break: break-all` on the image cell, same as current `.container-row span:nth-child(2)`.
- **Multiple backup actions:** The grid naturally aligns Restore and Delete buttons in their own columns so they don't wrap around text.
- **Health dots:** Use the existing `.health-dot` classes unchanged.
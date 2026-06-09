# Responsive Admin UI

## Problem

The admin UI is not usable on small viewports (phones). Tables overflow, action
buttons wrap unpredictably, the sidebar consumes too much space, and log text
requires horizontal scrolling.

## Scope

Full phone support down to 320px viewport width. Keep the existing 640px
breakpoint from the first UI cleanup as the transition point. Pure CSS + one
inline JS click handler for the hamburger toggle. No new framework dependency.

## Breakpoints

| Range | Target |
|-------|--------|
| ≤640px | Phone / narrow tablet — hamburger nav, cards, stacked layouts |
| >640px | Desktop / laptop — current layout unchanged |

## Sidebar — hamburger navigation

- **Desktop (>640px):** sidebar visible, fixed width, unchanged.
- **Mobile (≤640px):** sidebar hidden off-screen by default. A hamburger icon
  (☰) appears in the top-left of the content area. Tapping it slides the sidebar
  in as a fixed overlay with a semi-transparent backdrop. Tapping the backdrop
  or a sidebar link closes it.
- **JS:** one click handler on the hamburger icon toggles class `open` on
  `.sidebar`. The backdrop is a pseudo-element or an adjacent `<div>`.

## Tables — card layout on mobile

All tables (dashboard host summary, host instances, jobs list) have ≤3 columns
with short labels and data. On mobile each row renders as a bordered card with
label/data pairs.

- **Desktop (>640px):** unchanged `<table class="pure-table">`.
- **Mobile (≤640px):** `<thead>` hidden. Each `<td>` displays as a block with
  its `<th>` text as a `::before` pseudo-element label. The `<tr>` gets a
  bottom border to separate rows visually. Clickable rows retain their cursor
  and hover state.

## Instance detail page

- **Action buttons** (`<span class="actions">`): wrap naturally with
  `flex-wrap: wrap` and a small gap. Buttons remain full-size text.
- **Containers section**: each `.container-row` stacks vertically
  (`flex-direction: column`) on mobile. Name, image, status, and health each
  occupy their own line. Image gets `word-break: break-all` so long registry
  paths don't overflow.
- **Backups section**: each backup `<div>` already stacks vertically by default;
  tighten spacing on mobile (reduce margin/padding). Action buttons (Restore,
  Delete) sit below the metadata line.
- **Volumes and ENV sections**: unchanged — single-line entries already work at
  small widths.
- **Logs page**: `<pre id="log-lines">` gets `white-space: pre-wrap` to wrap
  long lines, `max-height: 60vh` with `overflow-y: auto` so controls stay
  visible. The log controls row (container selector + follow/pause) wraps
  naturally.

## Forms

Already stacked via `pure-form-stacked`. On mobile:
- Reduce horizontal padding on the form container.
- Input fields remain full-width (already 100% by default).
- No structural changes.

## CSS changes

All additions go in `internal/ui/static/app.css`. The new rules are scoped under
`@media (max-width: 640px)` where they differ from desktop, plus a few base
additions (hamburger, backdrop).

New classes / additions:
- `.hamburger` — fixed-position toggle button, hidden on desktop.
- `.sidebar.open` — sidebar slides into view on mobile.
- `.sidebar-backdrop` — semi-transparent overlay behind the open sidebar.
- `.card-table` applied to tables at ≤640px — transforms rows into cards.
- `.container-row` gets `flex-direction: column` at ≤640px.
- `pre#log-lines` gets `pre-wrap` + `max-height` unconditionally (safe on
  desktop too).
- `.inst-head .actions` gets `flex-wrap: wrap`.

## Template changes

- `layout.html`: add hamburger icon `<button>` before the sidebar, backdrop
  `<div>`, and the toggle `<script>` inline.

- `dashboard.html`: add `data-label="Host"` and `data-label="Instances"` to the
  two `<td>` elements.

- `host-instances.html`: add `data-label="Template/Slug"` and `data-label="Status"`
  to the two `<td>` elements.

- `jobs.html`: add `data-label="ID"`, `data-label="Kind"`, `data-label="State"`
  to the three `<td>` elements.

The card-table CSS uses `attr(data-label)` in `::before` to display the column
name — no hardcoded strings in CSS, no JS.

## JS footprint

One event listener in `layout.html`:

```js
(function(){
  var hamburger = document.getElementById('hamburger');
  var sidebar = document.querySelector('.sidebar');
  if (!hamburger || !sidebar) return;
  hamburger.addEventListener('click', function() {
    sidebar.classList.toggle('open');
  });
  // Close sidebar on backdrop click
  sidebar.addEventListener('click', function(e) {
    if (e.target === sidebar) sidebar.classList.remove('open');
  });
  // Close sidebar after navigating (HTMX swap triggers this via htmx:afterSwap)
  document.body.addEventListener('htmx:afterSwap', function() {
    sidebar.classList.remove('open');
  });
})();
```

## Files touched

| File | Changes |
|------|---------|
| `internal/ui/static/app.css` | All responsive CSS rules |
| `internal/ui/templates/layout.html` | Hamburger button, backdrop `<div>`, toggle script |
| `internal/ui/templates/dashboard.html` | `data-label` on `<td>` for card layout |
| `internal/ui/templates/host-instances.html` | `data-label` on `<td>` for card layout |
| `internal/ui/templates/jobs.html` | `data-label` on `<td>` for card layout |

## Testing

Open every page at ≤640px viewport width and verify:
- [ ] Sidebar hidden by default, opens on hamburger tap
- [ ] Sidebar closes on backdrop tap and after HTMX navigation
- [ ] Tables render as cards with labels readable
- [ ] Container info stacks vertically
- [ ] Backup entries stack with buttons below
- [ ] Action buttons wrap without overflow
- [ ] Log text wraps and is vertically scrollable
- [ ] Forms render full-width with comfortable tap targets
- [ ] Login page centered and usable
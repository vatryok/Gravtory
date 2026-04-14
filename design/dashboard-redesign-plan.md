# Dashboard Redesign Plan

## Current Problems

1. **CDN dependency** -- loads TailwindCSS from cdn.tailwindcss.com, breaks offline/air-gapped
2. **No self-contained CSS** -- relies on runtime Tailwind JIT which is 300KB+ JS download
3. **Poor UX** -- no loading skeletons, no toast notifications, no confirmation dialogs
4. **No responsive design** -- sidebar is fixed 48px, breaks on mobile
5. **No dark mode toggle** -- hardcoded light theme only
6. **Primitive tables** -- no sorting, no column resize, no row selection
7. **No real-time feel** -- SSE connected but no visual indicators of live updates
8. **No search** -- can only filter by status dropdown, no text search across all fields
9. **No keyboard shortcuts** -- mouse-only navigation
10. **No error boundaries** -- API failures show raw "Failed to load" text
11. **Signal sending has no form UI** -- only accessible via API
12. **No workflow visualization** -- no DAG graph view of step dependencies
13. **No log/traceback viewer** -- error_traceback stored but never displayed

## Design Principles

- **Zero external dependencies** -- all CSS/JS inline in single HTML file
- **Offline-first** -- works without internet connection
- **Professional appearance** -- clean, modern, data-dense like Grafana/Datadog
- **Dark/light mode** -- system preference detection + manual toggle
- **Responsive** -- works on mobile, tablet, desktop
- **Accessible** -- keyboard navigation, ARIA labels, focus indicators
- **Real-time** -- live update indicators, auto-refresh with visual feedback

## Architecture

Single file: `src/gravtory/dashboard/static/index.html`

```
Structure:
  <style>     -- All CSS inline (custom properties for theming, grid layout, component styles)
  <body>      -- Semantic HTML structure
  <script>    -- Vanilla JS SPA (no framework, no build step)
    - Router        -- hash-based SPA routing
    - API client    -- fetch wrapper with auth, error handling, retry
    - State         -- simple reactive state store
    - Components    -- render functions for each view
    - SSE           -- real-time event handling with visual feedback
```

## Layout

```
+----------------------------------------------------------+
| HEADER: Logo | Health | Workers | DLQ | Theme | Search   |
+------+-----------------------------------------------+---+
| NAV  | MAIN CONTENT                                  |   |
|      |                                               |   |
| [i]  |  Breadcrumb / Page Title                      |   |
| Wf   |  +-----------------------------------------+  |   |
| DLQ  |  | Content area with cards/tables/graphs   |  |   |
| Wrk  |  |                                         |  |   |
| Sch  |  |                                         |  |   |
| Stat |  +-----------------------------------------+  |   |
|      |                                               |   |
|      |  Pagination / Actions bar                     |   |
+------+-----------------------------------------------+---+
| FOOTER: Gravtory v0.1.0 | Uptime | Last refresh         |
+----------------------------------------------------------+
```

## Pages

### 1. Overview Dashboard (default)
- Summary cards: total, running, completed, failed, DLQ, workers
- Mini bar chart of workflow status distribution
- Recent activity feed (last 10 events from SSE)
- Quick actions: view failed, view running

### 2. Workflows List
- Sortable table: ID, Name, Status, Step, Created, Duration
- Filters: status dropdown, name search, date range
- Bulk actions: retry selected, cancel selected
- Pagination with page size selector (25/50/100)
- Click row to open detail

### 3. Workflow Detail
- Header card: name, ID, status badge, created/updated/completed times
- Action buttons: Retry (if failed), Cancel (if running), Send Signal
- Step timeline: horizontal bar chart showing duration per step
- Steps table: order, name, status, duration, retries, error
- Error panel: expandable traceback viewer with syntax highlighting
- Signal form: send a signal with JSON data editor

### 4. Dead Letter Queue
- Table: run ID, step, error (expandable), retries, created
- Actions per row: retry, discard
- Bulk actions: purge all (with confirmation)
- Error detail expandable on click

### 5. Workers
- Table: ID, node, status (with health dot), heartbeat, current task
- Health indicator: green (<30s), yellow (<2min), red (>2min)
- Auto-refresh every 10s

### 6. Schedules
- Table: workflow, type, config, enabled toggle, last run, next run
- Toggle enable/disable inline

### 7. Statistics
- Large number cards with trend indicators
- Status distribution (horizontal stacked bar)

## Component Library (CSS)

All built with CSS custom properties for theming:

```css
:root {
  --bg-primary, --bg-secondary, --bg-card
  --text-primary, --text-secondary, --text-muted
  --border, --border-active
  --accent, --success, --warning, --danger, --info
  --radius-sm, --radius-md, --radius-lg
  --shadow-sm, --shadow-md
  --font-mono
}

[data-theme="dark"] {
  /* dark overrides */
}
```

Components:
- `.card` -- rounded container with shadow
- `.badge` -- status badges (completed, failed, running, etc.)
- `.btn` -- primary, secondary, danger, ghost variants
- `.table` -- sortable data table with hover, striped rows
- `.input` -- text input, select, textarea
- `.modal` -- confirmation dialog overlay
- `.toast` -- notification popups (success, error, info)
- `.skeleton` -- loading placeholder animation
- `.sidebar` -- collapsible navigation
- `.breadcrumb` -- navigation trail
- `.timeline` -- horizontal step duration bars
- `.code` -- monospace code/traceback viewer
- `.empty-state` -- centered illustration + message for empty views
- `.tooltip` -- hover information

## Implementation Order

1. CSS foundation (variables, reset, layout grid, components)
2. JS core (router, API client, state, utilities)
3. Sidebar + header + footer shell
4. Overview dashboard page
5. Workflows list page
6. Workflow detail page (with timeline, error viewer, signal form)
7. DLQ page
8. Workers page
9. Schedules page
10. Statistics page
11. Dark mode toggle
12. Toast notifications
13. Confirmation dialogs
14. Keyboard shortcuts
15. SSE live update indicators
16. Search

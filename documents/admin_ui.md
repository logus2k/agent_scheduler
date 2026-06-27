# Admin Web UI

A lightweight administration UI for Scheduler Agent, served by the **same
container** as the API — no separate frontend service, no CORS, no build step.

## Where it lives

- **Public URL:** `https://logus2k.com/scheduler/` (behind the proxy; owner-only
  Google login — see Access below). `/scheduler` redirects to `/scheduler/`.
- **Direct (host):** `http://127.0.0.1:6816/` — the UI is at the app **root**.
- **Served by:** FastAPI `StaticFiles` mounted at `/` (see [api.py](../src/agent_scheduler/api.py)),
  from the `frontend/` directory (`FRONTEND_DIR`, default `frontend`), `COPY`'d into
  the image. It is the **last** mount so it only matches paths not already handled
  by the API routes or the `/docs` mount. If the directory is absent the mount is
  skipped and a warning is logged.

## Access (auth)

The proxy gates `/scheduler/` with `auth_request /oauth2/auth-admin`, which rewrites
to `/oauth2/auth?allowed_emails=logus2k%40gmail.com` — so it **requires Google
sign-in and accepts only the owner's email** (same gate as `/avatar/admin`,
`/jobunter/admin`, `/llm/admin`). The app itself has no auth; it relies on the proxy.

## Why single-container

For an internal CRUD admin over one service, a separate nginx container adds a
container, config, and CORS/proxy for no real benefit. Serving the static files
from FastAPI keeps it same-origin (the browser hits the app for both UI and API),
so the SDK client uses a path-derived base and there is nothing to configure.
Revisit only if the UI grows into a heavy SPA with a build step or needs an
independent release cadence.

## Stack

Vanilla **ES6** (classes + modules), zero dependencies, no bundler:

```
frontend/
  index.html                 # form + jobs table
  styles.css                 # dark theme
  app.js                     # AdminApp class — wires the UI to the client
  agentSchedulerClient.js    # VENDORED copy of sdk/javascript/ for browser delivery
```

`app.js` imports the **same ES6 client** other consumers use ([sdk/javascript/agentSchedulerClient.js](../sdk/javascript/agentSchedulerClient.js)); the copy under `frontend/` is what the browser downloads. Keep it in sync with the canonical SDK file (header comment notes the source).

## Features

- **Health badge** — polls `/health` every 10s (status dot + job count).
- **Light / dark theme** — header toggle, persisted in `localStorage` (set before
  first paint to avoid a flash); the button shows the theme it will switch *to*.
- **Per-field help** — a `?` badge beside every form field (hover or keyboard
  focus) explaining its purpose and when/how to set it.
- **Help panel** — header **Help** button toggles a **floating, draggable,
  resizable, non-modal** panel (drag by its title bar, resize from the
  bottom-right corner, close with ✕ or Esc) so you can read it while filling the
  form. It fetches and renders [use_cases.md](use_cases.md) (six worked examples).
  Docs are served at `/docs` (`DOCS_DIR`, default `documents`, `COPY`'d into the
  image) and rendered by a small dependency-free Markdown renderer in `app.js` —
  single source of truth, no duplicated help content.
- **Create job** — trigger-type selector that swaps the relevant args (interval
  fields / cron expression / date picker), plus optional target stream, event
  type, room, paused, and a JSON event-data box (validated client-side).
- **Jobs table** — id, trigger, next run, resolved stream, event type, state;
  per-row **Pause/Resume**, **Run now**, **Delete** (with confirm).
- Typed-error messages surfaced inline / via toast (e.g. 409 duplicate, 422
  validation) using the SDK's error classes.

## Verification

- Static assets serve with correct MIME types (`/admin/{index,app.js,styles.css,
  agentSchedulerClient.js}` → 200).
- ES modules pass `node --check`; the full create → list → pause → run → delete
  flow was exercised against the running container via the same client the UI uses.
- 28 backend tests pass with the mount active.
</content>

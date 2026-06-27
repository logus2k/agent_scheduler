# Admin Web UI

A lightweight administration UI for the Agent Scheduler, served by the **same
container** as the API — no separate frontend service, no CORS, no build step.

## Where it lives

- **URL:** `http://agent-scheduler-app:6816/admin/` (localhost-bound on the host:
  `http://127.0.0.1:6816/admin/`). `/admin` 307-redirects to `/admin/`.
- **Served by:** FastAPI `StaticFiles` mounted at `/admin` (see [api.py](../src/agent_scheduler/api.py)),
  from the `frontend/` directory (`FRONTEND_DIR`, default `frontend`), `COPY`'d into
  the image. If the directory is absent the mount is skipped and a warning is logged.

## Why single-container

For an internal CRUD admin over one service, a separate nginx container adds a
container, config, and CORS/proxy for no real benefit. Serving the static files
from FastAPI keeps it same-origin (the browser hits `:6816` for both UI and API),
so the SDK client uses a relative base (`new SchedulerClient("")`) and there is
nothing to configure. Revisit only if the UI grows into a heavy SPA with a build
step or needs an independent release cadence.

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

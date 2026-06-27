// Agent Scheduler — Admin UI (vanilla ES6, class-based).
// Reuses the same SDK client shipped to other consumers; same-origin base ("")
// because this page and the API are served by one process.

import {
  SchedulerClient,
  SchedulerError,
  ValidationError,
} from "./agentSchedulerClient.js";

class AdminApp {
  constructor() {
    // Derive the API base from where this UI is served, so it works both
    // directly (".../admin/" -> base "") and behind a reverse-proxy prefix
    // (e.g. "/scheduler/admin/" -> base "/scheduler"). The API lives one level
    // up from the /admin mount, same origin.
    const base = window.location.pathname.replace(/\/admin\/?.*$/, "");
    this.client = new SchedulerClient(base);
    this.$ = (sel) => document.querySelector(sel);
  }

  init() {
    this.applyTheme(localStorage.getItem("theme") || "dark");
    this.$("#theme-toggle").addEventListener("click", () => this.toggleTheme());

    this.form = this.$("#create-form");
    this.form.addEventListener("submit", (e) => this.onCreate(e));
    this.$("#trigger-type").addEventListener("change", () => this.syncTriggerArgs());
    this.$("#refresh").addEventListener("click", () => this.loadJobs());
    this.$("#jobs-body").addEventListener("click", (e) => this.onRowAction(e));

    this.syncTriggerArgs();
    this.pollHealth();
    this.loadJobs();
    setInterval(() => this.pollHealth(), 10000);
  }

  // --- theme --------------------------------------------------------------

  applyTheme(theme) {
    document.documentElement.setAttribute("data-theme", theme);
    localStorage.setItem("theme", theme);
    // The button shows the theme it will switch TO, not the current one.
    const light = theme === "light";
    this.$("#theme-icon").textContent = light ? "🌙" : "☀️";
    this.$("#theme-label").textContent = light ? "Dark" : "Light";
  }

  toggleTheme() {
    const current = document.documentElement.getAttribute("data-theme");
    this.applyTheme(current === "light" ? "dark" : "light");
  }

  // --- health -------------------------------------------------------------

  async pollHealth() {
    const dot = this.$("#health-dot");
    const text = this.$("#health-text");
    try {
      const h = await this.client.health();
      dot.className = "dot ok";
      text.textContent = `ok · ${h.jobs} job(s)`;
    } catch {
      dot.className = "dot bad";
      text.textContent = "unavailable";
    }
  }

  // --- create form --------------------------------------------------------

  syncTriggerArgs() {
    const type = this.$("#trigger-type").value;
    for (const block of document.querySelectorAll(".trigger-args")) {
      block.hidden = block.dataset.for !== type;
    }
  }

  async onCreate(event) {
    event.preventDefault();
    const msg = this.$("#create-msg");
    msg.className = "form-msg";
    msg.textContent = "";

    const f = this.form;
    const get = (name) => f.elements[name].value.trim();
    const type = get("trigger_type");

    // Optional shared fields.
    const opts = {};
    if (get("target_stream_id")) opts.targetStreamId = get("target_stream_id");
    if (get("event_type")) opts.eventType = get("event_type");
    if (get("room")) opts.room = get("room");
    if (f.elements["paused"].checked) opts.paused = true;

    const rawData = get("event_data");
    if (rawData) {
      try {
        opts.eventData = JSON.parse(rawData);
      } catch {
        return this.formError("Event data must be valid JSON");
      }
    }

    const jobId = get("job_id");
    try {
      let view;
      if (type === "interval") {
        const interval = {
          seconds: Number(get("seconds")) || 0,
          minutes: Number(get("minutes")) || 0,
          hours: Number(get("hours")) || 0,
          days: Number(get("days")) || 0,
        };
        if (!Object.values(interval).some((v) => v > 0)) {
          return this.formError("Interval needs at least one non-zero field");
        }
        view = await this.client.createInterval(jobId, interval, opts);
      } else if (type === "cron") {
        if (!get("cron_expression")) return this.formError("Cron expression required");
        view = await this.client.createCron(jobId, get("cron_expression"), opts);
      } else {
        if (!get("run_date")) return this.formError("Run date required");
        view = await this.client.createDate(jobId, get("run_date"), opts);
      }
      msg.className = "form-msg ok";
      msg.textContent = `Created → ${view.resolved_stream}`;
      f.reset();
      this.syncTriggerArgs();
      this.loadJobs();
      this.pollHealth();
    } catch (err) {
      this.formError(this.describe(err));
    }
  }

  formError(text) {
    const msg = this.$("#create-msg");
    msg.className = "form-msg bad";
    msg.textContent = text;
  }

  // --- jobs table ---------------------------------------------------------

  async loadJobs() {
    const body = this.$("#jobs-body");
    try {
      const jobs = await this.client.listJobs();
      this.$("#job-count").textContent = `(${jobs.length})`;
      if (!jobs.length) {
        body.innerHTML = `<tr><td colspan="7" class="muted">no jobs yet</td></tr>`;
        return;
      }
      body.innerHTML = jobs.map((j) => this.rowHtml(j)).join("");
    } catch (err) {
      body.innerHTML = `<tr><td colspan="7" class="form-msg bad">${this.describe(err)}</td></tr>`;
    }
  }

  rowHtml(j) {
    const next = j.next_run_time
      ? new Date(j.next_run_time).toLocaleString()
      : "—";
    const state = j.paused
      ? `<span class="badge paused">paused</span>`
      : `<span class="badge active">active</span>`;
    const id = this.esc(j.job_id);
    const toggle = j.paused
      ? `<button class="sm" data-act="resume" data-id="${id}">Resume</button>`
      : `<button class="sm" data-act="pause" data-id="${id}">Pause</button>`;
    return `<tr>
      <td><strong>${id}</strong></td>
      <td>${this.esc(j.trigger)}</td>
      <td>${next}</td>
      <td><code>${this.esc(j.resolved_stream)}</code></td>
      <td><code>${this.esc(j.event_type)}</code></td>
      <td>${state}</td>
      <td class="row-actions">
        ${toggle}
        <button class="sm" data-act="run" data-id="${id}">Run now</button>
        <button class="sm danger" data-act="delete" data-id="${id}">Delete</button>
      </td>
    </tr>`;
  }

  async onRowAction(event) {
    const btn = event.target.closest("button[data-act]");
    if (!btn) return;
    const { act, id } = btn.dataset;
    try {
      if (act === "pause") await this.client.pauseJob(id);
      else if (act === "resume") await this.client.resumeJob(id);
      else if (act === "run") {
        const r = await this.client.runJob(id);
        return this.toast(`Fired ${id} → ${r.entry_id}`, "ok"), this.pollHealth();
      } else if (act === "delete") {
        if (!confirm(`Delete job "${id}"?`)) return;
        await this.client.deleteJob(id);
        this.toast(`Deleted ${id}`, "ok");
      }
      if (act !== "run") this.toast(`${act} ${id}`, "ok");
      this.loadJobs();
      this.pollHealth();
    } catch (err) {
      this.toast(this.describe(err), "bad");
    }
  }

  // --- helpers ------------------------------------------------------------

  describe(err) {
    if (err instanceof ValidationError) {
      const d = err.detail;
      if (Array.isArray(d)) return d.map((e) => e.msg).join("; ");
      return typeof d === "string" ? d : JSON.stringify(d);
    }
    if (err instanceof SchedulerError) {
      return typeof err.detail === "string" ? err.detail : err.message;
    }
    return err.message || "request failed";
  }

  esc(s) {
    return String(s).replace(/[&<>"']/g, (c) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c])
    );
  }

  toast(text, kind) {
    const el = this.$("#toast");
    el.textContent = text;
    el.className = `toast ${kind}`;
    el.hidden = false;
    clearTimeout(this._toastTimer);
    this._toastTimer = setTimeout(() => (el.hidden = true), 3000);
  }
}

new AdminApp().init();

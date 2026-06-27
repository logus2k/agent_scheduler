/**
 * Agent Scheduler — JavaScript (ES6) client SDK.
 *
 * Zero-dependency ES module over the scheduler's REST API. Uses the native
 * `fetch` (browser, Node 18+, Deno, Bun). Methods return Promises that resolve
 * to plain objects shaped like the API's `JobView`, and reject with a typed
 * `SchedulerError` subclass so callers can branch on `err.status`.
 *
 * @example
 *   import { SchedulerClient } from "./agentSchedulerClient.js";
 *   const sched = new SchedulerClient("http://agent-scheduler-app:6816");
 *   await sched.createCron("nightly-report", "0 2 * * *", {
 *     eventData: { report: "daily_sales" },
 *   });
 *   for (const job of await sched.listJobs()) {
 *     console.log(job.job_id, "->", job.resolved_stream, job.next_run_time);
 *   }
 */

export class SchedulerError extends Error {
  constructor(status, detail) {
    super(`[${status}] ${typeof detail === "string" ? detail : JSON.stringify(detail)}`);
    this.name = "SchedulerError";
    this.status = status;
    this.detail = detail;
  }
}
export class JobNotFoundError extends SchedulerError {}      // 404
export class JobConflictError extends SchedulerError {}      // 409
export class ValidationError extends SchedulerError {}       // 422
export class ServiceUnavailableError extends SchedulerError {} // 503

const ERROR_BY_STATUS = {
  404: JobNotFoundError,
  409: JobConflictError,
  422: ValidationError,
  503: ServiceUnavailableError,
};

export class SchedulerClient {
  /**
   * @param {string} baseUrl  e.g. "http://agent-scheduler-app:6816"
   * @param {object} [opts]
   * @param {number} [opts.timeoutMs=10000]
   * @param {typeof fetch} [opts.fetch]  inject a custom fetch (tests/SSR)
   */
  constructor(baseUrl, { timeoutMs = 10000, fetch: fetchImpl } = {}) {
    this.baseUrl = baseUrl.replace(/\/+$/, "");
    this.timeoutMs = timeoutMs;
    this._fetch = fetchImpl || globalThis.fetch.bind(globalThis);
  }

  async _request(method, path, body) {
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), this.timeoutMs);
    let resp;
    try {
      resp = await this._fetch(this.baseUrl + path, {
        method,
        headers: body !== undefined ? { "Content-Type": "application/json" } : undefined,
        body: body !== undefined ? JSON.stringify(body) : undefined,
        signal: ctrl.signal,
      });
    } finally {
      clearTimeout(timer);
    }

    if (resp.ok) {
      if (resp.status === 204) return null;
      const text = await resp.text();
      return text ? JSON.parse(text) : null;
    }

    let detail;
    try {
      detail = (await resp.json()).detail;
    } catch {
      detail = await resp.text().catch(() => resp.statusText);
    }
    const Err = ERROR_BY_STATUS[resp.status] || SchedulerError;
    throw new Err(resp.status, detail);
  }

  // --- ops ------------------------------------------------------------------

  /** Liveness + Valkey/job-store reachability. Rejects on 503. */
  health() {
    return this._request("GET", "/health");
  }

  listJobs() {
    return this._request("GET", "/jobs");
  }

  getJob(jobId) {
    return this._request("GET", `/jobs/${encodeURIComponent(jobId)}`);
  }

  /**
   * Create a job.
   * @param {object} spec
   * @param {string} spec.jobId
   * @param {"interval"|"cron"|"date"} spec.triggerType
   * @param {object} spec.triggerArgs
   * @param {string|null} [spec.targetStreamId]
   * @param {string} [spec.eventType="schedule.fired"]
   * @param {object} [spec.eventData={}]
   * @param {string|null} [spec.room]
   * @param {boolean} [spec.paused=false]
   */
  createJob({
    jobId,
    triggerType,
    triggerArgs,
    targetStreamId = null,
    eventType = "schedule.fired",
    eventData = {},
    room = null,
    paused = false,
  }) {
    return this._request("POST", "/jobs", {
      job_id: jobId,
      trigger_type: triggerType,
      trigger_args: triggerArgs,
      target_stream_id: targetStreamId,
      event_type: eventType,
      event_data: eventData,
      room,
      paused,
    });
  }

  // convenience constructors --------------------------------------------------

  /** @param {object} interval e.g. { seconds: 300 } or { minutes: 15 } */
  createInterval(jobId, interval, opts = {}) {
    const args = Object.fromEntries(
      Object.entries(interval).filter(([, v]) => v)
    );
    return this.createJob({ jobId, triggerType: "interval", triggerArgs: args, ...opts });
  }

  createCron(jobId, cronExpression, opts = {}) {
    return this.createJob({
      jobId,
      triggerType: "cron",
      triggerArgs: { cron_expression: cronExpression },
      ...opts,
    });
  }

  /** @param {string|Date} runDate ISO-8601 string or Date */
  createDate(jobId, runDate, opts = {}) {
    const iso = runDate instanceof Date ? runDate.toISOString() : runDate;
    return this.createJob({
      jobId,
      triggerType: "date",
      triggerArgs: { run_date: iso },
      ...opts,
    });
  }

  /** Partial update. Pass triggerType + triggerArgs together to reschedule. */
  updateJob(jobId, fields = {}) {
    const map = {
      triggerType: "trigger_type",
      triggerArgs: "trigger_args",
      targetStreamId: "target_stream_id",
      eventType: "event_type",
      eventData: "event_data",
      room: "room",
    };
    const body = {};
    for (const [k, apiKey] of Object.entries(map)) {
      if (fields[k] !== undefined && fields[k] !== null) body[apiKey] = fields[k];
    }
    return this._request("PATCH", `/jobs/${encodeURIComponent(jobId)}`, body);
  }

  deleteJob(jobId) {
    return this._request("DELETE", `/jobs/${encodeURIComponent(jobId)}`);
  }

  pauseJob(jobId) {
    return this._request("POST", `/jobs/${encodeURIComponent(jobId)}/pause`);
  }

  resumeJob(jobId) {
    return this._request("POST", `/jobs/${encodeURIComponent(jobId)}/resume`);
  }

  /** Emit once now, off-schedule. Resolves to { status, job_id, entry_id }. */
  runJob(jobId) {
    return this._request("POST", `/jobs/${encodeURIComponent(jobId)}/run`);
  }
}

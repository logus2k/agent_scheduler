# Scheduler Agent — Usage Guide & Examples

Six common things you can do from this admin screen. Each shows exactly what to
put in the **Create job** form. Every field also has a `?` badge — hover or focus
it for an inline explanation.

How the form works: pick a **Trigger** type and the matching fields appear
(interval boxes / crontab / date picker). Fill in **Event data** (the JSON your
consumer receives), optionally set a **Target stream**, then click **Create**.
The new job shows up in the **Jobs** table below, where each row has **Pause /
Resume**, **Run now**, and **Delete**.

> The scheduler only *emits an event* when a job fires — it doesn't do the work
  itself. Some other service listens on the job's stream and acts on it. Each
  job's create result shows its **Stream** column so you know where events land.

---

## 1. Nightly report (run on a calendar schedule)

Generate a report every day at 02:00.

| Field | Value |
| --- | --- |
| Job ID | `nightly-report` |
| Trigger | `cron` |
| Crontab (5-field) | `0 2 * * *` |
| Event data (JSON) | `{ "report": "daily_sales", "format": "pdf" }` |

Click **Create**. The job appears in the table with **Next run** at the next
02:00, and its events go to **stream:nightly-report**.

*Other crontab examples:* `*/15 * * * *` every 15 min · `0 9 * * 1` Mondays 09:00
· `0 0 1 * *` first of each month.

---

## 2. Service heartbeat (repeat at a fixed spacing)

Emit a liveness tick every 30 seconds.

| Field | Value |
| --- | --- |
| Job ID | `heartbeat` |
| Trigger | `interval` |
| Seconds | `30` |
| Event data (JSON) | `{ "check": "liveness" }` |

The interval boxes add up, so **Minutes = 1** + **Seconds = 30** would mean every
90 seconds. At least one box must be greater than 0.

---

## 3. One-shot scheduled launch (fire once at a moment)

Kick something off once, at a specific date/time — e.g. a campaign go-live.

| Field | Value |
| --- | --- |
| Job ID | `campaign-launch-q3` |
| Trigger | `date` |
| Run date | pick `2026-07-01 09:00` in the date picker |
| Event data (JSON) | `{ "campaign": "summer_sale" }` |

After it fires once, the job is finished (it won't repeat).

---

## 4. Trigger an existing workflow on a schedule (custom Event type)

If another service already reacts to a particular event type — say the workflow
pipeline starts on `request` — you can drive it on a schedule with no extra
glue, by setting **Event type** and pointing **Target stream** at that service.

| Field | Value |
| --- | --- |
| Job ID | `hourly-digest-workflow` |
| Trigger | `cron` |
| Crontab (5-field) | `0 * * * *` |
| Target stream | `digest-bot` |
| Event type | `request` |
| Event data (JSON) | `{ "text": "Summarize the last hour of activity" }` |

Every hour this sends a `request` event to **stream:digest-bot**, which the
existing agent handles as if a user had asked. Leave **Event type** at its default
(`schedule.fired`) unless you specifically want to match an existing consumer.

---

## 5. Fan-out to a shared stream / "room" (Target stream)

Have several services react to the same tick by pointing the job at one shared
stream instead of the per-job default.

| Field | Value |
| --- | --- |
| Job ID | `ops-tick` |
| Trigger | `interval` |
| Minutes | `15` |
| Target stream | `ops-dashboard` |
| Room | `ops-dashboard` |
| Event data (JSON) | `{ "tick": "refresh" }` |

Events now land on **stream:ops-dashboard** (shown in the **Stream** column), not
`stream:ops-tick`. Leave **Target stream** empty and a job just uses
`stream:<Job ID>`. **Room** is an optional hint for a future room-aware delivery
layer; it doesn't change where events are published.

---

## 6. Staged or manual-only job (Create paused + Run now)

Define a job now but don't let it fire yet — useful to stage before go-live, or
to make a job that only ever runs on demand.

| Field | Value |
| --- | --- |
| Job ID | `reindex-search` |
| Trigger | `cron` |
| Crontab (5-field) | `0 3 * * 0` |
| Event data (JSON) | `{ "task": "reindex" }` |
| Create paused | ✓ checked |

Click **Create**. In the table the job shows as **paused** with no next run. Then:

- **Run now** — fires it once immediately, off-schedule (works even while
  paused) — handy to test the consumer.
- **Resume** — activates the schedule so it starts firing at 03:00 on Sundays.
- **Pause** — stops a running job again without deleting it.

---

## Reading the Jobs table

| Column | Meaning |
| --- | --- |
| Job ID | the name you gave it |
| Trigger | the schedule (interval / cron / date, expanded) |
| Next run | when it will fire next — blank when paused |
| Stream | exactly where its events are published |
| Event type | the type stamped on emitted events |
| State | active or paused |

Use **Refresh** to re-read the list, the **Help** button to reopen this guide,
and the theme toggle for light/dark.

---

*Prefer to automate this from code instead of the screen? The same actions are
available as a REST API and Python / JavaScript SDKs — see the interface and SDK
documents.*
</content>

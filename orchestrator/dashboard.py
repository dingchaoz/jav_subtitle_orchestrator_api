from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

from orchestrator.models import (
    CallbackStatusResponse,
    DashboardJobSummary,
    DashboardStateResponse,
    JobDetailResponse,
    JobLogSummary,
    JobLogTailResponse,
    JobLogsResponse,
    JobStatus,
)
from orchestrator.store import CallbackEventRecord, JobRecord, JobStore


MAC_ACTIVE_STATUSES = {
    JobStatus.DOWNLOADING_METADATA,
    JobStatus.DOWNLOADING_AUDIO,
}

WINDOWS_ACTIVE_STATUSES = {
    JobStatus.TRANSCRIPTION_CLAIMED,
    JobStatus.TRANSCRIBING,
    JobStatus.TRANSCRIPTION_DONE,
    JobStatus.TRANSLATING,
}


def job_summary(job: JobRecord) -> DashboardJobSummary:
    return DashboardJobSummary(
        id=job.id,
        movie_number=job.normalized_movie_number,
        status=job.status,
        priority=job.priority,
        updated_at=job.updated_at,
        claimed_by=job.claimed_by,
        error=job.error,
    )


def callback_status(event: CallbackEventRecord | None) -> CallbackStatusResponse | None:
    if event is None:
        return None
    return CallbackStatusResponse(
        event_type=event.event_type,
        status=event.status,
        attempt_count=event.attempt_count,
        updated_at=event.updated_at,
        delivered_at=event.delivered_at,
        last_error=event.last_error,
    )


def build_job_detail(
    job: JobRecord,
    callback_event: CallbackEventRecord | None = None,
) -> JobDetailResponse:
    return JobDetailResponse(
        id=job.id,
        movie_number=job.movie_number,
        normalized_movie_number=job.normalized_movie_number,
        status=job.status,
        priority=job.priority,
        attempt_count=job.attempt_count,
        worker_attempt_count=job.worker_attempt_count,
        claimed_by=job.claimed_by,
        lease_expires_at=job.lease_expires_at,
        created_at=job.created_at,
        updated_at=job.updated_at,
        error=job.error,
        job_dir_mac=job.job_dir_mac,
        job_dir_windows=job.job_dir_windows,
        metadata_path_mac=job.metadata_path_mac,
        audio_path_mac=job.audio_path_mac,
        audio_path_windows=job.audio_path_windows,
        japanese_srt_path_mac=job.japanese_srt_path_mac,
        japanese_srt_path_windows=job.japanese_srt_path_windows,
        english_srt_path_mac=job.english_srt_path_mac,
        english_srt_path_windows=job.english_srt_path_windows,
        callback=callback_status(callback_event),
    )


def dashboard_recency_key(job: JobRecord) -> tuple[str, str, str]:
    return (job.updated_at, job.created_at, job.id)


def _latest_active_job(jobs: list[JobRecord], statuses: set[JobStatus]) -> JobRecord | None:
    candidates = [job for job in jobs if job.status in statuses]
    if not candidates:
        return None
    return sorted(candidates, key=dashboard_recency_key, reverse=True)[0]


def _activity_payload(job: JobRecord | None) -> dict[str, str | None]:
    if job is None:
        return {
            "status": "idle",
            "movie_number": None,
            "job_id": None,
            "worker_id": None,
            "updated_at": None,
        }
    return {
        "status": job.status.value,
        "movie_number": job.normalized_movie_number,
        "job_id": job.id,
        "worker_id": job.claimed_by,
        "updated_at": job.updated_at,
    }


def build_dashboard_state(store: JobStore, *, latest_limit: int = 50) -> DashboardStateResponse:
    jobs = store.list_jobs()
    counts = Counter(job.status.value for job in jobs)
    latest = sorted(jobs, key=dashboard_recency_key, reverse=True)[:latest_limit]
    errors = [
        job
        for job in sorted(jobs, key=dashboard_recency_key, reverse=True)
        if job_has_active_error(job)
    ]
    mac_job = _latest_active_job(jobs, MAC_ACTIVE_STATUSES)
    windows_job = _latest_active_job(jobs, WINDOWS_ACTIVE_STATUSES)
    return DashboardStateResponse(
        api={
            "online": True,
            "server_time": datetime.now(UTC).replace(microsecond=0).isoformat(),
            "jobs_root_mac": str(store.jobs_root_mac),
            "jobs_root_windows": store.jobs_root_windows,
        },
        activity={
            "mac": _activity_payload(mac_job),
            "windows": _activity_payload(windows_job),
        },
        counts=dict(counts),
        latest_jobs=[job_summary(job) for job in latest],
        active_errors=[job_summary(job) for job in errors],
    )


def job_has_active_error(job: JobRecord) -> bool:
    return job.status == JobStatus.FAILED or bool(job.error)


ALLOWED_LOG_NAMES = (
    "mac-download.log",
    "windows-worker.log",
    "whisper.log",
    "translate.log",
)


def _job_logs_dir(job: JobRecord) -> Path:
    return Path(job.job_dir_mac) / "logs"


def _resolve_allowed_log_path(job: JobRecord, log_name: str) -> Path:
    if log_name not in ALLOWED_LOG_NAMES:
        raise FileNotFoundError(log_name)
    job_dir = Path(job.job_dir_mac)
    if job_dir.is_symlink():
        raise FileNotFoundError(log_name)
    logs_dir_raw = _job_logs_dir(job)
    if logs_dir_raw.is_symlink():
        raise FileNotFoundError(log_name)
    logs_dir = logs_dir_raw.resolve()
    path = (logs_dir / log_name).resolve()
    if path.parent != logs_dir:
        raise FileNotFoundError(log_name)
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(log_name)
    return path


def list_job_logs(job: JobRecord) -> JobLogsResponse:
    logs: list[JobLogSummary] = []
    for log_name in ALLOWED_LOG_NAMES:
        try:
            path = _resolve_allowed_log_path(job, log_name)
        except FileNotFoundError:
            continue
        logs.append(
            JobLogSummary(
                name=log_name,
                size_bytes=path.stat().st_size,
                available=True,
            )
        )
    return JobLogsResponse(job_id=job.id, logs=logs)


def read_job_log_tail(job: JobRecord, log_name: str, tail: int = 200) -> JobLogTailResponse:
    bounded_tail = min(max(tail, 1), 1000)
    path = _resolve_allowed_log_path(job, log_name)
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return JobLogTailResponse(
        job_id=job.id,
        log_name=log_name,
        tail=bounded_tail,
        lines=lines[-bounded_tail:],
    )


def dashboard_html() -> str:
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>JAV Subtitle Orchestrator</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f6f7f9;
      --panel: #ffffff;
      --panel-soft: #f0f4f8;
      --text: #16202a;
      --muted: #5d6b78;
      --border: #d7dee7;
      --accent: #1463ff;
      --accent-dark: #0b4fd0;
      --danger: #b42318;
      --ok: #067647;
      --warn: #b54708;
      --shadow: 0 1px 2px rgba(16, 24, 40, 0.08);
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      min-width: 320px;
      background: var(--bg);
      color: var(--text);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      line-height: 1.45;
    }

    a { color: var(--accent); text-decoration: none; }
    a:hover { text-decoration: underline; }

    header {
      position: sticky;
      top: 0;
      z-index: 10;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      padding: 14px clamp(16px, 3vw, 32px);
      border-bottom: 1px solid var(--border);
      background: rgba(255, 255, 255, 0.96);
      backdrop-filter: blur(10px);
    }

    h1, h2, h3, p { margin: 0; }

    h1 {
      font-size: 20px;
      line-height: 1.2;
      overflow-wrap: anywhere;
    }

    h2 { font-size: 16px; }
    h3 { font-size: 14px; }

    nav {
      display: flex;
      align-items: center;
      gap: 12px;
      flex-wrap: wrap;
      font-size: 14px;
    }

    main {
      display: grid;
      grid-template-columns: minmax(0, 1fr);
      gap: 18px;
      width: min(1440px, 100%);
      margin: 0 auto;
      padding: 18px clamp(16px, 3vw, 32px) 32px;
    }

    .health-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
    }

    .health-card,
    .panel {
      border: 1px solid var(--border);
      border-radius: 8px;
      background: var(--panel);
      box-shadow: var(--shadow);
    }

    .health-card {
      min-height: 112px;
      padding: 14px;
      display: grid;
      gap: 8px;
      align-content: start;
    }

    .health-title {
      color: var(--muted);
      font-size: 12px;
      font-weight: 700;
      letter-spacing: 0;
      text-transform: uppercase;
    }

    .health-value {
      font-size: 20px;
      font-weight: 700;
      overflow-wrap: anywhere;
    }

    .health-meta {
      color: var(--muted);
      font-size: 13px;
      overflow-wrap: anywhere;
    }

    .status-ok { color: var(--ok); }
    .status-warn { color: var(--warn); }
    .status-error { color: var(--danger); }

    .content-grid {
      display: grid;
      grid-template-columns: minmax(300px, 380px) minmax(0, 1fr);
      gap: 18px;
      align-items: start;
    }

    .side-stack,
    .main-stack {
      display: grid;
      gap: 18px;
      min-width: 0;
    }

    .panel-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      min-height: 48px;
      padding: 12px 14px;
      border-bottom: 1px solid var(--border);
    }

    .panel-header button { width: auto; }

    .panel-body {
      padding: 14px;
      min-width: 0;
    }

    form {
      display: grid;
      gap: 12px;
    }

    label {
      display: grid;
      gap: 6px;
      color: var(--muted);
      font-size: 13px;
      font-weight: 600;
      min-width: 0;
    }

    input,
    textarea,
    select,
    button {
      width: 100%;
      min-width: 0;
      border-radius: 8px;
      font: inherit;
    }

    input,
    textarea,
    select {
      border: 1px solid var(--border);
      background: #ffffff;
      color: var(--text);
      padding: 9px 10px;
    }

    textarea {
      min-height: 118px;
      resize: vertical;
    }

    button {
      min-height: 40px;
      border: 1px solid var(--accent-dark);
      background: var(--accent);
      color: #ffffff;
      padding: 9px 12px;
      font-weight: 700;
      cursor: pointer;
    }

    button:hover { background: var(--accent-dark); }
    button:disabled { cursor: not-allowed; opacity: 0.62; }

    .message {
      min-height: 20px;
      color: var(--muted);
      font-size: 13px;
      overflow-wrap: anywhere;
    }

    .jobs-list {
      display: grid;
      gap: 0;
    }

    .job-row {
      display: grid;
      grid-template-columns:
        minmax(120px, 1.2fr)
        minmax(112px, 1fr)
        72px
        minmax(120px, 1fr)
        minmax(150px, 1.2fr)
        minmax(150px, 1.2fr);
      gap: 12px;
      align-items: center;
      width: 100%;
      padding: 11px 14px;
      border: 0;
      border-bottom: 1px solid var(--border);
      border-radius: 0;
      background: #ffffff;
      color: var(--text);
      text-align: left;
      font-weight: 400;
    }

    .job-row:hover,
    .job-row:focus {
      background: var(--panel-soft);
      outline: none;
    }

    .job-row:last-child { border-bottom: 0; }

    .job-code,
    .job-status,
    .job-priority,
    .job-worker,
    .job-error,
    .job-updated {
      min-width: 0;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }

    .job-code { font-weight: 700; }
    .job-status, .job-priority, .job-worker, .job-error, .job-updated { color: var(--muted); font-size: 13px; }
    .job-error { color: var(--danger); }

    .detail-grid {
      display: grid;
      grid-template-columns: minmax(120px, 180px) minmax(0, 1fr);
      gap: 8px 12px;
      font-size: 13px;
    }

    .detail-grid dt {
      color: var(--muted);
      font-weight: 700;
    }

    .detail-grid dd {
      margin: 0;
      min-width: 0;
      overflow-wrap: anywhere;
    }

    .log-buttons {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 14px;
    }

    .log-buttons button {
      width: auto;
      min-height: 34px;
      max-width: 100%;
      border-color: var(--border);
      background: #ffffff;
      color: var(--text);
      overflow-wrap: anywhere;
    }

    .log-buttons button:hover { background: var(--panel-soft); }

    #log-output {
      min-height: 220px;
      max-height: 520px;
      overflow: auto;
      border-radius: 8px;
      background: #111827;
      color: #f9fafb;
      padding: 12px;
      font-family: "SFMono-Regular", Consolas, monospace;
      font-size: 12px;
      line-height: 1.5;
      white-space: pre-wrap;
      overflow-wrap: anywhere;
    }

    .empty {
      color: var(--muted);
      padding: 12px 0;
    }

    @media (max-width: 980px) {
      .health-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .content-grid { grid-template-columns: minmax(0, 1fr); }
    }

    @media (max-width: 640px) {
      header {
        align-items: flex-start;
        flex-direction: column;
      }

      .health-grid { grid-template-columns: minmax(0, 1fr); }

      .job-row {
        grid-template-columns: minmax(0, 1fr);
        gap: 4px;
      }

      .job-code,
      .job-status,
      .job-priority,
      .job-worker,
      .job-error,
      .job-updated {
        white-space: normal;
      }

      .detail-grid { grid-template-columns: minmax(0, 1fr); }
    }
  </style>
</head>
<body>
  <header>
    <h1>JAV Subtitle Orchestrator</h1>
    <nav aria-label="Primary">
      <a href="/dashboard">Dashboard</a>
      <a href="/docs">Swagger</a>
    </nav>
  </header>
  <main>
    <section class="health-grid" aria-label="Health">
      <article class="health-card">
        <div class="health-title">API</div>
        <div class="health-value" id="api-status">Loading</div>
        <div class="health-meta" id="api-meta">Fetching state</div>
      </article>
      <article class="health-card">
        <div class="health-title">Mac worker</div>
        <div class="health-value" id="mac-status">Loading</div>
        <div class="health-meta" id="mac-meta">Fetching state</div>
      </article>
      <article class="health-card">
        <div class="health-title">Windows worker</div>
        <div class="health-value" id="windows-status">Loading</div>
        <div class="health-meta" id="windows-meta">Fetching state</div>
      </article>
      <article class="health-card">
        <div class="health-title">Active errors</div>
        <div class="health-value" id="errors-status">Loading</div>
        <div class="health-meta" id="errors-meta">Fetching state</div>
      </article>
    </section>

    <section class="content-grid">
      <div class="side-stack">
        <section class="panel" aria-labelledby="single-submit-title">
          <div class="panel-header">
            <h2 id="single-submit-title">Single movie</h2>
          </div>
          <div class="panel-body">
            <form id="single-movie-form">
              <label>
                Movie number
                <input id="single-movie-number" name="movie_number" autocomplete="off" required>
              </label>
              <label>
                Priority
                <input id="single-priority" name="priority" type="number" value="100" min="0" max="9999" required>
              </label>
              <button type="submit">Submit movie</button>
              <div class="message" id="single-message" role="status"></div>
            </form>
          </div>
        </section>

        <section class="panel" aria-labelledby="batch-submit-title">
          <div class="panel-header">
            <h2 id="batch-submit-title">Batch movies</h2>
          </div>
          <div class="panel-body">
            <form id="batch-movie-form">
              <label>
                Movie numbers
                <textarea id="batch-movie-numbers" name="movie_numbers" required></textarea>
              </label>
              <label>
                Priority
                <input id="batch-priority" name="priority" type="number" value="100" min="0" max="9999" required>
              </label>
              <button type="submit">Submit batch</button>
              <div class="message" id="batch-message" role="status"></div>
            </form>
          </div>
        </section>

        <section class="panel" aria-labelledby="import-requested-title">
          <div class="panel-header">
            <h2 id="import-requested-title">Requested subtitles</h2>
          </div>
          <div class="panel-body">
            <form id="import-requested-form">
              <label>
                Minimum requests
                <input id="import-requested-min-count" name="min_count" type="number" value="1" min="1" max="9999" required>
              </label>
              <label>
                Limit
                <input id="import-requested-limit" name="limit" type="number" value="500" min="1" max="500" required>
              </label>
              <label>
                Priority
                <input id="import-requested-priority" name="priority" type="number" value="100" min="0" max="9999" required>
              </label>
              <button type="submit">Import requested subtitles</button>
              <div class="message" id="import-requested-message" role="status"></div>
            </form>
          </div>
        </section>
      </div>

      <div class="main-stack">
        <section class="panel" aria-labelledby="latest-jobs-title">
          <div class="panel-header">
            <h2 id="latest-jobs-title">Latest jobs</h2>
            <button type="button" id="refresh-button">Refresh</button>
          </div>
          <div id="jobs-list" class="jobs-list"></div>
        </section>

        <section class="panel" aria-labelledby="job-detail-title">
          <div class="panel-header">
            <h2 id="job-detail-title">Selected job</h2>
          </div>
          <div class="panel-body">
            <div id="job-detail" class="empty">Select a job from the latest jobs list.</div>
          </div>
        </section>

        <section class="panel" aria-labelledby="logs-title">
          <div class="panel-header">
            <h2 id="logs-title">Logs</h2>
          </div>
          <div class="panel-body">
            <pre id="log-output">Select a job log.</pre>
          </div>
        </section>
      </div>
    </section>
  </main>

  <script>
    let selectedJobId = null;

    async function fetchJson(url, options = {}) {
      const response = await fetch(url, {
        headers: {
          "Accept": "application/json",
          ...(options.body ? {"Content-Type": "application/json"} : {})
        },
        ...options
      });
      const body = await response.json().catch(() => ({}));
      if (!response.ok) {
        const message = typeof body.detail === "string" ? body.detail : response.statusText;
        throw new Error(message || `Request failed: ${response.status}`);
      }
      return body;
    }

    function text(value, fallback = "None") {
      return value === null || value === undefined || value === "" ? fallback : String(value);
    }

    function concise(value, limit = 120) {
      const rendered = text(value, "");
      return rendered.length > limit ? `${rendered.slice(0, limit - 3)}...` : rendered;
    }

    function formatDate(value) {
      if (!value) {
        return "No timestamp";
      }
      const date = new Date(value);
      if (Number.isNaN(date.getTime())) {
        return value;
      }
      return date.toLocaleString();
    }

    function setHealth(id, value, meta, className) {
      const valueEl = document.getElementById(`${id}-status`);
      const metaEl = document.getElementById(`${id}-meta`);
      valueEl.textContent = value;
      valueEl.className = `health-value ${className || ""}`.trim();
      metaEl.textContent = meta;
    }

    function workerStatusClass(status) {
      return status && status !== "idle" ? "status-warn" : "status-ok";
    }

    function renderHealth(state) {
      setHealth(
        "api",
        state.api.online ? "Online" : "Offline",
        `Server time ${formatDate(state.api.server_time)}`,
        state.api.online ? "status-ok" : "status-error"
      );

      const mac = state.activity.mac || {};
      const windows = state.activity.windows || {};
      const errors = state.active_errors || [];

      setHealth(
        "mac",
        text(mac.status, "Idle"),
        mac.movie_number ? `${mac.movie_number} updated ${formatDate(mac.updated_at)}` : "No active Mac job",
        workerStatusClass(mac.status)
      );
      setHealth(
        "windows",
        text(windows.status, "Idle"),
        windows.movie_number ? `${windows.movie_number} updated ${formatDate(windows.updated_at)}` : "No active Windows job",
        workerStatusClass(windows.status)
      );
      setHealth(
        "errors",
        String(errors.length),
        errors.length ? errors.map((job) => job.movie_number).slice(0, 3).join(", ") : "No active errors",
        errors.length ? "status-error" : "status-ok"
      );
    }

    function renderJobs(jobs) {
      const list = document.getElementById("jobs-list");
      list.replaceChildren();

      if (!jobs.length) {
        const empty = document.createElement("div");
        empty.className = "empty";
        empty.textContent = "No jobs yet.";
        list.append(empty);
        return;
      }

      for (const job of jobs) {
        const row = document.createElement("button");
        row.type = "button";
        row.className = "job-row";
        row.dataset.jobId = job.id;
        const code = document.createElement("span");
        const status = document.createElement("span");
        const priority = document.createElement("span");
        const worker = document.createElement("span");
        const error = document.createElement("span");
        const updated = document.createElement("span");
        code.className = "job-code";
        status.className = "job-status";
        priority.className = "job-priority";
        worker.className = "job-worker";
        error.className = "job-error";
        updated.className = "job-updated";
        row.append(code, status, priority, worker, error, updated);
        row.querySelector(".job-code").textContent = job.movie_number;
        row.querySelector(".job-status").textContent = job.status;
        row.querySelector(".job-priority").textContent = `P${job.priority}`;
        row.querySelector(".job-worker").textContent = job.claimed_by ? `Claimed ${job.claimed_by}` : "";
        row.querySelector(".job-error").textContent = job.error ? `Error ${concise(job.error)}` : "";
        row.querySelector(".job-updated").textContent = formatDate(job.updated_at);
        row.addEventListener("click", () => selectJob(job.id));
        list.append(row);
      }
    }

    function detailRows(detail) {
      const fields = [
        ["Job ID", detail.id],
        ["Original movie", detail.movie_number],
        ["Normalized movie", detail.normalized_movie_number],
        ["Status", detail.status],
        ["Priority", detail.priority],
        ["Mac attempts", detail.attempt_count],
        ["Worker attempts", detail.worker_attempt_count],
        ["Claimed by", detail.claimed_by],
        ["Lease expires", formatDate(detail.lease_expires_at)],
        ["Created", formatDate(detail.created_at)],
        ["Updated", formatDate(detail.updated_at)],
        ["Job dir Mac", detail.job_dir_mac],
        ["Job dir Windows", detail.job_dir_windows],
        ["Metadata Mac", detail.metadata_path_mac],
        ["Audio Mac", detail.audio_path_mac],
        ["Audio Windows", detail.audio_path_windows],
        ["Japanese SRT Mac", detail.japanese_srt_path_mac],
        ["Japanese SRT Windows", detail.japanese_srt_path_windows],
        ["English SRT Mac", detail.english_srt_path_mac],
        ["English SRT Windows", detail.english_srt_path_windows],
        ["Error", detail.error]
      ];
      return fields.map(([name, value]) => `<dt>${name}</dt><dd>${escapeHtml(text(value))}</dd>`).join("");
    }

    function escapeHtml(value) {
      return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#039;");
    }

    async function selectJob(jobId) {
      selectedJobId = jobId;
      const detailEl = document.getElementById("job-detail");
      const logOutput = document.getElementById("log-output");
      detailEl.textContent = "Loading job detail...";
      logOutput.textContent = "Loading logs...";

      try {
        const [detail, logsResponse] = await Promise.all([
          fetchJson(`/jobs/${jobId}/detail`),
          fetchJson(`/jobs/${jobId}/logs`)
        ]);
        detailEl.className = "";
        detailEl.innerHTML = `<dl class="detail-grid">${detailRows(detail)}</dl><div class="log-buttons"></div>`;
        const logButtons = detailEl.querySelector(".log-buttons");
        if (!logsResponse.logs.length) {
          logButtons.textContent = "No logs available.";
          logOutput.textContent = "No logs available.";
          return;
        }
        for (const log of logsResponse.logs) {
          const button = document.createElement("button");
          button.type = "button";
          button.textContent = `${log.name} (${log.size_bytes} bytes)`;
          button.addEventListener("click", () => loadLog(jobId, log.name));
          logButtons.append(button);
        }
        await loadLog(jobId, logsResponse.logs[0].name);
      } catch (error) {
        detailEl.className = "empty";
        detailEl.textContent = error.message;
        logOutput.textContent = "";
      }
    }

    async function loadLog(jobId, logName) {
      const logOutput = document.getElementById("log-output");
      logOutput.textContent = `Loading ${logName}...`;
      try {
        const payload = await fetchJson(`/jobs/${jobId}/logs/${encodeURIComponent(logName)}?tail=200`);
        logOutput.textContent = payload.lines.length ? payload.lines.join("\\n") : `${logName} is empty.`;
      } catch (error) {
        logOutput.textContent = error.message;
      }
    }

    async function refreshState() {
      const list = document.getElementById("jobs-list");
      try {
        const state = await fetchJson("/dashboard/state");
        renderHealth(state);
        renderJobs(state.latest_jobs || []);
      } catch (error) {
        list.innerHTML = `<div class="empty">${escapeHtml(error.message)}</div>`;
        setHealth("api", "Error", error.message, "status-error");
      }
    }

    async function submitSingle(event) {
      event.preventDefault();
      const form = event.currentTarget;
      const message = document.getElementById("single-message");
      const movie = document.getElementById("single-movie-number").value.trim();
      const priority = Number(document.getElementById("single-priority").value);
      message.textContent = "Submitting...";
      try {
        const result = await fetchJson("/jobs", {
          method: "POST",
          body: JSON.stringify({movie_number: movie, priority, force: false})
        });
        message.textContent = `Submitted ${result.movie_number}`;
        form.reset();
        document.getElementById("single-priority").value = "100";
        await refreshState();
      } catch (error) {
        message.textContent = error.message;
      }
    }

    async function submitBatch(event) {
      event.preventDefault();
      const form = event.currentTarget;
      const message = document.getElementById("batch-message");
      const movies = document.getElementById("batch-movie-numbers").value
        .split(/[\\s,]+/)
        .map((item) => item.trim())
        .filter(Boolean);
      const priority = Number(document.getElementById("batch-priority").value);
      message.textContent = "Submitting...";
      try {
        const result = await fetchJson("/jobs/batch", {
          method: "POST",
          body: JSON.stringify({movie_numbers: movies, priority, force: false})
        });
        message.textContent = `Created ${result.created.length}, existing ${result.existing.length}, invalid ${result.invalid.length}`;
        form.reset();
        document.getElementById("batch-priority").value = "100";
        await refreshState();
      } catch (error) {
        message.textContent = error.message;
      }
    }

    function importRequestRange(requested) {
      const counts = (requested || []).map((item) => Number(item.request_count || 0));
      if (!counts.length) return "no request counts";
      const min = Math.min(...counts);
      const max = Math.max(...counts);
      return min === max ? `request count ${min}` : `request counts ${min}-${max}`;
    }

    async function importRequestedSubtitles(event) {
      event.preventDefault();
      const message = document.getElementById("import-requested-message");
      const minCount = Number(document.getElementById("import-requested-min-count").value || "1");
      const limit = Number(document.getElementById("import-requested-limit").value || "500");
      const priority = Number(document.getElementById("import-requested-priority").value || "100");
      message.textContent = "Importing requested subtitles...";
      try {
        const result = await fetchJson("/jobs/import-subtitle-requests", {
          method: "POST",
          body: JSON.stringify({
            min_count: minCount,
            limit,
            priority,
            force: false
          })
        });
        message.textContent = `Requested ${result.requested.length} (${importRequestRange(result.requested)}), imported ${result.imported.length}, skipped available ${result.skipped_available.length}, created ${result.created.length}, existing ${result.existing.length}, invalid ${result.invalid.length}`;
        await refreshState();
      } catch (error) {
        message.textContent = error.message;
      }
    }

    document.getElementById("refresh-button").addEventListener("click", refreshState);
    document.getElementById("single-movie-form").addEventListener("submit", submitSingle);
    document.getElementById("batch-movie-form").addEventListener("submit", submitBatch);
    document.getElementById("import-requested-form").addEventListener("submit", importRequestedSubtitles);
    window.addEventListener("load", refreshState);
  </script>
</body>
</html>
"""

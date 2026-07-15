import sqlite3
from contextlib import closing

from fastapi.testclient import TestClient

from orchestrator.api import create_app
from orchestrator.models import JobStatus
from orchestrator.store import JobStore


def insert_running_historical_repair(store, sqlite_path, movie_code):
    job = store.submit_job(movie_code, priority=100, force=False).job
    with closing(sqlite3.connect(sqlite_path)) as conn:
        conn.execute(
            "UPDATE jobs SET status = ?, translation_origin = 'historical', "
            "claimed_by = 'mac-translation-1', updated_at = ? WHERE id = ?",
            (
                JobStatus.CATALOG_SYNCING.value,
                "2026-07-05T10:00:00+00:00",
                job.id,
            ),
        )
        conn.execute(
            """
            INSERT INTO historical_translation_repairs (
              id, batch_id, job_id, movie_code, allowlist_sha256, state,
              attempt_count, next_attempt_at, reason_code, japanese_sha256,
              audio_probe_snapshot_sha256, audio_sha256,
              source_english_sha256, english_sha256, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, 'running', 1, NULL, NULL, ?, ?, ?, ?, NULL, ?, ?)
            """,
            (
                "repair_api_001",
                "batch_api_001",
                job.id,
                movie_code,
                "1" * 64,
                "2" * 64,
                "3" * 64,
                "4" * 64,
                "5" * 64,
                "2026-07-05T10:00:00+00:00",
                "2026-07-05T10:00:00+00:00",
            ),
        )
        conn.commit()
    return job


def test_dashboard_state_endpoint_returns_counts_latest_jobs_and_errors(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    store.submit_job("ktb-096", priority=100, force=False)
    failed = store.submit_job("ktb-095", priority=100, force=False).job
    store.record_download_failure(failed.id, JobStatus.FAILED, 3, "download interrupted")
    client = TestClient(create_app(store))

    response = client.get("/dashboard/state")

    assert response.status_code == 200
    body = response.json()
    assert body["api"]["online"] is True
    assert body["counts"]["queued"] == 1
    assert body["counts"]["failed"] == 1
    assert [job["movie_number"] for job in body["active_errors"]] == ["ktb-095"]
    assert {job["movie_number"] for job in body["latest_jobs"]} == {"ktb-096", "ktb-095"}


def test_dashboard_state_endpoint_serializes_typed_historical_progress(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = insert_running_historical_repair(store, sqlite_path, "old-401")
    client = TestClient(create_app(store))

    response = client.get("/dashboard/state")

    assert response.status_code == 200
    body = response.json()
    assert body["historical_repairs"] == {
        "counts": {
            "total": 1,
            "planned": 0,
            "pending": 0,
            "running": 1,
            "retry_wait": 0,
            "paused": 0,
            "succeeded": 0,
            "permanent_failed": 0,
            "unknown": 0,
        },
        "current": {
            "batch_id": "batch_api_001",
            "repair_id": "repair_api_001",
            "job_id": job.id,
            "movie_number": "old-401",
            "stage": "catalog_syncing",
            "state": "running",
            "reason_code": None,
            "updated_at": "2026-07-05T10:00:00+00:00",
        },
        "lane_paused": False,
        "reason_code": None,
        "consecutive_quality_failures": 0,
        "updated_at": body["historical_repairs"]["updated_at"],
    }
    assert body["activity"]["historical_translation"]["job_id"] == job.id
    assert body["activity"]["historical_translation"]["stage"] == "catalog_syncing"


def test_dashboard_state_includes_claimed_worker_and_error_in_latest_jobs(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    claimed_job = store.submit_job("ktb-097", priority=100, force=False).job
    failed_job = store.submit_job("ktb-098", priority=100, force=False).job
    store.mark_audio_ready(claimed_job.id)
    store.mark_audio_ready(failed_job.id)
    claimed = store.claim_next_worker_job("windows-gpu-1", lease_seconds=1800)
    assert claimed is not None
    store.heartbeat(claimed.id, "windows-gpu-1", JobStatus.TRANSCRIBING, lease_seconds=1800)
    failed = store.claim_next_worker_job("windows-gpu-2", lease_seconds=1800)
    assert failed is not None
    store.fail_worker_job(
        failed.id,
        "windows-gpu-2",
        JobStatus.TRANSCRIBING,
        "CUDA out of memory",
        max_worker_attempts=3,
    )
    client = TestClient(create_app(store))

    response = client.get("/dashboard/state")

    assert response.status_code == 200
    body = response.json()
    latest_jobs = {job["movie_number"]: job for job in body["latest_jobs"]}
    assert latest_jobs["ktb-097"]["claimed_by"] == "windows-gpu-1"
    assert latest_jobs["ktb-098"]["error"] == "transcribing: CUDA out of memory"


def test_job_detail_endpoint_returns_full_paths(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-112", priority=50, force=False).job
    store.mark_audio_ready(job.id)
    client = TestClient(create_app(store))

    response = client.get(f"/jobs/{job.id}/detail")

    assert response.status_code == 200
    body = response.json()
    assert body["id"] == job.id
    assert body["movie_number"] == "ktb-112"
    assert body["normalized_movie_number"] == "ktb-112"
    assert body["status"] == "audio_ready"
    assert body["priority"] == 50
    assert body["job_dir_mac"].endswith("/ktb-112")
    assert body["job_dir_windows"] == "M:\\ktb-112"
    assert body["audio_path_windows"] == "M:\\ktb-112\\audio.wav"
    assert body["publish_attempt_count"] == 0
    assert body["next_publish_attempt_at"] is None
    assert body["catalog_movie_uuid"] is None
    assert body["metadata_status"] is None
    assert body["metadata_source"] is None
    for sensitive_key in ("title", "description", "raw_metadata", "subtitle_text"):
        assert sensitive_key not in body


def test_job_detail_endpoint_exposes_ready_catalog_warning_as_secondary_state(
    sqlite_path,
    mac_jobs_root,
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-104", priority=50, force=False).job
    with store.connection() as conn:
        conn.execute(
            """
            UPDATE jobs
            SET status = 'english_srt_ready', artifact_status = 'ready',
                catalog_sync_status = 'failed',
                catalog_sync_warning_code = 'catalog_sync_failed',
                catalog_sync_warning_message = 'Catalog synchronization failed.',
                catalog_sync_last_http_status = 500, error = NULL
            WHERE id = ?
            """,
            (job.id,),
        )
    client = TestClient(create_app(store))

    detail = client.get(f"/jobs/{job.id}/detail")
    ready = client.get("/jobs/browser?view=ready")
    failed = client.get("/jobs/browser?view=failed")

    assert detail.status_code == ready.status_code == failed.status_code == 200
    assert detail.json()["status"] == "english_srt_ready"
    assert detail.json()["artifact_status"] == "ready"
    assert detail.json()["catalog_sync_status"] == "failed"
    assert detail.json()["catalog_sync_warning_code"] == "catalog_sync_failed"
    assert detail.json()["catalog_sync_last_http_status"] == 500
    assert ready.json()["total"] == 1
    assert failed.json()["total"] == 0


def test_log_endpoints_list_and_tail_allowlisted_logs(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-112", priority=100, force=False).job
    logs_dir = mac_jobs_root / "ktb-112" / "logs"
    logs_dir.mkdir(parents=True)
    (logs_dir / "translate.log").write_text("one\ntwo\nthree\n", encoding="utf-8")
    client = TestClient(create_app(store))

    list_response = client.get(f"/jobs/{job.id}/logs")
    tail_response = client.get(f"/jobs/{job.id}/logs/translate.log?tail=2")

    assert list_response.status_code == 200
    assert list_response.json()["logs"] == [
        {"name": "translate.log", "size_bytes": len("one\ntwo\nthree\n"), "available": True}
    ]
    assert tail_response.status_code == 200
    assert tail_response.json()["lines"] == ["two", "three"]


def test_log_tail_endpoint_rejects_unknown_and_traversal_names(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-112", priority=100, force=False).job
    client = TestClient(create_app(store))

    unknown = client.get(f"/jobs/{job.id}/logs/secret.log")
    traversal = client.get(f"/jobs/{job.id}/logs/..%2Ftranslate.log")

    assert unknown.status_code == 404
    assert traversal.status_code in {404, 422}


def test_dashboard_routes_return_404_for_missing_job(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    client = TestClient(create_app(store))

    detail = client.get("/jobs/job_missing/detail")
    logs = client.get("/jobs/job_missing/logs")

    assert detail.status_code == 404
    assert logs.status_code == 404


def test_dashboard_page_returns_operator_html_without_force_controls(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    client = TestClient(create_app(store))

    response = client.get("/dashboard")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    html = response.text
    assert "JAV Subtitle Orchestrator" in html
    assert 'href="/docs"' in html
    assert 'id="single-movie-form"' in html
    assert 'id="batch-movie-form"' in html
    assert 'id="jobs-list"' in html
    assert 'id="job-detail"' in html
    assert 'id="log-output"' in html
    assert "Job ID" in html
    assert "Original movie" in html
    assert "Created" in html
    assert "Lease expires" in html
    assert "Metadata Mac" in html
    assert "Audio Mac" in html
    assert "Japanese SRT Mac" in html
    assert "English SRT Mac" in html
    assert "Publish attempts" in html
    assert "Next publish attempt" in html
    assert "Catalog movie UUID" in html
    assert "Metadata status" in html
    assert "Metadata source" in html
    assert "detail.publish_attempt_count" in html
    assert "detail.next_publish_attempt_at" in html
    assert "detail.catalog_movie_uuid" in html
    assert "detail.metadata_status" in html
    assert "detail.metadata_source" in html
    for sensitive_row in (
        '["Title", detail.title]',
        '["Description", detail.description]',
        '["Raw metadata", detail.raw_metadata]',
        '["Subtitle text", detail.subtitle_text]',
    ):
        assert sensitive_row not in html
    assert "job-worker" in html
    assert "job-error" in html
    assert "job.claimed_by" in html
    assert "job.error" in html
    assert 'id="force"' not in html.lower()
    assert 'name="force"' not in html.lower()
    assert 'type="checkbox"' not in html.lower()
    assert "force: false" in html
    assert 'status !== "idle"' in html
    assert "Mac Downloader" in html
    assert "Windows Transcription" in html
    assert "Mac Translation" in html
    assert 'id="import-requested-form"' not in html
    assert "/jobs/import-subtitle-requests" not in html


def test_dashboard_contains_safe_independent_subtitle_quality_section(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    html = TestClient(create_app(store)).get("/dashboard").text

    assert "Subtitle Quality" in html
    for name in ("bad", "invalid", "missing", "review"):
        assert f'id="subtitle-quality-{name}"' in html
    assert 'id="subtitle-quality-progress"' in html
    assert 'id="subtitle-quality-status-filter"' in html
    assert 'id="subtitle-quality-language-filter"' in html
    assert 'fetchJson("/subtitle-audits/summary", {signal: controller.signal})' in html
    assert "renderSubtitleQualityUnavailable" in html
    assert "subtitleRow.replaceChildren" in html
    assert "subtitleRow.innerHTML" not in html


def test_dashboard_separates_operations_and_subtitle_quality_into_tabs(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    html = TestClient(create_app(store)).get("/dashboard").text

    assert 'class="dashboard-tabs"' in html
    assert 'id="dashboard-tab-operations"' in html
    assert 'data-dashboard-tab="operations"' in html
    assert 'id="dashboard-tab-subtitle-quality"' in html
    assert 'data-dashboard-tab="subtitle-quality"' in html
    assert 'id="dashboard-view-operations"' in html
    assert 'id="dashboard-view-subtitle-quality"' in html
    assert 'aria-controls="dashboard-view-subtitle-quality"' in html
    assert "function selectDashboardTab" in html
    assert 'window.location.hash = tab === "operations" ? "" : `#${tab}`' in html

    operations_start = html.index('id="dashboard-view-operations"')
    quality_start = html.index('id="dashboard-view-subtitle-quality"')
    operations_html = html[operations_start:quality_start]
    assert 'id="subtitle-quality-title"' not in operations_html


def test_dashboard_audit_refresh_failure_is_isolated_from_job_state(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    html = TestClient(create_app(store)).get("/dashboard").text

    assert "subtitleAuditRequestGeneration" in html
    assert "subtitleAuditAbortController.abort()" in html
    assert "new AbortController()" in html
    assert 'error.name === "AbortError"' in html
    assert "renderSubtitleQualityUnavailable(error)" in html
    assert "refreshState" in html


def test_dashboard_shows_only_read_only_historical_repair_guidance(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    html = TestClient(create_app(store)).get("/dashboard").text

    assert "Historical repair planning is dry-run only" in html
    assert "plan-historical-subtitle-repair" in html
    assert "--allowlist abc-001 --limit 1" in html
    assert "repair/apply" not in html


def test_dashboard_contains_safe_read_only_historical_progress_card(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    html = TestClient(create_app(store)).get("/dashboard").text

    assert "History repair" in html
    for element_id in (
        "history-repair-status",
        "history-repair-meta",
        "history-repair-counts",
        "history-repair-current",
        "history-repair-pause",
    ):
        assert f'id="{element_id}"' in html
    assert "state.historical_repairs" in html
    assert "state.activity.historical_translation" in html
    assert "renderHistoricalRepairs" in html
    assert "progress.current" in html
    assert "counts.planned" in html
    assert "counts.paused" in html
    assert "counts.unknown" in html
    assert "current.state" in html
    assert "history-repair-counts\").textContent" in html
    assert "history-repair-current\").textContent" in html
    assert "history-repair-pause\").textContent" in html
    assert "history-repair-counts\").innerHTML" not in html
    assert "history-repair-current\").innerHTML" not in html
    assert "history-repair-pause\").innerHTML" not in html
    for forbidden_control in (
        "history-repair-resume",
        "history-repair-requeue",
        "history-repair-delete",
        "history-repair-upload",
    ):
        assert forbidden_control not in html

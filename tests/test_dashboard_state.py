from orchestrator.dashboard import build_dashboard_state, build_job_detail
from orchestrator.models import JobStatus
from orchestrator.store import JobStore


def test_build_dashboard_state_counts_latest_jobs_and_active_errors(
    sqlite_path,
    mac_jobs_root,
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    store.submit_job("ktb-096", priority=100, force=False).job
    failed = store.submit_job("ktb-095", priority=90, force=False).job
    ready = store.submit_job("ktb-094", priority=80, force=False).job

    store.mark_audio_ready(ready.id)
    store.record_download_failure(
        failed.id,
        JobStatus.FAILED,
        attempt_count=3,
        error="metadata failed: Movie not found in MissAV catalog",
    )

    state = build_dashboard_state(store)

    assert state.api["online"] is True
    assert state.api["jobs_root_mac"] == str(mac_jobs_root)
    assert state.api["jobs_root_windows"] == "M:\\"
    assert state.counts["queued"] == 1
    assert state.counts["audio_ready"] == 1
    assert state.counts["failed"] == 1
    assert [job.movie_number for job in state.latest_jobs] == [
        "ktb-094",
        "ktb-095",
        "ktb-096",
    ]
    assert state.active_errors[0].movie_number == "ktb-095"
    assert (
        state.active_errors[0].error
        == "metadata failed: Movie not found in MissAV catalog"
    )


def test_build_dashboard_state_derives_worker_activity(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    downloading = store.submit_job("ktb-096", priority=100, force=False).job
    transcribing = store.submit_job("ktb-095", priority=100, force=False).job

    store.update_download_status(downloading.id, JobStatus.DOWNLOADING_AUDIO)
    store.mark_audio_ready(transcribing.id)
    claimed = store.claim_next_worker_job("windows-gpu-1", lease_seconds=1800)
    store.heartbeat(
        claimed.id,
        "windows-gpu-1",
        JobStatus.TRANSCRIBING,
        lease_seconds=1800,
    )

    state = build_dashboard_state(store)

    assert state.activity["mac"]["status"] == "downloading_audio"
    assert state.activity["mac"]["movie_number"] == "ktb-096"
    assert state.activity["windows"]["status"] == "transcribing"
    assert state.activity["windows"]["movie_number"] == "ktb-095"
    assert state.activity["windows"]["worker_id"] == "windows-gpu-1"


def test_build_job_detail_returns_full_operational_fields(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-112", priority=50, force=False).job
    ready = store.mark_audio_ready(job.id)

    detail = build_job_detail(ready)

    assert detail.id == job.id
    assert detail.movie_number == "ktb-112"
    assert detail.normalized_movie_number == "ktb-112"
    assert detail.status == "audio_ready"
    assert detail.priority == 50
    assert detail.attempt_count == 0
    assert detail.worker_attempt_count == 0
    assert detail.claimed_by is None
    assert detail.job_dir_mac == str(mac_jobs_root / "ktb-112")
    assert detail.job_dir_windows == "M:\\ktb-112"
    assert detail.metadata_path_mac == str(mac_jobs_root / "ktb-112" / "metadata.json")
    assert detail.audio_path_mac == str(mac_jobs_root / "ktb-112" / "audio.wav")
    assert detail.audio_path_windows == "M:\\ktb-112\\audio.wav"

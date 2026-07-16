import os
from pathlib import Path

from orchestrator.audio_cleanup import delete_published_job_audio
from orchestrator.models import JobStatus
from orchestrator.store import JobStore


def _job_with_audio(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("abc-001", priority=100, force=False).job
    job = store.mark_audio_ready(job.id)
    audio = Path(job.audio_path_mac)
    audio.parent.mkdir(parents=True, exist_ok=True)
    audio.write_bytes(b"published audio")
    with store.connection() as conn:
        conn.execute(
            """
            UPDATE jobs SET status = ?, translation_origin = 'normal',
                artifact_status = 'ready', catalog_movie_uuid = ?,
                metadata_status = 'complete', metadata_source = 'missav',
                published_subtitle_id = ?, published_storage_path = ?,
                published_content_sha256 = ?, published_file_size = ?
            WHERE id = ?
            """,
            (
                JobStatus.ENGLISH_SRT_READY.value,
                "00000000-0000-0000-0000-000000000001",
                "00000000-0000-0000-0000-000000000002",
                "abc/abc-001/abc-001-English_AI.srt",
                "a" * 64,
                123,
                job.id,
            ),
        )
    return store, store.get_job(job.id), audio


def test_delete_published_job_audio_removes_only_canonical_wav(
    sqlite_path, mac_jobs_root
):
    _store, job, audio = _job_with_audio(sqlite_path, mac_jobs_root)
    sibling = audio.with_name("metadata.json")
    sibling.write_text("{}\n", encoding="utf-8")

    result = delete_published_job_audio(job, mac_jobs_root)

    assert result.outcome == "deleted"
    assert result.bytes_deleted == len(b"published audio")
    assert not audio.exists()
    assert sibling.exists()
    assert "deleted" in (
        audio.parent / "logs" / "audio-cleanup.log"
    ).read_text(encoding="utf-8")


def test_delete_published_job_audio_is_idempotent_when_missing(
    sqlite_path, mac_jobs_root
):
    _store, job, audio = _job_with_audio(sqlite_path, mac_jobs_root)
    audio.unlink()

    result = delete_published_job_audio(job, mac_jobs_root)

    assert result.outcome == "missing"
    assert result.bytes_deleted == 0


def test_delete_published_job_audio_refuses_symlink(sqlite_path, mac_jobs_root):
    _store, job, audio = _job_with_audio(sqlite_path, mac_jobs_root)
    outside = mac_jobs_root.parent / "outside-audio.wav"
    outside.write_bytes(b"must remain")
    audio.unlink()
    audio.symlink_to(outside)

    result = delete_published_job_audio(job, mac_jobs_root)

    assert result.outcome == "unsafe"
    assert outside.read_bytes() == b"must remain"
    assert audio.is_symlink()


def test_delete_published_job_audio_logs_oserror_without_raising(
    sqlite_path, mac_jobs_root, monkeypatch
):
    _store, job, audio = _job_with_audio(sqlite_path, mac_jobs_root)

    def fail_unlink(path, *, dir_fd=None):
        raise OSError("simulated busy file")

    monkeypatch.setattr(os, "unlink", fail_unlink)

    result = delete_published_job_audio(job, mac_jobs_root)

    assert result.outcome == "error"
    assert audio.exists()
    log = (audio.parent / "logs" / "audio-cleanup.log").read_text(encoding="utf-8")
    assert "error" in log
    assert "simulated busy file" in log


def test_delete_published_job_audio_refuses_unpublished_job(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("abc-002", priority=100, force=False).job
    job = store.mark_audio_ready(job.id)
    audio = Path(job.audio_path_mac)
    audio.parent.mkdir(parents=True, exist_ok=True)
    audio.write_bytes(b"unpublished audio")

    result = delete_published_job_audio(job, mac_jobs_root)

    assert result.outcome == "ineligible"
    assert audio.exists()


def test_delete_published_job_audio_refuses_historical_job(
    sqlite_path, mac_jobs_root
):
    store, job, audio = _job_with_audio(sqlite_path, mac_jobs_root)
    with store.connection() as conn:
        conn.execute(
            "UPDATE jobs SET translation_origin = 'historical' WHERE id = ?",
            (job.id,),
        )

    result = delete_published_job_audio(store.get_job(job.id), mac_jobs_root)

    assert result.outcome == "ineligible"
    assert audio.exists()

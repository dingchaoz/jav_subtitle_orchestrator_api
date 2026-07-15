from orchestrator.models import JobStatus


def test_job_statuses_match_design_spec_order():
    assert [status.value for status in JobStatus] == [
        "queued",
        "downloading_metadata",
        "downloading_audio",
        "audio_ready",
        "transcription_claimed",
        "transcribing",
        "transcription_done",
        "translating",
        "publish_pending",
        "publishing",
        "catalog_sync_pending",
        "catalog_syncing",
        "english_srt_ready",
        "failed",
        "cancelled",
    ]


def test_catalog_sync_job_status_values():
    assert JobStatus.CATALOG_SYNC_PENDING == "catalog_sync_pending"
    assert JobStatus.CATALOG_SYNCING == "catalog_syncing"

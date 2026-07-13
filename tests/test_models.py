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
        "english_srt_ready",
        "failed",
        "cancelled",
    ]

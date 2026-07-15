from fastapi.testclient import TestClient

from orchestrator.api import create_app
from orchestrator.callbacks import (
    CallbackClient,
    CallbackNotifier,
    callback_signature,
    canonical_payload_json,
)
from orchestrator.models import JobStatus
from orchestrator.store import JobStore


class RecordingCallbackSender:
    def __init__(self, *, error: Exception | None = None) -> None:
        self.error = error
        self.calls: list[dict[str, object]] = []

    def send(self, *, url, secret, payload, timeout_seconds):
        self.calls.append(
            {
                "url": url,
                "secret": secret,
                "payload": payload,
                "timeout_seconds": timeout_seconds,
            }
        )
        if self.error is not None:
            raise self.error


def ready_job(store: JobStore, movie: str = "ktb-112", client_key="machine-a.access"):
    job = store.submit_job(
        movie,
        priority=100,
        force=False,
        callback_client_key=client_key,
    ).job
    with store.connection() as conn:
        conn.execute(
            "UPDATE jobs SET status = ?, artifact_status = ?, "
            "catalog_sync_status = ?, error = NULL WHERE id = ?",
            (
                JobStatus.ENGLISH_SRT_READY.value,
                "ready",
                "pending",
                job.id,
            ),
        )
    return store.get_job(job.id)


def test_callback_signature_uses_canonical_payload_without_exposing_secret():
    payload = {"status": "english_srt_ready", "event": "subtitle.ready"}
    rendered = canonical_payload_json(payload)
    signature = callback_signature("never-store-this-secret", "2026-07-15T00:00:00Z", rendered)

    assert rendered == '{"event":"subtitle.ready","status":"english_srt_ready"}'
    assert signature.startswith("sha256=")
    assert "never-store-this-secret" not in rendered
    assert "never-store-this-secret" not in signature


def test_api_associates_only_configured_callback_client_and_tracks_all_requests(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    client = TestClient(
        create_app(
            store,
            callback_clients={
                "machine-a.access": CallbackClient(
                    "https://client.example/ready",
                    "hmac-secret",
                )
            },
        )
    )

    first = client.post(
        "/jobs",
        headers={"CF-Access-Client-Id": "machine-a.access"},
        json={"movie_number": "ktb-112"},
    )
    second = client.post(
        "/jobs",
        headers={"CF-Access-Client-Id": "unknown.access"},
        json={"movie_number": "ktb-112"},
    )

    job = store.get_job(first.json()["id"])
    assert first.status_code == second.status_code == 200
    assert job.callback_client_key == "machine-a.access"
    assert store.list_callback_client_keys(job.id) == ["machine-a.access"]


def test_ready_notifier_sends_exact_payload_once_per_client(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = ready_job(store)
    sender = RecordingCallbackSender()
    notifier = CallbackNotifier(
        store,
        {
            "machine-a.access": CallbackClient(
                "https://client.example/ready",
                "hmac-secret",
            )
        },
        sender=sender,
        timeout_seconds=5,
    )

    notifier.notify_subtitle_ready(job)
    notifier.notify_subtitle_ready(job)

    assert len(sender.calls) == 1
    assert sender.calls[0]["payload"] == {
        "event": "subtitle.ready",
        "job_id": job.id,
        "movie_number": "ktb-112",
        "status": "english_srt_ready",
        "published_to_supabase": True,
        "ready_at": job.updated_at,
    }
    event = store.get_callback_event_for_client(
        job.id,
        "subtitle.ready",
        "machine-a.access",
    )
    assert event.status == "delivered"
    assert "hmac-secret" not in event.payload_json


def test_callback_failure_is_redacted_and_cannot_downgrade_ready_job(
    sqlite_path, mac_jobs_root
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = ready_job(store)
    sender = RecordingCallbackSender(
        error=RuntimeError("secret response body hmac-secret")
    )
    notifier = CallbackNotifier(
        store,
        {
            "machine-a.access": CallbackClient(
                "https://client.example/ready",
                "hmac-secret",
            )
        },
        sender=sender,
    )

    notifier.notify_subtitle_ready(job)

    event = store.get_callback_event_for_client(
        job.id,
        "subtitle.ready",
        "machine-a.access",
    )
    assert event.status == "failed"
    assert event.last_error == "callback_delivery_failed"
    assert "hmac-secret" not in event.last_error
    assert store.get_job(job.id).status is JobStatus.ENGLISH_SRT_READY

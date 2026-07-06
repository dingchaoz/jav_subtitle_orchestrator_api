from pathlib import Path

from fastapi.testclient import TestClient

from orchestrator.api import create_app
from orchestrator.callbacks import CallbackClient
from orchestrator.store import JobStore


class RecordingPublisher:
    def __init__(self, *, fail=False):
        self.fail = fail
        self.calls = []

    def publish_english_ai(self, movie_code, srt_path):
        self.calls.append((movie_code, Path(srt_path)))
        if self.fail:
            raise RuntimeError("supabase unavailable")


class RecordingCallbackSender:
    def __init__(self, *, fail=False):
        self.fail = fail
        self.calls = []

    def send(self, *, url, secret, payload, timeout_seconds):
        self.calls.append(
            {
                "url": url,
                "secret": secret,
                "payload": payload,
                "timeout_seconds": timeout_seconds,
            }
        )
        if self.fail:
            raise RuntimeError("client webhook unavailable")


def complete_claimed_job(store, mac_jobs_root, movie_number="ktb-112"):
    job = store.submit_job(movie_number, priority=100, force=False).job
    store.mark_audio_ready(job.id)
    claimed = store.claim_next_worker_job("windows-gpu-1", lease_seconds=1800)
    assert claimed is not None
    english_srt = mac_jobs_root / claimed.normalized_movie_number / (
        f"{claimed.normalized_movie_number}.English.srt"
    )
    japanese_srt = mac_jobs_root / claimed.normalized_movie_number / (
        f"{claimed.normalized_movie_number}.Japanese.srt"
    )
    english_srt.parent.mkdir(parents=True, exist_ok=True)
    english_srt.write_text("translated\n", encoding="utf-8")
    japanese_srt.write_text("japanese\n", encoding="utf-8")
    return claimed


def test_post_job_stores_known_cloudflare_callback_client(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    client = TestClient(
        create_app(
            store,
            callback_clients={
                "machine-a.access": CallbackClient(
                    url="https://client.example.com/ready",
                    secret="hmac-secret",
                )
            },
        )
    )

    response = client.post(
        "/jobs",
        headers={"CF-Access-Client-Id": "machine-a.access"},
        json={"movie_number": "ktb-096", "priority": 100, "force": False},
    )

    assert response.status_code == 200
    job = store.get_job(response.json()["id"])
    assert job.callback_client_key == "machine-a.access"


def test_post_job_ignores_unknown_cloudflare_callback_client(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    client = TestClient(
        create_app(
            store,
            callback_clients={
                "machine-a.access": CallbackClient(
                    url="https://client.example.com/ready",
                    secret="hmac-secret",
                )
            },
        )
    )

    response = client.post(
        "/jobs",
        headers={"CF-Access-Client-Id": "unknown.access"},
        json={"movie_number": "ktb-096", "priority": 100, "force": False},
    )

    assert response.status_code == 200
    job = store.get_job(response.json()["id"])
    assert job.callback_client_key is None


def test_worker_complete_delivers_callback_after_supabase_publish(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job(
        "ktb-112",
        priority=100,
        force=False,
        callback_client_key="machine-a.access",
    ).job
    store.mark_audio_ready(job.id)
    claimed = store.claim_next_worker_job("windows-gpu-1", lease_seconds=1800)
    english_srt = mac_jobs_root / "ktb-112" / "ktb-112.English.srt"
    english_srt.parent.mkdir(parents=True, exist_ok=True)
    english_srt.write_text("translated\n", encoding="utf-8")
    (mac_jobs_root / "ktb-112" / "ktb-112.Japanese.srt").write_text("japanese\n", encoding="utf-8")
    publisher = RecordingPublisher()
    sender = RecordingCallbackSender()
    client = TestClient(
        create_app(
            store,
            publisher=publisher,
            callback_clients={
                "machine-a.access": CallbackClient(
                    url="https://client.example.com/ready",
                    secret="hmac-secret",
                )
            },
            callback_sender=sender,
            callback_timeout_seconds=5,
        )
    )

    response = client.post(
        f"/worker/jobs/{claimed.id}/complete",
        json={
            "worker_id": "windows-gpu-1",
            "japanese_srt_path_windows": "M:\\ktb-112\\ktb-112.Japanese.srt",
            "english_srt_path_windows": "M:\\ktb-112\\ktb-112.English.srt",
        },
    )

    assert response.status_code == 200
    assert publisher.calls == [("ktb-112", english_srt)]
    assert sender.calls[0]["url"] == "https://client.example.com/ready"
    assert sender.calls[0]["secret"] == "hmac-secret"
    assert sender.calls[0]["timeout_seconds"] == 5
    assert sender.calls[0]["payload"]["event"] == "subtitle.ready"
    assert sender.calls[0]["payload"]["job_id"] == claimed.id
    assert sender.calls[0]["payload"]["published_to_supabase"] is True
    assert store.get_latest_callback_event(claimed.id).status == "delivered"


def test_worker_complete_does_not_callback_when_supabase_publish_fails(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    claimed = complete_claimed_job(store, mac_jobs_root)
    sender = RecordingCallbackSender()
    app = create_app(
        store,
        publisher=RecordingPublisher(fail=True),
        callback_clients={
            "machine-a.access": CallbackClient(
                url="https://client.example.com/ready",
                secret="hmac-secret",
            )
        },
        callback_sender=sender,
    )
    client = TestClient(app, raise_server_exceptions=False)

    response = client.post(
        f"/worker/jobs/{claimed.id}/complete",
        json={
            "worker_id": "windows-gpu-1",
            "japanese_srt_path_windows": "M:\\ktb-112\\ktb-112.Japanese.srt",
            "english_srt_path_windows": "M:\\ktb-112\\ktb-112.English.srt",
        },
    )

    assert response.status_code == 500
    assert sender.calls == []
    assert store.get_latest_callback_event(claimed.id) is None


def test_worker_complete_records_callback_failure_without_failing_job(
    sqlite_path,
    mac_jobs_root,
):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job(
        "ktb-112",
        priority=100,
        force=False,
        callback_client_key="machine-a.access",
    ).job
    store.mark_audio_ready(job.id)
    claimed = store.claim_next_worker_job("windows-gpu-1", lease_seconds=1800)
    english_srt = mac_jobs_root / "ktb-112" / "ktb-112.English.srt"
    english_srt.parent.mkdir(parents=True, exist_ok=True)
    english_srt.write_text("translated\n", encoding="utf-8")
    (mac_jobs_root / "ktb-112" / "ktb-112.Japanese.srt").write_text("japanese\n", encoding="utf-8")
    client = TestClient(
        create_app(
            store,
            publisher=RecordingPublisher(),
            callback_clients={
                "machine-a.access": CallbackClient(
                    url="https://client.example.com/ready",
                    secret="hmac-secret",
                )
            },
            callback_sender=RecordingCallbackSender(fail=True),
        )
    )

    response = client.post(
        f"/worker/jobs/{claimed.id}/complete",
        json={
            "worker_id": "windows-gpu-1",
            "japanese_srt_path_windows": "M:\\ktb-112\\ktb-112.Japanese.srt",
            "english_srt_path_windows": "M:\\ktb-112\\ktb-112.English.srt",
        },
    )

    assert response.status_code == 200
    event = store.get_latest_callback_event(claimed.id)
    assert event.status == "failed"
    assert event.attempt_count == 1
    assert "client webhook unavailable" in event.last_error


def test_job_detail_includes_latest_callback_status(sqlite_path, mac_jobs_root):
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    job = store.submit_job("ktb-112", priority=100, force=False).job
    event = store.create_callback_event(
        job_id=job.id,
        event_type="subtitle.ready",
        target_url="https://client.example.com/ready",
        payload_json='{"event":"subtitle.ready"}',
    )
    store.record_callback_delivery(event.id, status="failed", last_error="timeout")
    client = TestClient(create_app(store))

    response = client.get(f"/jobs/{job.id}/detail")

    assert response.status_code == 200
    assert response.json()["callback"]["status"] == "failed"
    assert response.json()["callback"]["attempt_count"] == 1
    assert response.json()["callback"]["last_error"] == "timeout"

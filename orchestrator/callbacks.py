import hashlib
import hmac
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

import requests

from orchestrator.models import JobStatus
from orchestrator.store import JobRecord, JobStore


@dataclass(frozen=True)
class CallbackClient:
    url: str
    secret: str


class CallbackSender(Protocol):
    def send(
        self,
        *,
        url: str,
        secret: str,
        payload: dict[str, object],
        timeout_seconds: int,
    ) -> None: ...


def canonical_payload_json(payload: dict[str, object]) -> str:
    return json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def callback_signature(secret: str, timestamp: str, payload_json: str) -> str:
    signed = f"{timestamp}.{payload_json}".encode("utf-8")
    digest = hmac.new(secret.encode("utf-8"), signed, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


class RequestsCallbackSender:
    def send(
        self,
        *,
        url: str,
        secret: str,
        payload: dict[str, object],
        timeout_seconds: int,
    ) -> None:
        payload_json = canonical_payload_json(payload)
        timestamp = datetime.now(UTC).replace(microsecond=0).isoformat()
        response = requests.post(
            url,
            data=payload_json,
            headers={
                "Content-Type": "application/json",
                "X-JSO-Timestamp": timestamp,
                "X-JSO-Signature": callback_signature(
                    secret,
                    timestamp,
                    payload_json,
                ),
            },
            timeout=timeout_seconds,
            allow_redirects=False,
        )
        response.raise_for_status()


class CallbackNotifier:
    def __init__(
        self,
        store: JobStore,
        clients: dict[str, CallbackClient],
        *,
        sender: CallbackSender | None = None,
        timeout_seconds: int = 10,
    ) -> None:
        self.store = store
        self.clients = clients
        self.sender = sender or RequestsCallbackSender()
        self.timeout_seconds = timeout_seconds

    def notify_subtitle_ready(
        self,
        job: JobRecord,
        client_keys: list[str] | None = None,
        *,
        retry_failed: bool = False,
    ) -> None:
        if (
            job.status is not JobStatus.ENGLISH_SRT_READY
            or job.artifact_status != "ready"
        ):
            return
        payload: dict[str, object] = {
            "event": "subtitle.ready",
            "job_id": job.id,
            "movie_number": job.normalized_movie_number,
            "status": JobStatus.ENGLISH_SRT_READY.value,
            "published_to_supabase": True,
            "ready_at": job.updated_at,
        }
        keys = (
            client_keys
            if client_keys is not None
            else self.store.list_callback_client_keys(job.id)
        )
        for client_key in dict.fromkeys(keys):
            client = self.clients.get(client_key)
            if client is None:
                continue
            existing = self.store.get_callback_event_for_client(
                job.id,
                "subtitle.ready",
                client_key,
            )
            if existing is None:
                event = self.store.claim_new_callback_event(
                    job_id=job.id,
                    event_type="subtitle.ready",
                    target_url=client.url,
                    payload_json=canonical_payload_json(payload),
                    client_key=client_key,
                )
                if (
                    event is None
                    or event.status != "pending"
                    or event.attempt_count != 0
                ):
                    continue
            elif existing.status == "failed" and retry_failed:
                event = self.store.prepare_failed_callback_retry(
                    existing.id,
                    target_url=client.url,
                    payload_json=canonical_payload_json(payload),
                )
                if event is None:
                    continue
            else:
                continue
            try:
                self.sender.send(
                    url=client.url,
                    secret=client.secret,
                    payload=payload,
                    timeout_seconds=self.timeout_seconds,
                )
            except Exception:
                self.store.record_callback_delivery(
                    event.id,
                    status="failed",
                    last_error="callback_delivery_failed",
                )
                continue
            self.store.record_callback_delivery(event.id, status="delivered")

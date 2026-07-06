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
    ) -> None:
        ...


def canonical_payload_json(payload: dict[str, object]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def callback_signature(secret: str, timestamp: str, payload_json: str) -> str:
    body = f"{timestamp}.{payload_json}".encode("utf-8")
    digest = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
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
                "X-JSO-Signature": callback_signature(secret, timestamp, payload_json),
            },
            timeout=timeout_seconds,
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

    def notify_subtitle_ready(self, job: JobRecord) -> None:
        if not job.callback_client_key:
            return
        client = self.clients.get(job.callback_client_key)
        if client is None:
            return

        payload: dict[str, object] = {
            "event": "subtitle.ready",
            "job_id": job.id,
            "movie_number": job.normalized_movie_number,
            "status": JobStatus.ENGLISH_SRT_READY.value,
            "published_to_supabase": True,
            "ready_at": job.updated_at,
        }
        event = self.store.create_callback_event(
            job_id=job.id,
            event_type="subtitle.ready",
            target_url=client.url,
            payload_json=canonical_payload_json(payload),
        )
        try:
            self.sender.send(
                url=client.url,
                secret=client.secret,
                payload=payload,
                timeout_seconds=self.timeout_seconds,
            )
        except Exception as exc:
            self.store.record_callback_delivery(
                event.id,
                status="failed",
                last_error=str(exc),
            )
            return

        self.store.record_callback_delivery(event.id, status="delivered")

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Protocol

from orchestrator.store import JobRecord, JobStore


class ExistingPublicationVerifier(Protocol):
    def verify_existing_publication(
        self,
        *,
        movie_code: str,
        movie_uuid: str,
        subtitle_id: str,
        storage_path: str,
        content_sha256: str,
        file_size: int,
    ) -> None: ...


class ReadyNotifier(Protocol):
    def notify_subtitle_ready(
        self,
        job: JobRecord,
        *,
        retry_failed: bool = False,
    ) -> None: ...


@dataclass(frozen=True)
class ReconciliationItem:
    job_id: str
    movie_code: str
    outcome: str


@dataclass(frozen=True)
class ReconciliationReport:
    mode: str
    items: tuple[ReconciliationItem, ...]

    @property
    def counts(self) -> dict[str, int]:
        return dict(Counter(item.outcome for item in self.items))


class CatalogSyncReconciler:
    def __init__(
        self,
        store: JobStore,
        verifier: ExistingPublicationVerifier,
        *,
        notifier: ReadyNotifier | None = None,
    ) -> None:
        self.store = store
        self.verifier = verifier
        self.notifier = notifier

    def run(
        self,
        *,
        movie_codes: list[str] | None = None,
        limit: int = 100,
        execute: bool = False,
        retry_catalog_sync: bool = False,
        resend_ready_webhook: bool = False,
    ) -> ReconciliationReport:
        if not execute and (retry_catalog_sync or resend_ready_webhook):
            raise ValueError("side-effect option requires execute")
        candidates = self.store.list_catalog_sync_failure_candidates(
            movie_codes=movie_codes,
            limit=limit,
        )
        items: list[ReconciliationItem] = []
        for snapshot in candidates:
            receipt = {
                "movie_code": snapshot.normalized_movie_number,
                "movie_uuid": snapshot.catalog_movie_uuid,
                "subtitle_id": snapshot.published_subtitle_id,
                "storage_path": snapshot.published_storage_path,
                "content_sha256": snapshot.published_content_sha256,
                "file_size": snapshot.published_file_size,
            }
            try:
                self.verifier.verify_existing_publication(**receipt)
            except Exception:
                items.append(
                    ReconciliationItem(
                        job_id=snapshot.id,
                        movie_code=snapshot.normalized_movie_number,
                        outcome="remote_not_verified",
                    )
                )
                continue
            if not execute:
                items.append(
                    ReconciliationItem(
                        job_id=snapshot.id,
                        movie_code=snapshot.normalized_movie_number,
                        outcome="verified",
                    )
                )
                continue
            restored = self.store.restore_verified_catalog_sync_failure(
                snapshot,
                retry_catalog_sync=retry_catalog_sync,
            )
            if restored is None:
                items.append(
                    ReconciliationItem(
                        job_id=snapshot.id,
                        movie_code=snapshot.normalized_movie_number,
                        outcome="state_changed",
                    )
                )
                continue
            if resend_ready_webhook and self.notifier is not None:
                self.notifier.notify_subtitle_ready(restored, retry_failed=True)
            items.append(
                ReconciliationItem(
                    job_id=snapshot.id,
                    movie_code=snapshot.normalized_movie_number,
                    outcome="restored",
                )
            )
        return ReconciliationReport(
            mode="execute" if execute else "dry_run",
            items=tuple(items),
        )

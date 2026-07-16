import hashlib
from uuid import NAMESPACE_URL, uuid5

import pytest

from orchestrator.catalog_sync_reconciliation import CatalogSyncReconciler
from orchestrator.__main__ import build_parser
from orchestrator.models import JobStatus
from orchestrator.store import JobStore


def _store(sqlite_path, mac_jobs_root) -> JobStore:
    store = JobStore(sqlite_path, mac_jobs_root, "M:\\")
    store.initialize()
    return store


def _seed_catalog_failure(
    store: JobStore,
    movie_code: str,
    reason_code: str = "catalog_sync_failed",
):
    job = store.submit_job(movie_code, priority=100, force=False).job
    assert job is not None
    canonical = job.normalized_movie_number
    movie_uuid = str(uuid5(NAMESPACE_URL, f"movie:{canonical}"))
    subtitle_id = str(uuid5(NAMESPACE_URL, f"subtitle:{canonical}"))
    storage_path = (
        f"{canonical.split('-', 1)[0]}/{canonical}/"
        f"{canonical}-English_AI.srt"
    )
    with store.connection() as conn:
        conn.execute(
            """
            UPDATE jobs
            SET status = ?, error = ?, catalog_movie_uuid = ?,
                metadata_status = 'complete', metadata_source = 'local',
                published_subtitle_id = ?, published_storage_path = ?,
                published_content_sha256 = ?, published_file_size = 123,
                catalog_sync_attempt_count = 3,
                catalog_sync_last_http_status = 500,
                catalog_sync_last_response_json = '{"success":false}'
            WHERE id = ?
            """,
            (
                JobStatus.FAILED.value,
                f"catalog_sync: {reason_code}",
                movie_uuid,
                subtitle_id,
                storage_path,
                "a" * 64,
                job.id,
            ),
        )
    restored = store.get_job(job.id)
    assert restored is not None
    return restored


class RecordingVerifier:
    def __init__(self, *, failures=None, after_verify=None):
        self.failures = failures or {}
        self.after_verify = after_verify
        self.calls = []

    def verify_existing_publication(self, **receipt):
        self.calls.append(receipt)
        movie_code = receipt["movie_code"]
        if movie_code in self.failures:
            raise RuntimeError(self.failures[movie_code])
        if self.after_verify is not None:
            self.after_verify(movie_code)


class RecordingNotifier:
    def __init__(self):
        self.jobs = []

    def notify_subtitle_ready(self, job, *, retry_failed=False):
        self.jobs.append(job)


def test_reconciliation_dry_run_verifies_ktb_records_without_mutating(
    sqlite_path,
    mac_jobs_root,
):
    store = _store(sqlite_path, mac_jobs_root)
    jobs = [
        _seed_catalog_failure(store, "KTB-104", "catalog_response_mismatch"),
        _seed_catalog_failure(store, "KTB-110"),
        _seed_catalog_failure(store, "KTB-111"),
    ]
    verifier = RecordingVerifier()
    database_sha256_before = hashlib.sha256(sqlite_path.read_bytes()).hexdigest()

    report = CatalogSyncReconciler(store, verifier).run(
        movie_codes=["KTB-104", "KTB-110", "KTB-111"],
    )

    assert report.mode == "dry_run"
    assert report.counts == {"verified": 3}
    assert [item.movie_code for item in report.items] == [
        "ktb-104",
        "ktb-110",
        "ktb-111",
    ]
    assert len(verifier.calls) == 3
    assert hashlib.sha256(sqlite_path.read_bytes()).hexdigest() == database_sha256_before
    for original in jobs:
        current = store.get_job(original.id)
        assert current is not None
        assert current.status is JobStatus.FAILED
        assert current.error == original.error


def test_reconciliation_restores_verified_failures_and_preserves_diagnostics(
    sqlite_path,
    mac_jobs_root,
):
    store = _store(sqlite_path, mac_jobs_root)
    mismatch = _seed_catalog_failure(
        store,
        "KTB-104",
        "catalog_response_mismatch",
    )
    generic = _seed_catalog_failure(store, "KTB-110")

    report = CatalogSyncReconciler(store, RecordingVerifier()).run(execute=True)

    assert report.mode == "execute"
    assert report.counts == {"restored": 2}
    for original, expected_warning in (
        (mismatch, "catalog_response_mismatch"),
        (generic, "catalog_sync_failed"),
    ):
        job = store.get_job(original.id)
        assert job is not None
        assert job.status is JobStatus.ENGLISH_SRT_READY
        assert job.artifact_status == "ready"
        assert job.catalog_sync_status == "failed"
        assert job.catalog_sync_warning_code == expected_warning
        assert job.catalog_sync_warning_message == (
            "Catalog sync failed after Supabase publication; artifact verified ready."
        )
        assert job.error is None
        assert job.catalog_sync_last_http_status == 500
        assert job.catalog_sync_last_response_json == '{"success":false}'
        assert job.published_storage_path == original.published_storage_path


def test_reconciliation_can_requeue_catalog_sync_and_resend_missing_ready_webhook(
    sqlite_path,
    mac_jobs_root,
):
    store = _store(sqlite_path, mac_jobs_root)
    original = _seed_catalog_failure(store, "KTB-111")
    notifier = RecordingNotifier()

    report = CatalogSyncReconciler(
        store,
        RecordingVerifier(),
        notifier=notifier,
    ).run(
        execute=True,
        retry_catalog_sync=True,
        resend_ready_webhook=True,
    )

    assert report.counts == {"restored": 1}
    job = store.get_job(original.id)
    assert job is not None
    assert job.catalog_sync_status == "pending"
    assert job.catalog_sync_attempt_count == 0
    assert job.next_catalog_sync_attempt_at is None
    assert notifier.jobs == [job]


def test_reconciliation_skips_remote_mismatch_and_state_changed_during_verify(
    sqlite_path,
    mac_jobs_root,
):
    store = _store(sqlite_path, mac_jobs_root)
    missing = _seed_catalog_failure(store, "KTB-104")
    changed = _seed_catalog_failure(store, "KTB-110")

    def mutate_verified_receipt(movie_code):
        if movie_code != "ktb-110":
            return
        with store.connection() as conn:
            conn.execute(
                "UPDATE jobs SET published_content_sha256 = ? WHERE id = ?",
                ("b" * 64, changed.id),
            )

    verifier = RecordingVerifier(
        failures={missing.normalized_movie_number: "catalog_mismatch"},
        after_verify=mutate_verified_receipt,
    )

    report = CatalogSyncReconciler(store, verifier).run(execute=True)

    assert report.counts == {"remote_not_verified": 1, "state_changed": 1}
    assert store.get_job(missing.id).status is JobStatus.FAILED
    assert store.get_job(changed.id).status is JobStatus.FAILED


def test_reconciliation_candidates_exclude_non_catalog_and_claimed_failures(
    sqlite_path,
    mac_jobs_root,
):
    store = _store(sqlite_path, mac_jobs_root)
    claimed = _seed_catalog_failure(store, "KTB-104")
    other = _seed_catalog_failure(store, "KTB-110")
    with store.connection() as conn:
        conn.execute(
            "UPDATE jobs SET claimed_by = 'worker' WHERE id = ?",
            (claimed.id,),
        )
        conn.execute(
            "UPDATE jobs SET error = 'catalogXsync: failed' WHERE id = ?",
            (other.id,),
        )

    report = CatalogSyncReconciler(store, RecordingVerifier()).run(execute=True)

    assert report.items == ()
    assert report.counts == {}


def test_reconciliation_is_idempotent(sqlite_path, mac_jobs_root):
    store = _store(sqlite_path, mac_jobs_root)
    _seed_catalog_failure(store, "KTB-104")
    reconciler = CatalogSyncReconciler(store, RecordingVerifier())

    first = reconciler.run(execute=True)
    second = reconciler.run(execute=True)

    assert first.counts == {"restored": 1}
    assert second.items == ()


def test_reconciliation_requires_execute_for_side_effect_options(
    sqlite_path,
    mac_jobs_root,
):
    store = _store(sqlite_path, mac_jobs_root)
    reconciler = CatalogSyncReconciler(store, RecordingVerifier())

    with pytest.raises(ValueError, match="requires execute"):
        reconciler.run(retry_catalog_sync=True)
    with pytest.raises(ValueError, match="requires execute"):
        reconciler.run(resend_ready_webhook=True)


def test_reconciliation_cli_is_dry_run_by_default_and_accepts_explicit_movies():
    args = build_parser().parse_args(
        [
            "reconcile-catalog-sync-failures",
            "--movie",
            "KTB-104",
            "--movie",
            "KTB-110",
            "--limit",
            "3",
        ]
    )

    assert args.movie_codes == ["KTB-104", "KTB-110"]
    assert args.limit == 3
    assert args.execute is False
    assert args.retry_catalog_sync is False
    assert args.resend_ready_webhook is False

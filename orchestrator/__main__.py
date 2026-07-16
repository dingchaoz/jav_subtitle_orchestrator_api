import argparse
import hmac
import json
import logging
import os
import re
import shlex
import subprocess
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from orchestrator.logging_config import configure_logging


LOGGER = logging.getLogger(__name__)


_LOWERCASE_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def _positive_int(value: str) -> int:
    if re.fullmatch(r"[1-9][0-9]*", value) is None:
        raise argparse.ArgumentTypeError("must be a positive integer") from None
    return int(value)


def _lowercase_sha256(value: str) -> str:
    if _LOWERCASE_SHA256_RE.fullmatch(value) is None:
        raise argparse.ArgumentTypeError("must be exactly 64 lowercase hex characters")
    return value


def _catalog_movie_code(value: str) -> str:
    from orchestrator.movie_code import canonical_movie_code

    try:
        return canonical_movie_code(value)
    except (AttributeError, TypeError, ValueError):
        raise argparse.ArgumentTypeError("must be a valid movie code") from None


class OrchestratorArgumentParser(argparse.ArgumentParser):
    def parse_args(self, args=None, namespace=None):
        parsed = super().parse_args(args, namespace)
        if parsed.command == "catalog-visibility-audit" and parsed.allowlist:
            normalized = tuple(parsed.allowlist)
            if len(normalized) != len(set(normalized)):
                self.error("--allowlist contains duplicate canonical movie codes")
            parsed.allowlist = tuple(sorted(normalized))
        if parsed.command == "catalog-visibility-repair":
            if parsed.execute and parsed.confirm_report_sha256 is None:
                self.error(
                    "--confirm-report-sha256 is required when --execute is used"
                )
            if not parsed.execute and parsed.confirm_report_sha256 is not None:
                self.error("--confirm-report-sha256 requires --execute")
        return parsed


@dataclass(frozen=True, slots=True)
class ProcessRecord:
    pid: int
    command: tuple[str, ...]

    @property
    def is_translation_worker(self) -> bool:
        marker = ("-m", "orchestrator", "mac-translation-worker")
        return any(
            index > 0
            and re.fullmatch(
                r"python(?:\d+(?:\.\d+)*)?",
                Path(self.command[index - 1]).name,
            )
            is not None
            and self.command[index : index + len(marker)] == marker
            and index + len(marker) == len(self.command)
            for index in range(len(self.command) - len(marker) + 1)
        )


class ProcessInventory(Protocol):
    def list_processes(self) -> tuple[ProcessRecord, ...]: ...


class PsProcessInventory:
    def list_processes(self) -> tuple[ProcessRecord, ...]:
        completed = subprocess.run(
            ["ps", "-axo", "pid=,command="],
            shell=False,
            timeout=5,
            check=False,
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
        )
        if completed.returncode != 0:
            raise RuntimeError("process inventory unavailable")
        processes: list[ProcessRecord] = []
        for line in completed.stdout.splitlines():
            fields = line.strip().split(maxsplit=1)
            if len(fields) != 2:
                continue
            try:
                pid = int(fields[0])
                command = tuple(shlex.split(fields[1], posix=True))
            except (ValueError, TypeError):
                continue
            if pid > 0 and command:
                processes.append(ProcessRecord(pid, command))
        return tuple(processes)


class TranslationWorkerProcessProbe:
    def __init__(
        self,
        process_inventory: ProcessInventory,
    ) -> None:
        self.process_inventory = process_inventory

    def __call__(self) -> str | None:
        try:
            process_count = sum(
                process.is_translation_worker
                for process in self.process_inventory.list_processes()
            )
        except Exception:
            return "translation_worker_count_mismatch"
        if process_count != 1:
            return "translation_worker_count_mismatch"
        return None


class TranslationWorkerHeartbeatProbe:
    def __init__(
        self,
        store,
        *,
        expected_worker_id: str,
        freshness_seconds: int = 60,
    ) -> None:
        self.store = store
        self.expected_worker_id = expected_worker_id
        self.freshness_seconds = freshness_seconds

    def __call__(self) -> str | None:
        now = datetime.now(UTC)
        matches = []
        for worker in self.store.list_worker_statuses():
            if (
                worker.worker_id != self.expected_worker_id
                or worker.role != "mac_translator"
            ):
                continue
            try:
                age = (
                    now - datetime.fromisoformat(worker.last_seen_at)
                ).total_seconds()
            except (TypeError, ValueError):
                continue
            if 0 <= age <= self.freshness_seconds:
                matches.append(worker)
        if len(matches) != 1:
            return "translation_worker_heartbeat_mismatch"
        return None


def run_historical_repair_controller_loop(
    controller,
    *,
    poll_interval_seconds: int,
    sleep_fn=time.sleep,
    max_cycles: int | None = None,
) -> int:
    from orchestrator.historical_batch import render_historical_controller_report

    cycles = 0
    while max_cycles is None or cycles < max_cycles:
        result = controller.run_once()
        print(render_historical_controller_report(result))
        cycles += 1
        if result.hard_pause:
            return 2
        if result.complete:
            return 0
        if max_cycles is None or cycles < max_cycles:
            sleep_fn(poll_interval_seconds)
    return 3


def run_historical_repair_controller(
    *,
    allowlist_file: Path,
    initial_batch_size: int,
    batch_size: int,
    poll_interval_seconds: int,
) -> int:
    from orchestrator.config import MacSettings
    from orchestrator.historical_batch import HistoricalRepairController
    from orchestrator.store import JobStore

    settings = MacSettings()
    store = JobStore(settings.db_path, settings.jobs_root_mac, settings.jobs_root_windows)
    store.initialize()

    controller = HistoricalRepairController(
        store,
        allowlist_file,
        initial_batch_size=initial_batch_size,
        batch_size=batch_size,
        process_health_probe=TranslationWorkerProcessProbe(
            PsProcessInventory(),
        ),
        worker_health_probe=TranslationWorkerHeartbeatProbe(
            store,
            expected_worker_id=settings.mac_translation_worker_id,
            freshness_seconds=max(60, min(300, poll_interval_seconds * 3)),
        ),
    )
    return run_historical_repair_controller_loop(
        controller,
        poll_interval_seconds=poll_interval_seconds,
    )


def build_subtitle_audit_api_service(settings):
    if (
        not settings.subtitle_audit_visibility_enabled
        or not settings.supabase_url
        or not settings.supabase_service_role_key
    ):
        return None
    from orchestrator.subtitle_audit_api import SubtitleAuditApiService

    return SubtitleAuditApiService(
        settings.supabase_url,
        settings.supabase_service_role_key,
        timeout_seconds=settings.subtitle_audit_timeout_seconds,
    )


def build_requested_subtitle_importer(settings):
    if (
        not settings.cloudflare_account_id
        or not settings.cloudflare_d1_api_token
        or not settings.cloudflare_d1_database_id
        or not settings.supabase_url
        or not settings.supabase_service_role_key
    ):
        return None
    from orchestrator.subtitle_request_importer import RequestedSubtitleImporter

    return RequestedSubtitleImporter(
        cloudflare_account_id=settings.cloudflare_account_id,
        cloudflare_d1_api_token=settings.cloudflare_d1_api_token,
        cloudflare_d1_database_id=settings.cloudflare_d1_database_id,
        supabase_url=settings.supabase_url,
        supabase_service_role_key=settings.supabase_service_role_key,
        timeout_seconds=settings.requested_subtitle_import_timeout_seconds,
    )


def build_supabase_publisher(settings):
    if not settings.mac_translation_publish_enabled:
        return None
    if (
        not settings.supabase_url
        or not settings.supabase_service_role_key
        or not settings.supabase_subtitle_bucket
    ):
        raise RuntimeError(
            "Supabase publication is enabled but URL, service key, or bucket is missing"
        )
    import requests

    from orchestrator.movie_catalog import SupabaseMovieCatalogEnsurer
    from orchestrator.supabase_publisher import SupabaseSubtitlePublisher

    session = requests.Session()
    catalog_ensurer = SupabaseMovieCatalogEnsurer(
        settings.supabase_url,
        settings.supabase_service_role_key,
        session=session,
    )

    return SupabaseSubtitlePublisher(
        settings.supabase_url,
        settings.supabase_service_role_key,
        bucket=settings.supabase_subtitle_bucket,
        verification_timeout_seconds=(
            settings.supabase_publish_verify_timeout_seconds
        ),
        session=session,
        catalog_ensurer=catalog_ensurer,
    )


def build_supabase_publication_verifier(settings):
    if (
        not settings.supabase_url
        or not settings.supabase_service_role_key
        or not settings.supabase_subtitle_bucket
    ):
        raise RuntimeError(
            "Supabase URL, service key, and subtitle bucket are required"
        )
    from orchestrator.supabase_publisher import SupabaseSubtitlePublisher

    return SupabaseSubtitlePublisher(
        settings.supabase_url,
        settings.supabase_service_role_key,
        bucket=settings.supabase_subtitle_bucket,
        verification_timeout_seconds=(
            settings.supabase_publish_verify_timeout_seconds
        ),
    )


def build_catalog_sync_client(
    settings,
    *,
    require_publish_enabled: bool = True,
):
    if not isinstance(require_publish_enabled, bool):
        raise TypeError("require_publish_enabled must be a boolean")
    if require_publish_enabled and not settings.mac_translation_publish_enabled:
        return None
    if not settings.javsubtitle_api_base or not settings.javsubtitle_admin_api_token:
        if require_publish_enabled:
            return None
        raise RuntimeError(
            "Catalog sync credentials are required for catalog visibility repair"
        )
    from orchestrator.catalog_sync import CatalogSyncClient

    return CatalogSyncClient(
        settings.javsubtitle_api_base,
        settings.javsubtitle_admin_api_token,
    )


def run_api() -> None:
    import uvicorn

    from orchestrator.api import create_app
    from orchestrator.config import MacSettings
    from orchestrator.store import JobStore

    settings = MacSettings()
    store = JobStore(settings.db_path, settings.jobs_root_mac, settings.jobs_root_windows)
    store.initialize()
    app = create_app(
        store,
        worker_lease_seconds=settings.worker_lease_seconds,
        max_worker_attempts=settings.max_worker_attempts,
        subtitle_audit_service=build_subtitle_audit_api_service(settings),
        requested_subtitle_importer=build_requested_subtitle_importer(settings),
        callback_clients=build_callback_clients(settings),
    )
    uvicorn.run(app, host=settings.host, port=settings.port)


def _release_worker_lock(lock, *, preserve_worker_error: bool) -> None:
    try:
        lock.release()
    except BaseException:
        if not preserve_worker_error:
            raise
        LOGGER.exception("worker lock cleanup failed while preserving worker error")


def build_callback_clients(settings):
    from orchestrator.callbacks import CallbackClient

    return {
        key: CallbackClient(url=value.url, secret=value.secret)
        for key, value in getattr(settings, "callback_clients", {}).items()
    }


def build_callback_notifier(store, settings):
    if not getattr(settings, "callback_clients", {}):
        return None
    from orchestrator.callbacks import CallbackNotifier

    return CallbackNotifier(
        store,
        build_callback_clients(settings),
        timeout_seconds=getattr(settings, "callback_timeout_seconds", 10),
    )


def run_catalog_sync_reconciliation(
    *,
    movie_codes: list[str] | None,
    limit: int,
    execute: bool,
    retry_catalog_sync: bool,
    resend_ready_webhook: bool,
) -> None:
    from orchestrator.catalog_sync_reconciliation import CatalogSyncReconciler
    from orchestrator.config import MacSettings
    from orchestrator.store import JobStore

    if not execute and (retry_catalog_sync or resend_ready_webhook):
        raise ValueError("retry and resend options require --execute")
    settings = MacSettings()
    store = JobStore(
        settings.db_path,
        settings.jobs_root_mac,
        settings.jobs_root_windows,
    )
    if execute:
        store.initialize()
    reconciler = CatalogSyncReconciler(
        store,
        build_supabase_publication_verifier(settings),
        notifier=(
            build_callback_notifier(store, settings)
            if resend_ready_webhook
            else None
        ),
    )
    report = reconciler.run(
        movie_codes=movie_codes,
        limit=limit,
        execute=execute,
        retry_catalog_sync=retry_catalog_sync,
        resend_ready_webhook=resend_ready_webhook,
    )
    print(
        json.dumps(
            {
                "mode": report.mode,
                "counts": report.counts,
                "items": [
                    {
                        "job_id": item.job_id,
                        "movie_code": item.movie_code,
                        "outcome": item.outcome,
                    }
                    for item in report.items
                ],
            },
            sort_keys=True,
        )
    )


def run_mac_worker() -> None:
    from orchestrator.config import PROJECT_ROOT, MacSettings
    from orchestrator.process_lock import SingleInstanceLock

    lock = SingleInstanceLock(PROJECT_ROOT / "data" / "mac-worker.lock").acquire()
    worker_failed = False
    try:
        from orchestrator.mac_worker import MacDownloadWorker, run_forever as run_mac_forever
        from orchestrator.missav_adapter import MissAVAdapter
        from orchestrator.store import JobStore

        settings = MacSettings()
        store = JobStore(settings.db_path, settings.jobs_root_mac, settings.jobs_root_windows)
        store.initialize()
        worker = MacDownloadWorker(
            store,
            MissAVAdapter(settings.missav_pipeline_root),
            settings.max_download_attempts,
            worker_id=settings.mac_download_worker_id,
        )
        run_mac_forever(worker)
    except BaseException:
        worker_failed = True
        raise
    finally:
        _release_worker_lock(lock, preserve_worker_error=worker_failed)


def _export_windows_runtime_env(settings) -> None:
    exports = {
        "OPENAI_API_KEY": settings.openai_api_key,
        "TRANSLATELOCALLY_PATH": settings.translatelocally_path,
        "TRANSLATELOCALLY_MODEL": settings.translatelocally_model,
        "CODEX_TRANSLATE_SCRIPT_PATH": settings.codex_translate_script_path,
        "CODEX_TRANSLATE_PYTHON_EXECUTABLE": settings.codex_translate_python_executable,
        "CODEX_BIN_PATH": settings.codex_bin_path,
        "CODEX_TRANSLATION_PROVIDER": settings.codex_translation_provider,
        "CODEX_TRANSLATION_TARGETS": settings.codex_translation_targets,
        "CODEX_TRANSLATION_WORKERS": settings.codex_translation_workers,
        "CODEX_TRANSLATION_BATCH_WORKERS": settings.codex_translation_batch_workers,
        "CODEX_TRANSLATION_ANTHROPIC_MODELS": settings.codex_translation_anthropic_models,
        "CODEX_TRANSLATION_ANTHROPIC_RECHECK_MINUTES": (
            settings.codex_translation_anthropic_recheck_minutes
        ),
    }
    for key, value in exports.items():
        if value:
            os.environ[key] = value


def _build_windows_transcriber(settings):
    from orchestrator.transcription import ExternalScriptTranscriber, FasterWhisperTranscriber

    if settings.transcribe_script_path:
        return ExternalScriptTranscriber(
            settings.transcribe_script_path.replace("\\", "/"),
            python_executable=settings.transcribe_python_executable or None,
            model_name=settings.whisper_model,
            device=settings.whisper_device,
        )
    return FasterWhisperTranscriber(
        settings.whisper_model,
        settings.whisper_device,
        settings.whisper_compute_type,
    )


def _run_mac_translation_smoke(settings, translator):
    from orchestrator.translation_smoke import run_translation_startup_smoke_test

    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=Path(__file__).resolve().parents[1],
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        check=False,
    ).stdout.strip()
    LOGGER.info(
        "mac translation runtime wrapper=%s executable=%s model=%s git_commit=%s",
        settings.mac_translate_script_path,
        settings.translatelocally_path,
        settings.translatelocally_model,
        commit or "unknown",
    )
    try:
        startup_report = run_translation_startup_smoke_test(translator)
    except Exception as exc:
        LOGGER.critical("translation startup smoke failed: %s", exc)
        raise
    LOGGER.info(
        "translation startup smoke passed cues=%d unique_ratio=%.3f known_bad=%d",
        startup_report.english_cue_count,
        startup_report.english_unique_ratio,
        startup_report.known_bad_phrase_count,
    )
    return startup_report


def _export_mac_translation_runtime_env(settings) -> None:
    if settings.translatelocally_path:
        os.environ["TRANSLATELOCALLY_PATH"] = settings.translatelocally_path
    os.environ["TRANSLATELOCALLY_MODEL"] = settings.translatelocally_model


def run_mac_translation_smoke_test() -> None:
    from orchestrator.config import MacSettings
    from orchestrator.translation import SubtitleTranslator

    settings = MacSettings()
    _export_mac_translation_runtime_env(settings)
    translator = SubtitleTranslator(settings.mac_translate_script_path)
    _run_mac_translation_smoke(settings, translator)


def run_mac_translation_worker() -> None:
    from orchestrator.config import PROJECT_ROOT, MacSettings
    from orchestrator.process_lock import SingleInstanceLock

    lock = SingleInstanceLock(
        PROJECT_ROOT / "data" / "mac-translation-worker.lock"
    ).acquire()
    worker_failed = False
    try:
        from orchestrator.mac_worker import (
            MacTranslationWorker,
            run_translation_forever,
        )
        from orchestrator.store import JobStore
        from orchestrator.translation import SubtitleTranslator

        settings = MacSettings()
        publisher = build_supabase_publisher(settings)
        catalog_sync_client = build_catalog_sync_client(settings)
        _export_mac_translation_runtime_env(settings)
        translator = SubtitleTranslator(settings.mac_translate_script_path)
        _run_mac_translation_smoke(settings, translator)
        store = JobStore(settings.db_path, settings.jobs_root_mac, settings.jobs_root_windows)
        store.initialize()
        worker = MacTranslationWorker(
            store,
            translator,
            max_translation_attempts=settings.max_translation_attempts,
            worker_id=settings.mac_translation_worker_id,
            lease_seconds=settings.mac_translation_lease_seconds,
            quality_failure_limit=settings.translation_quality_failure_limit,
            publisher=publisher,
            max_publish_attempts=settings.max_publish_attempts,
            publish_retry_seconds=settings.mac_publish_retry_seconds,
            catalog_sync_client=catalog_sync_client,
            max_catalog_sync_attempts=settings.max_catalog_sync_attempts,
            catalog_sync_retry_seconds=settings.catalog_sync_retry_seconds,
            catalog_sync_max_retry_seconds=(
                getattr(settings, "catalog_sync_max_retry_seconds", 900)
            ),
            callback_notifier=build_callback_notifier(store, settings),
        )
        run_translation_forever(worker, settings.mac_translation_poll_interval_seconds)
    except BaseException:
        worker_failed = True
        raise
    finally:
        _release_worker_lock(lock, preserve_worker_error=worker_failed)


def run_mac_translation_worker_once(job_id: str) -> None:
    from orchestrator.config import MacSettings
    from orchestrator.mac_worker import MacTranslationWorker
    from orchestrator.models import JobStatus
    from orchestrator.store import JobStore
    from orchestrator.translation import SubtitleTranslator

    settings = MacSettings()
    publisher = build_supabase_publisher(settings)
    catalog_sync_client = build_catalog_sync_client(settings)
    _export_mac_translation_runtime_env(settings)
    translator = SubtitleTranslator(settings.mac_translate_script_path)
    _run_mac_translation_smoke(settings, translator)
    store = JobStore(settings.db_path, settings.jobs_root_mac, settings.jobs_root_windows)
    store.initialize()
    worker = MacTranslationWorker(
        store,
        translator,
        max_translation_attempts=settings.max_translation_attempts,
        worker_id=f"{settings.mac_translation_worker_id}-once",
        lease_seconds=settings.mac_translation_lease_seconds,
        quality_failure_limit=settings.translation_quality_failure_limit,
        publisher=publisher,
        max_publish_attempts=settings.max_publish_attempts,
        publish_retry_seconds=settings.mac_publish_retry_seconds,
        catalog_sync_client=catalog_sync_client,
        max_catalog_sync_attempts=settings.max_catalog_sync_attempts,
        catalog_sync_retry_seconds=settings.catalog_sync_retry_seconds,
        catalog_sync_max_retry_seconds=getattr(
            settings,
            "catalog_sync_max_retry_seconds",
            900,
        ),
        callback_notifier=build_callback_notifier(store, settings),
    )
    worker.process_job_id(job_id)
    completed = store.get_job(job_id)
    if completed is None or completed.status is not JobStatus.ENGLISH_SRT_READY:
        status = completed.status.value if completed is not None else "missing"
        raise SystemExit(f"exact translation job did not become ready: {status}")


def run_plan_historical_repairs(
    *, allowlist: set[str] | None, limit: int
) -> None:
    from orchestrator.config import MacSettings
    from orchestrator.store import JobStore
    from orchestrator.subtitle_repair import (
        plan_historical_repairs,
        render_repair_report,
    )

    settings = MacSettings()
    store = JobStore(settings.db_path, settings.jobs_root_mac, settings.jobs_root_windows)
    plans = plan_historical_repairs(store, allowlist=allowlist, limit=limit)
    print(render_repair_report(plans))


def run_plan_historical_repair_batch(
    *, allowlist_file: Path, limit: int, output: Path
) -> None:
    from orchestrator.config import MacSettings
    from orchestrator.historical_batch import (
        plan_historical_batch,
        render_historical_batch_report,
        write_private_plan,
    )
    from orchestrator.store import JobStore

    settings = MacSettings()
    store = JobStore(settings.db_path, settings.jobs_root_mac, settings.jobs_root_windows)
    plan = plan_historical_batch(store, allowlist_file, limit=limit)
    write_private_plan(output, plan)
    print(render_historical_batch_report(plan))


def run_enqueue_historical_repair_batch(
    *,
    allowlist_file: Path,
    plan_file: Path,
    confirm_plan_sha256: str,
) -> None:
    from orchestrator.config import MacSettings
    from orchestrator.historical_batch import (
        enqueue_historical_batch,
        read_private_plan,
    )
    from orchestrator.store import JobStore

    settings = MacSettings()
    store = JobStore(settings.db_path, settings.jobs_root_mac, settings.jobs_root_windows)
    plan = read_private_plan(plan_file)
    records = enqueue_historical_batch(
        store,
        plan,
        allowlist_file,
        confirm_plan_sha256=confirm_plan_sha256,
    )
    identifiers = ",".join(record.id for record in records) or "none"
    print(
        f"enqueued=true batch_id={plan.batch_id} "
        f"plan_sha256={plan.plan_sha256} count={len(records)} "
        f"repair_ids={identifiers}"
    )


def run_plan_translation_only_repair_batch(
    *, allowlist_file: Path, limit: int, output: Path
) -> None:
    from orchestrator.config import MacSettings
    from orchestrator.store import JobStore
    from orchestrator.subtitle_repair import (
        plan_translation_only_repair_batch,
        render_translation_only_repair_batch_report,
        write_translation_only_repair_plan,
    )

    settings = MacSettings()
    store = JobStore(settings.db_path, settings.jobs_root_mac, settings.jobs_root_windows)
    plan = plan_translation_only_repair_batch(store, allowlist_file, limit=limit)
    write_translation_only_repair_plan(output, plan)
    print(render_translation_only_repair_batch_report(plan))


def run_enqueue_translation_only_repair_batch(
    *, plan_file: Path, confirm_plan_sha256: str
) -> None:
    from orchestrator.config import MacSettings
    from orchestrator.store import JobStore
    from orchestrator.subtitle_repair import (
        enqueue_translation_only_repair_batch,
        read_translation_only_repair_plan,
    )

    settings = MacSettings()
    store = JobStore(settings.db_path, settings.jobs_root_mac, settings.jobs_root_windows)
    plan = read_translation_only_repair_plan(plan_file)
    records = enqueue_translation_only_repair_batch(
        store,
        plan,
        confirm_plan_sha256=confirm_plan_sha256,
    )
    identifiers = ",".join(record.id for record in records) or "none"
    print(
        f"enqueued=true plan_sha256={plan.plan_sha256} "
        f"count={len(records)} job_ids={identifiers}"
    )


def run_translation_only_repair_supervisor(
    *,
    allowlist_file: Path,
    work_dir: Path,
    batch_size: int,
    max_jobs: int,
    execute: bool,
    confirm_remaining_count: int | None,
    verify_public_api: bool,
    poll_interval_seconds: int,
    batch_timeout_seconds: int,
) -> None:
    from orchestrator.config import MacSettings
    from orchestrator.store import JobStore
    from orchestrator.translation_only_supervisor import (
        TranslationOnlySupervisorConfig,
        run_translation_only_supervisor,
    )

    settings = MacSettings()
    store = JobStore(settings.db_path, settings.jobs_root_mac, settings.jobs_root_windows)
    result = run_translation_only_supervisor(
        store,
        TranslationOnlySupervisorConfig(
            allowlist_file=allowlist_file,
            work_dir=work_dir,
            batch_size=batch_size,
            max_jobs=max_jobs,
            execute=execute,
            confirm_remaining_count=confirm_remaining_count,
            verify_public_api=verify_public_api,
            poll_interval_seconds=poll_interval_seconds,
            batch_timeout_seconds=batch_timeout_seconds,
        ),
    )
    print(
        f"action={result.action} remaining_count={result.remaining_count} "
        f"enqueued={result.enqueued_count} completed={result.completed_count} "
        f"failed={result.failed_count} batches={result.batches} "
        f"receipt_file={result.receipt_file or ''} "
        f"reason_code={result.reason_code or ''}"
    )


def run_plan_catalog_repairs(
    *, allowlist: set[str] | None, limit: int
) -> None:
    from orchestrator.catalog_repair import (
        plan_catalog_repairs,
        render_catalog_repair_report,
    )
    from orchestrator.config import MacSettings
    from orchestrator.store import JobStore

    settings = MacSettings()
    store = JobStore(settings.db_path, settings.jobs_root_mac, settings.jobs_root_windows)
    plans = plan_catalog_repairs(store, allowlist=allowlist, limit=limit)
    print(render_catalog_repair_report(plans))


def _parse_allowlist(value: str | None) -> set[str] | None:
    if not value:
        return None
    from orchestrator.movie_code import canonical_movie_code

    allowlist = {
        canonical_movie_code(token.strip())
        for token in value.split(",")
        if token.strip()
    }
    return allowlist or None


def _write_private_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            os.fchmod(handle.fileno(), 0o600)
            json.dump(payload, handle, ensure_ascii=True, sort_keys=True, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def run_select_historical_repair_canary(
    *,
    allowlist_file: Path,
    preferred_movie: str | None,
    output: Path,
) -> None:
    from orchestrator.config import MacSettings
    from orchestrator.historical_repair import select_historical_repair_canary
    from orchestrator.store import JobStore

    settings = MacSettings()
    store = JobStore(settings.db_path, settings.jobs_root_mac, settings.jobs_root_windows)
    candidate = select_historical_repair_canary(
        store,
        allowlist_file,
        preferred_movie=preferred_movie,
    )
    if candidate is None:
        raise SystemExit("no eligible historical repair canary")
    _write_private_json(output, candidate.to_safe_dict())
    print(
        f"selected=true job_id={candidate.job_id} "
        f"movie={candidate.movie_number} output={output.resolve()}"
    )


def run_prepare_historical_repair_canary(
    *,
    allowlist_file: Path,
    movie: str,
    limit: int,
    confirm_job_id: str,
) -> None:
    from orchestrator.config import MacSettings
    from orchestrator.historical_repair import prepare_historical_repair_canary
    from orchestrator.store import JobStore

    settings = MacSettings()
    store = JobStore(settings.db_path, settings.jobs_root_mac, settings.jobs_root_windows)
    store.initialize()
    prior = store.get_job(confirm_job_id)
    if prior is None:
        raise SystemExit("confirmed historical repair job does not exist")
    prepared = prepare_historical_repair_canary(
        store,
        allowlist_file,
        movie=movie,
        limit=limit,
        confirm_job_id=confirm_job_id,
    )
    print(
        f"prepared=true job_id={prepared.id} movie={prepared.normalized_movie_number} "
        f"prior_status={prior.status.value} new_status={prepared.status.value}"
    )


def run_prepare_catalog_publication_canary(
    *,
    allowlist_file: Path,
    movie: str,
    limit: int,
    confirm_job_id: str,
) -> None:
    from orchestrator.catalog_repair import prepare_catalog_publication_canary
    from orchestrator.config import MacSettings
    from orchestrator.store import JobStore

    settings = MacSettings()
    store = JobStore(settings.db_path, settings.jobs_root_mac, settings.jobs_root_windows)
    store.initialize()
    receipt = prepare_catalog_publication_canary(
        store,
        allowlist_file,
        movie=movie,
        limit=limit,
        confirm_job_id=confirm_job_id,
    )
    print(
        f"prepared=true job_id={receipt.job_id} movie={receipt.movie_code} "
        f"prior_status={receipt.prior_status.value} "
        f"new_status={receipt.new_status.value} "
        f"translation_attempt_count={receipt.translation_attempt_count} "
        f"english_sha256={receipt.english_sha256} "
        f"quality_passed={str(receipt.quality_passed).lower()} "
        f"cues={receipt.english_cue_count} "
        f"unique_ratio={receipt.english_unique_ratio:.3f} "
        f"known_bad={receipt.known_bad_phrase_count}"
    )


def run_local_english_ai_audit(
    *,
    output: Path,
    limit: int | None,
    workers: int,
    requests_per_second: float,
):
    from orchestrator.config import MacSettings
    from orchestrator.historical_english_ai_audit import (
        LocalEnglishAiAuditRunner,
        RequestRateLimiter,
        SupabaseEnglishAiReader,
    )

    settings = MacSettings()
    if not settings.supabase_url or not settings.supabase_service_role_key:
        raise SystemExit(
            "SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required"
        )
    limiter = RequestRateLimiter(requests_per_second)
    reader = SupabaseEnglishAiReader(
        settings.supabase_url,
        settings.supabase_service_role_key,
        bucket=settings.supabase_subtitle_bucket,
        timeout_seconds=settings.local_audit_timeout_seconds,
        rate_limiter=limiter,
    )
    summary = LocalEnglishAiAuditRunner(reader, workers=workers).scan(
        output,
        limit=limit,
    )
    print(
        "local English_AI audit "
        f"discovered={summary.discovered} "
        f"passed={summary.passed} "
        f"hard_failure={summary.hard_failure} "
        f"errors={summary.errors} "
        f"skipped={summary.skipped} "
        f"complete={str(summary.complete).lower()} "
        f"bounded={str(summary.bounded).lower()}"
    )
    print(f"reports={output.resolve()}")
    if summary.bounded:
        print(
            "resume: python -m orchestrator audit-english-ai-local "
            f"--output {output} --workers {workers} "
            f"--requests-per-second {requests_per_second:g}"
        )
    if summary.catalog_error is not None:
        raise SystemExit(f"catalog audit interrupted: {summary.catalog_error}")
    return summary


def _absolute_cli_path(path: Path) -> Path:
    return Path(path).absolute()


def _validated_cli_output_dir(path: Path, *, operation: str) -> Path:
    output = _absolute_cli_path(path)
    try:
        invalid = output.is_symlink() or (output.exists() and not output.is_dir())
    except OSError:
        raise SystemExit(f"catalog visibility {operation} output invalid") from None
    if invalid:
        raise SystemExit(f"catalog visibility {operation} output invalid")
    return output


def run_catalog_visibility_audit(
    *,
    output: Path,
    allowlist: tuple[str, ...] | None,
    limit: int | None,
):
    from orchestrator.catalog_visibility import (
        CatalogVisibilityAuditor,
        PublicCatalogVisibilityClient,
    )
    from orchestrator.config import MacSettings
    from orchestrator.store import JobStore

    output_dir = _validated_cli_output_dir(output, operation="audit")
    try:
        settings = MacSettings()
        if not settings.javsubtitle_api_base:
            raise ValueError("catalog API origin is not configured")
        store = JobStore(
            settings.db_path,
            settings.jobs_root_mac,
            settings.jobs_root_windows,
        )
        client = PublicCatalogVisibilityClient(
            settings.javsubtitle_api_base,
            timeout_seconds=settings.subtitle_audit_timeout_seconds,
        )
        summary = CatalogVisibilityAuditor(store, client).scan(
            output_dir,
            allowlist=None if allowlist is None else set(allowlist),
            limit=limit,
        )
    except Exception:
        raise SystemExit("catalog visibility audit failed") from None

    counts = summary.counts
    print(
        "audit_complete=true "
        f"discovered={summary.discovered} "
        f"checked={summary.checked} "
        f"visible={counts.get('visible', 0)} "
        f"missing={counts.get('missing', 0)} "
        f"not_found={counts.get('not_found', 0)} "
        f"fetch_failed={counts.get('fetch_failed', 0)} "
        f"response_invalid={counts.get('response_invalid', 0)} "
        f"invalid_receipt={counts.get('invalid_receipt', 0)} "
        f"report_sha256={summary.report_sha256} "
        f"report={_absolute_cli_path(summary.report_path)}"
    )
    return summary


def _validated_cli_report_path(path: Path) -> Path:
    report = _absolute_cli_path(path)
    try:
        valid = report.is_file() and not report.is_symlink()
    except OSError:
        valid = False
    if not valid:
        raise SystemExit("catalog visibility repair input invalid")
    return report


def _safe_stopped_reason(value: object) -> str:
    if value is None:
        return "none"
    if isinstance(value, str) and re.fullmatch(r"[a-z0-9_]{1,64}", value):
        return value
    return "unknown"


def run_catalog_visibility_repair(
    *,
    report: Path,
    output: Path,
    execute: bool,
    confirm_report_sha256: str | None,
) -> int:
    if not isinstance(execute, bool):
        raise SystemExit("catalog visibility repair authorization failed")
    if execute:
        if (
            not isinstance(confirm_report_sha256, str)
            or _LOWERCASE_SHA256_RE.fullmatch(confirm_report_sha256) is None
        ):
            raise SystemExit("catalog visibility repair authorization failed")
    elif confirm_report_sha256 is not None:
        raise SystemExit("catalog visibility repair authorization failed")

    from orchestrator.catalog_visibility_repair import (
        execute_catalog_visibility_repair,
        plan_catalog_visibility_repair,
    )
    from orchestrator.config import MacSettings
    from orchestrator.store import JobStore

    report_path = _validated_cli_report_path(report)
    output_dir = _validated_cli_output_dir(output, operation="repair")
    try:
        settings = MacSettings()
        if not settings.javsubtitle_api_base:
            raise ValueError("catalog API origin is not configured")
        store = JobStore(
            settings.db_path,
            settings.jobs_root_mac,
            settings.jobs_root_windows,
        )
        plan = plan_catalog_visibility_repair(
            store,
            report_path,
            expected_api_origin=settings.javsubtitle_api_base,
            output_dir=output_dir,
        )
    except Exception:
        raise SystemExit("catalog visibility repair planning failed") from None

    receipt_path = output_dir / "repair-execution.jsonl"
    if not execute:
        skipped = dict(sorted(plan.skipped.items()))
        resume_command = shlex.join(
            [
                "python",
                "-m",
                "orchestrator",
                "catalog-visibility-repair",
                "--report",
                str(report_path),
                "--output",
                str(output_dir),
                "--execute",
                "--confirm-report-sha256",
                plan.report_sha256,
            ]
        )
        print(
            "action=dry_run "
            f"eligible={len(plan.items)} "
            f"skipped_total={sum(skipped.values())} "
            f"skipped={json.dumps(skipped, sort_keys=True, separators=(',', ':'))} "
            f"report_sha256={plan.report_sha256} "
            f"plan_sha256={plan.plan_sha256} "
            f"plan={_absolute_cli_path(plan.plan_path)} "
            f"receipt={receipt_path} "
            f"resume={resume_command}"
        )
        return 0

    if (
        not isinstance(confirm_report_sha256, str)
        or not hmac.compare_digest(confirm_report_sha256, plan.report_sha256)
    ):
        raise SystemExit("catalog visibility repair authorization failed")
    if not settings.javsubtitle_admin_api_token:
        raise SystemExit("catalog visibility repair authorization failed")
    try:
        sync_client = build_catalog_sync_client(
            settings,
            require_publish_enabled=False,
        )
    except Exception:
        raise SystemExit("catalog visibility repair authorization failed") from None
    if sync_client is None:
        raise SystemExit("catalog visibility repair authorization failed")
    try:
        result = execute_catalog_visibility_repair(
            store,
            plan,
            sync_client=sync_client,
            output_dir=output_dir,
            execute=True,
            confirm_report_sha256=confirm_report_sha256,
        )
    except Exception:
        raise SystemExit("catalog visibility repair execution failed") from None

    stopped_reason = _safe_stopped_reason(result.stopped_reason)
    print(
        "action=executed "
        f"repaired={len(result.repaired)} "
        f"failed={len(result.failed)} "
        f"skipped_receipt_changed={len(result.skipped_receipt_changed)} "
        f"stopped_reason={stopped_reason} "
        f"report_sha256={plan.report_sha256} "
        f"receipt={_absolute_cli_path(result.receipt_path)}"
    )
    return 2 if result.failed or result.stopped_reason is not None else 0


def run_windows_worker() -> None:
    from orchestrator.config import WindowsSettings
    from orchestrator.windows_worker import (
        MacApiClient,
        WindowsWorker,
        run_forever as run_windows_forever,
    )

    settings = WindowsSettings()
    _export_windows_runtime_env(settings)
    client = MacApiClient(settings.mac_api_base_url, settings.worker_id)
    transcriber = _build_windows_transcriber(settings)
    worker = WindowsWorker(
        client,
        transcriber,
        heartbeat_interval_seconds=settings.heartbeat_interval_seconds,
    )
    run_windows_forever(worker, settings.poll_interval_seconds)


def run_recover_interrupted_audio(
    *,
    job_id: str,
    movie: str,
    expected_sha256: str,
) -> None:
    from orchestrator.audio_recovery import recover_interrupted_audio
    from orchestrator.config import MacSettings
    from orchestrator.store import JobStore

    settings = MacSettings()
    store = JobStore(settings.db_path, settings.jobs_root_mac, settings.jobs_root_windows)
    store.initialize()
    receipt = recover_interrupted_audio(
        store,
        job_id=job_id,
        movie=movie,
        expected_sha256=expected_sha256,
    )
    print(
        f"job_id={receipt.job_id} movie={receipt.movie_code} "
        f"status={receipt.status.value} sha256={receipt.sha256} "
        f"size={receipt.size_bytes} duration={receipt.duration_seconds:.6f} "
        f"reused_final={str(receipt.reused_final).lower()}"
    )


def run_repair_interrupted_audio_wav(
    *,
    job_id: str,
    movie: str,
    expected_sha256: str,
) -> None:
    from orchestrator.audio_recovery import repair_interrupted_audio_wav
    from orchestrator.config import MacSettings
    from orchestrator.store import JobStore

    settings = MacSettings()
    store = JobStore(settings.db_path, settings.jobs_root_mac, settings.jobs_root_windows)
    store.initialize()
    receipt = repair_interrupted_audio_wav(
        store,
        job_id=job_id,
        movie=movie,
        expected_sha256=expected_sha256,
    )
    print(
        f"job_id={receipt.job_id} movie={receipt.movie_code} "
        f"status={receipt.status.value} "
        f"original_sha256={receipt.original_sha256} "
        f"canonical_sha256={receipt.canonical_sha256} "
        f"size={receipt.size_bytes} duration={receipt.duration_seconds:.6f}"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = OrchestratorArgumentParser(prog="python -m orchestrator")
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser("api")
    subcommands.add_parser("mac-worker")
    subcommands.add_parser("mac-translation-smoke-test")
    subcommands.add_parser("mac-translation-worker")
    one_shot = subcommands.add_parser("mac-translation-worker-once")
    one_shot.add_argument("--job-id", required=True)
    subcommands.add_parser("windows-worker")
    audio_recovery = subcommands.add_parser("recover-interrupted-audio")
    audio_recovery.add_argument("--job-id", required=True)
    audio_recovery.add_argument("--movie", required=True)
    audio_recovery.add_argument("--expected-sha256", required=True)
    audio_repair = subcommands.add_parser("repair-interrupted-audio-wav")
    audio_repair.add_argument("--job-id", required=True)
    audio_repair.add_argument("--movie", required=True)
    audio_repair.add_argument("--expected-sha256", required=True)
    repair_parser = subcommands.add_parser(
        "plan-historical-subtitle-repair",
        help="print a read-only translation-stage repair plan",
    )
    repair_parser.add_argument(
        "--allowlist",
        nargs="+",
        metavar="MOVIE_NUMBER",
        help="only inspect these movie numbers",
    )
    repair_parser.add_argument("--limit", type=int, default=100)
    batch_plan = subcommands.add_parser("plan-historical-repair-batch")
    batch_plan.add_argument("--allowlist-file", type=Path, required=True)
    batch_plan.add_argument("--limit", type=int, choices=range(1, 21), required=True)
    batch_plan.add_argument("--output", type=Path, required=True)
    batch_enqueue = subcommands.add_parser("enqueue-historical-repair-batch")
    batch_enqueue.add_argument("--allowlist-file", type=Path, required=True)
    batch_enqueue.add_argument("--plan-file", type=Path, required=True)
    batch_enqueue.add_argument("--confirm-plan-sha256", required=True)
    translation_batch_plan = subcommands.add_parser(
        "plan-translation-only-repair-batch"
    )
    translation_batch_plan.add_argument("--allowlist-file", type=Path, required=True)
    translation_batch_plan.add_argument(
        "--limit", type=int, choices=range(1, 21), required=True
    )
    translation_batch_plan.add_argument("--output", type=Path, required=True)
    translation_batch_enqueue = subcommands.add_parser(
        "enqueue-translation-only-repair-batch"
    )
    translation_batch_enqueue.add_argument("--plan-file", type=Path, required=True)
    translation_batch_enqueue.add_argument("--confirm-plan-sha256", required=True)
    translation_supervisor = subcommands.add_parser(
        "run-translation-only-repair-supervisor"
    )
    translation_supervisor.add_argument("--allowlist-file", type=Path, required=True)
    translation_supervisor.add_argument("--work-dir", type=Path, required=True)
    translation_supervisor.add_argument(
        "--batch-size", type=int, choices=range(1, 21), required=True
    )
    translation_supervisor.add_argument("--max-jobs", type=int, required=True)
    translation_supervisor.add_argument("--execute", action="store_true")
    translation_supervisor.add_argument("--confirm-remaining-count", type=int)
    translation_supervisor.add_argument("--verify-public-api", action="store_true")
    translation_supervisor.add_argument(
        "--poll-interval-seconds", type=int, default=30
    )
    translation_supervisor.add_argument(
        "--batch-timeout-seconds", type=int, default=3600
    )
    controller = subcommands.add_parser("historical-repair-controller")
    controller.add_argument("--allowlist-file", type=Path, required=True)
    controller.add_argument(
        "--initial-batch-size", type=int, choices=range(1, 6), default=5
    )
    controller.add_argument(
        "--batch-size", type=int, choices=range(1, 21), default=20
    )
    controller.add_argument(
        "--poll-interval-seconds", type=int, choices=range(1, 3601), default=30
    )
    catalog_repair_parser = subcommands.add_parser(
        "plan-catalog-repairs",
        help="print a read-only catalog publication repair plan",
    )
    catalog_repair_parser.add_argument(
        "--allowlist",
        help="comma-separated movie codes to inspect",
    )
    catalog_repair_parser.add_argument("--limit", type=int, default=100)
    reconciliation = subcommands.add_parser(
        "reconcile-catalog-sync-failures",
        help="verify Supabase artifacts and repair false catalog-sync failures",
    )
    reconciliation.add_argument(
        "--movie",
        action="append",
        dest="movie_codes",
        help="movie code to inspect; repeat for an explicit allowlist",
    )
    reconciliation.add_argument("--limit", type=int, default=100)
    reconciliation.add_argument("--execute", action="store_true")
    reconciliation.add_argument("--retry-catalog-sync", action="store_true")
    reconciliation.add_argument("--resend-ready-webhook", action="store_true")
    audit_parser = subcommands.add_parser(
        "audit-english-ai-local",
        help="GET-only local audit of exact English_AI catalog subtitles",
    )
    audit_parser.add_argument("--output", type=Path, required=True)
    audit_parser.add_argument("--limit", type=int)
    audit_parser.add_argument("--workers", type=int, choices=range(1, 5), default=4)
    audit_parser.add_argument(
        "--requests-per-second",
        type=float,
        default=2.0,
    )
    catalog_visibility_audit = subcommands.add_parser(
        "catalog-visibility-audit",
        help="GET-only audit of exact public catalog subtitle visibility",
    )
    catalog_visibility_audit.add_argument("--output", type=Path, required=True)
    catalog_visibility_audit.add_argument(
        "--allowlist",
        nargs="+",
        type=_catalog_movie_code,
        metavar="CODE",
    )
    catalog_visibility_audit.add_argument("--limit", type=_positive_int)
    catalog_visibility_repair = subcommands.add_parser(
        "catalog-visibility-repair",
        help="plan or execute digest-authorized catalog-only repairs",
    )
    catalog_visibility_repair.add_argument("--report", type=Path, required=True)
    catalog_visibility_repair.add_argument("--output", type=Path, required=True)
    catalog_visibility_repair.add_argument("--execute", action="store_true")
    catalog_visibility_repair.add_argument(
        "--confirm-report-sha256",
        type=_lowercase_sha256,
    )
    selector = subcommands.add_parser("select-historical-repair-canary")
    selector.add_argument("--allowlist-file", type=Path, required=True)
    selector.add_argument("--preferred-movie")
    selector.add_argument("--output", type=Path, required=True)
    prepare = subcommands.add_parser("prepare-historical-repair-canary")
    prepare.add_argument("--allowlist-file", type=Path, required=True)
    prepare.add_argument("--movie", required=True)
    prepare.add_argument("--limit", type=int, required=True)
    prepare.add_argument("--confirm-job-id", required=True)
    catalog_prepare = subcommands.add_parser(
        "prepare-catalog-publication-canary"
    )
    catalog_prepare.add_argument("--allowlist-file", type=Path, required=True)
    catalog_prepare.add_argument("--movie", required=True)
    catalog_prepare.add_argument("--limit", type=int, required=True)
    catalog_prepare.add_argument("--confirm-job-id", required=True)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    configure_logging()

    if args.command == "api":
        run_api()
    elif args.command == "mac-worker":
        run_mac_worker()
    elif args.command == "mac-translation-smoke-test":
        run_mac_translation_smoke_test()
    elif args.command == "mac-translation-worker":
        run_mac_translation_worker()
    elif args.command == "mac-translation-worker-once":
        run_mac_translation_worker_once(args.job_id)
    elif args.command == "windows-worker":
        run_windows_worker()
    elif args.command == "recover-interrupted-audio":
        run_recover_interrupted_audio(
            job_id=args.job_id,
            movie=args.movie,
            expected_sha256=args.expected_sha256,
        )
    elif args.command == "repair-interrupted-audio-wav":
        run_repair_interrupted_audio_wav(
            job_id=args.job_id,
            movie=args.movie,
            expected_sha256=args.expected_sha256,
        )
    elif args.command == "plan-historical-subtitle-repair":
        run_plan_historical_repairs(
            allowlist=set(args.allowlist) if args.allowlist else None,
            limit=args.limit,
        )
    elif args.command == "plan-historical-repair-batch":
        run_plan_historical_repair_batch(
            allowlist_file=args.allowlist_file,
            limit=args.limit,
            output=args.output,
        )
    elif args.command == "enqueue-historical-repair-batch":
        run_enqueue_historical_repair_batch(
            allowlist_file=args.allowlist_file,
            plan_file=args.plan_file,
            confirm_plan_sha256=args.confirm_plan_sha256,
        )
    elif args.command == "plan-translation-only-repair-batch":
        run_plan_translation_only_repair_batch(
            allowlist_file=args.allowlist_file,
            limit=args.limit,
            output=args.output,
        )
    elif args.command == "enqueue-translation-only-repair-batch":
        run_enqueue_translation_only_repair_batch(
            plan_file=args.plan_file,
            confirm_plan_sha256=args.confirm_plan_sha256,
        )
    elif args.command == "run-translation-only-repair-supervisor":
        run_translation_only_repair_supervisor(
            allowlist_file=args.allowlist_file,
            work_dir=args.work_dir,
            batch_size=args.batch_size,
            max_jobs=args.max_jobs,
            execute=args.execute,
            confirm_remaining_count=args.confirm_remaining_count,
            verify_public_api=args.verify_public_api,
            poll_interval_seconds=args.poll_interval_seconds,
            batch_timeout_seconds=args.batch_timeout_seconds,
        )
    elif args.command == "historical-repair-controller":
        raise SystemExit(
            run_historical_repair_controller(
                allowlist_file=args.allowlist_file,
                initial_batch_size=args.initial_batch_size,
                batch_size=args.batch_size,
                poll_interval_seconds=args.poll_interval_seconds,
            )
        )
    elif args.command == "plan-catalog-repairs":
        run_plan_catalog_repairs(
            allowlist=_parse_allowlist(args.allowlist),
            limit=args.limit,
        )
    elif args.command == "reconcile-catalog-sync-failures":
        run_catalog_sync_reconciliation(
            movie_codes=args.movie_codes,
            limit=args.limit,
            execute=args.execute,
            retry_catalog_sync=args.retry_catalog_sync,
            resend_ready_webhook=args.resend_ready_webhook,
        )
    elif args.command == "audit-english-ai-local":
        run_local_english_ai_audit(
            output=args.output,
            limit=args.limit,
            workers=args.workers,
            requests_per_second=args.requests_per_second,
        )
    elif args.command == "catalog-visibility-audit":
        run_catalog_visibility_audit(
            output=args.output,
            allowlist=args.allowlist,
            limit=args.limit,
        )
    elif args.command == "catalog-visibility-repair":
        raise SystemExit(
            run_catalog_visibility_repair(
                report=args.report,
                output=args.output,
                execute=args.execute,
                confirm_report_sha256=args.confirm_report_sha256,
            )
        )
    elif args.command == "select-historical-repair-canary":
        run_select_historical_repair_canary(
            allowlist_file=args.allowlist_file,
            preferred_movie=args.preferred_movie,
            output=args.output,
        )
    elif args.command == "prepare-historical-repair-canary":
        run_prepare_historical_repair_canary(
            allowlist_file=args.allowlist_file,
            movie=args.movie,
            limit=args.limit,
            confirm_job_id=args.confirm_job_id,
        )
    elif args.command == "prepare-catalog-publication-canary":
        run_prepare_catalog_publication_canary(
            allowlist_file=args.allowlist_file,
            movie=args.movie,
            limit=args.limit,
            confirm_job_id=args.confirm_job_id,
        )


if __name__ == "__main__":
    main()

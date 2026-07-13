import argparse
import json
import logging
import os
import subprocess
from pathlib import Path

from orchestrator.logging_config import configure_logging


LOGGER = logging.getLogger(__name__)


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


def build_catalog_sync_client(settings):
    if not settings.mac_translation_publish_enabled:
        return None
    if not settings.javsubtitle_api_base or not settings.javsubtitle_admin_api_token:
        raise RuntimeError(
            "Supabase publication and catalog sync is enabled but website API base "
            "or admin token is missing"
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
    )
    uvicorn.run(app, host=settings.host, port=settings.port)


def run_mac_worker() -> None:
    from orchestrator.config import MacSettings
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
    )
    run_mac_forever(worker)


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
    from orchestrator.config import MacSettings
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
    )
    run_translation_forever(worker, settings.mac_translation_poll_interval_seconds)


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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m orchestrator")
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
    catalog_repair_parser = subcommands.add_parser(
        "plan-catalog-repairs",
        help="print a read-only catalog publication repair plan",
    )
    catalog_repair_parser.add_argument(
        "--allowlist",
        help="comma-separated movie codes to inspect",
    )
    catalog_repair_parser.add_argument("--limit", type=int, default=100)
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
    elif args.command == "plan-catalog-repairs":
        run_plan_catalog_repairs(
            allowlist=_parse_allowlist(args.allowlist),
            limit=args.limit,
        )
    elif args.command == "audit-english-ai-local":
        run_local_english_ai_audit(
            output=args.output,
            limit=args.limit,
            workers=args.workers,
            requests_per_second=args.requests_per_second,
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

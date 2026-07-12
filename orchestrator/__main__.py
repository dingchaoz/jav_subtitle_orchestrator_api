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
    )
    run_translation_forever(worker, settings.mac_translation_poll_interval_seconds)


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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m orchestrator")
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser("api")
    subcommands.add_parser("mac-worker")
    subcommands.add_parser("mac-translation-smoke-test")
    subcommands.add_parser("mac-translation-worker")
    subcommands.add_parser("windows-worker")
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
    elif args.command == "windows-worker":
        run_windows_worker()
    elif args.command == "plan-historical-subtitle-repair":
        run_plan_historical_repairs(
            allowlist=set(args.allowlist) if args.allowlist else None,
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


if __name__ == "__main__":
    main()

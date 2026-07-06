import argparse
import os
from pathlib import Path

from orchestrator.logging_config import configure_logging


def build_supabase_publisher(settings):
    if not settings.publish_to_supabase:
        return None
    if not settings.supabase_url or not settings.supabase_service_role_key:
        raise RuntimeError(
            "PUBLISH_TO_SUPABASE requires SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY"
        )

    from orchestrator.supabase_publisher import SupabaseSubtitlePublisher

    catalog_sync = None
    if settings.javsubtitle_post_sync_enabled:
        if not settings.javsubtitle_admin_api_token:
            raise RuntimeError(
                "JAVSUBTITLE_POST_SYNC_ENABLED requires JAVSUBTITLE_ADMIN_API_TOKEN"
            )
        from orchestrator.catalog_sync import CatalogSyncClient

        catalog_sync = CatalogSyncClient(
            settings.javsubtitle_api_base,
            settings.javsubtitle_admin_api_token,
        )

    return SupabaseSubtitlePublisher(
        settings.supabase_url,
        settings.supabase_service_role_key,
        bucket=settings.supabase_storage_bucket,
        catalog_sync=catalog_sync,
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
        publisher=build_supabase_publisher(settings),
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
    os.environ["OPENAI_API_KEY"] = settings.openai_api_key


def run_windows_worker() -> None:
    from orchestrator.config import WindowsSettings
    from orchestrator.transcription import FasterWhisperTranscriber
    from orchestrator.translation import SubtitleTranslator
    from orchestrator.windows_worker import (
        MacApiClient,
        WindowsWorker,
        run_forever as run_windows_forever,
    )

    settings = WindowsSettings()
    _export_windows_runtime_env(settings)
    client = MacApiClient(settings.mac_api_base_url, settings.worker_id)
    transcriber = FasterWhisperTranscriber(
        settings.whisper_model,
        settings.whisper_device,
        settings.whisper_compute_type,
    )
    translator = SubtitleTranslator(settings.translate_script_path)
    worker = WindowsWorker(
        client,
        transcriber,
        translator,
        heartbeat_interval_seconds=settings.heartbeat_interval_seconds,
    )
    run_windows_forever(worker, settings.poll_interval_seconds)


def run_publish_job(movie_number: str) -> None:
    from orchestrator.config import MacSettings
    from orchestrator.supabase_publisher import (
        SupabaseSubtitlePublisher,
        canonical_movie_code,
    )

    settings = MacSettings()
    if not settings.supabase_url or not settings.supabase_service_role_key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required")
    canonical = canonical_movie_code(movie_number)
    srt_path = settings.jobs_root_mac / canonical / f"{canonical}.English.srt"
    publisher = SupabaseSubtitlePublisher(
        settings.supabase_url,
        settings.supabase_service_role_key,
        bucket=settings.supabase_storage_bucket,
    )
    result = publisher.publish_english_ai(canonical, Path(srt_path))
    print(f"published {result.movie_code} {result.language} -> {result.storage_path}")


def main() -> None:
    parser = argparse.ArgumentParser(prog="python -m orchestrator")
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser("api")
    subcommands.add_parser("mac-worker")
    subcommands.add_parser("windows-worker")
    publish_parser = subcommands.add_parser("publish-job")
    publish_parser.add_argument("movie_number")
    args = parser.parse_args()

    configure_logging()

    if args.command == "api":
        run_api()
    elif args.command == "mac-worker":
        run_mac_worker()
    elif args.command == "windows-worker":
        run_windows_worker()
    elif args.command == "publish-job":
        run_publish_job(args.movie_number)


if __name__ == "__main__":
    main()

import argparse

from orchestrator.logging_config import configure_logging


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


def main() -> None:
    parser = argparse.ArgumentParser(prog="python -m orchestrator")
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser("api")
    subcommands.add_parser("mac-worker")
    subcommands.add_parser("windows-worker")
    args = parser.parse_args()

    configure_logging()

    if args.command == "api":
        run_api()
    elif args.command == "mac-worker":
        run_mac_worker()
    elif args.command == "windows-worker":
        run_windows_worker()


if __name__ == "__main__":
    main()

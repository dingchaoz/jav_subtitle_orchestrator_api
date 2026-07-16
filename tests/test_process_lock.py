import fcntl
import os
import plistlib
import sys
from pathlib import Path
from types import ModuleType

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PRODUCTION_ROOT = Path("/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator")


def load_plist(relative_path: str) -> dict[str, object]:
    with (PROJECT_ROOT / relative_path).open("rb") as handle:
        return plistlib.load(handle)


def module_with(**attributes: object) -> ModuleType:
    module = ModuleType("test_double")
    for name, value in attributes.items():
        setattr(module, name, value)
    return module


def recording_lock_type(events: list[object], *, release_error: BaseException | None = None):
    class RecordingLock:
        def __init__(self, path):
            events.append(("lock_path", path))

        def acquire(self):
            events.append("lock_acquire")
            return self

        def release(self):
            events.append("lock_release")
            if release_error is not None:
                raise release_error

    return RecordingLock


def translation_settings_type(tmp_path: Path, events: list[object] | None = None):
    class FakeTranslationSettings:
        def __init__(self):
            if events is not None:
                events.append("settings")
            self.db_path = tmp_path / "jobs.sqlite3"
            self.jobs_root_mac = tmp_path / "jobs"
            self.jobs_root_windows = "M:\\"
            self.mac_translate_script_path = "translate.py"
            self.mac_translation_worker_id = "mac-translation-stable"
            self.mac_translation_lease_seconds = 1800
            self.max_translation_attempts = 3
            self.translation_quality_failure_limit = 3
            self.max_publish_attempts = 10
            self.mac_publish_retry_seconds = 30
            self.max_catalog_sync_attempts = 10
            self.catalog_sync_retry_seconds = 30
            self.mac_translation_poll_interval_seconds = 10

    return FakeTranslationSettings


class FailingPersistenceHandle:
    def __init__(self, handle, failing_operation: str):
        self.handle = handle
        self.failing_operation = failing_operation

    @property
    def closed(self) -> bool:
        return self.handle.closed

    def fileno(self) -> int:
        return self.handle.fileno()

    def seek(self, offset: int):
        if self.failing_operation == "seek":
            raise OSError("pid seek failed")
        return self.handle.seek(offset)

    def truncate(self):
        if self.failing_operation == "truncate":
            raise OSError("pid truncate failed")
        return self.handle.truncate()

    def write(self, value: str):
        if self.failing_operation == "write":
            raise OSError("pid write failed")
        return self.handle.write(value)

    def flush(self):
        if self.failing_operation == "flush":
            raise OSError("pid flush failed")
        return self.handle.flush()

    def close(self):
        return self.handle.close()


class FailingCloseHandle:
    def __init__(self, error: BaseException):
        self.error = error
        self.close_calls = 0

    def fileno(self) -> int:
        return 123

    def close(self) -> None:
        self.close_calls += 1
        raise self.error


def test_second_lock_for_same_worker_is_rejected(tmp_path):
    from orchestrator.process_lock import AlreadyRunningError, SingleInstanceLock

    first = SingleInstanceLock(tmp_path / "worker.lock").acquire()
    try:
        with pytest.raises(AlreadyRunningError, match="^worker_already_running$"):
            SingleInstanceLock(tmp_path / "worker.lock").acquire()
    finally:
        first.release()


def test_lock_creates_parent_writes_pid_and_release_is_idempotent(tmp_path):
    from orchestrator.process_lock import SingleInstanceLock

    lock_path = tmp_path / "nested" / "worker.lock"
    lock = SingleInstanceLock(lock_path).acquire()

    assert lock_path.read_text(encoding="utf-8") == f"{os.getpid()}\n"

    lock.release()
    lock.release()
    replacement = SingleInstanceLock(lock_path).acquire()
    replacement.release()


@pytest.mark.parametrize("failing_operation", ["seek", "truncate", "write", "flush"])
def test_pid_persistence_failure_releases_lock_and_propagates_original_error(
    tmp_path, monkeypatch, failing_operation
):
    from orchestrator.process_lock import SingleInstanceLock

    lock_path = tmp_path / "worker.lock"
    real_open = Path.open
    wrapped_handles: list[FailingPersistenceHandle] = []

    def fail_first_open(path, *args, **kwargs):
        handle = real_open(path, *args, **kwargs)
        if path == lock_path and not wrapped_handles:
            wrapped = FailingPersistenceHandle(handle, failing_operation)
            wrapped_handles.append(wrapped)
            return wrapped
        return handle

    monkeypatch.setattr(Path, "open", fail_first_open)
    lock = SingleInstanceLock(lock_path)

    with pytest.raises(OSError, match=f"^pid {failing_operation} failed$"):
        lock.acquire()

    assert lock.handle is None
    assert wrapped_handles[0].closed is True
    replacement = SingleInstanceLock(lock_path).acquire()
    replacement.release()


def test_release_closes_and_detaches_handle_when_unlock_fails(tmp_path, monkeypatch):
    from orchestrator.process_lock import SingleInstanceLock

    lock_path = tmp_path / "worker.lock"
    lock = SingleInstanceLock(lock_path).acquire()
    handle = lock.handle
    real_flock = fcntl.flock
    unlock_error = OSError("unlock failed")

    def fail_unlock(fd, operation):
        if operation == fcntl.LOCK_UN:
            raise unlock_error
        return real_flock(fd, operation)

    monkeypatch.setattr(fcntl, "flock", fail_unlock)
    try:
        with pytest.raises(OSError, match="^unlock failed$") as caught:
            lock.release()
    finally:
        monkeypatch.setattr(fcntl, "flock", real_flock)
        if handle is not None and not handle.closed:
            handle.close()

    assert caught.value is unlock_error
    assert lock.handle is None
    assert handle is not None and handle.closed is True
    replacement = SingleInstanceLock(lock_path).acquire()
    replacement.release()


def test_release_detaches_handle_and_does_not_retry_after_close_failure(monkeypatch, tmp_path):
    from orchestrator.process_lock import SingleInstanceLock

    close_error = OSError("close failed")
    handle = FailingCloseHandle(close_error)
    lock = SingleInstanceLock(tmp_path / "worker.lock")
    lock.handle = handle
    monkeypatch.setattr(fcntl, "flock", lambda _fd, _operation: None)

    with pytest.raises(OSError, match="^close failed$") as caught:
        lock.release()

    assert caught.value is close_error
    assert lock.handle is None
    lock.release()
    assert handle.close_calls == 1


def test_release_preserves_unlock_error_when_unlock_and_close_both_fail(
    monkeypatch, tmp_path
):
    from orchestrator.process_lock import SingleInstanceLock

    unlock_error = OSError("unlock failed")
    close_error = OSError("close failed")
    handle = FailingCloseHandle(close_error)
    lock = SingleInstanceLock(tmp_path / "worker.lock")
    lock.handle = handle

    def fail_unlock(_fd, _operation):
        raise unlock_error

    monkeypatch.setattr(fcntl, "flock", fail_unlock)

    with pytest.raises(OSError, match="^unlock failed$") as caught:
        lock.release()

    assert caught.value is unlock_error
    assert lock.handle is None
    assert handle.close_calls == 1


def test_different_worker_locks_can_be_held_and_reacquired_together(tmp_path):
    from orchestrator.process_lock import SingleInstanceLock

    downloader_path = tmp_path / "mac-worker.lock"
    translator_path = tmp_path / "mac-translation-worker.lock"
    downloader = SingleInstanceLock(downloader_path).acquire()
    translator = SingleInstanceLock(translator_path).acquire()
    try:
        assert downloader_path.read_text(encoding="utf-8") == f"{os.getpid()}\n"
        assert translator_path.read_text(encoding="utf-8") == f"{os.getpid()}\n"
    finally:
        downloader.release()
        translator.release()

    replacement_downloader = SingleInstanceLock(downloader_path).acquire()
    replacement_translator = SingleInstanceLock(translator_path).acquire()
    try:
        assert replacement_downloader.handle is not None
        assert replacement_translator.handle is not None
    finally:
        replacement_downloader.release()
        replacement_translator.release()


def test_launchd_plists_keep_workers_separate():
    downloader = load_plist(
        "deployment/launchd/com.javsubtitle.mac-worker.plist"
    )
    translator = load_plist(
        "deployment/launchd/com.javsubtitle.mac-translation-worker.plist"
    )

    assert downloader["ProgramArguments"][-1] == "mac-worker"
    assert translator["ProgramArguments"][-1] == "mac-translation-worker"
    assert downloader["Label"] != translator["Label"]
    assert downloader["KeepAlive"] is True
    assert translator["KeepAlive"] is True


@pytest.mark.parametrize(
    ("relative_path", "label", "command", "log_prefix"),
    [
        (
            "deployment/launchd/com.javsubtitle.mac-worker.plist",
            "com.javsubtitle.mac-worker",
            "mac-worker",
            "mac-worker",
        ),
        (
            "deployment/launchd/com.javsubtitle.mac-translation-worker.plist",
            "com.javsubtitle.mac-translation-worker",
            "mac-translation-worker",
            "mac-translation-worker",
        ),
    ],
)
def test_launchd_plist_uses_bounded_production_runtime(
    relative_path, label, command, log_prefix
):
    plist = load_plist(relative_path)

    assert plist["Label"] == label
    assert plist["ProgramArguments"] == [
        str(PRODUCTION_ROOT / ".venv" / "bin" / "python"),
        "-m",
        "orchestrator",
        command,
    ]
    assert plist["WorkingDirectory"] == str(PRODUCTION_ROOT)
    assert plist["StandardOutPath"] == str(
        PRODUCTION_ROOT / "logs" / f"{log_prefix}.stdout.log"
    )
    assert plist["StandardErrorPath"] == str(
        PRODUCTION_ROOT / "logs" / f"{log_prefix}.stderr.log"
    )
    assert plist["RunAtLoad"] is True
    assert plist["KeepAlive"] is True
    assert plist["ThrottleInterval"] == 10
    if command == "mac-worker":
        assert plist["EnvironmentVariables"]["PATH"].split(":")[0] == (
            "/opt/homebrew/bin"
        )
    else:
        assert "EnvironmentVariables" not in plist


def test_mac_downloader_worker_id_is_stable_and_configurable(monkeypatch):
    from orchestrator.config import MacSettings

    monkeypatch.delenv("MAC_DOWNLOAD_WORKER_ID", raising=False)
    defaults = MacSettings(_env_file=None)
    assert defaults.mac_download_worker_id == "mac-downloader-1"

    monkeypatch.setenv("MAC_DOWNLOAD_WORKER_ID", "mac-downloader-stable")
    configured = MacSettings(_env_file=None)
    assert configured.mac_download_worker_id == "mac-downloader-stable"


@pytest.mark.parametrize("preserve_worker_error", [False, True])
def test_release_worker_lock_cleanup_error_semantics(
    caplog,
    preserve_worker_error,
):
    from orchestrator import __main__ as cli

    cleanup_error = RuntimeError("lock cleanup failed")

    class FailingLock:
        def release(self):
            raise cleanup_error

    if not preserve_worker_error:
        with pytest.raises(RuntimeError, match="^lock cleanup failed$") as caught:
            cli._release_worker_lock(
                FailingLock(),
                preserve_worker_error=False,
            )
        assert caught.value is cleanup_error
        return

    caplog.set_level("ERROR", logger=cli.LOGGER.name)
    preserved_worker_error = None
    try:
        raise RuntimeError("worker failed")
    except RuntimeError as worker_error:
        preserved_worker_error = worker_error
        cli._release_worker_lock(
            FailingLock(),
            preserve_worker_error=True,
        )

    assert str(preserved_worker_error) == "worker failed"
    cleanup_records = [
        record
        for record in caplog.records
        if record.getMessage()
        == "worker lock cleanup failed while preserving worker error"
    ]
    assert len(cleanup_records) == 1
    assert cleanup_records[0].exc_info is not None
    assert cleanup_records[0].exc_info[1] is cleanup_error


def test_downloader_runtime_holds_its_lock_and_uses_configured_worker_id(
    monkeypatch, tmp_path
):
    from orchestrator import __main__ as cli

    events: list[object] = []
    FakeLock = recording_lock_type(events)

    class FakeSettings:
        def __init__(self):
            events.append("settings")
            self.db_path = tmp_path / "jobs.sqlite3"
            self.jobs_root_mac = tmp_path / "jobs"
            self.jobs_root_windows = "M:\\"
            self.missav_pipeline_root = tmp_path / "missav"
            self.max_download_attempts = 3
            self.mac_download_worker_id = "configured-downloader"

    class FakeStore:
        def __init__(self, *_args):
            events.append("store")

        def initialize(self):
            events.append("store_initialize")

    class FakeAdapter:
        def __init__(self, _root):
            events.append("adapter")

    class FakeWorker:
        def __init__(self, _store, _adapter, _attempts, *, worker_id):
            events.append(("worker_id", worker_id))

    def fake_run_forever(_worker):
        events.append("run_forever")

    monkeypatch.setitem(
        sys.modules,
        "orchestrator.config",
        module_with(MacSettings=FakeSettings, PROJECT_ROOT=tmp_path),
    )
    monkeypatch.setitem(
        sys.modules,
        "orchestrator.process_lock",
        module_with(SingleInstanceLock=FakeLock),
    )
    monkeypatch.setitem(
        sys.modules,
        "orchestrator.mac_worker",
        module_with(MacDownloadWorker=FakeWorker, run_forever=fake_run_forever),
    )
    monkeypatch.setitem(
        sys.modules,
        "orchestrator.missav_adapter",
        module_with(MissAVAdapter=FakeAdapter),
    )
    monkeypatch.setitem(
        sys.modules,
        "orchestrator.store",
        module_with(JobStore=FakeStore),
    )

    cli.run_mac_worker()

    assert events[0] == ("lock_path", tmp_path / "data" / "mac-worker.lock")
    assert events.index("lock_acquire") < events.index("settings")
    assert ("worker_id", "configured-downloader") in events
    assert events[-2:] == ["run_forever", "lock_release"]


def test_downloader_runtime_releases_lock_when_store_initialization_fails(
    monkeypatch, tmp_path
):
    from orchestrator import __main__ as cli

    events: list[object] = []
    FakeLock = recording_lock_type(events)

    class FakeSettings:
        def __init__(self):
            events.append("settings")
            self.db_path = tmp_path / "jobs.sqlite3"
            self.jobs_root_mac = tmp_path / "jobs"
            self.jobs_root_windows = "M:\\"

    class FailingStore:
        def __init__(self, *_args):
            events.append("store")

        def initialize(self):
            events.append("store_initialize")
            raise RuntimeError("store initialization failed")

    monkeypatch.setitem(
        sys.modules,
        "orchestrator.config",
        module_with(MacSettings=FakeSettings, PROJECT_ROOT=tmp_path),
    )
    monkeypatch.setitem(
        sys.modules,
        "orchestrator.process_lock",
        module_with(SingleInstanceLock=FakeLock),
    )
    monkeypatch.setitem(
        sys.modules,
        "orchestrator.mac_worker",
        module_with(MacDownloadWorker=object, run_forever=lambda _worker: None),
    )
    monkeypatch.setitem(
        sys.modules,
        "orchestrator.missav_adapter",
        module_with(MissAVAdapter=object),
    )
    monkeypatch.setitem(
        sys.modules,
        "orchestrator.store",
        module_with(JobStore=FailingStore),
    )

    with pytest.raises(RuntimeError, match="^store initialization failed$"):
        cli.run_mac_worker()

    assert events == [
        ("lock_path", tmp_path / "data" / "mac-worker.lock"),
        "lock_acquire",
        "settings",
        "store",
        "store_initialize",
        "lock_release",
    ]


def test_translation_runtime_holds_a_distinct_lock_before_smoke_or_store(
    monkeypatch, tmp_path
):
    from orchestrator import __main__ as cli

    events: list[object] = []
    FakeLock = recording_lock_type(events)
    FakeSettings = translation_settings_type(tmp_path, events)

    class FakeStore:
        def __init__(self, *_args):
            events.append("store")

        def initialize(self):
            events.append("store_initialize")

    class FakeTranslator:
        def __init__(self, _script):
            events.append("translator")

    class FakeWorker:
        def __init__(self, _store, _translator, **kwargs):
            events.append(("worker_id", kwargs["worker_id"]))

    def fake_run_forever(_worker, _poll_interval):
        events.append("run_forever")

    monkeypatch.setattr(
        cli,
        "build_supabase_publisher",
        lambda _settings: events.append("publisher") or object(),
    )
    monkeypatch.setattr(
        cli,
        "build_catalog_sync_client",
        lambda _settings: events.append("catalog_sync") or object(),
    )
    monkeypatch.setattr(
        cli,
        "_export_mac_translation_runtime_env",
        lambda _settings: events.append("export_env"),
    )
    monkeypatch.setattr(
        cli,
        "_run_mac_translation_smoke",
        lambda _settings, _translator: events.append("smoke"),
    )
    monkeypatch.setitem(
        sys.modules,
        "orchestrator.config",
        module_with(MacSettings=FakeSettings, PROJECT_ROOT=tmp_path),
    )
    monkeypatch.setitem(
        sys.modules,
        "orchestrator.process_lock",
        module_with(SingleInstanceLock=FakeLock),
    )
    monkeypatch.setitem(
        sys.modules,
        "orchestrator.mac_worker",
        module_with(
            MacTranslationWorker=FakeWorker,
            run_translation_forever=fake_run_forever,
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        "orchestrator.store",
        module_with(JobStore=FakeStore),
    )
    monkeypatch.setitem(
        sys.modules,
        "orchestrator.translation",
        module_with(SubtitleTranslator=FakeTranslator),
    )

    cli.run_mac_translation_worker()

    assert events[0] == (
        "lock_path",
        tmp_path / "data" / "mac-translation-worker.lock",
    )
    assert events.index("lock_acquire") < events.index("settings")
    assert events.index("lock_acquire") < events.index("smoke")
    assert events.index("lock_acquire") < events.index("store_initialize")
    assert ("worker_id", "mac-translation-stable") in events
    assert events[-2:] == ["run_forever", "lock_release"]


def test_translation_runtime_preserves_worker_error_when_lock_release_also_fails(
    monkeypatch, tmp_path
):
    from orchestrator import __main__ as cli

    events: list[object] = []
    FakeLock = recording_lock_type(
        events,
        release_error=RuntimeError("lock release failed"),
    )
    FakeSettings = translation_settings_type(tmp_path, events)

    class FakeStore:
        def __init__(self, *_args):
            events.append("store")

        def initialize(self):
            events.append("store_initialize")

    class FakeTranslator:
        def __init__(self, _script):
            events.append("translator")

    class FakeWorker:
        def __init__(self, _store, _translator, **_kwargs):
            events.append("worker")

    def failing_run_forever(_worker, _poll_interval):
        events.append("run_forever")
        raise RuntimeError("translation loop failed")

    monkeypatch.setattr(cli, "build_supabase_publisher", lambda _settings: object())
    monkeypatch.setattr(cli, "build_catalog_sync_client", lambda _settings: object())
    monkeypatch.setattr(cli, "_export_mac_translation_runtime_env", lambda _settings: None)
    monkeypatch.setattr(cli, "_run_mac_translation_smoke", lambda _settings, _translator: None)
    monkeypatch.setitem(
        sys.modules,
        "orchestrator.config",
        module_with(MacSettings=FakeSettings, PROJECT_ROOT=tmp_path),
    )
    monkeypatch.setitem(
        sys.modules,
        "orchestrator.process_lock",
        module_with(SingleInstanceLock=FakeLock),
    )
    monkeypatch.setitem(
        sys.modules,
        "orchestrator.mac_worker",
        module_with(
            MacTranslationWorker=FakeWorker,
            run_translation_forever=failing_run_forever,
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        "orchestrator.store",
        module_with(JobStore=FakeStore),
    )
    monkeypatch.setitem(
        sys.modules,
        "orchestrator.translation",
        module_with(SubtitleTranslator=FakeTranslator),
    )

    with pytest.raises(RuntimeError, match="^translation loop failed$"):
        cli.run_mac_translation_worker()

    assert events.count("lock_release") == 1
    assert events[-2:] == ["run_forever", "lock_release"]


def test_one_shot_cli_dispatcher_does_not_import_process_lock(monkeypatch):
    from orchestrator import __main__ as cli

    events: list[str] = []

    class ExplodingProcessLockModule(ModuleType):
        def __getattr__(self, name):
            raise AssertionError(f"dispatcher imported process_lock.{name}")

    exploding_process_lock = ExplodingProcessLockModule("orchestrator.process_lock")
    monkeypatch.setitem(
        sys.modules,
        "orchestrator.process_lock",
        exploding_process_lock,
    )
    import orchestrator as orchestrator_package

    monkeypatch.setattr(
        orchestrator_package,
        "process_lock",
        exploding_process_lock,
        raising=False,
    )
    monkeypatch.setattr(
        cli,
        "configure_logging",
        lambda: events.append("configure_logging"),
    )
    monkeypatch.setattr(
        cli,
        "run_mac_translation_worker_once",
        lambda job_id: events.append(f"run_once:{job_id}"),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "orchestrator",
            "mac-translation-worker-once",
            "--job-id",
            "job-safe",
        ],
    )

    cli.main()

    assert events == ["configure_logging", "run_once:job-safe"]


def test_launchd_installer_is_scoped_to_exact_worker_services():
    script = (
        PROJECT_ROOT / "scripts" / "install_mac_worker_launchd.sh"
    ).read_text(encoding="utf-8")

    downloader = "com.javsubtitle.mac-worker"
    translator = "com.javsubtitle.mac-translation-worker"
    assert "set -euo pipefail" in script
    assert f"{downloader}.plist" in script
    assert f"{translator}.plist" in script
    assert f'plutil -lint -s "$SOURCE_DOWNLOADER"' in script
    assert f'plutil -lint -s "$SOURCE_TRANSLATOR"' in script
    assert 'plutil -lint -s "$SOURCE_DOWNLOADER" >/dev/null' not in script
    assert 'plutil -lint -s "$SOURCE_TRANSLATOR" >/dev/null' not in script
    assert 'launchctl bootout "$DOMAIN/$DOWNLOADER_LABEL"' in script
    assert 'launchctl bootout "$DOMAIN/$TRANSLATOR_LABEL"' in script
    assert 'launchctl bootstrap "$DOMAIN" "$DEST_DOWNLOADER"' in script
    assert 'launchctl bootstrap "$DOMAIN" "$DEST_TRANSLATOR"' in script
    assert 'launchctl print "$DOMAIN/$DOWNLOADER_LABEL"' in script
    assert 'launchctl print "$DOMAIN/$TRANSLATOR_LABEL"' in script
    assert "*.plist" not in script
    assert ".env" not in script
    assert "token" not in script.lower()
    assert "api" not in script.lower()


def test_launchd_installer_completes_downloader_before_stopping_translator():
    script = (
        PROJECT_ROOT / "scripts" / "install_mac_worker_launchd.sh"
    ).read_text(encoding="utf-8")

    downloader_bootout = script.index(
        'launchctl bootout "$DOMAIN/$DOWNLOADER_LABEL"'
    )
    downloader_bootstrap = script.index(
        'launchctl bootstrap "$DOMAIN" "$DEST_DOWNLOADER"'
    )
    translator_bootout = script.index(
        'launchctl bootout "$DOMAIN/$TRANSLATOR_LABEL"'
    )
    translator_bootstrap = script.index(
        'launchctl bootstrap "$DOMAIN" "$DEST_TRANSLATOR"'
    )
    assert (
        downloader_bootout
        < downloader_bootstrap
        < translator_bootout
        < translator_bootstrap
    )

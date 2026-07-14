import inspect
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
    assert "EnvironmentVariables" not in plist


def test_mac_downloader_worker_id_is_stable_and_configurable(monkeypatch):
    from orchestrator.config import MacSettings

    monkeypatch.delenv("MAC_DOWNLOAD_WORKER_ID", raising=False)
    defaults = MacSettings(_env_file=None)
    assert defaults.mac_download_worker_id == "mac-downloader-1"

    monkeypatch.setenv("MAC_DOWNLOAD_WORKER_ID", "mac-downloader-stable")
    configured = MacSettings(_env_file=None)
    assert configured.mac_download_worker_id == "mac-downloader-stable"


def test_downloader_runtime_holds_its_lock_and_uses_configured_worker_id(
    monkeypatch, tmp_path
):
    from orchestrator import __main__ as cli

    events: list[object] = []

    class FakeLock:
        def __init__(self, path):
            events.append(("lock_path", path))

        def acquire(self):
            events.append("lock_acquire")
            return self

        def release(self):
            events.append("lock_release")

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


def test_translation_runtime_holds_a_distinct_lock_before_smoke_or_store(
    monkeypatch, tmp_path
):
    from orchestrator import __main__ as cli

    events: list[object] = []

    class FakeLock:
        def __init__(self, path):
            events.append(("lock_path", path))

        def acquire(self):
            events.append("lock_acquire")
            return self

        def release(self):
            events.append("lock_release")

    class FakeSettings:
        def __init__(self):
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


def test_one_shot_translation_entrypoint_does_not_take_persistent_lock():
    from orchestrator.__main__ import run_mac_translation_worker_once

    source = inspect.getsource(run_mac_translation_worker_once)

    assert "SingleInstanceLock" not in source
    assert "mac-translation-worker.lock" not in source


def test_launchd_installer_is_scoped_to_exact_worker_services():
    script = (
        PROJECT_ROOT / "scripts" / "install_mac_worker_launchd.sh"
    ).read_text(encoding="utf-8")

    downloader = "com.javsubtitle.mac-worker"
    translator = "com.javsubtitle.mac-translation-worker"
    assert "set -euo pipefail" in script
    assert f"{downloader}.plist" in script
    assert f"{translator}.plist" in script
    assert f'plutil -lint "$SOURCE_DOWNLOADER"' in script
    assert f'plutil -lint "$SOURCE_TRANSLATOR"' in script
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

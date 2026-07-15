from __future__ import annotations

import shlex
from pathlib import Path
from types import SimpleNamespace

import pytest


SHA256 = "a" * 64
OTHER_SHA256 = "b" * 64
API_ORIGIN = "https://javsubtitle.example"


def _audit_argv(tmp_path: Path, *extra: str) -> list[str]:
    return [
        "catalog-visibility-audit",
        "--output",
        str(tmp_path / "audit"),
        *extra,
    ]


def _repair_argv(tmp_path: Path, *extra: str) -> list[str]:
    return [
        "catalog-visibility-repair",
        "--report",
        str(tmp_path / "audit-report.json"),
        "--output",
        str(tmp_path / "repair"),
        *extra,
    ]


def test_parser_exposes_catalog_visibility_commands_and_normalizes_allowlist(
    tmp_path: Path,
):
    from orchestrator.__main__ import build_parser

    parser = build_parser()
    audit = parser.parse_args(
        _audit_argv(
            tmp_path,
            "--allowlist",
            "KTB111",
            "IENE-963",
            "--limit",
            "2",
        )
    )
    repair = parser.parse_args(_repair_argv(tmp_path))

    assert audit.command == "catalog-visibility-audit"
    assert audit.output == tmp_path / "audit"
    assert audit.allowlist == ("iene-963", "ktb-111")
    assert audit.limit == 2
    assert repair.command == "catalog-visibility-repair"
    assert repair.report == tmp_path / "audit-report.json"
    assert repair.output == tmp_path / "repair"
    assert repair.execute is False
    assert repair.confirm_report_sha256 is None


@pytest.mark.parametrize("value", ["0", "-1", "1.5", "true", "+1", "01", "1_0"])
def test_parser_requires_exact_positive_audit_limit(tmp_path: Path, value: str):
    from orchestrator.__main__ import build_parser

    with pytest.raises(SystemExit):
        build_parser().parse_args(_audit_argv(tmp_path, "--limit", value))


@pytest.mark.parametrize(
    "allowlist",
    [
        ("bad/code",),
        ("KTB111", "ktb-111"),
    ],
)
def test_parser_rejects_invalid_or_duplicate_normalized_allowlist(
    tmp_path: Path,
    allowlist: tuple[str, ...],
):
    from orchestrator.__main__ import build_parser

    with pytest.raises(SystemExit):
        build_parser().parse_args(
            _audit_argv(tmp_path, "--allowlist", *allowlist)
        )


def test_audit_parser_has_no_mutation_or_admin_token_surface(tmp_path: Path):
    from orchestrator.__main__ import build_parser

    args = build_parser().parse_args(_audit_argv(tmp_path))

    for forbidden in (
        "execute",
        "confirm_report_sha256",
        "admin_token",
        "force",
        "post",
        "sync",
    ):
        assert not hasattr(args, forbidden)


@pytest.mark.parametrize(
    "extra",
    [
        ("--execute",),
        ("--execute", "--confirm-report-sha256", "A" * 64),
        ("--execute", "--confirm-report-sha256", "short"),
        ("--confirm-report-sha256", SHA256),
    ],
)
def test_repair_parser_enforces_execute_confirmation_contract(
    tmp_path: Path,
    extra: tuple[str, ...],
):
    from orchestrator.__main__ import build_parser

    with pytest.raises(SystemExit):
        build_parser().parse_args(_repair_argv(tmp_path, *extra))


def test_repair_parser_accepts_exact_execute_confirmation(tmp_path: Path):
    from orchestrator.__main__ import build_parser

    args = build_parser().parse_args(
        _repair_argv(
            tmp_path,
            "--execute",
            "--confirm-report-sha256",
            SHA256,
        )
    )

    assert args.execute is True
    assert args.confirm_report_sha256 == SHA256


def test_audit_runner_uses_get_only_client_and_prints_exact_safe_line(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
):
    import orchestrator.catalog_visibility as visibility
    import orchestrator.config as config
    import orchestrator.store as store_module
    from orchestrator.__main__ import run_catalog_visibility_audit

    events: list[object] = []

    class Settings:
        db_path = tmp_path / "jobs.sqlite3"
        jobs_root_mac = tmp_path / "jobs"
        jobs_root_windows = "M:\\"
        javsubtitle_api_base = API_ORIGIN
        javsubtitle_admin_api_token = "must-not-be-used"
        subtitle_audit_timeout_seconds = 17

    class Store:
        def __init__(self, *args: object) -> None:
            events.append(("store", args))

        def initialize(self) -> None:
            raise AssertionError("read-only audit must not initialize the database")

    class Session:
        def get(self, *_args: object, **_kwargs: object) -> object:
            events.append("GET")
            return object()

        def post(self, *_args: object, **_kwargs: object) -> object:
            raise AssertionError("audit must not POST")

    class Client:
        def __init__(
            self,
            base_url: str,
            timeout_seconds: int,
            session: object | None = None,
        ) -> None:
            assert session is None
            self.base_url = base_url
            self.session = Session()
            events.append(("client", base_url, timeout_seconds))

    class Auditor:
        def __init__(self, store: object, client: Client) -> None:
            self.client = client

        def scan(self, output: Path, **kwargs: object) -> object:
            self.client.session.get("safe")
            events.append(("scan", output, kwargs))
            return SimpleNamespace(
                discovered=8,
                checked=7,
                counts={
                    "visible": 1,
                    "missing": 2,
                    "not_found": 1,
                    "fetch_failed": 1,
                    "response_invalid": 1,
                    "invalid_receipt": 2,
                },
                report_sha256=SHA256,
                report_path=output / "audit-report.json",
            )

    monkeypatch.setattr(config, "MacSettings", Settings)
    monkeypatch.setattr(store_module, "JobStore", Store)
    monkeypatch.setattr(visibility, "PublicCatalogVisibilityClient", Client)
    monkeypatch.setattr(visibility, "CatalogVisibilityAuditor", Auditor)

    summary = run_catalog_visibility_audit(
        output=tmp_path / "audit",
        allowlist=("ktb-111", "iene-963"),
        limit=8,
    )

    assert summary.discovered == 8
    assert events == [
        ("store", (Settings.db_path, Settings.jobs_root_mac, "M:\\")),
        ("client", API_ORIGIN, 17),
        "GET",
        (
            "scan",
            (tmp_path / "audit").absolute(),
            {"allowlist": {"iene-963", "ktb-111"}, "limit": 8},
        ),
    ]
    assert capsys.readouterr().out == (
        "audit_complete=true discovered=8 checked=7 visible=1 missing=2 "
        "not_found=1 fetch_failed=1 response_invalid=1 invalid_receipt=2 "
        f"report_sha256={SHA256} "
        f"report={(tmp_path / 'audit' / 'audit-report.json').absolute()}\n"
    )


def _install_repair_fakes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    token: str | None = "admin-secret",
    plan_sha256: str = OTHER_SHA256,
    result: object | None = None,
) -> tuple[list[object], type]:
    import orchestrator.catalog_visibility_repair as repair_module
    import orchestrator.config as config
    import orchestrator.store as store_module

    events: list[object] = []
    report_path = tmp_path / "audit-report.json"
    report_path.write_text("{}", encoding="utf-8")

    class Settings:
        db_path = tmp_path / "jobs.sqlite3"
        jobs_root_mac = tmp_path / "jobs"
        jobs_root_windows = "M:\\"
        javsubtitle_api_base = API_ORIGIN
        javsubtitle_admin_api_token = token
        mac_translation_publish_enabled = True

    class Store:
        def __init__(self, *args: object) -> None:
            events.append(("store", args))

        def initialize(self) -> None:
            raise AssertionError("repair planning must not initialize the database")

    def plan(store: object, report: Path, **kwargs: object) -> object:
        output = kwargs["output_dir"]
        assert isinstance(output, Path)
        output.mkdir(parents=True, exist_ok=True)
        plan_path = output / "repair-plan.json"
        plan_path.write_text("{}", encoding="utf-8")
        events.append(("plan", report, kwargs))
        return SimpleNamespace(
            report_path=report,
            plan_path=plan_path,
            report_sha256=SHA256,
            plan_sha256=plan_sha256,
            api_origin=API_ORIGIN,
            items=(object(), object()),
            skipped={"visible": 3, "invalid_receipt": 1},
        )

    def execute(*args: object, **kwargs: object) -> object:
        events.append(("execute", args, kwargs))
        return result or SimpleNamespace(
            action="executed",
            repaired=("ktb-111",),
            failed=(),
            skipped_receipt_changed=("iene-963",),
            stopped_reason=None,
            receipt_path=(tmp_path / "repair" / "repair-execution.jsonl"),
        )

    monkeypatch.setattr(config, "MacSettings", Settings)
    monkeypatch.setattr(store_module, "JobStore", Store)
    monkeypatch.setattr(repair_module, "plan_catalog_visibility_repair", plan)
    monkeypatch.setattr(repair_module, "execute_catalog_visibility_repair", execute)
    return events, Settings


def test_repair_dry_run_builds_plan_without_token_or_sync_client_and_prints_resume(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
):
    import orchestrator.__main__ as cli

    events, _ = _install_repair_fakes(monkeypatch, tmp_path, token=None)
    monkeypatch.setattr(
        cli,
        "build_catalog_sync_client",
        lambda _settings: (_ for _ in ()).throw(
            AssertionError("dry run must not build a sync client")
        ),
    )

    exit_code = cli.run_catalog_visibility_repair(
        report=tmp_path / "audit-report.json",
        output=tmp_path / "repair",
        execute=False,
        confirm_report_sha256=None,
    )

    assert exit_code == 0
    assert [event[0] for event in events] == ["store", "plan"]
    line = capsys.readouterr().out.rstrip("\n")
    prefix, resume = line.split(" resume=", 1)
    assert prefix == (
        "action=dry_run eligible=2 skipped_total=4 "
        'skipped={"invalid_receipt":1,"visible":3} '
        f"report_sha256={SHA256} plan_sha256={OTHER_SHA256} "
        f"plan={(tmp_path / 'repair' / 'repair-plan.json').absolute()} "
        f"receipt={(tmp_path / 'repair' / 'repair-execution.jsonl').absolute()}"
    )
    assert shlex.split(resume) == [
        "python",
        "-m",
        "orchestrator",
        "catalog-visibility-repair",
        "--report",
        str((tmp_path / "audit-report.json").absolute()),
        "--output",
        str((tmp_path / "repair").absolute()),
        "--execute",
        "--confirm-report-sha256",
        SHA256,
    ]


def test_repair_execute_plans_first_then_requires_token_without_building_client(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    import orchestrator.__main__ as cli

    events, _ = _install_repair_fakes(monkeypatch, tmp_path, token=None)
    monkeypatch.setattr(
        cli,
        "build_catalog_sync_client",
        lambda _settings: (_ for _ in ()).throw(
            AssertionError("missing-token execution must not build a client")
        ),
    )

    with pytest.raises(SystemExit, match="catalog visibility repair authorization failed"):
        cli.run_catalog_visibility_repair(
            report=tmp_path / "audit-report.json",
            output=tmp_path / "repair",
            execute=True,
            confirm_report_sha256=SHA256,
        )

    assert [event[0] for event in events] == ["store", "plan"]


@pytest.mark.parametrize("confirmation", [None, "A" * 64, "short"])
def test_repair_runner_rejects_malformed_execute_confirmation_before_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    confirmation: str | None,
):
    import orchestrator.__main__ as cli

    events, _ = _install_repair_fakes(monkeypatch, tmp_path)
    monkeypatch.setattr(
        cli,
        "build_catalog_sync_client",
        lambda _settings: (_ for _ in ()).throw(
            AssertionError("malformed authorization must precede client construction")
        ),
    )

    with pytest.raises(SystemExit, match="catalog visibility repair authorization failed"):
        cli.run_catalog_visibility_repair(
            report=tmp_path / "audit-report.json",
            output=tmp_path / "repair",
            execute=True,
            confirm_report_sha256=confirmation,
        )

    assert events == []
    assert not (tmp_path / "repair").exists()


def test_repair_execute_uses_exact_builder_and_canonical_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
):
    import orchestrator.__main__ as cli

    events, Settings = _install_repair_fakes(monkeypatch, tmp_path)
    client = SimpleNamespace(
        base_url=API_ORIGIN,
        public_visibility_verification_enabled=True,
        public_visibility_client=SimpleNamespace(
            base_url=API_ORIGIN,
            check=lambda *_args: None,
        ),
        sync=lambda *_args, **_kwargs: None,
    )

    def build(settings: object) -> object:
        assert isinstance(settings, Settings)
        events.append(("build_client", settings))
        return client

    monkeypatch.setattr(cli, "build_catalog_sync_client", build)

    exit_code = cli.run_catalog_visibility_repair(
        report=tmp_path / "audit-report.json",
        output=tmp_path / "repair",
        execute=True,
        confirm_report_sha256=SHA256,
    )

    assert exit_code == 0
    assert [event[0] for event in events] == [
        "store",
        "plan",
        "build_client",
        "execute",
    ]
    execute_event = events[-1]
    assert execute_event[1][1].plan_path.parent == (tmp_path / "repair").absolute()
    assert execute_event[2] == {
        "sync_client": client,
        "output_dir": (tmp_path / "repair").absolute(),
        "execute": True,
        "confirm_report_sha256": SHA256,
    }
    assert capsys.readouterr().out == (
        "action=executed repaired=1 failed=0 skipped_receipt_changed=1 "
        f"stopped_reason=none report_sha256={SHA256} "
        f"receipt={(tmp_path / 'repair' / 'repair-execution.jsonl').absolute()}\n"
    )


def test_repair_execute_rejects_mismatched_digest_before_privileged_client_or_executor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
):
    import orchestrator.__main__ as cli
    import orchestrator.catalog_visibility_repair as repair_module

    secret = "admin-secret-do-not-print"
    events, _ = _install_repair_fakes(monkeypatch, tmp_path, token=secret)
    privileged_events: list[str] = []

    def build_client(_settings: object) -> object:
        privileged_events.append("build_client")
        return SimpleNamespace()

    def execute(*_args: object, **_kwargs: object) -> object:
        privileged_events.append("execute")
        return SimpleNamespace(
            action="executed",
            repaired=(),
            failed=(),
            skipped_receipt_changed=(),
            stopped_reason=None,
            receipt_path=tmp_path / "repair" / "repair-execution.jsonl",
        )

    monkeypatch.setattr(cli, "build_catalog_sync_client", build_client)
    monkeypatch.setattr(
        repair_module,
        "execute_catalog_visibility_repair",
        execute,
    )

    with pytest.raises(
        SystemExit,
        match="catalog visibility repair authorization failed",
    ):
        cli.run_catalog_visibility_repair(
            report=tmp_path / "audit-report.json",
            output=tmp_path / "repair",
            execute=True,
            confirm_report_sha256=OTHER_SHA256,
        )

    captured = capsys.readouterr()
    assert [event[0] for event in events] == ["store", "plan"]
    assert privileged_events == []
    assert (tmp_path / "repair" / "repair-plan.json").is_file()
    assert not (tmp_path / "repair" / "repair-execution.jsonl").exists()
    assert secret not in captured.out
    assert secret not in captured.err


@pytest.mark.parametrize(
    ("result", "expected_reason"),
    [
        (
            SimpleNamespace(
                action="executed",
                repaired=(),
                failed=("ktb-111",),
                skipped_receipt_changed=(),
                stopped_reason=None,
                receipt_path=Path("repair-execution.jsonl"),
            ),
            "none",
        ),
        (
            SimpleNamespace(
                action="executed",
                repaired=(),
                failed=(),
                skipped_receipt_changed=(),
                stopped_reason="catalog_auth_failed",
                receipt_path=Path("repair-execution.jsonl"),
            ),
            "catalog_auth_failed",
        ),
    ],
)
def test_repair_execute_returns_nonzero_for_failures_or_stop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    result: object,
    expected_reason: str,
):
    import orchestrator.__main__ as cli

    _install_repair_fakes(monkeypatch, tmp_path, result=result)
    monkeypatch.setattr(
        cli,
        "build_catalog_sync_client",
        lambda _settings: SimpleNamespace(),
    )

    exit_code = cli.run_catalog_visibility_repair(
        report=tmp_path / "audit-report.json",
        output=tmp_path / "repair",
        execute=True,
        confirm_report_sha256=SHA256,
    )

    assert exit_code == 2
    assert f"stopped_reason={expected_reason}" in capsys.readouterr().out


def test_cli_failures_never_print_exception_or_secret(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
):
    import orchestrator.catalog_visibility as visibility
    import orchestrator.config as config
    import orchestrator.store as store_module
    from orchestrator.__main__ import run_catalog_visibility_audit

    secret = "admin-secret-do-not-print"

    class Settings:
        db_path = tmp_path / "jobs.sqlite3"
        jobs_root_mac = tmp_path / "jobs"
        jobs_root_windows = "M:\\"
        javsubtitle_api_base = API_ORIGIN
        javsubtitle_admin_api_token = secret
        subtitle_audit_timeout_seconds = 17

    class Auditor:
        def __init__(self, *_args: object) -> None:
            pass

        def scan(self, *_args: object, **_kwargs: object) -> object:
            raise RuntimeError(f"remote exploded {secret}")

    monkeypatch.setattr(config, "MacSettings", Settings)
    monkeypatch.setattr(store_module, "JobStore", lambda *_args: object())
    monkeypatch.setattr(
        visibility,
        "PublicCatalogVisibilityClient",
        lambda *_args, **_kwargs: object(),
    )
    monkeypatch.setattr(visibility, "CatalogVisibilityAuditor", Auditor)

    with pytest.raises(SystemExit, match="^catalog visibility audit failed$"):
        run_catalog_visibility_audit(
            output=tmp_path / "audit",
            allowlist=None,
            limit=None,
        )

    captured = capsys.readouterr()
    assert secret not in captured.out
    assert secret not in captured.err


def test_main_dispatches_catalog_visibility_commands_before_execution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    import orchestrator.__main__ as cli

    calls: list[tuple[str, dict[str, object]]] = []
    monkeypatch.setattr(cli, "configure_logging", lambda: None)
    monkeypatch.setattr(
        cli,
        "run_catalog_visibility_audit",
        lambda **kwargs: calls.append(("audit", kwargs)),
        raising=False,
    )
    monkeypatch.setattr(
        cli,
        "run_catalog_visibility_repair",
        lambda **kwargs: calls.append(("repair", kwargs)) or 0,
        raising=False,
    )

    monkeypatch.setattr("sys.argv", ["orchestrator", *_audit_argv(tmp_path)])
    cli.main()
    monkeypatch.setattr(
        "sys.argv",
        [
            "orchestrator",
            *_repair_argv(
                tmp_path,
                "--execute",
                "--confirm-report-sha256",
                SHA256,
            ),
        ],
    )
    with pytest.raises(SystemExit) as exited:
        cli.main()

    assert exited.value.code == 0
    assert calls == [
        (
            "audit",
            {
                "output": tmp_path / "audit",
                "allowlist": None,
                "limit": None,
            },
        ),
        (
            "repair",
            {
                "report": tmp_path / "audit-report.json",
                "output": tmp_path / "repair",
                "execute": True,
                "confirm_report_sha256": SHA256,
            },
        ),
    ]

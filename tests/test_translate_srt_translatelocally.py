import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path("scripts").resolve()))
import translate_srt_translatelocally as tl


def make_srt(path: Path, count: int) -> Path:
    blocks = [
        f"{index}\n00:00:{index % 60:02d},000 --> 00:00:{index % 60:02d},900\n"
        f"日本語の字幕行{index}\n"
        for index in range(1, count + 1)
    ]
    path.write_text("\n".join(blocks), encoding="utf-8")
    return path


def test_iter_batches_enforces_line_and_character_limits():
    lines = ["a" * 100] * 80

    batches = list(tl.iter_translation_batches(lines, max_lines=50, max_chars=4000))

    assert [len(batch) for batch in batches] == [40, 40]
    assert all(sum(len(line) for line in batch) <= 4000 for batch in batches)


def test_iter_batches_rejects_one_line_over_character_limit():
    with pytest.raises(ValueError, match="exceeds 4000 characters"):
        list(tl.iter_translation_batches(["x" * 4001], max_lines=50, max_chars=4000))


def test_batched_runner_uses_fresh_call_per_batch_and_retries(monkeypatch):
    calls = []

    def fake_run(lines, **kwargs):
        calls.append(list(lines))
        if len(calls) == 1:
            raise RuntimeError("transient failure")
        return [f"EN:{line}" for line in lines]

    monkeypatch.setattr(tl, "run_translate_locally", fake_run)

    translated = tl.run_translate_locally_batched(
        ["one", "two", "three"],
        translate_locally="fake.exe",
        model="ja-en-tiny",
        batch_lines=2,
        batch_chars=4000,
        retries=1,
        timeout_seconds=30,
    )

    assert translated == ["EN:one", "EN:two", "EN:three"]
    assert calls == [["one", "two"], ["one", "two"], ["three"]]


def test_translate_process_timeout_is_enforced(monkeypatch):
    def timeout(*args, **kwargs):
        assert kwargs["timeout"] == 7
        raise subprocess.TimeoutExpired(args[0], 7)

    monkeypatch.setattr(tl.subprocess, "run", timeout)

    with pytest.raises(RuntimeError, match="timed out after 7"):
        tl.run_translate_locally(
            ["日本語"],
            translate_locally="fake.exe",
            model="ja-en-tiny",
            timeout_seconds=7,
        )


def test_translate_process_prefers_stdio_for_macos_sandbox(monkeypatch):
    calls = []

    def fake_run(command, **kwargs):
        calls.append((command, kwargs))
        return subprocess.CompletedProcess(command, 0, stdout="Hello\n", stderr="")

    monkeypatch.setattr(tl.subprocess, "run", fake_run)

    translated = tl.run_translate_locally(
        ["こんにちは"],
        translate_locally="translateLocally",
        model="ja-en-tiny",
        timeout_seconds=7,
    )

    assert translated == ["Hello"]
    assert calls[0][0] == ["translateLocally", "-m", "ja-en-tiny"]
    assert calls[0][1]["input"] == "こんにちは\n"


def test_failed_later_batch_does_not_replace_existing_final_output(tmp_path, monkeypatch):
    input_srt = make_srt(tmp_path / "sample.Japanese.srt", 51)
    output_srt = tmp_path / "sample.English.srt"
    output_srt.write_text("existing good output\n", encoding="utf-8")
    calls = 0

    monkeypatch.setattr(tl, "ensure_model_available", lambda *args: None)

    def fail_second_batch(lines, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("batch two failed")
        return [f"EN:{line}" for line in lines]

    monkeypatch.setattr(tl, "run_translate_locally", fail_second_batch)

    with pytest.raises(RuntimeError, match="batch two failed"):
        tl.translate_srt(
            input_srt,
            output_srt,
            translate_locally_path=sys.executable,
            model="ja-en-tiny",
            retries=0,
        )

    assert output_srt.read_text(encoding="utf-8") == "existing good output\n"


def test_batch_log_contains_statistics_not_subtitle_text(tmp_path, monkeypatch):
    batch_log = tmp_path / "logs" / "translate-batches.log"
    secret_line = "safe-test-marker-that-must-not-be-logged"
    monkeypatch.setattr(
        tl,
        "run_translate_locally",
        lambda lines, **kwargs: ["translated"] * len(lines),
    )

    tl.run_translate_locally_batched(
        [secret_line],
        translate_locally="fake.exe",
        model="ja-en-tiny",
        batch_log_path=batch_log,
    )

    text = batch_log.read_text(encoding="utf-8")
    assert '"batch_number": 1' in text
    assert '"input_line_count": 1' in text
    assert '"input_character_count": 40' in text
    assert '"output_line_count": 1' in text
    assert secret_line not in text


def test_sanitize_translation_input_removes_only_replacement_characters():
    result = tl.sanitize_translation_input(
        ["前\ufffd後", "unchanged punctuation!?", "normal"]
    )

    assert result.lines == ("前後", "unchanged punctuation!?", "normal")
    assert result.replacement_character_count == 1
    assert result.sanitized_line_count == 1


def test_sanitize_translation_input_rejects_line_empty_after_removal():
    with pytest.raises(
        ValueError,
        match=(
            r"translation_input_corrupt: line 2 empty after removing "
            r"replacement characters"
        ),
    ):
        tl.sanitize_translation_input(["normal", "\ufffd\ufffd"])


def test_translate_srt_sanitizes_model_input_preserves_source_and_logs_counts(
    tmp_path, monkeypatch
):
    input_srt = tmp_path / "safe.Japanese.srt"
    source_bytes = (
        "1\n00:00:00,000 --> 00:00:01,000\n"
        "safe-test-before\ufffdsafe-test-after\n\n"
    ).encode("utf-8")
    input_srt.write_bytes(source_bytes)
    output_srt = tmp_path / "safe.English.srt"
    batch_log = tmp_path / "logs" / "translate-batches.log"
    observed_lines: list[list[str]] = []

    monkeypatch.setattr(tl, "ensure_model_available", lambda *args: None)

    def fake_run(lines, **kwargs):
        observed_lines.append(list(lines))
        return ["clean English"] * len(lines)

    monkeypatch.setattr(tl, "run_translate_locally", fake_run)

    tl.translate_srt(
        input_srt,
        output_srt,
        translate_locally_path=sys.executable,
        model="ja-en-tiny",
        batch_log_path=batch_log,
    )

    assert observed_lines == [["safe-test-beforesafe-test-after"]]
    assert input_srt.read_bytes() == source_bytes
    assert "\ufffd" not in output_srt.read_text(encoding="utf-8")
    log_text = batch_log.read_text(encoding="utf-8")
    assert '"event": "input_sanitization"' in log_text
    assert '"input_replacement_character_count": 1' in log_text
    assert '"sanitized_input_line_count": 1' in log_text
    assert "safe-test-before" not in log_text
    assert "safe-test-after" not in log_text


def test_translate_srt_rejects_empty_sanitized_line_before_model(
    tmp_path, monkeypatch
):
    input_srt = tmp_path / "safe.Japanese.srt"
    source_bytes = (
        "1\n00:00:00,000 --> 00:00:01,000\n\ufffd\ufffd\n\n"
    ).encode("utf-8")
    input_srt.write_bytes(source_bytes)
    output_srt = tmp_path / "safe.English.srt"
    model_checked = False
    model_called = False

    def fake_ensure_model_available(*args):
        nonlocal model_checked
        model_checked = True

    monkeypatch.setattr(tl, "ensure_model_available", fake_ensure_model_available)

    def fake_run(lines, **kwargs):
        nonlocal model_called
        model_called = True
        return ["unexpected"] * len(lines)

    monkeypatch.setattr(tl, "run_translate_locally", fake_run)

    with pytest.raises(ValueError, match="translation_input_corrupt"):
        tl.translate_srt(
            input_srt,
            output_srt,
            translate_locally_path=sys.executable,
            model="ja-en-tiny",
        )

    assert model_checked is False
    assert model_called is False
    assert input_srt.read_bytes() == source_bytes
    assert not output_srt.exists()

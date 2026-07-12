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

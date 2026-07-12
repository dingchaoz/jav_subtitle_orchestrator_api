from __future__ import annotations


def _timestamp(seconds: int) -> str:
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d},000"


def _srt(lines: list[str]) -> bytes:
    return "\n\n".join(
        (
            f"{index}\n"
            f"{_timestamp(index - 1)} --> {_timestamp(index)}\n"
            f"{line}"
        )
        for index, line in enumerate(lines, 1)
    ).encode("utf-8")


def test_inspector_accepts_diverse_valid_english():
    from orchestrator.historical_english_ai_audit import inspect_english_srt

    report = inspect_english_srt(
        _srt([f"Distinct sentence {index}" for index in range(1, 26)])
    )

    assert report.status == "passed"
    assert report.reason_codes == ()
    assert report.metrics["cue_count"] == 25
    assert report.metrics["unique_text_ratio"] == 1.0


def test_inspector_locks_refusal_threshold():
    from orchestrator.historical_english_ai_audit import inspect_english_srt

    report = inspect_english_srt(
        _srt(["Cannot translate"] * 3 + [f"Line {index}" for index in range(147)])
    )

    assert report.status == "hard_failure"
    assert "KNOWN_BAD_TRANSLATION" in report.reason_codes


def test_inspector_locks_dominant_line_threshold():
    from orchestrator.historical_english_ai_audit import inspect_english_srt

    report = inspect_english_srt(
        _srt(["Repeated output"] * 10 + [f"Line {index}" for index in range(10)])
    )

    assert "DOMINANT_TEXT_COLLAPSE" in report.reason_codes
    assert report.metrics["dominant_text_ratio"] == 0.5


def test_inspector_locks_low_diversity_threshold():
    from orchestrator.historical_english_ai_audit import inspect_english_srt

    report = inspect_english_srt(
        _srt(["Same output"] * 25 + [f"Variant {index % 10}" for index in range(75)])
    )

    assert "LOW_DIVERSITY_COLLAPSE" in report.reason_codes
    assert report.metrics["unique_text_ratio"] < 0.15


def test_inspector_rejects_empty_invalid_and_corrupted_srt_without_text():
    from orchestrator.historical_english_ai_audit import inspect_english_srt

    empty = inspect_english_srt(b"")
    invalid = inspect_english_srt(b"not an srt")
    mojibake = inspect_english_srt(_srt(["synthetic \ufffd \ufffd \ufffd"] * 5))

    assert empty.reason_codes == ("EMPTY_FILE", "NO_VALID_CUES")
    assert invalid.reason_codes == ("NO_VALID_CUES",)
    assert "SEVERE_MOJIBAKE" in mojibake.reason_codes
    assert "dominant_normalized_text" not in mojibake.metrics
    assert "synthetic" not in repr(mojibake)


def test_inspector_rejects_invalid_timeline_beyond_locked_tolerance():
    from orchestrator.historical_english_ai_audit import inspect_english_srt

    data = (
        "1\n00:00:10,000 --> 00:00:09,000\nFirst\n\n"
        "2\n00:00:01,000 --> 00:00:02,000\nSecond\n"
    ).encode("utf-8")

    report = inspect_english_srt(data)

    assert "INVALID_TIMELINE" in report.reason_codes

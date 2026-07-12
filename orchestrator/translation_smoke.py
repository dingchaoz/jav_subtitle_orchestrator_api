from pathlib import Path
from tempfile import TemporaryDirectory

from orchestrator.subtitle_quality import QualityReport, validate_translation_quality


STARTUP_SMOKE_SENTENCES = (
    "これはテストです。",
    "明日は東京で会議があります。",
    "今日はいい天気ですね。",
    "駅まで一緒に行きましょう。",
    "何をしているのですか。",
    "ありがとうございます。",
    "お腹が空きました。",
    "私は学生です。",
    "ドアを閉めてください。",
    "また明日会いましょう。",
)


class TranslationRuntimeUnhealthyError(RuntimeError):
    pass


def run_translation_startup_smoke_test(translator) -> QualityReport:
    with TemporaryDirectory(prefix="translation-startup-smoke-") as temp_dir:
        temp_path = Path(temp_dir)
        japanese_srt = temp_path / "startup-smoke.Japanese.srt"
        english_srt = temp_path / "startup-smoke.English.srt"
        blocks = []
        for index, sentence in enumerate(STARTUP_SMOKE_SENTENCES, start=1):
            start = (index - 1) * 2
            blocks.append(
                f"{index}\n"
                f"00:00:{start:02d},000 --> 00:00:{start + 1:02d},000\n"
                f"{sentence}\n"
            )
        japanese_srt.write_text("\n".join(blocks), encoding="utf-8")
        translator.translate_to_english(japanese_srt, english_srt)
        report = validate_translation_quality(japanese_srt, english_srt)

    if report.english_cue_count != len(STARTUP_SMOKE_SENTENCES):
        _append_once(report.reason_codes, "startup_output_count_mismatch")
    if report.english_unique_ratio < 0.50:
        _append_once(report.reason_codes, "startup_low_diversity")
    if report.known_bad_phrase_count:
        _append_once(report.reason_codes, "startup_known_bad_phrase")
    report.passed = not report.reason_codes
    if not report.passed:
        raise TranslationRuntimeUnhealthyError(
            "translation startup smoke test failed: " + ",".join(report.reason_codes)
        )
    return report


def _append_once(items: list[str], value: str) -> None:
    if value not in items:
        items.append(value)

from pathlib import Path

from orchestrator.subtitle_quality import validate_translation_quality


def write_srt(path: Path, texts: list[str]) -> Path:
    blocks = []
    for index, text in enumerate(texts, start=1):
        start = (index - 1) * 2
        blocks.append(
            f"{index}\n"
            f"00:{start // 60:02d}:{start % 60:02d},000 --> "
            f"00:{(start + 1) // 60:02d}:{(start + 1) % 60:02d},000\n"
            f"{text}\n"
        )
    path.write_text("\n".join(blocks), encoding="utf-8")
    return path


def aligned_pair(tmp_path: Path, japanese: list[str], english: list[str]):
    return (
        write_srt(tmp_path / "sample.Japanese.srt", japanese),
        write_srt(tmp_path / "sample.English.srt", english),
    )


def test_diverse_aligned_translation_passes(tmp_path):
    japanese = [f"日本語の文{i}です。" for i in range(40)]
    english = [f"This is distinct sentence number {i}." for i in range(40)]
    ja, en = aligned_pair(tmp_path, japanese, english)

    report = validate_translation_quality(ja, en)

    assert report.passed is True
    assert report.reason_codes == []
    assert report.japanese_cue_count == 40
    assert report.english_cue_count == 40
    assert report.english_unique_count == 40
    assert report.english_unique_ratio == 1.0


def test_roe_style_known_bad_collapse_is_rejected(tmp_path):
    japanese = [f"日本語{i}" for i in range(100)]
    english = ["I don't know what to do."] * 90 + [f"Different {i}" for i in range(10)]
    ja, en = aligned_pair(tmp_path, japanese, english)

    report = validate_translation_quality(ja, en)

    assert report.passed is False
    assert "known_bad_collapse" in report.reason_codes
    assert "dominant_text_collapse" in report.reason_codes
    assert report.known_bad_phrase_count == 90
    assert report.dominant_text_ratio == 0.9


def test_hodv_style_generic_repetition_is_rejected(tmp_path):
    japanese = [f"日本語{i}" for i in range(120)]
    english = ["What are you doing?"] * 110 + [f"Different {i}" for i in range(10)]
    ja, en = aligned_pair(tmp_path, japanese, english)

    report = validate_translation_quality(ja, en)

    assert report.passed is False
    assert "dominant_text_collapse" in report.reason_codes
    assert "low_diversity_collapse" in report.reason_codes


def test_empty_english_srt_is_rejected(tmp_path):
    ja = write_srt(tmp_path / "sample.Japanese.srt", ["こんにちは"])
    en = tmp_path / "sample.English.srt"
    en.write_text("", encoding="utf-8")

    report = validate_translation_quality(ja, en)

    assert report.passed is False
    assert "english_srt_empty" in report.reason_codes


def test_cue_count_mismatch_is_rejected(tmp_path):
    ja, en = aligned_pair(tmp_path, ["一", "二"], ["One"])

    report = validate_translation_quality(ja, en)

    assert report.passed is False
    assert "cue_count_mismatch" in report.reason_codes


def test_refusal_templates_are_rejected(tmp_path):
    japanese = [f"日本語{i}" for i in range(20)]
    english = ["I cannot assist with translating this content."] * 3 + [
        f"Normal translation {i}" for i in range(17)
    ]
    ja, en = aligned_pair(tmp_path, japanese, english)

    report = validate_translation_quality(ja, en)

    assert report.passed is False
    assert "refusal_template" in report.reason_codes
    assert report.refusal_phrase_count == 3


def test_single_incidental_refusal_phrase_does_not_reject_diverse_translation(tmp_path):
    japanese = [f"日本語{i}" for i in range(100)]
    english = ["I cannot translate that word."] + [
        f"Distinct normal translation {i}" for i in range(99)
    ]
    ja, en = aligned_pair(tmp_path, japanese, english)

    report = validate_translation_quality(ja, en)

    assert report.passed is True
    assert report.refusal_phrase_count == 1
    assert "refusal_template" not in report.reason_codes


def test_replacement_characters_and_mojibake_are_rejected(tmp_path):
    japanese = [f"日本語{i}" for i in range(20)]
    english = [f"Broken replacement ��� {i}" for i in range(20)]
    ja, en = aligned_pair(tmp_path, japanese, english)

    report = validate_translation_quality(ja, en)

    assert report.passed is False
    assert "encoding_corruption" in report.reason_codes
    assert report.replacement_character_count == 60


def test_realistic_short_repetition_with_overall_diversity_passes(tmp_path):
    english = (
        ["Ah"] * 40
        + ["Yes"] * 30
        + ["No"] * 20
        + [f"Distinct longer sentence {i}" for i in range(30)]
    )
    japanese = [f"日本語{i}" for i in range(len(english))]
    ja, en = aligned_pair(tmp_path, japanese, english)

    report = validate_translation_quality(ja, en)

    assert report.passed is True
    assert report.reason_codes == []


def test_broken_index_or_timestamp_structure_is_rejected(tmp_path):
    ja = write_srt(tmp_path / "sample.Japanese.srt", ["一", "二"])
    en = tmp_path / "sample.English.srt"
    en.write_text(
        "1\n00:00:00,000 --> 00:00:01,000\nOne\n\n"
        "7\nnot a timestamp\nTwo\n\n",
        encoding="utf-8",
    )

    report = validate_translation_quality(ja, en)

    assert report.passed is False
    assert "english_srt_parse_error" in report.reason_codes
    assert report.parse_errors

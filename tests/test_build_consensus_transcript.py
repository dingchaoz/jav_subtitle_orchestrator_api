from scripts.benchmark_transcription_coverage import Cue
from scripts.build_consensus_transcript import (
    consensus_cues,
    is_suspicious,
    merge_with_base,
    normalize_text,
    text_similarity,
)


def cue(index: int, start: float, end: float, text: str) -> Cue:
    return Cue(index=index, start=start, end=end, text=text)


def test_normalize_and_similarity_handle_japanese_punctuation() -> None:
    assert normalize_text(" マスク、取って。") == "マスク取って"
    assert text_similarity("マスク", "マスク取ってもらえませんか") == 1.0


def test_consensus_confirms_dialogue_split_differently_across_grids() -> None:
    grid_a = [
        cue(1, 1292.6, 1294.6, "顔が見たいんで"),
        cue(2, 1294.6, 1298.5, "マスク"),
        cue(3, 1298.5, 1300.5, "取ってもらえませんか"),
    ]
    grid_b = [
        cue(1, 1275.9, 1294.1, "あのー顔が見たいんで"),
        cue(2, 1294.1, 1299.9, "マスク取ってもらえませんか"),
    ]

    confirmed = consensus_cues(grid_a, grid_b)

    assert [item.text for item in confirmed] == [
        "顔が見たいんで",
        "マスク",
        "取ってもらえませんか",
    ]


def test_consensus_drops_boundary_hallucination_and_repeated_non_speech() -> None:
    grid_a = [
        cue(1, 1020, 1049.98, "ご視聴ありがとうございました"),
        cue(2, 1861, 1889.9, "あ" * 100),
    ]
    grid_b = [
        cue(1, 1245, 1274.98, "ご視聴ありがとうございました"),
        cue(2, 1800, 1801, "はい"),
    ]

    assert consensus_cues(grid_a, grid_b) == []
    assert is_suspicious(grid_a[0])
    assert is_suspicious(grid_a[1])


def test_merge_keeps_base_and_adds_only_non_duplicate_confirmed_cues() -> None:
    base = [
        cue(1, 100, 104, "既存の台詞"),
        cue(2, 200, 230, "おはようございます"),
    ]
    confirmed = [
        cue(1, 100.2, 104.2, "既存の台詞"),
        cue(2, 205, 208, "新しい台詞"),
    ]

    merged = merge_with_base(base, confirmed)

    assert [item.text for item in merged] == ["既存の台詞", "新しい台詞"]
    assert [item.index for item in merged] == [1, 2]

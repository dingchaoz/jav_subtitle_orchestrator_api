import subprocess
import sys
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "translate_srt_translatelocally.py"


def test_translate_srt_preserves_structure_and_translates_text_lines(tmp_path):
    fake_tl = tmp_path / "fake_translate_locally.py"
    fake_tl.write_text(
        "\n".join(
            [
                "import sys",
                "assert sys.argv[1:] == ['--model', 'ja-en-tiny']",
                "mapping = {",
                "    'これはテストです。': 'This is a test.',",
                "    '明日は東京で会議があります。': 'There is a meeting in Tokyo tomorrow.',",
                "}",
                "for line in sys.stdin.read().splitlines():",
                "    print(mapping[line])",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    input_srt = tmp_path / "input.Japanese.srt"
    input_srt.write_text(
        "\n".join(
            [
                "1",
                "00:00:01,000 --> 00:00:02,000",
                "これはテストです。",
                "",
                "2",
                "00:00:03,000 --> 00:00:04,000",
                "明日は東京で会議があります。",
                "",
            ]
        ),
        encoding="utf-8",
    )
    output_srt = tmp_path / "output.English.srt"

    subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            str(input_srt),
            str(output_srt),
            "--tl",
            sys.executable,
            "--tl-arg",
            str(fake_tl),
        ],
        check=True,
    )

    assert output_srt.read_text(encoding="utf-8") == "\n".join(
        [
            "1",
            "00:00:01,000 --> 00:00:02,000",
            "This is a test.",
            "",
            "2",
            "00:00:03,000 --> 00:00:04,000",
            "There is a meeting in Tokyo tomorrow.",
            "",
        ]
    )

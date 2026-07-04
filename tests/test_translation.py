from pathlib import Path

from orchestrator.translation import SubtitleTranslator


def test_translation_adapter_renames_script_output_to_english_srt(tmp_path):
    script = tmp_path / "fake_translate.py"
    script.write_text(
        "import argparse\n"
        "from pathlib import Path\n"
        "parser = argparse.ArgumentParser()\n"
        "parser.add_argument('--input')\n"
        "parser.add_argument('--langs')\n"
        "parser.add_argument('--output-dir')\n"
        "args = parser.parse_args()\n"
        "Path(args.output_dir, 'ktb-096.en.srt').write_text('1\\n00:00:00,000 --> 00:00:01,500\\nHello\\n\\n', encoding='utf-8')\n",
        encoding="utf-8",
    )
    input_srt = tmp_path / "ktb-096.Japanese.srt"
    input_srt.write_text("1\n00:00:00,000 --> 00:00:01,500\nこんにちは\n\n", encoding="utf-8")
    output_srt = tmp_path / "ktb-096.English.srt"
    translator = SubtitleTranslator(str(script))

    translator.translate_to_english(input_srt, output_srt)

    assert output_srt.read_text(encoding="utf-8").startswith("1\n00:00:00,000")

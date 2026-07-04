import pytest

from orchestrator.translation import SubtitleTranslator


JAPANESE_SRT = "1\n00:00:00,000 --> 00:00:01,500\nこんにちは\n\n"


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
        "translated = '1\\n00:00:00,000 --> 00:00:01,500\\nHello\\n\\n'\n"
        "Path(args.output_dir, 'ktb-096.en.srt').write_text(translated, encoding='utf-8')\n",
        encoding="utf-8",
    )
    input_srt = tmp_path / "ktb-096.Japanese.srt"
    input_srt.write_text(JAPANESE_SRT, encoding="utf-8")
    output_srt = tmp_path / "ktb-096.English.srt"
    translator = SubtitleTranslator(str(script))

    translator.translate_to_english(input_srt, output_srt)

    assert output_srt.read_text(encoding="utf-8").startswith("1\n00:00:00,000")


def test_translation_adapter_handles_real_hyphenated_script_output(tmp_path):
    script = tmp_path / "fake_translate.py"
    script.write_text(
        "import argparse\n"
        "from pathlib import Path\n"
        "parser = argparse.ArgumentParser()\n"
        "parser.add_argument('--input')\n"
        "parser.add_argument('--langs')\n"
        "parser.add_argument('--output-dir')\n"
        "args = parser.parse_args()\n"
        "input_path = Path(args.input)\n"
        "translated = '1\\n00:00:00,000 --> 00:00:01,500\\nHello\\n\\n'\n"
        "Path(args.output_dir, f'{input_path.stem}-{args.langs}.srt').write_text(\n"
        "    translated,\n"
        "    encoding='utf-8',\n"
        ")\n",
        encoding="utf-8",
    )
    input_srt = tmp_path / "ktb-096.Japanese.srt"
    input_srt.write_text(JAPANESE_SRT, encoding="utf-8")
    output_srt = tmp_path / "ktb-096.English.srt"
    translator = SubtitleTranslator(str(script))

    translator.translate_to_english(input_srt, output_srt)

    assert output_srt.read_text(encoding="utf-8").startswith("1\n00:00:00,000")


def test_translation_adapter_raises_runtime_error_with_script_context(tmp_path):
    script = tmp_path / "fake_translate.py"
    script.write_text(
        "import sys\n"
        "print('translation failed', file=sys.stderr)\n"
        "raise SystemExit(7)\n",
        encoding="utf-8",
    )
    input_srt = tmp_path / "ktb-096.Japanese.srt"
    input_srt.write_text(JAPANESE_SRT, encoding="utf-8")
    output_srt = tmp_path / "ktb-096.English.srt"
    translator = SubtitleTranslator(str(script))

    with pytest.raises(RuntimeError) as exc_info:
        translator.translate_to_english(input_srt, output_srt)

    message = str(exc_info.value)
    assert "fake_translate.py" in message
    assert "exit code 7" in message
    assert "translation failed" in message


def test_translation_adapter_ignores_stale_final_output_when_script_produces_nothing(tmp_path):
    script = tmp_path / "fake_translate.py"
    script.write_text(
        "import argparse\n"
        "parser = argparse.ArgumentParser()\n"
        "parser.add_argument('--input')\n"
        "parser.add_argument('--langs')\n"
        "parser.add_argument('--output-dir')\n"
        "parser.parse_args()\n",
        encoding="utf-8",
    )
    input_srt = tmp_path / "ktb-096.Japanese.srt"
    input_srt.write_text(JAPANESE_SRT, encoding="utf-8")
    output_srt = tmp_path / "ktb-096.English.srt"
    output_srt.write_text("stale subtitle\n", encoding="utf-8")
    translator = SubtitleTranslator(str(script))

    with pytest.raises(FileNotFoundError) as exc_info:
        translator.translate_to_english(input_srt, output_srt)

    message = str(exc_info.value)
    assert "ktb-096.Japanese-en.srt" in message
    assert "ktb-096.English.srt" in message
    assert output_srt.read_text(encoding="utf-8") == "stale subtitle\n"

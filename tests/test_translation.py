import os
import subprocess
import sys
from pathlib import Path

import pytest

from orchestrator.translation import SubtitleTranslator


JAPANESE_SRT = "1\n00:00:00,000 --> 00:00:01,500\nこんにちは\n\n"


def make_fake_translate_locally(tmp_path: Path, *, list_model: str = "ja-en-tiny") -> Path:
    fake_py = tmp_path / "fake_translate_locally.py"
    fake_py.write_text(
        "import sys\n"
        "from pathlib import Path\n"
        "args = sys.argv[1:]\n"
        "if '-l' in args:\n"
        f"    print('Japanese-English type: tiny version: 1; To invoke do -m {list_model}')\n"
        "    raise SystemExit(0)\n"
        "if '-m' not in args:\n"
        "    print('missing -m', file=sys.stderr)\n"
        "    raise SystemExit(2)\n"
        "model = args[args.index('-m') + 1]\n"
        "if model != 'ja-en-tiny':\n"
        "    print(f'model {model} is not installed', file=sys.stderr)\n"
        "    raise SystemExit(1)\n"
        "input_path = Path(args[args.index('-i') + 1])\n"
        "output_path = Path(args[args.index('-o') + 1])\n"
        "mapping = {\n"
        "    'こんにちは': 'Hello',\n"
        "    'これはテストです。': 'This is a test.',\n"
        "    '明日は東京で会議があります。': 'There is a meeting in Tokyo tomorrow.',\n"
        "}\n"
        "lines = input_path.read_text(encoding='utf-8-sig').splitlines()\n"
        "translated = [mapping.get(line, 'EN:' + line) for line in lines]\n"
        "output_path.write_text('\\n'.join(translated) + ('\\n' if translated else ''), encoding='utf-8')\n",
        encoding="utf-8",
    )
    if os.name == "nt":
        fake_cmd = tmp_path / "translateLocally.cmd"
        fake_cmd.write_text(
            f'@"{sys.executable}" "{fake_py}" %*\n',
            encoding="utf-8",
        )
        return fake_cmd

    fake_sh = tmp_path / "translateLocally"
    fake_sh.write_text(
        f'#!/usr/bin/env sh\nexec "{sys.executable}" "{fake_py}" "$@"\n',
        encoding="utf-8",
    )
    fake_sh.chmod(0o755)
    return fake_sh


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


def test_translation_adapter_uses_local_temp_output_dir_before_final_copy(tmp_path):
    final_dir = tmp_path / "final"
    script = tmp_path / "fake_translate.py"
    script.write_text(
        "import argparse\n"
        "from pathlib import Path\n"
        "parser = argparse.ArgumentParser()\n"
        "parser.add_argument('--input')\n"
        "parser.add_argument('--langs')\n"
        "parser.add_argument('--output-dir')\n"
        "args = parser.parse_args()\n"
        f"expected_parent = Path({str(final_dir)!r})\n"
        "output_dir = Path(args.output_dir)\n"
        "if output_dir.parent == expected_parent:\n"
        "    raise SystemExit('translation temp dir should not be inside final parent')\n"
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
    output_srt = final_dir / "ktb-096.English.srt"
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


def test_translation_adapter_supports_codex_batch_wrapper(tmp_path, monkeypatch):
    batch_script = tmp_path / "fake_translate_srts.py"
    batch_script.write_text(
        "import argparse\n"
        "from pathlib import Path\n"
        "parser = argparse.ArgumentParser()\n"
        "parser.add_argument('--source-root', required=True)\n"
        "parser.add_argument('--output-root', required=True)\n"
        "parser.add_argument('--targets', required=True)\n"
        "parser.add_argument('--provider')\n"
        "parser.add_argument('--workers')\n"
        "parser.add_argument('--batch-workers')\n"
        "parser.add_argument('--anthropic-models')\n"
        "parser.add_argument('--anthropic-recheck-minutes')\n"
        "parser.add_argument('--codex-bin')\n"
        "parser.add_argument('--resume', action='store_true')\n"
        "args = parser.parse_args()\n"
        "assert args.provider == 'codex'\n"
        "assert args.batch_workers == '5'\n"
        "source_root = Path(args.source_root)\n"
        "output_root = Path(args.output_root) / 'nested'\n"
        "output_root.mkdir(parents=True, exist_ok=True)\n"
        "input_srt = next(source_root.glob('*.srt'))\n"
        "translated = '1\\n00:00:00,000 --> 00:00:01,500\\nHello from Codex\\n\\n'\n"
        "Path(output_root, input_srt.name.replace('.Japanese.srt', '.English.srt')).write_text(\n"
        "    translated,\n"
        "    encoding='utf-8',\n"
        ")\n",
        encoding="utf-8",
    )
    wrapper = Path("scripts/codex_translate_single.py").resolve()
    monkeypatch.setenv("CODEX_TRANSLATE_SCRIPT_PATH", str(batch_script))
    monkeypatch.setenv("CODEX_TRANSLATE_PYTHON_EXECUTABLE", sys.executable)
    monkeypatch.setenv("CODEX_TRANSLATION_BATCH_WORKERS", "5")
    monkeypatch.delenv("CODEX_TRANSLATION_PROVIDER", raising=False)
    monkeypatch.delenv("CODEX_BIN_PATH", raising=False)
    input_srt = tmp_path / "ktb-096.Japanese.srt"
    input_srt.write_text(JAPANESE_SRT, encoding="utf-8")
    output_srt = tmp_path / "ktb-096.English.srt"
    translator = SubtitleTranslator(str(wrapper))

    translator.translate_to_english(input_srt, output_srt)

    assert output_srt.read_text(encoding="utf-8").startswith("1\n00:00:00,000")
    assert "Hello from Codex" in output_srt.read_text(encoding="utf-8")


def test_translation_adapter_codex_wrapper_requires_batch_script_env(tmp_path, monkeypatch):
    wrapper = Path("scripts/codex_translate_single.py").resolve()
    monkeypatch.delenv("CODEX_TRANSLATE_SCRIPT_PATH", raising=False)
    input_srt = tmp_path / "ktb-096.Japanese.srt"
    input_srt.write_text(JAPANESE_SRT, encoding="utf-8")
    output_srt = tmp_path / "ktb-096.English.srt"
    translator = SubtitleTranslator(str(wrapper))

    with pytest.raises(RuntimeError) as exc_info:
        translator.translate_to_english(input_srt, output_srt)

    assert "CODEX_TRANSLATE_SCRIPT_PATH must be set" in str(exc_info.value)


def test_translatelocally_srt_translation_preserves_cues_and_utf8(tmp_path):
    sys.path.insert(0, str(Path("scripts").resolve()))
    from translate_srt_translatelocally import translate_srt

    fake_tl = make_fake_translate_locally(tmp_path)
    input_srt = tmp_path / "mida-686.Japanese.srt"
    source_srt = (
        "1\r\n"
        "00:00:00,000 --> 00:00:01,500\r\n"
        "これはテストです。\r\n"
        "\r\n"
        "2\r\n"
        "00:00:02,000 --> 00:00:03,000\r\n"
        "明日は東京で会議があります。\r\n"
        "\r\n",
    )
    source_srt = (
        "1\r\n"
        "00:00:00,000 --> 00:00:01,500\r\n"
        "\u3053\u308c\u306f\u30c6\u30b9\u30c8\u3067\u3059\u3002\r\n"
        "\r\n"
        "2\r\n"
        "00:00:02,000 --> 00:00:03,000\r\n"
        "\u660e\u65e5\u306f\u6771\u4eac\u3067\u4f1a\u8b70\u304c\u3042\u308a\u307e\u3059\u3002\r\n"
        "\r\n"
    )
    input_srt.write_bytes(source_srt.encode("utf-8"))
    output_srt = tmp_path / "mida-686.English.translatelocally.srt"

    translate_srt(input_srt, output_srt, translate_locally_path=str(fake_tl), model="ja-en-tiny")

    translated = output_srt.read_bytes().decode("utf-8")
    assert "00:00:00,000 --> 00:00:01,500" in translated
    assert "This is a test." in translated
    assert "There is a meeting in Tokyo tomorrow." in translated
    assert "\r\n\r\n2\r\n" in translated


def test_translatelocally_worker_wrapper_argument_handling(tmp_path):
    fake_tl = make_fake_translate_locally(tmp_path)
    input_srt = tmp_path / "mida-686.Japanese.srt"
    input_srt.write_text(
        "1\n00:00:00,000 --> 00:00:01,500\nこれはテストです。\n\n",
        encoding="utf-8",
    )
    output_dir = tmp_path / "out"
    wrapper = Path("scripts/translatelocally_translate_single.py").resolve()
    env = {**os.environ, "TRANSLATELOCALLY_PATH": str(fake_tl), "TRANSLATELOCALLY_MODEL": "ja-en-tiny"}

    completed = subprocess.run(
        [
            sys.executable,
            str(wrapper),
            "--input",
            str(input_srt),
            "--langs",
            "en",
            "--output-dir",
            str(output_dir),
        ],
        text=True,
        capture_output=True,
        env=env,
        check=False,
    )

    output_srt = output_dir / "mida-686.English.srt"
    assert completed.returncode == 0, completed.stderr
    assert output_srt.exists()
    assert "This is a test." in output_srt.read_text(encoding="utf-8")


def test_translation_adapter_discovers_translatelocally_wrapper_output(tmp_path, monkeypatch):
    fake_tl = make_fake_translate_locally(tmp_path)
    monkeypatch.setenv("TRANSLATELOCALLY_PATH", str(fake_tl))
    monkeypatch.setenv("TRANSLATELOCALLY_MODEL", "ja-en-tiny")
    input_srt = tmp_path / "mida-686.Japanese.srt"
    input_srt.write_text(
        "1\n00:00:00,000 --> 00:00:01,500\nこれはテストです。\n\n",
        encoding="utf-8",
    )
    output_srt = tmp_path / "mida-686.English.srt"
    translator = SubtitleTranslator(str(Path("scripts/translatelocally_translate_single.py").resolve()))

    translator.translate_to_english(input_srt, output_srt)

    assert output_srt.exists()
    assert "This is a test." in output_srt.read_text(encoding="utf-8")


def test_translatelocally_translation_fails_clearly_when_executable_missing(tmp_path):
    sys.path.insert(0, str(Path("scripts").resolve()))
    from translate_srt_translatelocally import translate_srt

    input_srt = tmp_path / "mida-686.Japanese.srt"
    input_srt.write_text(
        "1\n00:00:00,000 --> 00:00:01,500\nこれはテストです。\n\n",
        encoding="utf-8",
    )

    with pytest.raises(FileNotFoundError) as exc_info:
        translate_srt(
            input_srt,
            tmp_path / "out.srt",
            translate_locally_path=str(tmp_path / "missing.exe"),
            model="ja-en-tiny",
        )

    assert "TRANSLATELOCALLY_PATH" in str(exc_info.value)


def test_translatelocally_translation_fails_clearly_when_model_missing(tmp_path):
    sys.path.insert(0, str(Path("scripts").resolve()))
    from translate_srt_translatelocally import translate_srt

    fake_tl = make_fake_translate_locally(tmp_path, list_model="de-en-tiny")
    input_srt = tmp_path / "mida-686.Japanese.srt"
    input_srt.write_text(
        "1\n00:00:00,000 --> 00:00:01,500\nこれはテストです。\n\n",
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError) as exc_info:
        translate_srt(
            input_srt,
            tmp_path / "out.srt",
            translate_locally_path=str(fake_tl),
            model="ja-en-tiny",
        )

    assert "ja-en-tiny" in str(exc_info.value)
    assert "not installed" in str(exc_info.value)

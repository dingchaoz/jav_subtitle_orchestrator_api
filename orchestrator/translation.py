import subprocess
import sys
from pathlib import Path


class SubtitleTranslator:
    def __init__(self, translate_script_path: str) -> None:
        self.translate_script_path = translate_script_path

    def translate_to_english(self, input_srt: Path, output_srt: Path) -> None:
        output_srt.parent.mkdir(parents=True, exist_ok=True)
        command = [
            sys.executable,
            self.translate_script_path,
            "--input",
            str(input_srt),
            "--langs",
            "en",
            "--output-dir",
            str(output_srt.parent),
        ]
        completed = subprocess.run(command, text=True, capture_output=True, check=False)
        if completed.returncode != 0:
            raise RuntimeError(completed.stderr or completed.stdout)

        candidates = [
            output_srt,
            output_srt.parent / f"{input_srt.stem}.en.srt",
            output_srt.parent / input_srt.name.replace(".Japanese.srt", ".en.srt"),
            output_srt.parent / input_srt.name.replace(".Japanese.srt", ".English.srt"),
        ]
        for candidate in candidates:
            if candidate.exists():
                if candidate != output_srt:
                    tmp_path = output_srt.with_suffix(output_srt.suffix + ".tmp")
                    tmp_path.write_text(candidate.read_text(encoding="utf-8"), encoding="utf-8")
                    tmp_path.replace(output_srt)
                return
        raise FileNotFoundError(f"translation script did not produce {output_srt}")

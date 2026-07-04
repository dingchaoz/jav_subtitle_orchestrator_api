import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory


class SubtitleTranslator:
    def __init__(self, translate_script_path: str) -> None:
        self.translate_script_path = translate_script_path

    def translate_to_english(self, input_srt: Path, output_srt: Path) -> None:
        output_srt.parent.mkdir(parents=True, exist_ok=True)
        with TemporaryDirectory(
            prefix=".subtitle-translation-",
            dir=output_srt.parent,
        ) as temp_output_dir:
            temp_output_path = Path(temp_output_dir)
            command = [
                sys.executable,
                self.translate_script_path,
                "--input",
                str(input_srt),
                "--langs",
                "en",
                "--output-dir",
                str(temp_output_path),
            ]
            completed = subprocess.run(command, text=True, capture_output=True, check=False)
            if completed.returncode != 0:
                output = (completed.stderr or completed.stdout).strip()
                raise RuntimeError(
                    f"translation script {self.translate_script_path} failed with "
                    f"exit code {completed.returncode}: {output}"
                )

            candidates = [
                temp_output_path / output_srt.name,
                temp_output_path / f"{input_srt.stem}-en.srt",
                temp_output_path / f"{input_srt.stem}.en.srt",
                temp_output_path / input_srt.name.replace(".Japanese.srt", ".en.srt"),
                temp_output_path / input_srt.name.replace(".Japanese.srt", ".English.srt"),
            ]
            for candidate in candidates:
                if candidate.exists():
                    candidate.replace(output_srt)
                    return
            checked_candidates = ", ".join(str(candidate) for candidate in candidates)
            raise FileNotFoundError(
                f"translation script did not produce {output_srt}; "
                f"checked candidates: {checked_candidates}"
            )

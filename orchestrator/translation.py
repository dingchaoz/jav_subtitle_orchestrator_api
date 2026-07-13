import os
import shutil
import subprocess
import sys
import uuid
from pathlib import Path
from tempfile import TemporaryDirectory

from orchestrator.job_files_lock import exclusive_job_files_lock


class SubtitleTranslator:
    def __init__(self, translate_script_path: str) -> None:
        self.translate_script_path = translate_script_path

    def translate_to_english(self, input_srt: Path, output_srt: Path) -> None:
        output_srt.parent.mkdir(parents=True, exist_ok=True)
        with TemporaryDirectory(prefix=".subtitle-translation-") as temp_output_dir:
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
            environment = os.environ.copy()
            environment["TRANSLATE_BATCH_LOG_PATH"] = str(
                input_srt.parent / "logs" / "translate-batches.log"
            )
            completed = subprocess.run(
                command,
                text=True,
                encoding="utf-8",
                errors="replace",
                capture_output=True,
                check=False,
                env=environment,
            )
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
                    temporary_final = output_srt.with_name(
                        f".{output_srt.name}.{uuid.uuid4().hex}.tmp"
                    )
                    try:
                        shutil.copyfile(candidate, temporary_final)
                        with exclusive_job_files_lock(
                            output_srt.parent.parent,
                            output_srt.parent.name,
                            blocking=True,
                        ):
                            os.replace(temporary_final, output_srt)
                        return
                    finally:
                        temporary_final.unlink(missing_ok=True)
            checked_candidates = ", ".join(str(candidate) for candidate in candidates)
            raise FileNotFoundError(
                f"translation script did not produce {output_srt}; "
                f"checked candidates: {checked_candidates}"
            )

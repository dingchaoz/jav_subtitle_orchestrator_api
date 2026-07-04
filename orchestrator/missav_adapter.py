import json
import subprocess
import sys
from pathlib import Path


class MissAVAdapter:
    def __init__(self, missav_pipeline_root: Path) -> None:
        self.missav_pipeline_root = missav_pipeline_root

    def download_metadata(self, movie_number: str, output_path: Path) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        command = [
            sys.executable,
            str(self.missav_pipeline_root / "new-release" / "unified_download.py"),
        ]
        completed = subprocess.run(
            command,
            cwd=self.missav_pipeline_root,
            text=True,
            capture_output=True,
            check=False,
        )
        if completed.returncode != 0:
            raise RuntimeError(completed.stderr or completed.stdout)
        if not output_path.exists():
            output_path.write_text(
                json.dumps({"movie_number": movie_number}) + "\n",
                encoding="utf-8",
            )

    def download_audio(self, movie_number: str, output_path: Path) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")
        command = [
            sys.executable,
            str(self.missav_pipeline_root / "new-release" / "batch_audio_downloader.py"),
            "--output-dir",
            str(output_path.parent),
            "--max-downloads",
            "1",
            "--only-pending",
            "--direct-audio",
        ]
        completed = subprocess.run(
            command,
            cwd=self.missav_pipeline_root,
            text=True,
            capture_output=True,
            check=False,
        )
        if completed.returncode != 0:
            raise RuntimeError(completed.stderr or completed.stdout)
        if not tmp_path.exists() and output_path.exists():
            return
        tmp_path.replace(output_path)

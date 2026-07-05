# Codex Translation Batch Helper

The repository includes this helper:

```bash
scripts/run_codex_translation_batch.sh
```

It runs the existing Codex-based batch translator:

```text
/Users/ytt/Documents/Codex/2026-06-10/dingchaoz-jav-translate-https-github-com/work/jav_translate/translate_srts.py
```

This command is useful for batch translation where those Mac paths and volumes exist.

Important: the Windows worker currently calls `TRANSLATE_SCRIPT_PATH` as a per-job script with this interface:

```text
python <script> --input <Japanese.srt> --langs en --output-dir <directory>
```

The Codex batch helper uses a different interface:

```text
python3 translate_srts.py --source-root <directory> --output-root <directory> ...
```

So do not point `TRANSLATE_SCRIPT_PATH` directly at `translate_srts.py` unless a compatible wrapper is added. For the current Windows worker, keep `TRANSLATE_SCRIPT_PATH` pointed at the single-file translation script described in `docs/setup/windows.md`.

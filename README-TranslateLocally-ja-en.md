# TranslateLocally Japanese to English

## Install

- OS: macOS 15.5, Apple Silicon arm64, Apple M1
- RAM: 8 GB
- TranslateLocally: `/Applications/translateLocally.app/Contents/MacOS/translateLocally`
- Version: `translateLocally v0.0.2+136745e`
- Model: `Japanese-English tiny`, `ja-en-tiny`
- Model directory: `/Users/ytt/Library/Containers/com.translatelocally.translateLocally/Data/.config/translateLocally/jpn-eng`
- Wrapper: `/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/ja2en`
- Global wrapper symlink: `/opt/homebrew/bin/ja2en`
- HPLT repository metadata: `https://raw.githubusercontent.com/hplt-project/bitextor-mt-models/refs/heads/main/models.json`
- HPLT model archive: `https://object.pouta.csc.fi/HPLT-bitextor-models/jpn-eng.tar.gz`

The installed macOS build does not expose a CLI command for adding repositories. The HPLT repository entry was verified from `models.json`, the `ja-en-tiny` archive checksum was verified, and the extracted model was installed into TranslateLocally's local model scan directory.

## Commands

Translate from stdin:

```bash
echo "これはテストです。" | /Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator/ja2en
```

Or, if `/opt/homebrew/bin` is on `PATH`:

```bash
echo "これはテストです。" | ja2en
```

Translate a text file:

```bash
ja2en < input-ja.txt > output-en.txt
```

Translate an SRT while preserving cue numbers and timecodes:

```bash
python scripts/translate_srt_translatelocally.py input.Japanese.srt output.English.srt
```

On another machine, set the TranslateLocally executable path explicitly if it is
not in a standard location:

```bash
TRANSLATELOCALLY_PATH="/Applications/translateLocally.app/Contents/MacOS/translateLocally" \
python scripts/translate_srt_translatelocally.py input.Japanese.srt output.English.srt
```

Windows PowerShell:

```powershell
$env:TRANSLATELOCALLY_PATH = "C:\Path\To\translateLocally.exe"
python scripts\translate_srt_translatelocally.py input.Japanese.srt output.English.srt
```

Direct TranslateLocally command:

```bash
echo "明日は東京で会議があります。" | /Applications/translateLocally.app/Contents/MacOS/translateLocally --model ja-en-tiny
```

List installed models:

```bash
/Applications/translateLocally.app/Contents/MacOS/translateLocally --list-models
```

## Troubleshooting

- If a file path fails under macOS sandboxing, pipe through stdin and stdout instead of using `--input` and `--output`.
- If `ja2en` is not found from another directory, invoke it by full path or add `/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator` to `PATH`.
- The old macOS build lists HPLT models only after the repository is added through the GUI settings. The local model is already installed and works offline via `--model ja-en-tiny`.

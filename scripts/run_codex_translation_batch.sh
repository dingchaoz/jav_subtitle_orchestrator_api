#!/usr/bin/env bash
set -euo pipefail

cd "/Users/ytt/Documents/Codex/2026-06-10/dingchaoz-jav-translate-https-github-com/work/jav_translate"

python3 translate_srts.py \
  --source-root "/Volumes/Expansion/to_be_uploaded_srts" \
  --output-root "/Volumes/Expansion/translated srts" \
  --targets "zh-CN,en" \
  --provider anthropic-first \
  --anthropic-models "haiku" \
  --anthropic-recheck-minutes 30 \
  --codex-bin "/Users/ytt/Documents/Codex/2026-06-10/dingchaoz-jav-translate-https-github-com/work/codex-cli/codex-minimal" \
  --workers 5 \
  --resume

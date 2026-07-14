#!/usr/bin/env bash
set -euo pipefail

ROOT="/Users/ytt/Documents/startup/JAV-Subtitle-Orchestrator"
SOURCE_DOWNLOADER="$ROOT/deployment/launchd/com.javsubtitle.mac-worker.plist"
SOURCE_TRANSLATOR="$ROOT/deployment/launchd/com.javsubtitle.mac-translation-worker.plist"
LAUNCH_AGENTS="$HOME/Library/LaunchAgents"
DEST_DOWNLOADER="$LAUNCH_AGENTS/com.javsubtitle.mac-worker.plist"
DEST_TRANSLATOR="$LAUNCH_AGENTS/com.javsubtitle.mac-translation-worker.plist"
DOWNLOADER_LABEL="com.javsubtitle.mac-worker"
TRANSLATOR_LABEL="com.javsubtitle.mac-translation-worker"
DOMAIN="gui/$(id -u)"

plutil -lint -s "$SOURCE_DOWNLOADER"
plutil -lint -s "$SOURCE_TRANSLATOR"

mkdir -p "$LAUNCH_AGENTS" "$ROOT/logs"
install -m 0644 "$SOURCE_DOWNLOADER" "$DEST_DOWNLOADER"
install -m 0644 "$SOURCE_TRANSLATOR" "$DEST_TRANSLATOR"

if launchctl print "$DOMAIN/$DOWNLOADER_LABEL" >/dev/null 2>&1; then
    launchctl bootout "$DOMAIN/$DOWNLOADER_LABEL"
fi
launchctl bootstrap "$DOMAIN" "$DEST_DOWNLOADER"

if launchctl print "$DOMAIN/$TRANSLATOR_LABEL" >/dev/null 2>&1; then
    launchctl bootout "$DOMAIN/$TRANSLATOR_LABEL"
fi
launchctl bootstrap "$DOMAIN" "$DEST_TRANSLATOR"

printf 'label=%s\n' "$DOWNLOADER_LABEL"
launchctl print "$DOMAIN/$DOWNLOADER_LABEL" | awk '/state =|pid =/'
printf 'label=%s\n' "$TRANSLATOR_LABEL"
launchctl print "$DOMAIN/$TRANSLATOR_LABEL" | awk '/state =|pid =/'

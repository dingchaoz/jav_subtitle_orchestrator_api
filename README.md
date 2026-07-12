# JAV Subtitle Orchestrator

Standalone Mac + Windows orchestration service for queued JAV subtitle generation.

The design spec is in:

```text
docs/superpowers/specs/2026-07-04-jav-subtitle-orchestrator-design.md
```

Version 1 target:

- Mac hosts the API, job database, SMB job folder, downloads, Japanese-to-English translation, and the translation quality gate.
- Windows NVIDIA laptop polls the Mac API, reads audio over SMB, writes Japanese SRT, and hands the job back as `transcription_done`.
- SQLite stores queue state for one or many movie IDs.

## Version 1 Run Commands

Mac API:

```bash
python -m orchestrator api
```

Mac downloader worker:

```bash
python -m orchestrator mac-worker
```

Mac translation worker:

```bash
python -m orchestrator mac-translation-smoke-test
python -m orchestrator mac-translation-worker
```

Windows worker:

```powershell
python -m orchestrator windows-worker
```

Setup details:

- [Mac setup](docs/setup/mac.md)
- [Windows setup](docs/setup/windows.md)
- [SMB setup](docs/setup/smb.md)
- [TranslateLocally setup](docs/setup/translatelocally.md)
- [Codex translation batch helper](docs/setup/codex-translation.md)

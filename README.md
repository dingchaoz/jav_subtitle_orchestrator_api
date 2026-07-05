# JAV Subtitle Orchestrator

Standalone Mac + Windows orchestration service for queued JAV subtitle generation.

The design spec is in:

```text
docs/superpowers/specs/2026-07-04-jav-subtitle-orchestrator-design.md
```

Version 1 target:

- Mac hosts the API, job database, SMB job folder, metadata download, and audio download.
- Windows NVIDIA laptop polls the Mac API, reads audio over SMB, runs Japanese transcription, runs English SRT translation, and writes final SRT files back to the shared folder.
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

Windows worker:

```powershell
python -m orchestrator windows-worker
```

Setup details:

- [Mac setup](docs/setup/mac.md)
- [Windows setup](docs/setup/windows.md)
- [SMB setup](docs/setup/smb.md)
- [Codex translation batch helper](docs/setup/codex-translation.md)

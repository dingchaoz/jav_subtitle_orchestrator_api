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


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

## Metadata-resilient publication

After Windows transcription, the Mac publication path is:

```text
transcription_done
→ translating
→ quality gate
→ publish_pending
→ publishing
→ ensure public.movies (MissAV/local metadata or code-only placeholder)
→ upsert the English_AI SRT in Storage
→ upsert and verify movie_languages
→ english_srt_ready
```

A code-only `placeholder` catalog row is a successful publication result, not a
translation failure. It has a stable movie UUID and can be enriched later without
changing subtitle ownership. Publication retries preserve the quality-approved
English SRT and audio instead of translating again.

The repository migration and worker flow do not imply that the migration has been
deployed. Production RPC behavior remains unverified until the Task 10 deployment
and one-canary gate is explicitly approved and completed. See the
[Mac setup and deployment runbook](docs/setup/mac.md) for failure semantics,
dry-run planning, and approval boundaries.

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

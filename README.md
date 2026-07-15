# JAV Subtitle Orchestrator

Standalone Mac + Windows orchestration service for queued JAV subtitle generation.

The design spec is in:

```text
docs/superpowers/specs/2026-07-04-jav-subtitle-orchestrator-design.md
```

Version 1 target:

- Mac hosts the API, job database, SMB job folder, downloads, Japanese-to-English
  translation, and the translation quality gate.
- Windows NVIDIA laptop polls the Mac API, reads audio over SMB, writes Japanese
  SRT, and hands the job back as `transcription_done`.
- SQLite stores queue state for one or many movie IDs.

## Metadata-resilient publication

The post-download production path is:

```text
audio_ready → transcription_claimed → transcribing → transcription_done
→ translating → publish_pending → publishing
→ catalog_sync_pending → catalog_syncing → english_srt_ready
```

Windows owns the transcription states. The single Mac translation worker owns
translation, its quality gate, verified Supabase publication, and javsubtitle.com
catalog synchronization. `english_srt_ready` is the terminal production state only
after both remote systems verify successfully.

When `MAC_TRANSLATION_PUBLISH_ENABLED=false` (the default), the worker remains in
local-only compatibility mode. It skips `publish_pending`, `publishing`, catalog
resolution, Storage, `movie_languages`, `catalog_sync_pending`, and
`catalog_syncing`; `english_srt_ready` then means only that the local English SRT
passed the quality gate, not that Supabase or javsubtitle.com was verified.

A code-only `placeholder` catalog row is a successful publication result, not a
translation failure. It has a stable movie UUID and can be enriched later without
changing subtitle ownership when publication is enabled. Publication retries
preserve the quality-approved English SRT and audio instead of translating again;
catalog retries resume from `catalog_sync_pending` without republishing or
retranslating.

The repository migration and worker flow do not imply that the migration has been
deployed. Production behavior remains unverified until a separately approved
deployment and one-canary gate are completed. See the
[Mac setup and deployment runbook](docs/setup/mac.md) for failure semantics,
exact audio recovery, immutable dry-run plans, the normal-first historical
controller, launchd operations, pause/resume, rollback, and approval boundaries.

The historical repair allowlist currently has 340 lines, but that is input evidence,
not an affected-job count. The authoritative affected count is `eligible_total` in a
fresh `plan-historical-repair-batch` dry-run; report its `already_repaired`,
`ineligible`, and `blocked` totals alongside it.

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
- [Codex translation batch helper](docs/setup/codex-translation.md)
- [TranslateLocally setup](docs/setup/translatelocally.md)
- [Cloudflare tunnel setup](docs/setup/cloudflare-tunnel.md)

# Dashboard Restoration Design

## Goal

Restore the existing operator dashboard, Supabase subtitle-audit visibility, and
read-only remediation planning on the current Windows-transcription/Mac-translation
branch without restoring any obsolete worker or translation path.

## Access model

- `https://orchestrator.javsubtitle.com/dashboard` remains behind the existing
  Cloudflare Access login and Cloudflare Tunnel.
- The tunnel continues forwarding to the Mac API at `127.0.0.1:8010`.
- Local access at `http://127.0.0.1:8010/dashboard` remains available without an
  additional application login because it does not pass through Cloudflare Access.
- No credentials, Access tokens, Supabase secrets, or subtitle text are embedded in
  dashboard HTML or client-side JavaScript.

## Restoration strategy

Selectively port the established dashboard implementation from
`codex/subtitle-quality-audit-remediation`. Do not merge that branch wholesale.
Bring over only the dashboard read models, bounded log helpers, HTML renderer,
dashboard API routes, read-only subtitle-audit API, required configuration, and
their tests. Adapt those components to the current store and models.

The current orchestration state machine remains authoritative:

`audio_ready -> transcription_claimed/transcribing -> transcription_done -> translating -> english_srt_ready`

The dashboard must display `transcription_done` and `translating`, show the Windows
transcription worker and Mac translation worker separately, and never infer English
readiness from file existence alone.

## Dashboard behavior

Restore the previous operator experience:

- overall API and worker health;
- status counts, recent jobs, active work, and errors;
- searchable/paginated job detail;
- bounded, allowlisted job log viewing;
- subtitle-quality summary cards and finding filters;
- existing safe job submission controls that always use `force=false`.

The page must remain useful when Supabase audit configuration is unavailable. In
that case, audit cards show an unavailable state while job and worker views remain
functional.

## Remediation safety

Historical remediation remains read-only. The dashboard may display affected jobs
and the actions a repair would take, but it must not expose or call apply, force,
requeue, delete, quarantine, upload, overwrite, or Supabase mutation operations.
The existing CLI planner remains dry-run only with an explicit allowlist and limit.

## Data and error handling

- Existing SQLite jobs remain unchanged; no production migration or reset is part
  of restoration unless a compatibility test proves a read-only schema addition is
  required.
- Dashboard log endpoints retain path traversal, symlink, size, and allowlist
  protections from the previous implementation.
- Audit endpoint failures return a concise unavailable response and do not break
  `/dashboard`, `/dashboard/state`, job routes, or worker routes.
- Dashboard responses and logs never include full subtitle text.

## Verification

Implementation is accepted only when:

1. Ported dashboard state, log-security, API, and audit tests pass.
2. The complete test suite passes.
3. Existing transcription-complete and legacy-complete behavior remains intact.
4. `http://127.0.0.1:8010/dashboard` renders locally in a browser.
5. `https://orchestrator.javsubtitle.com/dashboard` redirects unauthenticated users
   to Cloudflare Access and renders after the existing approved login.
6. No job is requeued, no subtitle is uploaded or overwritten, and no audio is
   deleted during verification.

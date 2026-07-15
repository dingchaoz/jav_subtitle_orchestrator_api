# Historical Batch Lock-Safety Design

## Scope

Harden historical repair planning/enqueue so large audio files never create a
long SQLite lock, while every selected repair has a true audio preservation
hash. Production repair, upload, deletion, and requeue remain out of scope.

## Lock and snapshot model

Planning and enqueue use three phases without a root-lock upgrade:

1. Under jobs-root EX, retain bounded allowlist descriptors, fully hash bounded
   Japanese/English SRT files, and scan every allowlisted WAV with at most
   `MAX_AUDIO_PROBE_BYTES`. `audio_probe_snapshot_sha256` fingerprints validated
   structure, selected samples, stat metadata, and duration for eligibility and
   deterministic counts. It is never a preservation proof.
2. Release EX. Under jobs-root SH and one selected job SH at a time, read every
   byte of `audio.wav` for only the selected `<= limit` items. Persist that true
   content digest as `audio_sha256`. No SQLite transaction is open; unrelated
   normal writers can take their own job EX lock.
3. Release SH, reacquire jobs-root EX, repeat the complete bounded scan, and
   compare each selected dev/inode/ctime/size/mtime identity with its full-hash
   snapshot. Then use one short SQLite snapshot/transaction to validate the
   selection and insert records. Root EX remains held through commit.

No job file is read after `BEGIN IMMEDIATE`; only the retained allowlist fd may
be reread, capped at 1 MiB. The allowlist parent/file descriptors remain pinned
through the final commit. Any filesystem, allowlist, selection, or database
identity change fails with `historical_plan_changed` and is safe to retry.

## Persistent schema

Strict plan format version 3 and `historical_translation_repairs` both carry:

- `audio_probe_snapshot_sha256`: bounded scan fingerprint only;
- `audio_sha256`: true byte-for-byte preservation hash for the selected item;
- `source_english_sha256`: immutable pre-repair English identity.

Legacy content hashes are never relabeled as probes, and legacy probe-only rows
never masquerade as preservation hashes. Runnable rows missing either identity
become `permanent_failed` with
`migration_audio_probe_snapshot_unavailable` or
`migration_audio_content_sha256_unavailable`. Missing source English identity
uses `migration_source_english_unavailable`. IDs, batch/job identity, timestamps,
foreign keys, uniqueness, and indexes are preserved transactionally.

## Private plan writer

After linking the final file, removing the temporary link, and syncing the held
parent directory, the writer performs a final parent-path and target
inode/type/mode check. Failure cleanup uses the held parent fd and removes only
the inode created by the invocation.

## Verification

Tests prove a non-selected 32 GiB sparse WAV receives only bounded probe reads;
only selected records up to the batch limit receive full-content reads; an
unrelated normal writer proceeds during selected hashing; sampled audio mutation
is rejected even if mtime is restored; SQLite writers proceed during prescan;
and migration, replay identity, allowlist, transaction, descriptor, and private
plan safety contracts remain green.

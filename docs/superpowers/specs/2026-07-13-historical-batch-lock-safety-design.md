# Historical Batch Lock-Safety Design

## Scope

Harden historical repair planning/enqueue so large audio files never create a
long SQLite lock, legacy repair rows have strict immutable identity, and private
plan output closes its final in-function parent-swap window. Production repair,
upload, deletion, and requeue behavior is out of scope.

## Lock and snapshot model

Both plan and enqueue use this order:

1. Acquire the jobs-root exclusive cooperative lock.
2. Open the bounded allowlist through an `O_NOFOLLOW` directory chain, retain
   its parent/file descriptors through commit, and build a complete bounded
   filesystem view from its initial exact bytes.
3. Fully hash Japanese/English SRT files, each capped by
   `MAX_SUBTITLE_BYTES`.
4. Validate audio as RIFF/WAVE using bounded positioned reads and seeks. Persist
   a canonical metadata fingerprint named `audio_snapshot_sha256`; it includes
   size, mtime, validated format/data offsets and sizes, and derived duration,
   but never claims to be an audio content hash.
5. Recheck the runtime inode/stat bindings.
6. Revalidate the allowlist path/inode/stat, then begin the short SQLite
   transaction, read jobs and repair rows once, and recompute the plan from the
   prescanned view. Immediately before insert, exactly reread and hash only the
   bounded allowlist through its retained fd.
7. Commit the SQLite transaction before releasing the jobs-root lock.

No job-file read, hash, stat, or seek occurs after `BEGIN IMMEDIATE`; the sole
exception is the explicitly bounded allowlist exact verification (at most 1
MiB). Windows/API SQLite writers remain unblocked throughout the full job-file
scan. Cooperative Mac file writers remain blocked by the root lock until the
short transaction commits, preventing a stale file snapshot from being
accepted.

Planning uses the same root-first order and an explicit short read transaction,
so its jobs and repair rows come from one SQLite snapshot.

## Persistent schema

Plan format version 2 replaces `audio_sha256` with
`audio_snapshot_sha256`. Reports expose deterministic `scan_entries` and
`audio_probe_max_bytes`; wall-clock duration is not part of a replayable plan.

`historical_translation_repairs` is rebuilt transactionally when its legacy
shape is detected. The new table has both `source_english_sha256` and
`audio_snapshot_sha256` as `NOT NULL`. A legacy non-null English hash is copied
to the immutable source field. Missing legacy source identity receives the
all-zero valid digest sentinel. Runnable rows lacking source identity become
`permanent_failed` with `migration_source_english_unavailable`. Legacy audio
content hashes are not relabeled as metadata fingerprints; runnable rows that
only have the old audio field become permanent failures with
`migration_audio_snapshot_unavailable`. Existing IDs, batch/job identity,
timestamps, foreign keys, uniqueness, and indexes are preserved.

## Private plan writer

After linking the final file, removing the temporary link, and syncing the held
parent directory, the writer performs one last parent-path binding check plus a
target inode/type/mode check. If it fails, cleanup through the held parent fd
removes only the target with the inode created by this invocation. A rename
after the function returns is outside the function's control; the last window
inside the function is closed.

## Verification

Tests instrument audio probe reads against a sparse tens-of-gigabytes file,
prove SQLite writers can commit during the prescan, assert no job-file calls
occur inside the write transaction, bound the one allowlist reread, verify
explicit read-snapshot planning, exercise real legacy table rebuilds, and
trigger the exact parent swap after the second binding check. Existing
idempotency, tamper rejection, bounded file descriptor, lock order, and full
test suites remain green.

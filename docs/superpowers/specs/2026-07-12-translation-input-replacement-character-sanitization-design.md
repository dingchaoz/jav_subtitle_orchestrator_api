# TranslateLocally Replacement-Character Input Sanitization Design

## Goal

Prevent Unicode replacement characters already present in a Japanese SRT from
propagating through TranslateLocally into the English SRT, while preserving the
original Japanese SRT byte-for-byte and retaining the existing server-side quality
gate as the final publication boundary.

This change is narrowly scoped to translation input preparation. It does not weaken
the quality gate, change its thresholds, retry permanent quality failures, modify
Japanese subtitles, delete audio, or broaden historical-repair authorization.

## Root cause and evidence

The rejected `ugug-059` English SRT contained six U+FFFD replacement characters.
Boundary tracing established that:

- the Japanese SRT already contained exactly six U+FFFD characters on six lines;
- the TranslateLocally process returned valid UTF-8, so subprocess decoding did not
  introduce the characters;
- each affected line reproduced the same output when translated independently;
- removing only U+FFFD from the temporary model input eliminated the replacement
  character from all six translations;
- no affected line became empty after removal.

The remaining authorized historical batch contains one U+FFFD in `awd-148`, none in
`same-057`, and one in `jame-003`. These counts are operational metadata only; no
subtitle text is recorded.

## Selected design

Add a small input-sanitization function in
`scripts/translate_srt_translatelocally.py`. For each translatable SRT text line it
will:

1. count and remove U+FFFD before the line is sent to TranslateLocally;
2. leave every other character unchanged;
3. reject the translation before invoking the model if a non-empty source line
   becomes empty after sanitization;
4. return the sanitized lines plus aggregate statistics needed for safe logging.

The sanitizer operates only on the in-memory `source_text` list. It must never write
the sanitized text back to the Japanese SRT. The existing SRT renderer continues to
copy cue numbers and timestamps from the original source and inserts only translated
English lines into the output.

The empty-after-sanitization error must identify a structured condition and line
number without including the source text. A suitable message is
`translation_input_corrupt: line <n> empty after removing replacement characters`.

## Logging and privacy

The batch statistics log may add two integer-only fields:

- `input_replacement_character_count`
- `sanitized_input_line_count`

No log may contain Japanese or English subtitle text. Existing quality logs remain
unchanged and continue to contain metrics, hashes, pass/fail, and structured reason
codes only.

## Quality and publication behavior

The server-side Japanese/English quality gate remains mandatory and unchanged. The
worker sequence stays:

1. preserve or quarantine the prior English SRT;
2. translate from the unchanged Japanese SRT using sanitized in-memory model input;
3. validate the complete Japanese/English SRT pair;
4. upload to Supabase only when the quality report passes;
5. verify Storage bytes and catalog metadata before marking
   `english_srt_ready`.

If sanitization fails, TranslateLocally is not invoked and Supabase is not contacted.
If the resulting English SRT still contains three or more replacement characters,
the existing deterministic `encoding_corruption` quality failure remains permanent.

## Tests

Test-driven implementation must prove:

- U+FFFD is removed from model input while all other characters are preserved;
- the original Japanese SRT bytes remain unchanged;
- aggregate sanitization statistics contain counts but no subtitle text;
- a line containing only U+FFFD fails before TranslateLocally is invoked;
- normal input without U+FFFD is unchanged;
- existing quality-gate, worker, Supabase no-upload-on-failure, quarantine, audio
  preservation, repaired upsert, and no-retry tests continue passing.

The regression test must be observed failing before production code is changed, then
passing after the minimal implementation.

## Deployment and authorized batch continuation

After focused and full tests pass:

1. commit and merge the fix into
   `codex/windows-transcription-mac-translation`;
2. install the editable package and restart the Mac API if required by the deployed
   module set;
3. run the fixed ten-sentence translation smoke test and require exit zero;
4. continue only the already selected `awd-148`, `same-057`, and `jame-003` jobs,
   one exact-job worker at a time;
5. stop immediately if any job fails;
6. independently verify Japanese preservation, audio preservation or continued
   absence, rejected-English retention, quality pass, Supabase Storage SHA-256, and
   catalog metadata before starting the next job;
7. restore the normal Mac translation worker after the three-job continuation ends.

`ugug-059` remains permanently failed and is not retried under this authorization.
No sixth or replacement historical task is authorized.

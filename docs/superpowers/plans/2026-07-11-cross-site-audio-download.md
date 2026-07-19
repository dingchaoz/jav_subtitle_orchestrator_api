# Cross-Site Audio Download Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a standalone CLI that resolves Jable or BestJAVPorn movie pages and writes the complete soundtrack as a validated 16 kHz mono PCM WAV.

**Architecture:** Use a lightweight `curl_cffi` resolver first for Jable and a persistent, visible Chrome context for Cloudflare or cross-frame players. Validate the resolved VOD HLS graph before passing a narrowly scoped request to ffmpeg, then validate the temporary WAV with ffprobe before publishing it atomically.

**Tech Stack:** Python 3.11+, curl-cffi, Playwright with system Google Chrome, ffmpeg, ffprobe, pytest.

---

### Task 1: Public contracts and CLI

- [ ] Add failing tests for supported provider URLs, default output naming, CLI arguments, and exit-code mapping.
- [ ] Implement immutable stream/request models, typed pipeline errors, provider detection, and the thin `scripts/download_site_audio.py` entrypoint.
- [ ] Run the focused tests, the complete suite, and commit.

### Task 2: HLS validation and Jable resolution

- [ ] Add failing fixture tests for inline signed `hlsUrl`, Cloudflare fallback, master selection, relative URIs, AES-128, ENDLIST, DRM rejection, and unsafe network targets.
- [ ] Implement Chrome-impersonated Jable requests, playlist traversal, duration calculation, and public HTTPS validation.
- [ ] Run the focused tests, the complete suite, and commit.

### Task 3: Cross-frame browser resolution

- [ ] Add failing tests for primary-player frame selection, manifest event capture, ad rejection, unavailable pages, timeout handling, and hostname-scoped cookies.
- [ ] Implement a persistent Playwright Chrome resolver that waits for manual challenges and never automates CAPTCHA interaction.
- [ ] Run the focused tests, the complete suite, and commit.

### Task 4: Download, validation, and retry pipeline

- [ ] Add failing tests for the ffmpeg argument vector, restricted protocols, header scoping, partial-file cleanup, ffprobe validation, duration tolerance, atomic replacement, and one manifest refresh retry.
- [ ] Implement the ffmpeg/ffprobe runner and orchestration pipeline.
- [ ] Run the focused tests, the complete suite, and commit.

### Task 5: Documentation and live acceptance

- [ ] Document installation, dedicated Chrome profile behavior, first-run manual verification, CLI examples, output guarantees, and exit codes.
- [ ] Run all tests and static checks.
- [ ] Download the complete Jable acceptance movie into a temporary directory and verify its format and duration with ffprobe.
- [ ] Verify the unavailable BestJAVPorn sample returns the source-unavailable exit code; defer successful full-site acceptance until a currently playable URL is supplied.


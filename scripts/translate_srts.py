#!/usr/bin/env python3
"""Translate Japanese SRT files with local Codex CLI."""

from __future__ import annotations

import argparse
import errno
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable


DEFAULT_SOURCE_ROOT = Path("/Volumes/Expansion 1/to_be_uploaded_srts")
DEFAULT_OUTPUT_ROOT = Path("/Volumes/Expansion 1/translated srts")
DEFAULT_TARGETS = ("zh-CN", "en")
DEFAULT_KIMI_BASE_URL = "https://api.moonshot.ai/v1"
DEFAULT_KIMI_CLI_BASE_URL = "https://api.kimi.com/coding/v1"
DEFAULT_KIMI_CLI_MODEL = "kimi-for-coding"
DEFAULT_ANTHROPIC_MODEL = "haiku"
DEFAULT_ANTHROPIC_SETTING_SOURCES = "project,local"
SCHEMA_PATH = Path(__file__).resolve().with_name("codex_translation.schema.json")
MULTI_SCHEMA_PATH = Path(__file__).resolve().with_name("codex_multi_translation.schema.json")
TAG_SCHEMA_PATH = Path(__file__).resolve().with_name("codex_tags.schema.json")
METADATA_INDEX_LOCK = threading.Lock()

TARGET_LANGUAGE_NAMES = {
    "zh-CN": "Simplified Chinese",
    "en": "English",
}

JAPANESE_RE = re.compile(r"[\u3040-\u30ff\u3400-\u4dbf\u4e00-\u9fff々〆〤ー]")
TIMING_RE = re.compile(
    r"^\d{2}:\d{2}:\d{2},\d{3}\s+-->\s+\d{2}:\d{2}:\d{2},\d{3}(?:\s+.*)?$"
)
MINOR_CODED_TERMS = {
    "少女",
    "幼女",
    "未成年",
    "児童",
    "女子高生",
    "女子中学生",
    "小学生",
    "中学生",
    "高校生",
    "jk",
    "jc",
    "js",
    "schoolgirl",
    "minor",
    "underage",
    "child",
    "children",
    "teen",
    "teenager",
    "loli",
}
BOILERPLATE_TEXTS = {
    "ご視聴ありがとうございました",
}
REPEATED_VOCALIZATION_RE = re.compile(
    r"^(?P<token>ハッ|あ|ア|ぁ|ァ|ん|ン|う|ウ|お|オ|は|ハ|く|ク|ふ|フ|ひ|ヒ|嗯|啊|哈)"
    r"(?:(?:[、,。.\s]*)(?P=token)){2,}[、,。.\s]*$"
)
LONG_WAVE_RE = re.compile(r"([あアぁァんンうウおオ嗯啊哈])(?:[~～ー−-]){2,}")
REPEATED_JA_PHRASE_RE = re.compile(r"(?P<token>[\u3040-\u30ff\u3400-\u4dbf\u4e00-\u9fff々〆〤ー]{2,6}?)(?P=token){5,}")
VOCALIZATION_ONLY_RE = re.compile(
    r"^(?:ハッ|あ|ア|ぁ|ァ|ん|ン|う|ウ|お|オ|は|ハ|く|ク|ふ|フ|ひ|ヒ|嗯|啊|哈)"
    r"(?:[、,。.\s]*(?:ハッ|あ|ア|ぁ|ァ|ん|ン|う|ウ|お|オ|は|ハ|く|ク|ふ|フ|ひ|ヒ|嗯|啊|哈))*[、,。.\s]*$"
)
EN_REPEATED_VOCALIZATION_RE = re.compile(
    r"^(?P<token>ah|ha|mm|oh|uh|um|hm)(?:(?:[,.\s]+)(?P=token)){2,}[,.!\s]*$",
    re.IGNORECASE,
)
ZH_SEPARATED_REPEAT_RE = re.compile(r"^(?P<token>[\u4e00-\u9fff]{1,4})(?:[，、,\s]*(?P=token)){2,}(?P<tail>[啊呀呢吧嘛]*[。.!！]*)$")
ZH_COMPACT_REPEAT_RE = re.compile(r"^(?P<token>[\u4e00-\u9fff]{1,4})(?P=token){2,}(?P<tail>[啊呀呢吧嘛]*[。.!！]*)$")
SEPARATED_TOKEN_RUN_RE = re.compile(r"(?P<token>[A-Za-z0-9\u4e00-\u9fff]{1,6})(?P<sep>[，、,\s]+)(?P=token)(?:(?:[，、,\s]+)(?P=token)){2,}")


@dataclass(frozen=True)
class ProcessResult:
    target: str
    source_path: Path
    target_path: Path
    action: str
    cue_count: int
    japanese_cue_count: int


class SubtitleError(ValueError):
    """Raised when an SRT file cannot be parsed safely."""


class ProviderQuotaError(RuntimeError):
    """Raised when a provider has no usable quota or balance."""


class ProviderRateLimitError(RuntimeError):
    """Raised when a provider asks the caller to slow down."""

    def __init__(self, message: str, retry_after_seconds: int | None = None):
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


class ProviderTransientError(RuntimeError):
    """Raised for provider errors that may succeed later."""


class ProviderResponseError(RuntimeError):
    """Raised when a provider returns malformed or unusable output."""


def log(message: str) -> None:
    print(message, flush=True)


@dataclass(frozen=True)
class SrtCue:
    number: str
    timing: str
    text_lines: tuple[str, ...]

    @property
    def text(self) -> str:
        return "\n".join(self.text_lines)


@dataclass(frozen=True)
class SrtDocument:
    cues: tuple[SrtCue, ...]
    line_ending: str
    has_bom: bool
    trailing_newline_count: int


def detect_line_ending(text: str) -> str:
    if "\r\n" in text:
        return "\r\n"
    if "\r" in text:
        return "\r"
    return "\n"


def decode_srt(data: bytes) -> tuple[str, bool]:
    has_bom = data.startswith(b"\xef\xbb\xbf")
    try:
        return data.decode("utf-8-sig"), has_bom
    except UnicodeDecodeError as exc:
        raise SubtitleError(f"SRT must be UTF-8 or UTF-8-BOM: {exc}") from exc


def parse_srt(data: bytes) -> SrtDocument:
    text, has_bom = decode_srt(data)
    line_ending = detect_line_ending(text)
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    trailing_newline_count = len(normalized) - len(normalized.rstrip("\n"))
    body = normalized.strip("\n")
    if not body.strip():
        return SrtDocument((), line_ending, has_bom, trailing_newline_count)

    cues: list[SrtCue] = []
    for block_no, block in enumerate(re.split(r"\n[ \t]*\n", body), start=1):
        lines = block.split("\n")
        if len(lines) < 2:
            raise SubtitleError(f"Invalid SRT block {block_no}: missing timing line")
        number = lines[0].strip()
        timing = lines[1].strip()
        if not number.isdigit():
            raise SubtitleError(f"Invalid SRT block {block_no}: cue number is not numeric")
        if not TIMING_RE.match(timing):
            raise SubtitleError(f"Invalid SRT block {block_no}: bad timing line")
        cues.append(SrtCue(number=number, timing=timing, text_lines=tuple(lines[2:])))

    return SrtDocument(tuple(cues), line_ending, has_bom, trailing_newline_count)


def rebuild_srt(document: SrtDocument, translations: dict[int, str] | None = None) -> bytes:
    translations = translations or {}
    blocks: list[str] = []
    for idx, cue in enumerate(document.cues, start=1):
        translated_text = translations.get(idx)
        text_lines = tuple(translated_text.split("\n")) if translated_text is not None else cue.text_lines
        blocks.append("\n".join((cue.number, cue.timing, *text_lines)))

    normalized = "\n\n".join(blocks)
    if normalized and document.trailing_newline_count:
        normalized += "\n" * document.trailing_newline_count
    output = normalized.replace("\n", document.line_ending)
    data = output.encode("utf-8")
    return (b"\xef\xbb\xbf" + data) if document.has_bom else data


def clean_subtitle_text(text: str) -> str | None:
    normalized = re.sub(r"\s+", " ", text).strip()
    if not normalized:
        return None
    if normalized in BOILERPLATE_TEXTS:
        return None

    normalized = LONG_WAVE_RE.sub(r"\1～", normalized)
    normalized = REPEATED_JA_PHRASE_RE.sub(lambda match: match.group("token") * 2, normalized)
    match = REPEATED_VOCALIZATION_RE.match(normalized)
    if match:
        token = match.group("token")
        separator = "、" if re.search(r"[、,。.\s]", normalized) else ""
        return f"{token}{separator}{token}"
    return normalized


def clean_translated_text(text: str) -> str | None:
    normalized = re.sub(r"\s+", " ", text).strip()
    if not normalized:
        return None

    normalized = re.sub(r"([啊哈嗯呃哦呀哎唔]|[A-Za-z])(?:[~～ー−-]){2,}", r"\1~", normalized)
    normalized = SEPARATED_TOKEN_RUN_RE.sub(lambda match: f"{match.group('token')}{match.group('sep')}{match.group('token')}", normalized)

    en_match = EN_REPEATED_VOCALIZATION_RE.match(normalized)
    if en_match:
        token = en_match.group("token")
        return f"{token.capitalize()}, {token.casefold()}."

    separated_match = ZH_SEPARATED_REPEAT_RE.match(normalized)
    if separated_match:
        token = separated_match.group("token")
        tail = separated_match.group("tail") or ""
        return f"{token}，{token}{tail}"

    compact_match = ZH_COMPACT_REPEAT_RE.match(normalized)
    if compact_match:
        token = compact_match.group("token")
        tail = compact_match.group("tail") or ""
        return f"{token}，{token}{tail}"

    return normalized


def clean_document(document: SrtDocument) -> SrtDocument:
    cleaned_cues: list[SrtCue] = []
    previous_vocalization: str | None = None
    consecutive_vocalization_count = 0
    for cue in document.cues:
        cleaned_text = clean_subtitle_text(cue.text)
        if cleaned_text is None:
            continue
        if VOCALIZATION_ONLY_RE.match(cleaned_text):
            if cleaned_text == previous_vocalization:
                consecutive_vocalization_count += 1
            else:
                previous_vocalization = cleaned_text
                consecutive_vocalization_count = 1
            if consecutive_vocalization_count > 2:
                continue
        else:
            previous_vocalization = None
            consecutive_vocalization_count = 0
        cleaned_cues.append(
            SrtCue(
                number=str(len(cleaned_cues) + 1),
                timing=cue.timing,
                text_lines=tuple(cleaned_text.split("\n")),
            )
        )
    return SrtDocument(
        cues=tuple(cleaned_cues),
        line_ending=document.line_ending,
        has_bom=document.has_bom,
        trailing_newline_count=document.trailing_newline_count,
    )


def apply_translations_and_clean(document: SrtDocument, translations: dict[int, str]) -> SrtDocument:
    cleaned_cues: list[SrtCue] = []
    for idx, cue in enumerate(document.cues, start=1):
        if idx in translations:
            cleaned_text = clean_translated_text(translations[idx])
            if cleaned_text is None:
                continue
            text_lines = tuple(cleaned_text.split("\n"))
        else:
            text_lines = cue.text_lines
        cleaned_cues.append(
            SrtCue(
                number=str(len(cleaned_cues) + 1),
                timing=cue.timing,
                text_lines=text_lines,
            )
        )
    return SrtDocument(
        cues=tuple(cleaned_cues),
        line_ending=document.line_ending,
        has_bom=document.has_bom,
        trailing_newline_count=document.trailing_newline_count,
    )


def contains_japanese(text: str) -> bool:
    return bool(JAPANESE_RE.search(text))


def retry_interrupted(call: Callable[[], object], *, label: str, attempts: int = 5) -> object:
    for attempt in range(1, attempts + 1):
        try:
            return call()
        except OSError as exc:
            if getattr(exc, "errno", None) != errno.EINTR or attempt == attempts:
                raise
            log(f"retry-interrupted\t{label}\tattempt={attempt + 1}/{attempts}")
            time.sleep(min(0.25 * attempt, 1.0))
    raise RuntimeError("unreachable")


def source_exists(path: Path) -> bool:
    return bool(retry_interrupted(path.exists, label=f"exists {path}"))


def scan_srt_tree(source_root: Path) -> list[Path]:
    found: list[Path] = []
    pending = [source_root]
    while pending:
        directory = pending.pop()

        def scan_current() -> tuple[list[Path], list[Path]]:
            files: list[Path] = []
            dirs: list[Path] = []
            with os.scandir(directory) as entries:
                for entry in entries:
                    entry_path = Path(entry.path)
                    if entry.is_dir(follow_symlinks=False):
                        dirs.append(entry_path)
                    elif entry.is_file(follow_symlinks=False) and entry_path.suffix.lower() == ".srt":
                        files.append(entry_path)
            return files, dirs

        files, dirs = retry_interrupted(scan_current, label=f"scandir {directory}")  # type: ignore[assignment]
        found.extend(files)
        pending.extend(sorted(dirs, reverse=True))
    return sorted(found)


def iter_srt_files(source_root: Path) -> Iterable[Path]:
    yield from scan_srt_tree(source_root)


def load_file_list(file_list: Path, source_root: Path) -> list[Path]:
    root = source_root.expanduser().resolve()
    files: list[Path] = []
    for line_no, raw_line in enumerate(file_list.expanduser().read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        candidate = Path(line).expanduser()
        resolved = candidate.resolve() if candidate.is_absolute() else (root / candidate).resolve()
        try:
            resolved.relative_to(root)
        except ValueError as exc:
            raise ValueError(f"file-list line {line_no} is outside source root: {line}") from exc
        if resolved.suffix.lower() != ".srt":
            raise ValueError(f"file-list line {line_no} is not an SRT file: {line}")
        if not resolved.is_file():
            raise ValueError(f"file-list line {line_no} does not exist: {line}")
        files.append(resolved)
    return files


def target_path_for(source_file: Path, source_root: Path, output_root: Path, target: str) -> Path:
    relative = source_file.relative_to(source_root)
    return output_root / target / relative


def ensure_safe_roots(source_root: Path, output_root: Path) -> tuple[Path, Path]:
    source = source_root.expanduser().resolve()
    output = output_root.expanduser().resolve()
    if source == output:
        raise ValueError("output root must not equal source root")
    if output.is_relative_to(source):
        raise ValueError("output root must not be inside source root")
    return source, output


def build_translation_prompt(cues: list[tuple[int, str]], target: str) -> str:
    target_name = TARGET_LANGUAGE_NAMES.get(target, target)
    payload = [{"id": cue_id, "text": text} for cue_id, text in cues]
    return (
        "Translate Japanese subtitle cue text to "
        f"{target_name}. Preserve line breaks inside each cue. "
        "Return only JSON matching the provided schema. Do not add commentary.\n\n"
        f"Input cues:\n{json.dumps(payload, ensure_ascii=False)}"
    )


def build_multi_translation_prompt(cues: list[tuple[int, str]], targets: tuple[str, ...]) -> str:
    payload = [{"id": cue_id, "text": text} for cue_id, text in cues]
    target_names = ", ".join(f"{target} ({TARGET_LANGUAGE_NAMES.get(target, target)})" for target in targets)
    return (
        "Translate Japanese subtitle cue text to all requested target languages in one pass. "
        f"Targets: {target_names}. Preserve line breaks inside each cue. "
        "Return only JSON matching the provided schema. Do not add commentary. "
        "Each translation item must include id plus one string field for every requested target code.\n\n"
        f"Input cues:\n{json.dumps(payload, ensure_ascii=False)}"
    )


def parse_codex_json_output(stdout: str, required_key: str) -> dict:
    decoder = json.JSONDecoder()
    matches: list[dict] = []
    for index, char in enumerate(stdout):
        if char != "{":
            continue
        try:
            value, _end = decoder.raw_decode(stdout[index:])
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and required_key in value:
            matches.append(value)
    if not matches:
        raise ValueError(f"model output did not contain JSON object with key {required_key!r}")
    return matches[-1]


def load_kimi_api_key(path: Path | None) -> str | None:
    for name in ("KIMI_API_KEY", "MOONSHOT_API_KEY"):
        env_key = os.environ.get(name)
        if env_key and env_key.strip():
            return env_key.strip()
    if not path:
        default_env = Path.cwd() / ".env"
        if not default_env.exists():
            return None
        path = default_env

    content = path.expanduser().read_text(encoding="utf-8").strip()
    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if line.startswith(("KIMI_API_KEY=", "MOONSHOT_API_KEY=")):
            value = line.split("=", 1)[1].strip()
            return value.strip("\"'")
    return content.strip("\"'") if content else None


def _parse_retry_after(headers: object) -> int | None:
    for key in ("Retry-After", "retry-after", "X-RateLimit-Reset", "x-ratelimit-reset"):
        try:
            value = headers.get(key)  # type: ignore[attr-defined]
        except AttributeError:
            value = None
        if not value:
            continue
        text = str(value).strip()
        if text.isdigit():
            seconds = int(text)
            now = int(time.time())
            return max(seconds - now, 0) if seconds > now + 60 else seconds
    return None


def _extract_available_balance(payload: dict) -> float | None:
    candidates = [payload]
    if isinstance(payload.get("data"), dict):
        candidates.append(payload["data"])
    for item in candidates:
        for key in ("available_balance", "balance", "cash_balance", "voucher_balance"):
            value = item.get(key)
            if value is None:
                continue
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
    return None


def quota_like_error(exc: Exception) -> bool:
    text = str(exc).casefold()
    return any(
        part in text
        for part in (
            "quota",
            "rate limit",
            "429",
            "403",
            "limit",
            "insufficient",
            "balance",
            "access_terminated",
            "overloaded",
            "authentication",
            "auth",
        )
    )


class KimiClient:
    def __init__(self, *, api_key: str, base_url: str = DEFAULT_KIMI_BASE_URL):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")

    def _request_json(self, path: str, *, method: str, payload: dict | None = None, timeout: int = 600) -> dict:
        url = f"{self.base_url}{path}"
        data = None if payload is None else json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(
            url,
            data=data,
            method=method,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            retry_after = _parse_retry_after(exc.headers)
            try:
                body = exc.read().decode("utf-8", errors="replace")
            except Exception:
                body = ""
            message = f"Kimi HTTP {exc.code}: {body or exc.reason}"
            if exc.code == 429:
                raise ProviderRateLimitError(message, retry_after_seconds=retry_after) from exc
            if exc.code in {401, 402, 403}:
                raise ProviderQuotaError(message) from exc
            if exc.code in {500, 502, 503, 504}:
                raise ProviderTransientError(message) from exc
            raise ProviderResponseError(message) from exc
        except urllib.error.URLError as exc:
            raise ProviderTransientError(f"Kimi request failed: {exc}") from exc
        except json.JSONDecodeError as exc:
            raise ProviderResponseError(f"Kimi response was not JSON: {exc}") from exc

    def chat_json(self, prompt: str, required_key: str, *, model: str, timeout: int) -> dict:
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": "Return JSON only."},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.2,
        }
        response = self._request_json("/chat/completions", method="POST", payload=payload, timeout=timeout)
        try:
            content = response["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError) as exc:
            raise ProviderResponseError("Kimi response missing choices[0].message.content") from exc
        try:
            return parse_codex_json_output(str(content), required_key)
        except ValueError as exc:
            raise ProviderResponseError(str(exc)) from exc

    def translate(self, cues: list[tuple[int, str]], target: str, *, model: str, timeout: int) -> dict[int, str]:
        if not cues:
            return {}
        payload = self.chat_json(build_translation_prompt(cues, target), "translations", model=model, timeout=timeout)
        translations = payload.get("translations", [])
        return {int(item["id"]): str(item["text"]) for item in translations}

    def translate_multi(self, cues: list[tuple[int, str]], targets: tuple[str, ...], *, model: str, timeout: int) -> dict[str, dict[int, str]]:
        if not cues:
            return {target: {} for target in targets}
        payload = self.chat_json(build_multi_translation_prompt(cues, targets), "translations", model=model, timeout=timeout)
        translations = payload.get("translations", [])
        by_target: dict[str, dict[int, str]] = {target: {} for target in targets}
        for item in translations:
            cue_id = int(item["id"])
            for target in targets:
                by_target[target][cue_id] = str(item[target])
        return by_target

    def extract_tags(self, source_file: Path, sampled_lines: list[str], *, model: str, timeout: int) -> dict:
        return self.chat_json(build_tag_prompt(source_file, sampled_lines), "tags_zh", model=model, timeout=timeout)

    def get_balance(self, timeout: int = 30) -> dict:
        payload = self._request_json("/users/me/balance", method="GET", timeout=timeout)
        if isinstance(payload.get("data"), dict):
            merged = dict(payload["data"])
            merged.update({key: value for key, value in payload.items() if key != "data"})
            return merged
        return payload


class KimiCliClient:
    supports_balance = False

    def __init__(self, *, api_key: str, base_url: str = DEFAULT_KIMI_CLI_BASE_URL, kimi_bin: str = "kimi"):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.kimi_bin = kimi_bin

    def _env(self, model: str) -> dict[str, str]:
        env = dict(os.environ)
        env["KIMI_BASE_URL"] = self.base_url
        env["KIMI_API_KEY"] = self.api_key
        env["KIMI_MODEL_NAME"] = model
        env["KIMI_CLI_NO_AUTO_UPDATE"] = "1"
        return env

    def _wrap_prompt(self, prompt: str) -> str:
        return (
            "You are running as a JSON-only batch worker for subtitle processing. "
            "Do not read files. Do not write files. Do not execute shell commands. "
            "Return JSON only, with no markdown and no commentary.\n\n"
            f"{prompt}"
        )

    def chat_json(self, prompt: str, required_key: str, *, model: str, timeout: int) -> dict:
        cmd = [
            self.kimi_bin,
            "--quiet",
            "--no-thinking",
            "--max-steps-per-turn",
            "1",
            "--prompt",
            self._wrap_prompt(prompt),
        ]
        try:
            result = subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True,
                input="",
                timeout=timeout,
                env=self._env(model),
            )
        except FileNotFoundError as exc:
            raise ProviderQuotaError(f"Kimi CLI missing: {self.kimi_bin}") from exc
        except subprocess.TimeoutExpired as exc:
            raise ProviderTransientError(f"Kimi CLI timed out after {timeout}s") from exc
        except subprocess.CalledProcessError as exc:
            stdout = exc.stdout or exc.output or ""
            stderr = exc.stderr or ""
            message = f"Kimi CLI failed ({exc.returncode}): {stderr or stdout}".strip()
            folded = message.casefold()
            if "429" in folded or "rate limit" in folded:
                raise ProviderRateLimitError(message) from exc
            if any(part in folded for part in ("quota", "403", "401", "auth", "access_terminated", "insufficient", "balance", "to resume this session")):
                raise ProviderQuotaError(message) from exc
            if "overloaded" in folded or "temporarily" in folded:
                raise ProviderTransientError(message) from exc
            raise ProviderResponseError(message) from exc
        try:
            return parse_codex_json_output(result.stdout, required_key)
        except ValueError as exc:
            raise ProviderResponseError(str(exc)) from exc

    def translate(self, cues: list[tuple[int, str]], target: str, *, model: str, timeout: int) -> dict[int, str]:
        if not cues:
            return {}
        payload = self.chat_json(build_translation_prompt(cues, target), "translations", model=model, timeout=timeout)
        translations = payload.get("translations", [])
        return {int(item["id"]): str(item["text"]) for item in translations}

    def translate_multi(self, cues: list[tuple[int, str]], targets: tuple[str, ...], *, model: str, timeout: int) -> dict[str, dict[int, str]]:
        if not cues:
            return {target: {} for target in targets}
        payload = self.chat_json(build_multi_translation_prompt(cues, targets), "translations", model=model, timeout=timeout)
        translations = payload.get("translations", [])
        by_target: dict[str, dict[int, str]] = {target: {} for target in targets}
        for item in translations:
            cue_id = int(item["id"])
            for target in targets:
                by_target[target][cue_id] = str(item[target])
        return by_target

    def extract_tags(self, source_file: Path, sampled_lines: list[str], *, model: str, timeout: int) -> dict:
        return self.chat_json(build_tag_prompt(source_file, sampled_lines), "tags_zh", model=model, timeout=timeout)

    def get_balance(self, timeout: int = 30) -> dict:
        return {}


def call_codex_translate(
    cues: list[tuple[int, str]],
    target: str,
    *,
    model: str | None = None,
    codex_bin: str = "codex",
    schema_path: Path = SCHEMA_PATH,
    timeout: int = 600,
) -> dict[int, str]:
    if not cues:
        return {}

    cmd = [
        codex_bin,
        "-a",
        "never",
        "exec",
        "--ephemeral",
        "--sandbox",
        "read-only",
        "--output-schema",
        str(schema_path),
    ]
    if model:
        cmd.extend(["--model", model])
    cmd.append(build_translation_prompt(cues, target))

    result = subprocess.run(cmd, check=True, capture_output=True, text=True, input="", timeout=timeout)
    payload = parse_codex_json_output(result.stdout, "translations")
    translations = payload.get("translations", [])
    return {int(item["id"]): str(item["text"]) for item in translations}


def call_codex_translate_multi(
    cues: list[tuple[int, str]],
    targets: tuple[str, ...],
    *,
    model: str | None = None,
    codex_bin: str = "codex",
    schema_path: Path = MULTI_SCHEMA_PATH,
    timeout: int = 600,
) -> dict[str, dict[int, str]]:
    if not cues:
        return {target: {} for target in targets}

    cmd = [
        codex_bin,
        "-a",
        "never",
        "exec",
        "--ephemeral",
        "--sandbox",
        "read-only",
        "--output-schema",
        str(schema_path),
    ]
    if model:
        cmd.extend(["--model", model])
    cmd.append(build_multi_translation_prompt(cues, targets))

    result = subprocess.run(cmd, check=True, capture_output=True, text=True, input="", timeout=timeout)
    payload = parse_codex_json_output(result.stdout, "translations")
    translations = payload.get("translations", [])
    by_target: dict[str, dict[int, str]] = {target: {} for target in targets}
    for item in translations:
        cue_id = int(item["id"])
        for target in targets:
            by_target[target][cue_id] = str(item[target])
    return by_target


def _anthropic_env(*, use_api_key: bool) -> dict[str, str]:
    env = dict(os.environ)
    if not use_api_key:
        for name in (
            "ANTHROPIC_API_KEY",
            "ANTHROPIC_AUTH_TOKEN",
            "ANTHROPIC_BASE_URL",
            "ANTHROPIC_CUSTOM_HEADERS",
        ):
            env.pop(name, None)
    return env


def _map_anthropic_cli_error(exc: subprocess.CalledProcessError) -> RuntimeError:
    stdout = exc.stdout or exc.output or ""
    stderr = exc.stderr or ""
    message = f"Claude Code failed ({exc.returncode}): {stderr or stdout}".strip()
    folded = message.casefold()
    if "429" in folded or "rate limit" in folded:
        return ProviderRateLimitError(message)
    if any(
        part in folded
        for part in (
            "quota",
            "402",
            "403",
            "401",
            "auth",
            "login",
            "limit reached",
            "usage limit",
            "hit your limit",
            "you've hit your limit",
            "resets 12am",
            "resets at",
            "billing",
            "payment",
            "coding plan",
            "套餐已到期",
            "请续费",
        )
    ):
        return ProviderQuotaError(message)
    if "overloaded" in folded or "temporarily" in folded or "timeout" in folded:
        return ProviderTransientError(message)
    return ProviderResponseError(message)


def _anthropic_translation_prompt(cues: list[tuple[int, str]], target: str) -> str:
    return (
        f"{build_translation_prompt(cues, target)}\n\n"
        'Required JSON shape: {"translations":[{"id":1,"text":"translated cue"}]}. '
        "Return the JSON object as plain text. Do not call tools."
    )


def _anthropic_multi_translation_prompt(cues: list[tuple[int, str]], targets: tuple[str, ...]) -> str:
    fields = ",".join(f'"{target}":"translated cue"' for target in targets)
    return (
        f"{build_multi_translation_prompt(cues, targets)}\n\n"
        f'Required JSON shape: {{"translations":[{{"id":1,{fields}}}]}}. '
        "Return the JSON object as plain text. Do not call tools."
    )


def _anthropic_tag_prompt(source_file: Path, sampled_lines: list[str]) -> str:
    return (
        f"{build_tag_prompt(source_file, sampled_lines)}\n\n"
        "Required JSON object keys: tags_zh, tags_en, setting, roles, relationship, "
        "body_traits, outfit, scenario, mood_style, audio_style, dialogue_density, "
        "tag_quality, warnings, blocked_minor_coded_terms. "
        "tags_zh and tags_en must be top-level arrays, not nested under another key. "
        "setting, roles, relationship, body_traits, outfit, scenario, mood_style, "
        "audio_style, and warnings must also be arrays, even when empty. "
        "dialogue_density must be one of low, medium, high, unknown. "
        "tag_quality must be one of low_information, inferred, good. "
        "blocked_minor_coded_terms must be boolean. "
        "Return the JSON object as plain text. Do not call tools."
    )


def call_anthropic_translate(
    cues: list[tuple[int, str]],
    target: str,
    *,
    model: str = DEFAULT_ANTHROPIC_MODEL,
    anthropic_bin: str = "claude",
    schema_path: Path = SCHEMA_PATH,
    timeout: int = 600,
    use_api_key: bool = False,
    setting_sources: str | None = DEFAULT_ANTHROPIC_SETTING_SOURCES,
    codex_bin: str | None = None,
) -> dict[int, str]:
    if not cues:
        return {}

    cmd = [
        anthropic_bin,
        "-p",
        "--model",
        model,
        "--permission-mode",
        "dontAsk",
        "--tools",
        "",
        "--no-session-persistence",
    ]
    if setting_sources:
        cmd.extend(["--setting-sources", setting_sources])
    cmd.extend([
        "--system-prompt",
        "You are a JSON-only subtitle batch worker. Do not use tools. Return JSON only.",
        _anthropic_translation_prompt(cues, target),
    ])
    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
            input="",
            timeout=timeout,
            env=_anthropic_env(use_api_key=use_api_key),
        )
    except FileNotFoundError as exc:
        raise ProviderQuotaError(f"Claude Code CLI missing: {anthropic_bin}") from exc
    except subprocess.TimeoutExpired as exc:
        raise ProviderTransientError(f"Claude Code timed out after {timeout}s") from exc
    except subprocess.CalledProcessError as exc:
        raise _map_anthropic_cli_error(exc) from exc
    try:
        payload = parse_codex_json_output(result.stdout, "translations")
    except ValueError as exc:
        raise ProviderResponseError(f"{exc}; stdout={result.stdout[:500]!r}") from exc
    translations = payload.get("translations", [])
    return {int(item["id"]): str(item["text"]) for item in translations}


def call_anthropic_translate_multi(
    cues: list[tuple[int, str]],
    targets: tuple[str, ...],
    *,
    model: str = DEFAULT_ANTHROPIC_MODEL,
    anthropic_bin: str = "claude",
    schema_path: Path = MULTI_SCHEMA_PATH,
    timeout: int = 600,
    use_api_key: bool = False,
    setting_sources: str | None = DEFAULT_ANTHROPIC_SETTING_SOURCES,
    codex_bin: str | None = None,
) -> dict[str, dict[int, str]]:
    if not cues:
        return {target: {} for target in targets}

    cmd = [
        anthropic_bin,
        "-p",
        "--model",
        model,
        "--permission-mode",
        "dontAsk",
        "--tools",
        "",
        "--no-session-persistence",
    ]
    if setting_sources:
        cmd.extend(["--setting-sources", setting_sources])
    cmd.extend([
        "--system-prompt",
        "You are a JSON-only subtitle batch worker. Do not use tools. Return JSON only.",
        _anthropic_multi_translation_prompt(cues, targets),
    ])
    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
            input="",
            timeout=timeout,
            env=_anthropic_env(use_api_key=use_api_key),
        )
    except FileNotFoundError as exc:
        raise ProviderQuotaError(f"Claude Code CLI missing: {anthropic_bin}") from exc
    except subprocess.TimeoutExpired as exc:
        raise ProviderTransientError(f"Claude Code timed out after {timeout}s") from exc
    except subprocess.CalledProcessError as exc:
        raise _map_anthropic_cli_error(exc) from exc
    try:
        payload = parse_codex_json_output(result.stdout, "translations")
    except ValueError as exc:
        raise ProviderResponseError(f"{exc}; stdout={result.stdout[:500]!r}") from exc
    translations = payload.get("translations", [])
    by_target: dict[str, dict[int, str]] = {target: {} for target in targets}
    for item in translations:
        cue_id = int(item["id"])
        for target in targets:
            by_target[target][cue_id] = str(item[target])
    return by_target


class JobStore:
    def __init__(self, output_root: Path):
        metadata_dir = output_root / "metadata"
        metadata_dir.mkdir(parents=True, exist_ok=True)
        self.path = metadata_dir / "jobs.sqlite"
        self.lock = threading.Lock()
        self.conn = sqlite3.connect(self.path, timeout=60, check_same_thread=False)
        with self.lock:
            self.conn.execute("PRAGMA busy_timeout=60000")
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                  source_path TEXT NOT NULL,
                  target TEXT NOT NULL,
                  target_path TEXT NOT NULL,
                  status TEXT NOT NULL,
                  error TEXT,
                  updated_at TEXT NOT NULL,
                  PRIMARY KEY (source_path, target)
                )
                """
            )
            self.conn.commit()

    def set_status(
        self,
        *,
        source_path: Path,
        target: str,
        target_path: Path,
        status: str,
        error: str | None = None,
    ) -> None:
        with self.lock:
            self.conn.execute(
                """
                INSERT INTO jobs (source_path, target, target_path, status, error, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_path, target) DO UPDATE SET
                  target_path=excluded.target_path,
                  status=excluded.status,
                  error=excluded.error,
                  updated_at=excluded.updated_at
                """,
                (
                    str(source_path),
                    target,
                    str(target_path),
                    status,
                    error,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
            self.conn.commit()

    def close(self) -> None:
        with self.lock:
            self.conn.close()


def collect_translatable_cues(document: SrtDocument) -> list[tuple[int, str]]:
    cues: list[tuple[int, str]] = []
    for idx, cue in enumerate(document.cues, start=1):
        text = cue.text
        if text.strip() and contains_japanese(text):
            cues.append((idx, text))
    return cues


def chunked(items: list[tuple[int, str]], size: int) -> Iterable[list[tuple[int, str]]]:
    if size <= 0:
        raise ValueError("batch size must be positive")
    for i in range(0, len(items), size):
        yield items[i : i + size]


def atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.tmp")
    tmp_path.write_bytes(data)
    tmp_path.replace(path)


def call_batch_with_retry(
    cue_batch: list[tuple[int, str]],
    call_func: Callable[[list[tuple[int, str]]], dict],
    *,
    label: str,
    logger: Callable[[str], None] | None = None,
    sleeper: Callable[[float], None] = time.sleep,
    retry_delays: tuple[int, ...] = (10, 30, 60),
    split_threshold: int = 20,
) -> dict:
    for attempt in range(len(retry_delays) + 1):
        try:
            return call_func(cue_batch)
        except Exception as exc:
            if attempt < len(retry_delays):
                retry_number = attempt + 1
                delay = retry_delays[attempt]
                if logger:
                    logger(f"retry batch {label} attempt {retry_number + 1} delay={delay}s reason={exc}")
                sleeper(delay)
                continue
            if len(cue_batch) > split_threshold:
                midpoint = len(cue_batch) // 2
                left = cue_batch[:midpoint]
                right = cue_batch[midpoint:]
                if logger:
                    logger(f"split batch {label} size={len(cue_batch)} -> {len(left)}+{len(right)}")
                left_result = call_batch_with_retry(
                    left,
                    call_func,
                    label=f"{label}.1",
                    logger=logger,
                    sleeper=sleeper,
                    retry_delays=retry_delays,
                    split_threshold=split_threshold,
                )
                right_result = call_batch_with_retry(
                    right,
                    call_func,
                    label=f"{label}.2",
                    logger=logger,
                    sleeper=sleeper,
                    retry_delays=retry_delays,
                    split_threshold=split_threshold,
                )
                if isinstance(left_result, dict) and isinstance(right_result, dict):
                    merged = dict(left_result)
                    for key, value in right_result.items():
                        if isinstance(value, dict) and isinstance(merged.get(key), dict):
                            combined = dict(merged[key])
                            combined.update(value)
                            merged[key] = combined
                        else:
                            merged[key] = value
                    return merged
            raise


def process_file(
    source_file: Path,
    source_root: Path,
    output_root: Path,
    targets: tuple[str, ...],
    *,
    force: bool,
    translate_func: Callable[..., dict[int, str]] = call_codex_translate,
    dry_run: bool = False,
    model: str | None = None,
    codex_bin: str = "codex",
    timeout: int = 600,
    batch_size: int = 40,
    job_store: JobStore | None = None,
    progress_label: str | None = None,
    logger: Callable[[str], None] | None = None,
    translate_multi_func: Callable[..., dict[str, dict[int, str]]] = call_codex_translate_multi,
    retry_sleeper: Callable[[float], None] = time.sleep,
) -> list[ProcessResult]:
    data = source_file.read_bytes()
    document = clean_document(parse_srt(data))
    translatable_cues = collect_translatable_cues(document)
    results: list[ProcessResult] = []
    needed_targets: list[str] = []

    for target in targets:
        target_file = target_path_for(source_file, source_root, output_root, target)
        if target_file.exists() and not force:
            result = ProcessResult(target, source_file, target_file, "skip-existing", len(document.cues), len(translatable_cues))
            results.append(result)
            if logger and progress_label:
                logger(f"{progress_label} skip-existing {target} -> {target_file}")
            if job_store:
                job_store.set_status(source_path=source_file, target=target, target_path=target_file, status="skipped")
            continue

        if not translatable_cues:
            result = ProcessResult(target, source_file, target_file, "skip-no-japanese", len(document.cues), 0)
            results.append(result)
            if logger and progress_label:
                logger(f"{progress_label} skip-no-japanese {target}")
            if job_store:
                job_store.set_status(source_path=source_file, target=target, target_path=target_file, status="skipped")
            continue

        if dry_run:
            if logger and progress_label:
                logger(f"{progress_label} dry-run translate {target} {len(translatable_cues)} ja cues")
            results.append(ProcessResult(target, source_file, target_file, "translate", len(document.cues), len(translatable_cues)))
            continue

        if job_store:
            job_store.set_status(source_path=source_file, target=target, target_path=target_file, status="running")
        needed_targets.append(target)

    if not needed_targets:
        return results

    if set(needed_targets) == {"zh-CN", "en"} and len(needed_targets) == 2:
        batch_count = (len(translatable_cues) + batch_size - 1) // batch_size
        translations_by_target: dict[str, dict[int, str]] = {target: {} for target in needed_targets}
        try:
            if logger and progress_label:
                logger(
                    f"{progress_label} multi-target translating {','.join(needed_targets)} "
                    f"{len(translatable_cues)} ja cues in {batch_count} batches"
                )
            for batch_index, cue_batch in enumerate(chunked(translatable_cues, batch_size), start=1):
                if logger and progress_label:
                    logger(
                        f"{progress_label} multi-target translating {','.join(needed_targets)} "
                        f"batch {batch_index}/{batch_count} ({len(cue_batch)} cues)"
                    )
                batch_result = call_batch_with_retry(
                    cue_batch,
                    lambda batch: translate_multi_func(
                        batch,
                        tuple(needed_targets),
                        model=model,
                        codex_bin=codex_bin,
                        timeout=timeout,
                    ),
                    label=f"{','.join(needed_targets)} {batch_index}/{batch_count}",
                    logger=logger,
                    sleeper=retry_sleeper,
                )
                for target, target_translations in batch_result.items():
                    translations_by_target[target].update(target_translations)

            for target in needed_targets:
                target_file = target_path_for(source_file, source_root, output_root, target)
                translated_document = apply_translations_and_clean(document, translations_by_target[target])
                output = rebuild_srt(translated_document)
                reparsed = parse_srt(output)
                if translatable_cues and not reparsed.cues:
                    raise SubtitleError("translated SRT validation failed: all cues were removed")
                atomic_write_bytes(target_file, output)
                results.append(ProcessResult(target, source_file, target_file, "written", len(document.cues), len(translatable_cues)))
                if logger and progress_label:
                    logger(f"{progress_label} written {target} -> {target_file}")
                if job_store:
                    job_store.set_status(source_path=source_file, target=target, target_path=target_file, status="done")
        except Exception as exc:
            if job_store:
                for target in needed_targets:
                    job_store.set_status(
                        source_path=source_file,
                        target=target,
                        target_path=target_path_for(source_file, source_root, output_root, target),
                        status="failed",
                        error=str(exc),
                    )
            if logger and progress_label:
                logger(f"{progress_label} failed {','.join(needed_targets)}: {exc}")
            raise
        return results

    for target in needed_targets:
        target_file = target_path_for(source_file, source_root, output_root, target)
        translations: dict[int, str] = {}
        try:
            batch_count = (len(translatable_cues) + batch_size - 1) // batch_size
            if logger and progress_label:
                logger(f"{progress_label} translating {target} {len(translatable_cues)} ja cues in {batch_count} batches")
            for batch_index, cue_batch in enumerate(chunked(translatable_cues, batch_size), start=1):
                if logger and progress_label:
                    logger(f"{progress_label} translating {target} batch {batch_index}/{batch_count} ({len(cue_batch)} cues)")
                batch_result = call_batch_with_retry(
                    cue_batch,
                    lambda batch: translate_func(
                        batch,
                        target,
                        model=model,
                        codex_bin=codex_bin,
                        timeout=timeout,
                    ),
                    label=f"{target} {batch_index}/{batch_count}",
                    logger=logger,
                    sleeper=retry_sleeper,
                )
                translations.update(batch_result)
            translated_document = apply_translations_and_clean(document, translations)
            output = rebuild_srt(translated_document)
            reparsed = parse_srt(output)
            if translatable_cues and not reparsed.cues:
                raise SubtitleError("translated SRT validation failed: all cues were removed")
            atomic_write_bytes(target_file, output)
            results.append(ProcessResult(target, source_file, target_file, "written", len(document.cues), len(translatable_cues)))
            if logger and progress_label:
                logger(f"{progress_label} written {target} -> {target_file}")
            if job_store:
                job_store.set_status(source_path=source_file, target=target, target_path=target_file, status="done")
        except Exception as exc:
            if job_store:
                job_store.set_status(source_path=source_file, target=target, target_path=target_file, status="failed", error=str(exc))
            if logger and progress_label:
                logger(f"{progress_label} failed {target}: {exc}")
            raise

    return results


def sample_tag_lines(document: SrtDocument, max_lines: int = 90) -> list[str]:
    lines: list[str] = []
    for cue in document.cues:
        for line in cue.text_lines:
            text = re.sub(r"\s+", " ", line).strip()
            if text and text not in {"ご視聴ありがとうございました"}:
                lines.append(text)
    if len(lines) <= max_lines:
        return lines
    third = max_lines // 3
    middle_start = max((len(lines) // 2) - (third // 2), 0)
    sampled = lines[:third] + lines[middle_start : middle_start + third] + lines[-third:]
    seen: set[str] = set()
    unique: list[str] = []
    for line in sampled:
        if line not in seen:
            unique.append(line)
            seen.add(line)
    return unique[:max_lines]


def build_tag_prompt(source_file: Path, sampled_lines: list[str]) -> str:
    return (
        "Extract searchable tags for a legal adult subtitle catalog from Japanese subtitle lines. "
        "Output only search metadata. Do not output confidence, evidence_ja, raw subtitle lines, "
        "or Japanese source words as tags. Tags must be Chinese and English. "
        "Infer only from evidence in the lines. Do not output sexualized minor-coded tags. "
        "If minor-coded terms appear, set blocked_minor_coded_terms=true and omit those terms from tags. "
        "Return JSON only matching the schema.\n\n"
        f"Filename: {source_file.name}\n"
        f"Subtitle lines:\n{json.dumps(sampled_lines, ensure_ascii=False)}"
    )


def call_codex_extract_tags(
    source_file: Path,
    sampled_lines: list[str],
    *,
    model: str = "gpt-5.4-mini",
    codex_bin: str = "codex",
    schema_path: Path = TAG_SCHEMA_PATH,
    timeout: int = 600,
) -> dict:
    cmd = [
        codex_bin,
        "-a",
        "never",
        "exec",
        "--ephemeral",
        "--sandbox",
        "read-only",
        "--output-schema",
        str(schema_path),
        "--model",
        model,
        build_tag_prompt(source_file, sampled_lines),
    ]
    result = subprocess.run(cmd, check=True, capture_output=True, text=True, input="", timeout=timeout)
    return parse_codex_json_output(result.stdout, "tags_zh")


def call_anthropic_extract_tags(
    source_file: Path,
    sampled_lines: list[str],
    *,
    model: str = DEFAULT_ANTHROPIC_MODEL,
    anthropic_bin: str = "claude",
    schema_path: Path = TAG_SCHEMA_PATH,
    timeout: int = 600,
    use_api_key: bool = False,
    setting_sources: str | None = DEFAULT_ANTHROPIC_SETTING_SOURCES,
    codex_bin: str | None = None,
) -> dict:
    cmd = [
        anthropic_bin,
        "-p",
        "--model",
        model,
        "--permission-mode",
        "dontAsk",
        "--tools",
        "",
        "--no-session-persistence",
    ]
    if setting_sources:
        cmd.extend(["--setting-sources", setting_sources])
    cmd.extend([
        "--system-prompt",
        "You are a JSON-only subtitle tag extraction worker. Do not use tools. Return JSON only.",
        _anthropic_tag_prompt(source_file, sampled_lines),
    ])
    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
            input="",
            timeout=timeout,
            env=_anthropic_env(use_api_key=use_api_key),
        )
    except FileNotFoundError as exc:
        raise ProviderQuotaError(f"Claude Code CLI missing: {anthropic_bin}") from exc
    except subprocess.TimeoutExpired as exc:
        raise ProviderTransientError(f"Claude Code timed out after {timeout}s") from exc
    except subprocess.CalledProcessError as exc:
        raise _map_anthropic_cli_error(exc) from exc
    try:
        return parse_codex_json_output(result.stdout, "tags_zh")
    except ValueError as exc:
        raise ProviderResponseError(f"{exc}; stdout={result.stdout[:500]!r}") from exc


@dataclass
class ProviderState:
    kimi_available: bool = True
    kimi_next_check_at: float = 0.0
    last_kimi_balance: float | None = None
    last_provider: str = "codex"
    last_kimi_error: Exception | None = None
    anthropic_next_check_at: dict[str, float] = field(default_factory=dict)
    last_anthropic_error: dict[str, Exception] = field(default_factory=dict)
    anthropic_index: int = 0


KIMI_HTTP_PROVIDERS = {"kimi", "kimi-first"}
KIMI_CLI_PROVIDERS = {"kimi-cli", "kimi-cli-first"}
KIMI_PROVIDERS = KIMI_HTTP_PROVIDERS | KIMI_CLI_PROVIDERS
KIMI_FALLBACK_PROVIDERS = {"kimi-first", "kimi-cli-first"}
KIMI_ONLY_PROVIDERS = {"kimi", "kimi-cli"}
ANTHROPIC_PROVIDERS = {"anthropic"}
ANTHROPIC_FIRST_PROVIDERS = {"anthropic-first"}


class ProviderRouter:
    def __init__(
        self,
        *,
        provider: str,
        kimi_client: KimiClient | object | None,
        kimi_model: str,
        kimi_tag_model: str,
        codex_translate_func: Callable[..., dict[int, str]],
        codex_tag_func: Callable[..., dict],
        codex_model: str | None,
        codex_tag_model: str,
        kimi_min_balance: float,
        kimi_recheck_seconds: int,
        both_providers_wait: bool,
        clock: Callable[[], float] = time.time,
        sleeper: Callable[[float], None] = time.sleep,
        logger: Callable[[str], None] | None = None,
        codex_translate_multi_func: Callable[..., dict[str, dict[int, str]]] | None = None,
        anthropic_translate_func: Callable[..., dict[int, str]] | None = None,
        anthropic_translate_multi_func: Callable[..., dict[str, dict[int, str]]] | None = None,
        anthropic_tag_func: Callable[..., dict] | None = None,
        anthropic_model: str = DEFAULT_ANTHROPIC_MODEL,
        anthropic_models: tuple[str, ...] | None = None,
        anthropic_tag_model: str | None = None,
        anthropic_tag_models: tuple[str, ...] | None = None,
        anthropic_bin: str = "claude",
        anthropic_use_api_key: bool = False,
        anthropic_recheck_seconds: int = 1800,
    ):
        self.provider = provider
        self.kimi_client = kimi_client
        self.kimi_model = kimi_model
        self.kimi_tag_model = kimi_tag_model
        self.codex_translate_func = codex_translate_func
        self.codex_translate_multi_func = codex_translate_multi_func or call_codex_translate_multi
        self.codex_tag_func = codex_tag_func
        self.codex_model = codex_model
        self.codex_tag_model = codex_tag_model
        self.kimi_min_balance = kimi_min_balance
        self.kimi_recheck_seconds = kimi_recheck_seconds
        self.both_providers_wait = both_providers_wait
        self.clock = clock
        self.sleeper = sleeper
        self.logger = logger or (lambda _message: None)
        self.state = ProviderState(kimi_available=provider in KIMI_PROVIDERS)
        self.anthropic_translate_func = anthropic_translate_func
        self.anthropic_translate_multi_func = anthropic_translate_multi_func
        self.anthropic_tag_func = anthropic_tag_func
        self.anthropic_models = anthropic_models or (anthropic_model,)
        self.anthropic_model = self.anthropic_models[0]
        if anthropic_tag_models is not None:
            self.anthropic_tag_models = anthropic_tag_models
        elif anthropic_tag_model is not None:
            self.anthropic_tag_models = (anthropic_tag_model,)
        else:
            self.anthropic_tag_models = self.anthropic_models
        self.anthropic_tag_model = self.anthropic_tag_models[0]
        self.anthropic_bin = anthropic_bin
        self.anthropic_use_api_key = anthropic_use_api_key
        self.anthropic_recheck_seconds = anthropic_recheck_seconds

    def _kimi_label(self) -> str:
        return "kimi-cli" if self.provider in KIMI_CLI_PROVIDERS else "kimi"

    def _is_kimi_only(self) -> bool:
        return self.provider in KIMI_ONLY_PROVIDERS

    def _is_kimi_fallback(self) -> bool:
        return self.provider in KIMI_FALLBACK_PROVIDERS

    def _is_anthropic_first(self) -> bool:
        return self.provider in ANTHROPIC_FIRST_PROVIDERS

    def _next_check_iso(self) -> str:
        return datetime.fromtimestamp(self.state.kimi_next_check_at, tz=timezone.utc).isoformat()

    def _log_kimi_unavailable(self, reason: str, retry_after: int | None = None) -> None:
        delay = retry_after if retry_after is not None else self.kimi_recheck_seconds
        self.state.kimi_available = False
        self.state.kimi_next_check_at = self.clock() + max(delay, 1)
        self.logger(f"provider-fallback {self._kimi_label()}->codex reason={reason} next_kimi_check={self._next_check_iso()}")

    def _check_kimi_balance(self, *, force: bool = False) -> bool:
        if self.provider not in KIMI_PROVIDERS or self.kimi_client is None:
            return False
        now = self.clock()
        if not force and not self.state.kimi_available and now < self.state.kimi_next_check_at:
            return False
        if getattr(self.kimi_client, "supports_balance", True) is False:
            previous_provider = self.state.last_provider
            self.state.kimi_available = True
            self.state.kimi_next_check_at = now + self.kimi_recheck_seconds
            if previous_provider == "codex":
                self.logger(f"provider-switch codex->{self._kimi_label()} reason=kimi_cli_available")
            return True
        try:
            balance_payload = self.kimi_client.get_balance(timeout=30)  # type: ignore[attr-defined]
            balance = _extract_available_balance(balance_payload)
        except (ProviderQuotaError, ProviderRateLimitError) as exc:
            self.state.last_kimi_error = exc
            retry_after = getattr(exc, "retry_after_seconds", None)
            self._log_kimi_unavailable(type(exc).__name__, retry_after)
            return False
        except Exception as exc:
            self.state.last_kimi_error = exc
            self._log_kimi_unavailable(type(exc).__name__)
            return False

        self.state.last_kimi_balance = balance
        self.state.last_kimi_error = None
        if balance is not None and balance < self.kimi_min_balance:
            self.state.kimi_available = False
            self.state.kimi_next_check_at = now + self.kimi_recheck_seconds
            self.logger(
                f"provider-fallback kimi->codex reason=low_balance balance={balance:.4f} "
                f"next_kimi_check={self._next_check_iso()}"
            )
            return False

        previous_provider = self.state.last_provider
        self.state.kimi_available = True
        self.state.kimi_next_check_at = now + self.kimi_recheck_seconds
        if previous_provider == "codex":
            self.logger(f"provider-switch codex->{self._kimi_label()} reason=kimi_available balance={balance}")
        else:
            self.logger(f"provider={self._kimi_label()} model={self.kimi_model} balance={balance}")
        return True

    def _can_try_kimi(self) -> bool:
        if self.provider == "codex":
            return False
        if self._is_kimi_only():
            return self._check_kimi_balance(force=not self.state.kimi_available)
        return self._check_kimi_balance()

    def _wait_for_kimi_recheck(self) -> None:
        wait_seconds = max(self.state.kimi_next_check_at - self.clock(), 1)
        self.logger(f"provider-wait both_unavailable wait_seconds={int(wait_seconds)} next_kimi_check={self._next_check_iso()}")
        self.sleeper(wait_seconds)

    def _anthropic_next_check_iso(self, model: str) -> str:
        return datetime.fromtimestamp(self.state.anthropic_next_check_at.get(model, 0.0), tz=timezone.utc).isoformat()

    def _log_anthropic_unavailable(self, model: str, reason: str, exc: Exception) -> None:
        self.state.last_anthropic_error[model] = exc
        self.state.anthropic_next_check_at[model] = self.clock() + max(self.anthropic_recheck_seconds, 1)
        self.logger(
            f"provider-fallback anthropic->codex reason={reason} model={model} "
            f"next_anthropic_check={self._anthropic_next_check_iso(model)}"
        )

    def _available_anthropic_models(self, models: tuple[str, ...]) -> list[str]:
        now = self.clock()
        return [model for model in models if self.state.anthropic_next_check_at.get(model, 0.0) <= now]

    def _set_anthropic_provider(self, model: str) -> None:
        previous_provider = self.state.last_provider
        self.state.last_provider = "anthropic"
        if previous_provider == "codex":
            self.logger(f"provider-switch codex->anthropic model={model} reason=available")
        self.logger(f"provider=anthropic model={model}")

    def _anthropic_failure_reason(self, exc: Exception) -> str:
        if isinstance(exc, ProviderRateLimitError):
            return "rate_limit"
        if isinstance(exc, ProviderQuotaError):
            return "quota"
        return type(exc).__name__

    def _log_anthropic_fallback_to_codex(self, models: tuple[str, ...]) -> None:
        next_checks = [self.state.anthropic_next_check_at[model] for model in models if model in self.state.anthropic_next_check_at]
        if next_checks:
            next_check = datetime.fromtimestamp(min(next_checks), tz=timezone.utc).isoformat()
            self.logger(f"provider-fallback anthropic->codex reason=all_models_unavailable next_anthropic_check={next_check}")
        else:
            self.logger("provider-fallback anthropic->codex reason=anthropic_unavailable")

    def _call_codex_translate(self, cues: list[tuple[int, str]], target: str, *, codex_bin: str, timeout: int) -> dict[int, str]:
        self.state.last_provider = "codex"
        self.logger(f"provider=codex model={self.codex_model or 'default'}")
        return self.codex_translate_func(cues, target, model=self.codex_model, codex_bin=codex_bin, timeout=timeout)

    def _call_codex_translate_multi(
        self,
        cues: list[tuple[int, str]],
        targets: tuple[str, ...],
        *,
        codex_bin: str,
        timeout: int,
    ) -> dict[str, dict[int, str]]:
        self.state.last_provider = "codex"
        self.logger(f"provider=codex model={self.codex_model or 'default'}")
        return self.codex_translate_multi_func(cues, targets, model=self.codex_model, codex_bin=codex_bin, timeout=timeout)

    def _call_codex_tags(self, source_file: Path, sampled_lines: list[str], *, codex_bin: str, timeout: int) -> dict:
        self.state.last_provider = "codex"
        self.logger(f"provider=codex model={self.codex_tag_model}")
        return self.codex_tag_func(source_file, sampled_lines, model=self.codex_tag_model, codex_bin=codex_bin, timeout=timeout)

    def _call_anthropic_translate(self, cues: list[tuple[int, str]], target: str, *, timeout: int, model: str | None = None) -> dict[int, str]:
        if self.anthropic_translate_func is None:
            raise ProviderQuotaError("anthropic unavailable")
        call_model = model or self.anthropic_model
        self._set_anthropic_provider(call_model)
        return self.anthropic_translate_func(
            cues,
            target,
            model=call_model,
            anthropic_bin=self.anthropic_bin,
            timeout=timeout,
            use_api_key=self.anthropic_use_api_key,
        )

    def _call_anthropic_translate_multi(self, cues: list[tuple[int, str]], targets: tuple[str, ...], *, timeout: int, model: str | None = None) -> dict[str, dict[int, str]]:
        if self.anthropic_translate_multi_func is None:
            raise ProviderQuotaError("anthropic unavailable")
        call_model = model or self.anthropic_model
        self._set_anthropic_provider(call_model)
        return self.anthropic_translate_multi_func(
            cues,
            targets,
            model=call_model,
            anthropic_bin=self.anthropic_bin,
            timeout=timeout,
            use_api_key=self.anthropic_use_api_key,
        )

    def _call_anthropic_tags(self, source_file: Path, sampled_lines: list[str], *, timeout: int, model: str | None = None) -> dict:
        if self.anthropic_tag_func is None:
            raise ProviderQuotaError("anthropic unavailable")
        call_model = model or self.anthropic_tag_model
        self._set_anthropic_provider(call_model)
        return self.anthropic_tag_func(
            source_file,
            sampled_lines,
            model=call_model,
            anthropic_bin=self.anthropic_bin,
            timeout=timeout,
            use_api_key=self.anthropic_use_api_key,
        )

    def _try_anthropic_first_translate(self, cues: list[tuple[int, str]], target: str, *, timeout: int) -> dict[int, str] | None:
        if self.anthropic_translate_func is None:
            return None
        for model in self._available_anthropic_models(self.anthropic_models):
            try:
                return self._call_anthropic_translate(cues, target, timeout=timeout, model=model)
            except ProviderResponseError:
                raise
            except (ProviderQuotaError, ProviderRateLimitError, ProviderTransientError) as exc:
                self._log_anthropic_unavailable(model, self._anthropic_failure_reason(exc), exc)
        return None

    def _try_anthropic_first_translate_multi(
        self,
        cues: list[tuple[int, str]],
        targets: tuple[str, ...],
        *,
        timeout: int,
    ) -> dict[str, dict[int, str]] | None:
        if self.anthropic_translate_multi_func is None:
            return None
        for model in self._available_anthropic_models(self.anthropic_models):
            try:
                return self._call_anthropic_translate_multi(cues, targets, timeout=timeout, model=model)
            except ProviderResponseError:
                raise
            except (ProviderQuotaError, ProviderRateLimitError, ProviderTransientError) as exc:
                self._log_anthropic_unavailable(model, self._anthropic_failure_reason(exc), exc)
        return None

    def _try_anthropic_first_tags(self, source_file: Path, sampled_lines: list[str], *, timeout: int) -> dict | None:
        if self.anthropic_tag_func is None:
            return None
        for model in self._available_anthropic_models(self.anthropic_tag_models):
            try:
                return self._call_anthropic_tags(source_file, sampled_lines, timeout=timeout, model=model)
            except ProviderResponseError:
                raise
            except (ProviderQuotaError, ProviderRateLimitError, ProviderTransientError) as exc:
                self._log_anthropic_unavailable(model, self._anthropic_failure_reason(exc), exc)
        return None

    def translate(
        self,
        cues: list[tuple[int, str]],
        target: str,
        *,
        model: str | None = None,
        codex_bin: str = "codex",
        timeout: int = 600,
    ) -> dict[int, str]:
        if self._is_anthropic_first():
            anthropic_result = self._try_anthropic_first_translate(cues, target, timeout=timeout)
            if anthropic_result is not None:
                return anthropic_result
            self._log_anthropic_fallback_to_codex(self.anthropic_models)
            return self._call_codex_translate(cues, target, codex_bin=codex_bin, timeout=timeout)

        if self._can_try_kimi() and self.kimi_client is not None:
            try:
                self.state.last_provider = self._kimi_label()
                self.logger(f"provider={self._kimi_label()} model={self.kimi_model}")
                return self.kimi_client.translate(cues, target, model=self.kimi_model, timeout=timeout)  # type: ignore[attr-defined]
            except ProviderRateLimitError as exc:
                self._log_kimi_unavailable("rate_limit", exc.retry_after_seconds)
                if self._is_kimi_only():
                    raise
            except ProviderQuotaError:
                self._log_kimi_unavailable("quota")
                if self._is_kimi_only():
                    raise
            except ProviderTransientError as exc:
                self._log_kimi_unavailable(type(exc).__name__)
                if self._is_kimi_only():
                    raise
            except ProviderResponseError:
                raise

        if self._is_kimi_only():
            if self.state.last_kimi_error:
                raise self.state.last_kimi_error
            raise ProviderQuotaError(f"{self._kimi_label()} unavailable and provider is {self.provider}")

        try:
            return self._call_codex_translate(cues, target, codex_bin=codex_bin, timeout=timeout)
        except Exception as exc:
            if not self._is_kimi_fallback() or not self.both_providers_wait or not quota_like_error(exc):
                raise
            if self.anthropic_translate_func is not None:
                try:
                    return self._call_anthropic_translate(cues, target, timeout=timeout)
                except (ProviderQuotaError, ProviderRateLimitError, ProviderTransientError) as anthropic_exc:
                    self.logger(f"provider-fallback anthropic unavailable reason={type(anthropic_exc).__name__}")
            if self.kimi_client is None:
                raise
            while True:
                self._wait_for_kimi_recheck()
                if self._check_kimi_balance(force=True) and self.kimi_client is not None:
                    try:
                        self.state.last_provider = self._kimi_label()
                        self.logger(f"provider={self._kimi_label()} model={self.kimi_model}")
                        return self.kimi_client.translate(cues, target, model=self.kimi_model, timeout=timeout)  # type: ignore[attr-defined]
                    except ProviderRateLimitError as kimi_exc:
                        self._log_kimi_unavailable("rate_limit", kimi_exc.retry_after_seconds)
                    except ProviderQuotaError:
                        self._log_kimi_unavailable("quota")

    def translate_multi(
        self,
        cues: list[tuple[int, str]],
        targets: tuple[str, ...],
        *,
        model: str | None = None,
        codex_bin: str = "codex",
        timeout: int = 600,
    ) -> dict[str, dict[int, str]]:
        if self._is_anthropic_first():
            anthropic_result = self._try_anthropic_first_translate_multi(cues, targets, timeout=timeout)
            if anthropic_result is not None:
                return anthropic_result
            self._log_anthropic_fallback_to_codex(self.anthropic_models)
            return self._call_codex_translate_multi(cues, targets, codex_bin=codex_bin, timeout=timeout)

        if self._can_try_kimi() and self.kimi_client is not None:
            try:
                self.state.last_provider = self._kimi_label()
                self.logger(f"provider={self._kimi_label()} model={self.kimi_model}")
                return self.kimi_client.translate_multi(cues, targets, model=self.kimi_model, timeout=timeout)  # type: ignore[attr-defined]
            except ProviderRateLimitError as exc:
                self._log_kimi_unavailable("rate_limit", exc.retry_after_seconds)
                if self._is_kimi_only():
                    raise
            except ProviderQuotaError:
                self._log_kimi_unavailable("quota")
                if self._is_kimi_only():
                    raise
            except ProviderTransientError as exc:
                self._log_kimi_unavailable(type(exc).__name__)
                if self._is_kimi_only():
                    raise
            except ProviderResponseError:
                raise

        if self._is_kimi_only():
            if self.state.last_kimi_error:
                raise self.state.last_kimi_error
            raise ProviderQuotaError(f"{self._kimi_label()} unavailable and provider is {self.provider}")

        try:
            return self._call_codex_translate_multi(cues, targets, codex_bin=codex_bin, timeout=timeout)
        except Exception as exc:
            if not self._is_kimi_fallback() or not self.both_providers_wait or not quota_like_error(exc):
                raise
            if self.anthropic_translate_multi_func is not None:
                try:
                    return self._call_anthropic_translate_multi(cues, targets, timeout=timeout)
                except (ProviderQuotaError, ProviderRateLimitError, ProviderTransientError) as anthropic_exc:
                    self.logger(f"provider-fallback anthropic unavailable reason={type(anthropic_exc).__name__}")
            if self.kimi_client is None:
                raise
            while True:
                self._wait_for_kimi_recheck()
                if self._check_kimi_balance(force=True) and self.kimi_client is not None:
                    try:
                        self.state.last_provider = self._kimi_label()
                        self.logger(f"provider={self._kimi_label()} model={self.kimi_model}")
                        return self.kimi_client.translate_multi(cues, targets, model=self.kimi_model, timeout=timeout)  # type: ignore[attr-defined]
                    except ProviderRateLimitError as kimi_exc:
                        self._log_kimi_unavailable("rate_limit", kimi_exc.retry_after_seconds)
                    except ProviderQuotaError:
                        self._log_kimi_unavailable("quota")

    def extract_tags(
        self,
        source_file: Path,
        sampled_lines: list[str],
        *,
        model: str = "gpt-5.4-mini",
        codex_bin: str = "codex",
        timeout: int = 600,
    ) -> dict:
        if self._is_anthropic_first():
            anthropic_result = self._try_anthropic_first_tags(source_file, sampled_lines, timeout=timeout)
            if anthropic_result is not None:
                return anthropic_result
            self._log_anthropic_fallback_to_codex(self.anthropic_tag_models)
            return self._call_codex_tags(source_file, sampled_lines, codex_bin=codex_bin, timeout=timeout)

        if self._can_try_kimi() and self.kimi_client is not None:
            try:
                self.state.last_provider = self._kimi_label()
                self.logger(f"provider={self._kimi_label()} model={self.kimi_tag_model}")
                return self.kimi_client.extract_tags(source_file, sampled_lines, model=self.kimi_tag_model, timeout=timeout)  # type: ignore[attr-defined]
            except ProviderRateLimitError as exc:
                self._log_kimi_unavailable("rate_limit", exc.retry_after_seconds)
                if self._is_kimi_only():
                    raise
            except ProviderQuotaError:
                self._log_kimi_unavailable("quota")
                if self._is_kimi_only():
                    raise
            except ProviderTransientError as exc:
                self._log_kimi_unavailable(type(exc).__name__)
                if self._is_kimi_only():
                    raise
            except ProviderResponseError as exc:
                if self._is_kimi_only():
                    raise
                self.logger(f"provider-fallback {self._kimi_label()}->codex reason=tag_response_error detail={exc}")

        if self._is_kimi_only():
            if self.state.last_kimi_error:
                raise self.state.last_kimi_error
            raise ProviderQuotaError(f"{self._kimi_label()} unavailable and provider is {self.provider}")

        try:
            return self._call_codex_tags(source_file, sampled_lines, codex_bin=codex_bin, timeout=timeout)
        except Exception as exc:
            if not self._is_kimi_fallback() or not self.both_providers_wait or not quota_like_error(exc):
                raise
            if self.anthropic_tag_func is not None:
                try:
                    return self._call_anthropic_tags(source_file, sampled_lines, timeout=timeout)
                except (ProviderQuotaError, ProviderRateLimitError, ProviderTransientError) as anthropic_exc:
                    self.logger(f"provider-fallback anthropic unavailable reason={type(anthropic_exc).__name__}")
            if self.kimi_client is None:
                raise
            while True:
                self._wait_for_kimi_recheck()
                if self._check_kimi_balance(force=True) and self.kimi_client is not None:
                    try:
                        self.state.last_provider = self._kimi_label()
                        self.logger(f"provider={self._kimi_label()} model={self.kimi_tag_model}")
                        return self.kimi_client.extract_tags(source_file, sampled_lines, model=self.kimi_tag_model, timeout=timeout)  # type: ignore[attr-defined]
                    except ProviderRateLimitError as kimi_exc:
                        self._log_kimi_unavailable("rate_limit", kimi_exc.retry_after_seconds)
                    except ProviderQuotaError:
                        self._log_kimi_unavailable("quota")


def _list_without_minor_terms(values: object) -> tuple[list[str], bool]:
    blocked = False
    safe: list[str] = []
    if not isinstance(values, list):
        return safe, blocked
    for value in values:
        text = str(value).strip()
        if not text:
            continue
        folded = text.casefold()
        if any(term in folded or term in text for term in MINOR_CODED_TERMS):
            blocked = True
            continue
        safe.append(text)
    return safe, blocked


def filter_tag_record(record: dict) -> dict:
    filtered = dict(record)
    safety = dict(filtered.get("safety") or {})
    quality = dict(filtered.get("quality") or {})
    tags = dict(filtered.get("tags") or {})
    facets = dict(filtered.get("facets") or {})
    blocked = bool(safety.get("blocked_minor_coded_terms"))

    for lang in ("zh", "en"):
        safe, key_blocked = _list_without_minor_terms(tags.get(lang, []))
        tags[lang] = safe
        blocked = blocked or key_blocked

    for key, value in list(facets.items()):
        if isinstance(value, list):
            safe, key_blocked = _list_without_minor_terms(value)
            facets[key] = safe
            blocked = blocked or key_blocked

    warnings = list(quality.get("warnings") or [])
    if blocked and "minor_coded_terms_removed" not in warnings:
        warnings.append("minor_coded_terms_removed")
    quality["warnings"] = warnings
    safety["blocked_minor_coded_terms"] = blocked
    filtered["tags"] = tags
    filtered["facets"] = facets
    filtered["quality"] = quality
    filtered["safety"] = safety
    return filtered


def _coerce_string_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value).strip()
    return [text] if text else []


def build_tag_record(
    *,
    source_file: Path,
    source_root: Path,
    output_root: Path,
    tag_payload: dict,
) -> dict:
    record = {
        "filename": source_file.name,
        "paths": {
            "source": str(source_file),
            "zh_srt": str(target_path_for(source_file, source_root, output_root, "zh-CN")),
            "en_srt": str(target_path_for(source_file, source_root, output_root, "en")),
        },
        "tags": {
            "zh": _coerce_string_list(tag_payload.get("tags_zh", [])),
            "en": _coerce_string_list(tag_payload.get("tags_en", [])),
        },
        "facets": {
            "setting": _coerce_string_list(tag_payload.get("setting", [])),
            "roles": _coerce_string_list(tag_payload.get("roles", [])),
            "relationship": _coerce_string_list(tag_payload.get("relationship", [])),
            "body_traits": _coerce_string_list(tag_payload.get("body_traits", [])),
            "outfit": _coerce_string_list(tag_payload.get("outfit", [])),
            "scenario": _coerce_string_list(tag_payload.get("scenario", [])),
            "mood_style": _coerce_string_list(tag_payload.get("mood_style", [])),
            "audio_style": _coerce_string_list(tag_payload.get("audio_style", [])),
            "dialogue_density": tag_payload.get("dialogue_density", "unknown"),
        },
        "quality": {
            "tag_quality": tag_payload.get("tag_quality", "inferred"),
            "warnings": _coerce_string_list(tag_payload.get("warnings", [])),
        },
        "safety": {
            "blocked_minor_coded_terms": tag_payload.get("blocked_minor_coded_terms", False),
        },
    }
    return filter_tag_record(record)


def tag_sidecar_path_for(source_file: Path, source_root: Path, output_root: Path) -> Path:
    relative = source_file.relative_to(source_root)
    return output_root / "metadata" / relative.with_suffix(".tags.json")


def write_tag_sidecar(output_root: Path, source_root: Path, source_file: Path, record: dict) -> Path:
    path = tag_sidecar_path_for(source_file, source_root, output_root)
    data = (json.dumps(record, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    atomic_write_bytes(path, data)
    return path


def write_tag_index_from_sidecars(output_root: Path) -> Path:
    metadata_dir = output_root / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    jsonl_path = metadata_dir / "subtitle_tags.jsonl"
    records: list[dict] = []
    for sidecar in sorted(metadata_dir.rglob("*.tags.json")):
        if sidecar == jsonl_path:
            continue
        try:
            records.append(json.loads(sidecar.read_text(encoding="utf-8")))
        except json.JSONDecodeError:
            continue
    with jsonl_path.open("w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
    return jsonl_path


def write_tag_indexes(
    output_root: Path,
    source_root: Path,
    tagged_records: list[tuple[Path, dict]],
) -> tuple[list[Path], Path]:
    sidecars = [
        write_tag_sidecar(output_root, source_root, source_file, record)
        for source_file, record in tagged_records
    ]
    return sidecars, write_tag_index_from_sidecars(output_root)


def process_tags_for_file(
    source_file: Path,
    source_root: Path,
    output_root: Path,
    *,
    tag_model: str,
    codex_bin: str,
    timeout: int,
    tag_extract_func: Callable[..., dict] = call_codex_extract_tags,
) -> dict:
    document = clean_document(parse_srt(source_file.read_bytes()))
    sampled = sample_tag_lines(document)
    payload = tag_extract_func(
        source_file,
        sampled,
        model=tag_model,
        codex_bin=codex_bin,
        timeout=timeout,
    )
    return build_tag_record(source_file=source_file, source_root=source_root, output_root=output_root, tag_payload=payload)


def parse_targets(raw: str) -> tuple[str, ...]:
    targets = tuple(part.strip() for part in raw.split(",") if part.strip())
    if not targets:
        raise argparse.ArgumentTypeError("at least one target language is required")
    return targets


def parse_model_list(raw: str) -> tuple[str, ...]:
    models = tuple(part.strip() for part in raw.split(",") if part.strip())
    if not models:
        raise argparse.ArgumentTypeError("at least one model is required")
    return models


def positive_int(raw: str) -> int:
    try:
        value = int(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if value < 1:
        raise argparse.ArgumentTypeError("must be >= 1")
    return value


def process_source_file_job(
    *,
    source_file: Path,
    file_index: int,
    total_files: int,
    args: argparse.Namespace,
    source_root: Path,
    output_root: Path,
    job_store: JobStore | None,
    translate_func: Callable[..., dict[int, str]],
    translate_multi_func: Callable[..., dict[str, dict[int, str]]],
    tag_extract_func: Callable[..., dict],
    logger: Callable[[str], None],
    worker_label: str | None = None,
) -> list[ProcessResult]:
    rel = source_file.relative_to(source_root)
    if worker_label is None:
        worker_match = re.fullmatch(r"subtitle-worker_(\d+)", threading.current_thread().name)
        if worker_match:
            worker_label = f"worker {int(worker_match.group(1)) + 1}"
    prefix = f"[{worker_label}] " if worker_label else ""
    progress_label = f"{prefix}[{file_index}/{total_files}] {rel}"
    results: list[ProcessResult] = []

    logger(f"{progress_label} start")
    if not args.tag_only:
        file_results = process_file(
            source_file,
            source_root,
            output_root,
            args.targets,
            force=args.force,
            dry_run=args.dry_run,
            translate_func=translate_func,
            translate_multi_func=translate_multi_func,
            model=args.model,
            codex_bin=args.codex_bin,
            timeout=args.timeout,
            batch_size=args.batch_size,
            job_store=job_store,
            progress_label=progress_label,
            logger=logger,
        )
        results.extend(file_results)
        for result in file_results:
            logger(
                f"{result.action}\t{result.target}\t{result.cue_count} cues\t"
                f"{result.japanese_cue_count} ja\t{rel} -> {result.target_path}"
            )
    if args.extract_tags:
        if args.dry_run:
            document = clean_document(parse_srt(source_file.read_bytes()))
            sampled_line_count = len(sample_tag_lines(document))
            logger(f"{progress_label} dry-run extract-tags {sampled_line_count} lines")
            logger(f"extract-tags\t{sampled_line_count} lines\t{rel}")
        else:
            existing_sidecar = tag_sidecar_path_for(source_file, source_root, output_root)
            if existing_sidecar.exists() and not args.force:
                logger(f"{progress_label} skip-tag-existing {existing_sidecar}")
            else:
                logger(f"{progress_label} extracting-tags")
                tag_record = process_tags_for_file(
                    source_file,
                    source_root,
                    output_root,
                    tag_model=args.tag_model,
                    codex_bin=args.codex_bin,
                    timeout=args.timeout,
                    tag_extract_func=tag_extract_func,
                )
                with METADATA_INDEX_LOCK:
                    sidecar_paths, jsonl_path = write_tag_indexes(output_root, source_root, [(source_file, tag_record)])
                for sidecar_path in sidecar_paths:
                    logger(f"wrote-tag-sidecar\t{sidecar_path}")
                logger(f"wrote-tag-index\t{jsonl_path}")
                logger(f"{progress_label} extracted-tags")
    logger(f"{progress_label} done")
    return results


def write_run_metrics(
    path: Path,
    *,
    provider: str,
    workers: int,
    batch_size: int,
    targets: tuple[str, ...],
    total_files: int,
    elapsed_seconds: float,
    results: list[ProcessResult],
) -> None:
    actions = Counter(result.action for result in results)
    payload = {
        "provider": provider,
        "workers": workers,
        "batch_size": batch_size,
        "targets": list(targets),
        "total_files": total_files,
        "elapsed_seconds": round(elapsed_seconds, 3),
        "files_per_hour": round((total_files / elapsed_seconds) * 3600, 3) if elapsed_seconds > 0 else 0,
        "actions": dict(actions),
    }
    atomic_write_bytes(path, (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8"))


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Translate Japanese SRT files with local Codex CLI.")
    parser.add_argument("--source-root", type=Path, default=DEFAULT_SOURCE_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--targets", type=parse_targets, default=DEFAULT_TARGETS)
    parser.add_argument("--provider", choices=("codex", "anthropic", "anthropic-first", "kimi", "kimi-first", "kimi-cli", "kimi-cli-first"), default="kimi-cli-first")
    parser.add_argument("--model", default=None, help="Optional Codex model override for translation.")
    parser.add_argument("--tag-model", default="gpt-5.4-mini", help="Codex model for future tag extraction.")
    parser.add_argument("--anthropic-model", default=DEFAULT_ANTHROPIC_MODEL, help="Claude Code model for Anthropic translation.")
    parser.add_argument("--anthropic-models", type=parse_model_list, default=None, help="Comma-separated Claude Code models for anthropic-first fallback.")
    parser.add_argument("--anthropic-tag-model", default=None, help="Claude Code model for Anthropic tag extraction.")
    parser.add_argument("--anthropic-bin", default="claude")
    parser.add_argument("--anthropic-use-api-key", action="store_true", help="Allow Claude Code to use ANTHROPIC_API_KEY from the environment.")
    parser.add_argument("--anthropic-recheck-minutes", type=positive_int, default=30)
    parser.add_argument("--kimi-model", default="kimi-k2.5")
    parser.add_argument("--kimi-tag-model", default=None)
    parser.add_argument("--kimi-base-url", default=os.environ.get("KIMI_BASE_URL") or os.environ.get("MOONSHOT_BASE_URL") or DEFAULT_KIMI_BASE_URL)
    parser.add_argument("--kimi-cli-bin", default="kimi")
    parser.add_argument("--kimi-cli-base-url", default=os.environ.get("KIMI_CLI_BASE_URL") or DEFAULT_KIMI_CLI_BASE_URL)
    parser.add_argument("--kimi-cli-model", default=DEFAULT_KIMI_CLI_MODEL)
    parser.add_argument("--kimi-api-key-file", type=Path, default=None)
    parser.add_argument("--kimi-min-balance", type=float, default=0.10)
    parser.add_argument("--kimi-recheck-minutes", type=int, default=30)
    parser.add_argument("--both-providers-wait", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--file", type=Path, default=None, help="Process one source SRT file.")
    parser.add_argument("--file-list", type=Path, default=None, help="Process source SRT files listed one per line.")
    parser.add_argument("--batch-size", type=int, default=80)
    parser.add_argument("--workers", type=positive_int, default=1, help="Number of source files to process concurrently.")
    parser.add_argument("--metrics-json", type=Path, default=None, help="Write run timing and action counts as JSON.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--resume", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--tag-only", action="store_true")
    parser.add_argument("--extract-tags", action="store_true")
    parser.add_argument("--codex-bin", default="codex")
    parser.add_argument("--timeout", type=int, default=600)
    return parser


def run(args: argparse.Namespace) -> int:
    source_root, output_root = ensure_safe_roots(args.source_root, args.output_root)
    if not source_exists(source_root):
        raise FileNotFoundError(f"source root does not exist: {source_root}")

    file_list = getattr(args, "file_list", None)
    workers = getattr(args, "workers", 1)
    metrics_json = getattr(args, "metrics_json", None)
    if args.file and file_list:
        raise ValueError("--file and --file-list are mutually exclusive")
    if workers < 1:
        raise ValueError("workers must be >= 1")

    if file_list:
        files = load_file_list(file_list, source_root)
    elif args.file:
        source_file = args.file.expanduser().resolve()
        source_file.relative_to(source_root)
        files = [source_file]
    else:
        files = list(iter_srt_files(source_root))
    if args.limit is not None:
        files = files[: args.limit]

    total_files = len(files)
    log(f"source_root={source_root}")
    log(f"output_root={output_root}")
    log(f"targets={','.join(args.targets)}")
    log(f"srt_files={total_files}")
    log(f"workers={workers}")

    job_store = None if args.dry_run else JobStore(output_root)
    translate_func: Callable[..., dict[int, str]] = call_codex_translate
    translate_multi_func: Callable[..., dict[str, dict[int, str]]] = call_codex_translate_multi
    tag_extract_func: Callable[..., dict] = call_codex_extract_tags
    anthropic_bin = getattr(args, "anthropic_bin", "claude")
    anthropic_model = getattr(args, "anthropic_model", DEFAULT_ANTHROPIC_MODEL)
    anthropic_models = getattr(args, "anthropic_models", None) or (anthropic_model,)
    anthropic_tag_model = getattr(args, "anthropic_tag_model", None) or anthropic_model
    anthropic_use_api_key = getattr(args, "anthropic_use_api_key", False)
    anthropic_recheck_seconds = getattr(args, "anthropic_recheck_minutes", 30) * 60
    anthropic_translate_fallback_func = call_anthropic_translate if shutil.which(anthropic_bin) is not None else None
    anthropic_translate_multi_fallback_func = call_anthropic_translate_multi if shutil.which(anthropic_bin) is not None else None
    anthropic_tag_fallback_func = call_anthropic_extract_tags if shutil.which(anthropic_bin) is not None else None

    if args.provider in ANTHROPIC_PROVIDERS and not args.dry_run:
        if shutil.which(anthropic_bin) is None:
            raise FileNotFoundError(f"anthropic-cli-missing {anthropic_bin}; install Claude Code or pass --provider codex")

        def anthropic_translate(cues, target, **kwargs):
            return call_anthropic_translate(
                cues,
                target,
                model=anthropic_model,
                anthropic_bin=anthropic_bin,
                timeout=kwargs.get("timeout", args.timeout),
                use_api_key=anthropic_use_api_key,
            )

        def anthropic_translate_multi(cues, targets, **kwargs):
            return call_anthropic_translate_multi(
                cues,
                targets,
                model=anthropic_model,
                anthropic_bin=anthropic_bin,
                timeout=kwargs.get("timeout", args.timeout),
                use_api_key=anthropic_use_api_key,
            )

        def anthropic_tags(source_file, sampled_lines, **kwargs):
            return call_anthropic_extract_tags(
                source_file,
                sampled_lines,
                model=anthropic_tag_model,
                anthropic_bin=anthropic_bin,
                timeout=kwargs.get("timeout", args.timeout),
                use_api_key=anthropic_use_api_key,
            )

        translate_func = anthropic_translate
        translate_multi_func = anthropic_translate_multi
        tag_extract_func = anthropic_tags

    if args.provider in ANTHROPIC_FIRST_PROVIDERS and not args.dry_run:
        if shutil.which(anthropic_bin) is None:
            log(f"anthropic-cli-missing {anthropic_bin}; falling back to codex")
        router = ProviderRouter(
            provider=args.provider,
            kimi_client=None,
            kimi_model=getattr(args, "kimi_model", "kimi-k2.5"),
            kimi_tag_model=getattr(args, "kimi_tag_model", None) or getattr(args, "kimi_model", "kimi-k2.5"),
            codex_translate_func=call_codex_translate,
            codex_tag_func=call_codex_extract_tags,
            codex_translate_multi_func=call_codex_translate_multi,
            codex_model=args.model,
            codex_tag_model=args.tag_model,
            kimi_min_balance=getattr(args, "kimi_min_balance", 0.10),
            kimi_recheck_seconds=getattr(args, "kimi_recheck_minutes", 30) * 60,
            both_providers_wait=args.both_providers_wait,
            logger=log,
            anthropic_translate_func=anthropic_translate_fallback_func,
            anthropic_translate_multi_func=anthropic_translate_multi_fallback_func,
            anthropic_tag_func=anthropic_tag_fallback_func,
            anthropic_model=anthropic_model,
            anthropic_models=anthropic_models,
            anthropic_tag_model=anthropic_tag_model,
            anthropic_bin=anthropic_bin,
            anthropic_use_api_key=anthropic_use_api_key,
            anthropic_recheck_seconds=anthropic_recheck_seconds,
        )
        translate_func = router.translate
        translate_multi_func = router.translate_multi
        tag_extract_func = router.extract_tags

    if args.provider in KIMI_PROVIDERS and not args.dry_run:
        kimi_api_key = load_kimi_api_key(args.kimi_api_key_file)
        if not kimi_api_key:
            raise ValueError("Kimi API key missing; set KIMI_API_KEY or pass --kimi-api-key-file")
        if args.provider in KIMI_CLI_PROVIDERS:
            if shutil.which(args.kimi_cli_bin) is None:
                message = f"kimi-cli-missing {args.kimi_cli_bin}; install Kimi CLI or pass --provider codex"
                if args.provider == "kimi-cli":
                    raise FileNotFoundError(message)
                log(message)
                kimi_client = None
                kimi_model = args.kimi_cli_model
                kimi_tag_model = args.kimi_cli_model
            else:
                kimi_client = KimiCliClient(
                    api_key=kimi_api_key,
                    base_url=args.kimi_cli_base_url,
                    kimi_bin=args.kimi_cli_bin,
                )
                kimi_model = args.kimi_cli_model
                kimi_tag_model = args.kimi_cli_model
        else:
            kimi_client = KimiClient(api_key=kimi_api_key, base_url=args.kimi_base_url)
            kimi_model = args.kimi_model
            kimi_tag_model = args.kimi_tag_model or args.kimi_model
        router = ProviderRouter(
            provider=args.provider,
            kimi_client=kimi_client,
            kimi_model=kimi_model,
            kimi_tag_model=kimi_tag_model,
            codex_translate_func=call_codex_translate,
            codex_tag_func=call_codex_extract_tags,
            codex_translate_multi_func=call_codex_translate_multi,
            codex_model=args.model,
            codex_tag_model=args.tag_model,
            kimi_min_balance=args.kimi_min_balance,
            kimi_recheck_seconds=args.kimi_recheck_minutes * 60,
            both_providers_wait=args.both_providers_wait,
            logger=log,
            anthropic_translate_func=anthropic_translate_fallback_func,
            anthropic_translate_multi_func=anthropic_translate_multi_fallback_func,
            anthropic_tag_func=anthropic_tag_fallback_func,
            anthropic_model=anthropic_model,
            anthropic_tag_model=anthropic_tag_model,
            anthropic_bin=anthropic_bin,
            anthropic_use_api_key=anthropic_use_api_key,
        )
        translate_func = router.translate
        translate_multi_func = router.translate_multi
        tag_extract_func = router.extract_tags

    all_results: list[ProcessResult] = []
    started_at = time.monotonic()
    try:
        if workers == 1 or total_files <= 1:
            for file_index, source_file in enumerate(files, start=1):
                all_results.extend(
                    process_source_file_job(
                        source_file=source_file,
                        file_index=file_index,
                        total_files=total_files,
                        args=args,
                        source_root=source_root,
                        output_root=output_root,
                        job_store=job_store,
                        translate_func=translate_func,
                        translate_multi_func=translate_multi_func,
                        tag_extract_func=tag_extract_func,
                        logger=log,
                    )
                )
        else:
            with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="subtitle-worker") as executor:
                futures = {}
                for file_index, source_file in enumerate(files, start=1):
                    future = executor.submit(
                        process_source_file_job,
                        source_file=source_file,
                        file_index=file_index,
                        total_files=total_files,
                        args=args,
                        source_root=source_root,
                        output_root=output_root,
                        job_store=job_store,
                        translate_func=translate_func,
                        translate_multi_func=translate_multi_func,
                        tag_extract_func=tag_extract_func,
                        logger=log,
                    )
                    futures[future] = source_file
                for future in as_completed(futures):
                    source_file = futures[future]
                    try:
                        all_results.extend(future.result())
                    except Exception as exc:
                        rel = source_file.relative_to(source_root)
                        log(f"failed-job\t{rel}\t{exc}")
                        raise
    finally:
        if job_store:
            job_store.close()
    if metrics_json:
        elapsed_seconds = time.monotonic() - started_at
        write_run_metrics(
            metrics_json,
            provider=args.provider,
            workers=workers,
            batch_size=args.batch_size,
            targets=args.targets,
            total_files=total_files,
            elapsed_seconds=elapsed_seconds,
            results=all_results,
        )
        log(f"wrote-metrics\t{metrics_json}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    try:
        return run(args)
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

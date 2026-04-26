from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any


SECRET_PATTERNS = [
    re.compile(r"\bghp_[A-Za-z0-9]{20,}\b"),
    re.compile(r"\bgithub_pat_[A-Za-z0-9_]{20,}\b"),
    re.compile(r"\bsk-[A-Za-z0-9]{20,}\b"),
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----"),
    re.compile(r"\b(AKIA[A-Z0-9]{16})\b"),
    re.compile(r"(?i)aws[_\-]?secret[_\-]?access[_\-]?key\s*[=:]\s*\S+"),
    re.compile(r"\bBearer\s+[A-Za-z0-9\-._~+/]+=*\b"),
    re.compile(r"(?i)(password|secret|token|api_key|apikey)\s*=\s*\S+"),
    re.compile(r"(?i)DefaultEndpointsProtocol=https;AccountName=\S+"),
]


def mask_secrets(value: Any, extra_patterns: list[re.Pattern] | None = None) -> Any:
    patterns = SECRET_PATTERNS + (extra_patterns or [])
    if isinstance(value, str):
        masked = value
        for pattern in patterns:
            masked = pattern.sub("***", masked)
        return masked
    if isinstance(value, list):
        return [mask_secrets(item, extra_patterns) for item in value]
    if isinstance(value, dict):
        return {key: mask_secrets(item, extra_patterns) for key, item in value.items()}
    return value


def truncate_log(text: str, max_bytes: int, *, head_ratio: float = 0.3) -> dict:
    encoded = text.encode("utf-8", errors="replace")
    original_bytes = len(encoded)
    if original_bytes <= max_bytes:
        return {
            "truncated": text,
            "original_bytes": original_bytes,
            "stored_bytes": original_bytes,
            "omitted_bytes": 0,
        }
    head_bytes = int(max_bytes * head_ratio)
    tail_bytes = max_bytes - head_bytes
    head = encoded[:head_bytes].decode("utf-8", errors="replace")
    tail = encoded[-tail_bytes:].decode("utf-8", errors="replace")
    omitted = original_bytes - head_bytes - tail_bytes
    marker = f"...<truncated: {omitted} bytes omitted>..."
    truncated = head + marker + tail
    stored_bytes = len(truncated.encode("utf-8", errors="replace"))
    return {
        "truncated": truncated,
        "original_bytes": original_bytes,
        "stored_bytes": stored_bytes,
        "omitted_bytes": omitted,
    }


def append_jsonl(path: str | Path, payload: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(mask_secrets(payload), ensure_ascii=False) + "\n")
        handle.flush()
        os.fsync(handle.fileno())

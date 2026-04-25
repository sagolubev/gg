from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


SECRET_PATTERNS = [
    re.compile(r"\bghp_[A-Za-z0-9]{20,}\b"),
    re.compile(r"\bgithub_pat_[A-Za-z0-9_]{20,}\b"),
    re.compile(r"\bsk-[A-Za-z0-9]{20,}\b"),
]


def mask_secrets(value: Any) -> Any:
    if isinstance(value, str):
        masked = value
        for pattern in SECRET_PATTERNS:
            masked = pattern.sub("***", masked)
        return masked
    if isinstance(value, list):
        return [mask_secrets(item) for item in value]
    if isinstance(value, dict):
        return {key: mask_secrets(item) for key, item in value.items()}
    return value


def append_jsonl(path: str | Path, payload: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(mask_secrets(payload), ensure_ascii=False) + "\n")

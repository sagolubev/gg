from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

MEMORY_FILES = {
    "session-handoff": ("session-handoff.md", "state"),
    "decisions": ("decisions.md", "decision"),
    "patterns": ("patterns.md", "pattern"),
}

ENTRY_START = "<!-- gg-memory-entry:start -->"
ENTRY_END = "<!-- gg-memory-entry:end -->"
SECRET_PATTERNS = (
    re.compile(r"\bgh[pousr]_[A-Za-z0-9_]{20,}\b"),
    re.compile(r"\bsk-[A-Za-z0-9_-]{20,}\b"),
    re.compile(r"(?i)\b(token|api[_-]?key|secret|password)\b\s*[:=]\s*['\"]?[^'\"\s]{8,}"),
)


@dataclass(frozen=True)
class MemoryEntry:
    id: str
    correlation_id: str
    at: str
    kind: str
    status: str
    author: str
    summary: str
    body: str
    tags: list[str]
    metadata: dict[str, Any]

    def to_block(self) -> str:
        frontmatter = {
            "schema_version": 1,
            "id": self.id,
            "correlation_id": self.correlation_id,
            "at": self.at,
            "kind": self.kind,
            "status": self.status,
            "author": self.author,
            "summary": self.summary,
            "tags": self.tags,
            **self.metadata,
        }
        return "\n".join(
            [
                ENTRY_START,
                "---",
                yaml.safe_dump(frontmatter, sort_keys=False, allow_unicode=True).strip(),
                "---",
                self.body.strip(),
                "",
                ENTRY_END,
                "",
            ]
        )


def append_memory_entry(
    project_path: str | Path,
    *,
    file: str,
    kind: str | None = None,
    status: str = "done",
    summary: str,
    body: str,
    tags: list[str] | None = None,
    author: str = "orchestrator",
    run_id: str = "",
    issue_number: int | None = None,
    candidate_id: str = "",
) -> MemoryEntry:
    root = Path(project_path).resolve()
    filename, expected_kind = _memory_file(file)
    entry_kind = kind or expected_kind
    metadata: dict[str, Any] = {}
    if run_id:
        metadata["run_id"] = run_id
    if issue_number is not None:
        metadata["issue_number"] = issue_number
    if candidate_id:
        metadata["candidate_id"] = candidate_id
    entry = _build_entry(
        kind=entry_kind,
        expected_kind=expected_kind,
        status=status,
        summary=summary,
        body=body,
        tags=tags or [],
        author=author,
        correlation_id=run_id or "manual",
        metadata=metadata,
    )
    errors = validate_entry(entry)
    if errors:
        raise ValueError("; ".join(errors))
    path = root / ".gg" / "memory" / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text(f"# {filename.removesuffix('.md').replace('-', ' ').title()}\n\n", encoding="utf-8")
    with path.open("a", encoding="utf-8") as handle:
        handle.write(entry.to_block())
    return entry


def latest_memory_entries(project_path: str | Path, *, file: str, limit: int = 3) -> list[MemoryEntry]:
    root = Path(project_path).resolve()
    filename, _ = _memory_file(file)
    entries = parse_memory_file(root / ".gg" / "memory" / filename)
    return entries[-max(0, limit):]


def validate_memory(project_path: str | Path) -> list[str]:
    root = Path(project_path).resolve()
    errors: list[str] = []
    for key, (filename, expected_kind) in MEMORY_FILES.items():
        path = root / ".gg" / "memory" / filename
        if not path.exists():
            continue
        for index, entry in enumerate(parse_memory_file(path), start=1):
            for error in validate_entry(entry, expected_kind=expected_kind):
                errors.append(f"{key}.{index}: {error}")
    return errors


def parse_memory_file(path: Path) -> list[MemoryEntry]:
    if not path.exists():
        return []
    text = path.read_text(encoding="utf-8")
    entries: list[MemoryEntry] = []
    for match in re.finditer(
        rf"{re.escape(ENTRY_START)}\s*\n---\s*\n(.*?)\n---\s*\n(.*?)\n{re.escape(ENTRY_END)}",
        text,
        flags=re.DOTALL,
    ):
        meta = yaml.safe_load(match.group(1)) or {}
        body = match.group(2).strip()
        if not isinstance(meta, dict):
            continue
        entries.append(
            MemoryEntry(
                id=str(meta.get("id") or ""),
                correlation_id=str(meta.get("correlation_id") or ""),
                at=str(meta.get("at") or ""),
                kind=str(meta.get("kind") or ""),
                status=str(meta.get("status") or ""),
                author=str(meta.get("author") or ""),
                summary=str(meta.get("summary") or ""),
                body=body,
                tags=list(meta.get("tags") or []),
                metadata={
                    key: value
                    for key, value in meta.items()
                    if key
                    not in {
                        "schema_version",
                        "id",
                        "correlation_id",
                        "at",
                        "kind",
                        "status",
                        "author",
                        "summary",
                        "tags",
                    }
                },
            )
        )
    return entries


def append_constitution_lesson(
    project_path: str | Path,
    *,
    summary: str,
    source: str,
    details: str = "",
) -> bool:
    root = Path(project_path).resolve()
    path = root / ".gg" / "constitution.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        text = path.read_text(encoding="utf-8")
    else:
        text = "# Project Constitution\n"
    marker = "## Learned Patterns"
    if summary in text:
        return False
    at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    line = f"- {at} [{source}] {summary}"
    if details:
        line += f" — {details.strip()[:240]}"
    if marker not in text:
        text = text.rstrip() + f"\n\n{marker}\n\n"
    text = text.rstrip() + "\n" + line + "\n"
    path.write_text(text, encoding="utf-8")
    return True


def validate_entry(entry: MemoryEntry, *, expected_kind: str | None = None) -> list[str]:
    errors: list[str] = []
    if expected_kind and entry.kind != expected_kind:
        errors.append(f"kind must be {expected_kind}, got {entry.kind}")
    if entry.kind not in {"state", "decision", "pattern"}:
        errors.append(f"invalid kind {entry.kind}")
    if entry.status not in {"in_progress", "done", "blocked", "rejected"}:
        errors.append(f"invalid status {entry.status}")
    if entry.author not in {"orchestrator", "agent", "human"}:
        errors.append(f"invalid author {entry.author}")
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]*", entry.id):
        errors.append("id must be lowercase kebab-case")
    if not entry.correlation_id:
        errors.append("correlation_id is required")
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", entry.at):
        errors.append("at must be UTC ISO 8601")
    if not entry.summary or "\n" in entry.summary or len(entry.summary) > 160:
        errors.append("summary must be one line <= 160 chars")
    if len(entry.body.strip()) < 20:
        errors.append("body must contain at least 20 non-whitespace characters")
    combined = f"{entry.summary}\n{entry.body}"
    if any(pattern.search(combined) for pattern in SECRET_PATTERNS):
        errors.append("entry appears to contain a secret")
    return errors


def _build_entry(
    *,
    kind: str,
    expected_kind: str,
    status: str,
    summary: str,
    body: str,
    tags: list[str],
    author: str,
    correlation_id: str,
    metadata: dict[str, Any],
) -> MemoryEntry:
    at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    slug = re.sub(r"[^a-z0-9]+", "-", summary.lower()).strip("-")[:48] or kind
    digest = hashlib.sha256(f"{correlation_id}:{summary}:{at}".encode("utf-8")).hexdigest()[:8]
    entry = MemoryEntry(
        id=f"{kind}-{digest}-{slug}".strip("-"),
        correlation_id=correlation_id,
        at=at,
        kind=kind,
        status=status,
        author=author,
        summary=summary,
        body=body,
        tags=[_tag(tag) for tag in tags if _tag(tag)],
        metadata=metadata,
    )
    validate_entry(entry, expected_kind=expected_kind)
    return entry


def _tag(value: str) -> str:
    return re.sub(r"[^a-z0-9-]+", "-", value.strip().lower()).strip("-")


def _memory_file(name: str) -> tuple[str, str]:
    key = name.removesuffix(".md")
    if key not in MEMORY_FILES:
        raise ValueError(f"unknown memory file {name!r}; expected one of {', '.join(sorted(MEMORY_FILES))}")
    return MEMORY_FILES[key]

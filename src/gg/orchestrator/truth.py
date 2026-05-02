from __future__ import annotations

import hashlib
import json
import re
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from gg.orchestrator.memory import append_constitution_lesson, parse_memory_file

REQUIREMENTS_PATH = ".gg/requirements.json"
SYNC_STATE_PATH = ".gg/memory/sync-state.json"
REQUIREMENT_MARKER_RE = re.compile(r"#\s*(?:gg|plumb):(req-[a-f0-9]{8})\b")
TEST_NAME_RE = re.compile(r"\btest_req_([a-f0-9]{8})_")
REQUIREMENT_WORD_RE = re.compile(
    r"\b(must|should|shall|required?|requires?|ensure|verify|validat(?:e|es|ed|ion)|"
    r"always|never|prevent|block|allow|support|persist|record|track|report)\b",
    re.IGNORECASE,
)
MARKDOWN_EXTENSIONS = {".md", ".markdown"}
CODE_EXTENSIONS = {
    ".py",
    ".js",
    ".jsx",
    ".ts",
    ".tsx",
    ".go",
    ".rs",
    ".java",
    ".kt",
    ".swift",
    ".rb",
    ".php",
    ".cs",
    ".c",
    ".cpp",
    ".h",
    ".hpp",
    ".sh",
    ".yaml",
    ".yml",
    ".toml",
}
IGNORED_PARTS = {
    ".git",
    ".gg",
    ".gg-worktrees",
    ".omx",
    ".ai-factory",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    "node_modules",
    "dist",
    "build",
}


@dataclass(frozen=True)
class Requirement:
    id: str
    text: str
    source_file: str
    line: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "text": self.text,
            "source_file": self.source_file,
            "line": self.line,
        }


def parse_requirements(project_path: str | Path) -> list[dict[str, Any]]:
    root = Path(project_path).resolve()
    requirements = _dedupe_requirements(
        requirement
        for source in _spec_sources(root)
        for requirement in _requirements_from_markdown(root, source)
    )
    path = root / REQUIREMENTS_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write_json(
        path,
        {
            "schema_version": 1,
            "generated_at": _now(),
            "sources": sorted({item.source_file for item in requirements}),
            "requirements": [item.to_dict() for item in requirements],
        },
    )
    return [item.to_dict() for item in requirements]


def load_requirements(project_path: str | Path, *, refresh: bool = False) -> list[dict[str, Any]]:
    root = Path(project_path).resolve()
    path = root / REQUIREMENTS_PATH
    if refresh or not path.exists():
        return parse_requirements(root)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return parse_requirements(root)
    requirements = payload.get("requirements")
    if not isinstance(requirements, list):
        return parse_requirements(root)
    return [item for item in requirements if isinstance(item, dict)]


def truth_coverage(project_path: str | Path, *, refresh: bool = False) -> dict[str, Any]:
    root = Path(project_path).resolve()
    requirements = load_requirements(root, refresh=refresh)
    test_markers = _collect_markers(root, tests_only=True)
    code_markers = _collect_markers(root, tests_only=False)
    requirement_ids = {str(item.get("id") or "") for item in requirements}
    tested = requirement_ids & test_markers
    implemented = requirement_ids & code_markers
    return {
        "schema_version": 1,
        "generated_at": _now(),
        "requirements_total": len(requirements),
        "spec_to_test": _coverage_dimension(requirements, tested),
        "spec_to_code": _coverage_dimension(requirements, implemented),
        "requirements_path": REQUIREMENTS_PATH,
    }


def sync_approved_decisions(project_path: str | Path) -> dict[str, Any]:
    root = Path(project_path).resolve()
    decisions_path = root / ".gg" / "memory" / "decisions.md"
    entries = _latest_by_id(parse_memory_file(decisions_path))
    state = _read_sync_state(root)
    synced_ids = set(state.get("synced_decisions", {}))
    eligible = [
        entry
        for entry in entries
        if entry.status in {"approved", "edited", "done"} and entry.id not in synced_ids
    ]
    synced: list[dict[str, str]] = []
    for entry in eligible:
        changed = append_constitution_lesson(
            root,
            summary=entry.summary,
            source="memory-sync",
            details=_compact_body(entry.body),
        )
        synced_at = _now()
        state.setdefault("synced_decisions", {})[entry.id] = {
            "synced_at": synced_at,
            "summary": entry.summary,
            "changed": changed,
        }
        synced.append({"id": entry.id, "summary": entry.summary, "changed": str(changed).lower()})
    state["schema_version"] = 1
    state["updated_at"] = _now()
    _write_sync_state(root, state)
    return {
        "schema_version": 1,
        "synced": len(synced),
        "skipped_existing": len(synced_ids),
        "decisions": synced,
        "sync_state_path": SYNC_STATE_PATH,
        "constitution_path": ".gg/constitution.md",
    }


def _spec_sources(root: Path) -> list[Path]:
    candidates: list[Path] = []
    for path in (
        root / ".gg" / "constitution.md",
        root / "README.md",
    ):
        if path.exists():
            candidates.append(path)
    for base in (root / "openspec", root / "docs"):
        if not base.exists():
            continue
        candidates.extend(
            path
            for path in sorted(base.rglob("*"))
            if path.is_file() and path.suffix.lower() in MARKDOWN_EXTENSIONS
        )
    return _unique_paths(candidates)


def _requirements_from_markdown(root: Path, path: Path) -> list[Requirement]:
    in_fence = False
    requirements: list[Requirement] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return requirements
    for line_number, line in enumerate(lines, start=1):
        stripped = line.strip()
        if stripped.startswith("```"):
            in_fence = not in_fence
            continue
        if in_fence or not stripped or stripped.startswith("#"):
            continue
        text = _clean_requirement_text(stripped)
        if not text or len(text) < 24:
            continue
        if not REQUIREMENT_WORD_RE.search(text):
            continue
        requirements.append(
            Requirement(
                id=_requirement_id(text),
                text=text,
                source_file=str(path.relative_to(root)),
                line=line_number,
            )
        )
    return requirements


def _clean_requirement_text(text: str) -> str:
    text = re.sub(r"^\s*[-*+]\s+", "", text)
    text = re.sub(r"^\s*\d+[.)]\s+", "", text)
    text = re.sub(r"`([^`]+)`", r"\1", text)
    text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)
    return re.sub(r"\s+", " ", text).strip()


def _requirement_id(text: str) -> str:
    digest = hashlib.sha256(text.strip().lower().encode("utf-8")).hexdigest()[:8]
    return f"req-{digest}"


def _dedupe_requirements(requirements: Any) -> list[Requirement]:
    by_id: dict[str, Requirement] = {}
    for requirement in requirements:
        by_id.setdefault(requirement.id, requirement)
    return sorted(by_id.values(), key=lambda item: (item.source_file, item.line, item.id))


def _collect_markers(root: Path, *, tests_only: bool) -> set[str]:
    markers: set[str] = set()
    for path in _scan_files(root):
        rel = str(path.relative_to(root))
        is_test = _is_test_path(rel, path.name)
        if tests_only and not is_test:
            continue
        if not tests_only and is_test:
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        markers.update(REQUIREMENT_MARKER_RE.findall(text))
        markers.update(f"req-{match}" for match in TEST_NAME_RE.findall(text))
    return markers


def _scan_files(root: Path) -> list[Path]:
    paths: list[Path] = []
    for path in root.rglob("*"):
        if not path.is_file() or path.suffix.lower() not in CODE_EXTENSIONS:
            continue
        rel_parts = set(path.relative_to(root).parts)
        if rel_parts & IGNORED_PARTS:
            continue
        paths.append(path)
    return paths


def _is_test_path(relative_path: str, filename: str) -> bool:
    parts = set(Path(relative_path).parts)
    return "tests" in parts or filename.startswith("test_") or filename.endswith("_test.py")


def _coverage_dimension(requirements: list[dict[str, Any]], covered_ids: set[str]) -> dict[str, Any]:
    covered = [item for item in requirements if str(item.get("id") or "") in covered_ids]
    missing = [item for item in requirements if str(item.get("id") or "") not in covered_ids]
    total = len(requirements)
    return {
        "covered": len(covered),
        "total": total,
        "percent": round((len(covered) / total) * 100, 1) if total else 0.0,
        "missing": [
            {
                "id": item.get("id", ""),
                "text": item.get("text", ""),
                "source_file": item.get("source_file", ""),
                "line": item.get("line", 0),
            }
            for item in missing
        ],
    }


def _latest_by_id(entries: list[Any]) -> list[Any]:
    by_id: dict[str, Any] = {}
    for entry in entries:
        by_id[entry.id] = entry
    return list(by_id.values())


def _read_sync_state(root: Path) -> dict[str, Any]:
    path = root / SYNC_STATE_PATH
    if not path.exists():
        return {"schema_version": 1, "synced_decisions": {}}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"schema_version": 1, "synced_decisions": {}}
    if not isinstance(payload, dict):
        return {"schema_version": 1, "synced_decisions": {}}
    if not isinstance(payload.get("synced_decisions"), dict):
        payload["synced_decisions"] = {}
    return payload


def _write_sync_state(root: Path, state: dict[str, Any]) -> None:
    path = root / SYNC_STATE_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write_json(path, state)


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=path.suffix)
    try:
        with open(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, ensure_ascii=False)
            handle.write("\n")
        Path(tmp).replace(path)
    except Exception:
        Path(tmp).unlink(missing_ok=True)
        raise


def _compact_body(body: str) -> str:
    return re.sub(r"\s+", " ", body.strip())[:240]


def _unique_paths(paths: list[Path]) -> list[Path]:
    seen: set[Path] = set()
    result: list[Path] = []
    for path in paths:
        resolved = path.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        result.append(path)
    return result


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

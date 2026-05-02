from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

PROTOCOL_SURFACE_FILES = (
    "agent_catalog.py",
    "agent_patterns.py",
    "protocol.py",
    "prompts.py",
    "executor.py",
    "finding_feedback.py",
    "prompt_manifest.py",
    "review.py",
    "review_gates.py",
    "task_analysis.py",
)


@dataclass(frozen=True)
class ManifestCheck:
    status: str
    message: str
    missing: list[str]
    mismatched: list[str]


def write_prompt_manifest(project_path: str | Path) -> Path:
    root = Path(project_path).resolve()
    path = root / ".gg" / "prompt-manifest.sha256"
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        f"{digest}  {relative}"
        for relative, digest in _current_protocol_surface_hashes().items()
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def verify_prompt_manifest(project_path: str | Path) -> ManifestCheck:
    root = Path(project_path).resolve()
    path = root / ".gg" / "prompt-manifest.sha256"
    if not path.exists():
        return ManifestCheck(
            status="warn",
            message=".gg/prompt-manifest.sha256 missing; run gg init or regenerate prompt manifest",
            missing=[".gg/prompt-manifest.sha256"],
            mismatched=[],
        )
    expected = _parse_manifest(path)
    current = _current_protocol_surface_hashes()
    missing = [
        *(relative for relative in expected if relative not in current),
        *(relative for relative in current if relative not in expected),
    ]
    mismatched = [
        relative
        for relative, digest in expected.items()
        if relative in current and current[relative] != digest
    ]
    if missing or mismatched:
        return ManifestCheck(
            status="fail",
            message="prompt manifest drift detected",
            missing=missing,
            mismatched=mismatched,
        )
    return ManifestCheck(status="pass", message="prompt manifest matches protocol and prompt sources", missing=[], mismatched=[])


def _current_protocol_surface_hashes() -> dict[str, str]:
    base = Path(__file__).resolve().parent
    hashes: dict[str, str] = {}
    for filename in PROTOCOL_SURFACE_FILES:
        path = base / filename
        if path.exists():
            hashes[f"gg/orchestrator/{filename}"] = hashlib.sha256(path.read_bytes()).hexdigest()
    return hashes


def _parse_manifest(path: Path) -> dict[str, str]:
    entries: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        digest, _, relative = stripped.partition("  ")
        if digest and relative:
            entries[relative.strip()] = digest.strip()
    return entries

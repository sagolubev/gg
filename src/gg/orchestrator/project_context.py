from __future__ import annotations

from pathlib import Path

from gg.orchestrator.agent_catalog import agent_catalog_context
from gg.orchestrator.memory import latest_memory_entries

DEFAULT_CONTEXT_CHARS = 4500


def build_project_precedence_context(
    project_path: str | Path,
    *,
    max_chars: int = DEFAULT_CONTEXT_CHARS,
) -> dict[str, object]:
    root = Path(project_path).resolve()
    sections: list[tuple[str, str, str]] = []
    _add_file_section(sections, root / ".gg" / "constitution.md", "constitution")
    _add_file_section(sections, root / ".gg" / "knowledge" / "repair-lessons.md", "repair_lessons")
    _add_file_section(sections, root / ".gg" / "knowledge" / "exemplars.md", "contributor_exemplars")
    catalog_path = root / ".gg" / "agent-catalog.json"
    if catalog_path.exists():
        sections.append(("agent_catalog", ".gg/agent-catalog.json", agent_catalog_context(root)))
    pattern_entries = latest_memory_entries(root, file="patterns", limit=3)
    if pattern_entries:
        body = "\n".join(f"- {entry.summary}: {entry.body}" for entry in pattern_entries)
        sections.append(("memory_patterns", ".gg/memory/patterns.md", body))
    if not sections:
        return {
            "schema_version": 1,
            "text": "",
            "sources": [],
            "truncated": False,
        }
    lines = [
        "PROJECT PRECEDENCE (authoritative; overrides generic agent advice):",
        "",
        "Follow these project-specific rules before applying backend/persona defaults.",
        "",
    ]
    sources: list[dict[str, str]] = []
    for title, source, body in sections:
        sources.append({"kind": title, "path": source})
        lines.append(f"## {title.replace('_', ' ').title()}")
        lines.append(body.strip())
        lines.append("")
    text = "\n".join(lines).strip()
    truncated = False
    if len(text) > max_chars:
        truncated = True
        text = text[: max(0, max_chars - 32)].rstrip() + "\n... (truncated)"
    return {
        "schema_version": 1,
        "text": text,
        "sources": sources,
        "truncated": truncated,
    }


def _add_file_section(sections: list[tuple[str, str, str]], path: Path, kind: str) -> None:
    if not path.exists():
        return
    text = _essentials(path.read_text(encoding="utf-8"))
    if not text.strip():
        return
    try:
        rel = str(path.relative_to(path.parents[1]))
    except ValueError:
        rel = str(path)
    sections.append((kind, rel, text))


def _essentials(text: str) -> str:
    marker = "\n## Deep Reference"
    if marker in text:
        return text.split(marker, 1)[0].strip()
    return text.strip()

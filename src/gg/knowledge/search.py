"""Search over knowledge artifacts.

Provides keyword search, relevance ranking, and context building
for agent prompts. Works over events, entities, facts, and decisions.

No external dependencies -- uses TF-IDF-like scoring on plain text.
"""
from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from pathlib import Path

from gg.knowledge.collectors import (
    collect_decisions_from_events,
    collect_entities_from_events,
    collect_error_patterns,
    collect_facts_from_events,
)
from gg.knowledge.events import Event, EventLog, EventType


@dataclass(frozen=True)
class SearchResult:
    kind: str  # "entity", "fact", "decision", "event", "error_pattern"
    title: str
    snippet: str
    score: float
    source_file: str = ""
    issue_number: int | None = None
    metadata: dict = field(default_factory=dict)


class KnowledgeSearch:
    def __init__(self, project_path: str | Path):
        self._root = Path(project_path).resolve()
        self._knowledge = self._root / ".gg" / "knowledge"
        self._log = EventLog(self._knowledge)

    def search(self, query: str, *, limit: int = 20) -> list[SearchResult]:
        """Full-text search across all knowledge artifacts."""
        tokens = _tokenize(query)
        if not tokens:
            return []

        results: list[SearchResult] = []
        events = self._log.read_all()

        results = [
            *results,
            *self._search_entities(tokens, events),
            *self._search_facts(tokens, events),
            *self._search_decisions(tokens, events),
            *self._search_events(tokens, events),
            *self._search_error_patterns(tokens, events),
            *self._search_markdown_files(tokens),
        ]

        ranked = sorted(results, key=lambda r: -r.score)
        return ranked[:limit]

    def find_related_to_issue(self, issue_title: str, issue_body: str = "") -> list[SearchResult]:
        """Find knowledge relevant to an issue -- used to build agent context."""
        text = f"{issue_title} {issue_body}"
        return self.search(text, limit=15)

    def find_by_files(self, file_paths: list[str]) -> list[SearchResult]:
        """Find knowledge related to specific files."""
        results: list[SearchResult] = []
        events = self._log.read_all()
        entities = collect_entities_from_events(events)

        for entity in entities:
            overlap = [f for f in file_paths if any(f.startswith(rf) for rf in entity.related_files)]
            if overlap or any(f.startswith(f"{entity.name}/") for f in file_paths):
                results = [
                    *results,
                    SearchResult(
                        kind="entity",
                        title=entity.name,
                        snippet=entity.description or f"Type: {entity.entity_type}",
                        score=1.0 + len(overlap) * 0.5,
                        metadata={"type": entity.entity_type, "files": entity.related_files},
                    ),
                ]

        for ev in events:
            if ev.event_type in (EventType.IMPLEMENTATION_DONE, EventType.RESEARCH_DONE):
                ev_files = ev.data.get("files_changed", ev.data.get("files_analyzed", []))
                overlap = set(file_paths) & set(ev_files)
                if overlap:
                    results = [
                        *results,
                        SearchResult(
                            kind="event",
                            title=ev.event_type.value,
                            snippet=f"Files: {', '.join(overlap)}",
                            score=0.5 + len(overlap) * 0.3,
                            issue_number=ev.issue_number,
                            metadata=ev.data,
                        ),
                    ]

        return sorted(results, key=lambda r: -r.score)

    def find_error_history(self, pattern: str = "") -> list[SearchResult]:
        """Find recurring errors, optionally filtered by pattern."""
        events = self._log.read_all()
        patterns = collect_error_patterns(events)
        results: list[SearchResult] = []

        for p, count in sorted(patterns.items(), key=lambda x: -x[1]):
            if pattern and pattern.lower() not in p.lower():
                continue
            results = [
                *results,
                SearchResult(
                    kind="error_pattern",
                    title=p,
                    snippet=f"Occurred {count} times",
                    score=count,
                    metadata={"count": count},
                ),
            ]
        return results

    def find_repair_lessons(
        self,
        *,
        issue_title: str,
        issue_body: str = "",
        file_paths: list[str] | None = None,
        limit: int = 5,
    ) -> list[SearchResult]:
        tokens = _tokenize(f"{issue_title} {issue_body} {' '.join(file_paths or [])}")
        results: list[SearchResult] = []
        for ev in self._log.read_all():
            if ev.event_type is not EventType.REPAIR_LESSON:
                continue
            data = ev.data
            files = [str(path) for path in data.get("files_changed") or []]
            text = _dict_to_text(data)
            score = _score(tokens, text)
            overlap = set(file_paths or []) & set(files)
            if overlap:
                score += 1.0 + len(overlap) * 0.5
            if score <= 0:
                continue
            results.append(
                SearchResult(
                    kind="repair_lesson",
                    title=str(data.get("fingerprint") or data.get("candidate_id") or "repair lesson"),
                    snippet=str(data.get("repair_reason") or data.get("failure_reason") or "")[:180],
                    score=score * 2.0,
                    issue_number=ev.issue_number,
                    metadata=dict(data),
                )
            )
        return sorted(results, key=lambda item: -item.score)[:limit]

    def build_context_for_issue(self, issue_title: str, issue_body: str = "") -> str:
        """Build a knowledge context string for agent prompts."""
        results = self.find_related_to_issue(issue_title, issue_body)
        repair_lessons = self.find_repair_lessons(
            issue_title=issue_title,
            issue_body=issue_body,
            file_paths=[],
            limit=3,
        )

        sections: list[str] = []

        goals_path = self._root / ".gg" / "goals.md"
        if goals_path.exists():
            goals = goals_path.read_text(encoding="utf-8").strip()
            if goals:
                sections.append(f"## Project Goals\n\n{goals}")

        risk_path = self._knowledge / "risk-register.md"
        if risk_path.exists():
            risk_content = risk_path.read_text(encoding="utf-8")
            high_risks = [line for line in risk_content.splitlines() if "| High |" in line]
            if high_risks:
                sections.append("\n## High-Priority Risks\n")
                for line in high_risks[:5]:
                    sections.append(line)

        if not results and not repair_lessons and not sections:
            return ""

        if results or repair_lessons:
            sections.append("\n## Relevant Knowledge")

        if repair_lessons:
            sections.append("\n### Similar Past Mistakes")
            for lesson in repair_lessons:
                failure = str(lesson.metadata.get("failure_reason") or "").strip()
                repair = str(lesson.metadata.get("repair_reason") or lesson.snippet).strip()
                files = ", ".join(map(str, lesson.metadata.get("files_changed") or []))
                sections.append(
                    f"- {lesson.title}: avoid repeating `{failure}`; prefer `{repair}`"
                    + (f" (files: {files})" if files else "")
                )

        exemplar_section = self._build_exemplar_context()
        if exemplar_section:
            sections.append(exemplar_section)

        entities = [r for r in results if r.kind == "entity"]
        if entities:
            sections.append("\n### Related Modules")
            for r in entities[:5]:
                sections.append(f"- **{r.title}**: {r.snippet}")

        decisions = [r for r in results if r.kind == "decision"]
        if decisions:
            sections.append("\n### Past Decisions")
            for r in decisions[:3]:
                sections.append(f"- **{r.title}**: {r.snippet}")

        facts = [r for r in results if r.kind == "fact"]
        if facts:
            sections.append("\n### Known Facts")
            for r in facts[:5]:
                sections.append(f"- {r.title}: {r.snippet}")

        errors = [r for r in results if r.kind == "error_pattern"]
        if errors:
            sections.append("\n### Common Errors")
            for r in errors[:3]:
                sections.append(f"- {r.title} ({r.snippet})")

        past_work = [r for r in results if r.kind == "event"]
        if past_work:
            issues_seen: set[int] = set()
            sections.append("\n### Past Related Work")
            for r in past_work[:5]:
                if r.issue_number and r.issue_number not in issues_seen:
                    issues_seen = {*issues_seen, r.issue_number}
                    sections.append(f"- Issue #{r.issue_number}: {r.title}")

        return "\n".join(sections)

    def _build_exemplar_context(self) -> str:
        exemplar_path = self._knowledge / "exemplars.json"
        if not exemplar_path.exists():
            return ""
        try:
            import json

            payload = json.loads(exemplar_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return ""
        contributors = payload.get("contributors") if isinstance(payload, dict) else None
        exemplars = payload.get("exemplars") if isinstance(payload, dict) else None
        lines = ["\n### Project Exemplars"]
        if contributors:
            lines.append("Strong contributors to mirror:")
            for contributor in list(contributors)[:3]:
                if not isinstance(contributor, dict):
                    continue
                name = str(contributor.get("name") or "").strip()
                reason = str(contributor.get("reason") or "").strip()
                if name:
                    lines.append(f"- {name}" + (f": {reason}" if reason else ""))
        if exemplars:
            lines.append("Reference local examples:")
            for exemplar in list(exemplars)[:3]:
                if not isinstance(exemplar, dict):
                    continue
                sha = str(exemplar.get("sha") or "")[:12]
                message = str(exemplar.get("message") or "").strip()
                if sha or message:
                    lines.append(f"- `{sha}` {message}".rstrip())
        return "\n".join(lines) if len(lines) > 1 else ""

    # -- Internal search methods --

    def _search_entities(self, tokens: list[str], events: list[Event]) -> list[SearchResult]:
        entities = collect_entities_from_events(events)
        results: list[SearchResult] = []
        for e in entities:
            text = f"{e.name} {e.entity_type} {e.description} {' '.join(e.related_files)}"
            for f in e.facts:
                text += f" {f.key} {f.value}"
            score = _score(tokens, text)
            if score > 0:
                results = [
                    *results,
                    SearchResult(
                        kind="entity",
                        title=e.name,
                        snippet=e.description or f"Type: {e.entity_type}",
                        score=score * 1.5,  # boost entities
                        metadata={"type": e.entity_type},
                    ),
                ]
        return results

    def _search_facts(self, tokens: list[str], events: list[Event]) -> list[SearchResult]:
        facts = collect_facts_from_events(events)
        results: list[SearchResult] = []
        for f in facts:
            text = f"{f.key} {f.value} {' '.join(f.tags)}"
            score = _score(tokens, text)
            if score > 0:
                results = [
                    *results,
                    SearchResult(
                        kind="fact",
                        title=f.key,
                        snippet=f.value,
                        score=score * f.confidence,
                        metadata={"source": f.source, "confidence": f.confidence},
                    ),
                ]
        return results

    def _search_decisions(self, tokens: list[str], events: list[Event]) -> list[SearchResult]:
        decisions = collect_decisions_from_events(events)
        results: list[SearchResult] = []
        for d in decisions:
            text = f"{d.title} {d.context} {d.decision} {d.consequences}"
            score = _score(tokens, text)
            if score > 0:
                results = [
                    *results,
                    SearchResult(
                        kind="decision",
                        title=d.title,
                        snippet=d.decision[:120],
                        score=score * 1.3,  # boost decisions
                        issue_number=d.issue_number,
                    ),
                ]
        return results

    def _search_events(self, tokens: list[str], events: list[Event]) -> list[SearchResult]:
        results: list[SearchResult] = []
        for ev in events:
            text = f"{ev.event_type.value} {ev.source} {_dict_to_text(ev.data)}"
            score = _score(tokens, text)
            if score > 0:
                results = [
                    *results,
                    SearchResult(
                        kind="event",
                        title=ev.event_type.value,
                        snippet=_dict_to_text(ev.data)[:120],
                        score=score * 0.5,  # lower weight for raw events
                        issue_number=ev.issue_number,
                        metadata={"timestamp": ev.timestamp},
                    ),
                ]
        return results

    def _search_error_patterns(self, tokens: list[str], events: list[Event]) -> list[SearchResult]:
        patterns = collect_error_patterns(events)
        results: list[SearchResult] = []
        for pattern, count in patterns.items():
            score = _score(tokens, pattern)
            if score > 0:
                results = [
                    *results,
                    SearchResult(
                        kind="error_pattern",
                        title=pattern,
                        snippet=f"Occurred {count} times",
                        score=score * math.log2(count + 1),
                        metadata={"count": count},
                    ),
                ]
        return results

    def _search_markdown_files(self, tokens: list[str]) -> list[SearchResult]:
        """Search compiled markdown artifacts on disk."""
        results: list[SearchResult] = []
        if not self._knowledge.exists():
            return results

        for md in self._knowledge.rglob("*.md"):
            rel = str(md.relative_to(self._knowledge))
            if rel == "RESOLVER.md":
                continue
            try:
                content = md.read_text(encoding="utf-8")
            except OSError:
                continue
            score = _score(tokens, content)
            if score > 0:
                first_line = content.split("\n", 1)[0].lstrip("# ").strip()
                results = [
                    *results,
                    SearchResult(
                        kind="file",
                        title=first_line or rel,
                        snippet=rel,
                        score=score * 0.3,  # low weight for file content
                        source_file=str(md),
                    ),
                ]
        return results


def _tokenize(text: str) -> list[str]:
    return [w.lower() for w in re.split(r"[^a-zA-Z0-9_.-]+", text) if len(w) >= 2]


def _score(query_tokens: list[str], text: str) -> float:
    text_lower = text.lower()
    text_tokens = set(_tokenize(text))
    if not query_tokens or not text_tokens:
        return 0.0

    matched = sum(1 for t in query_tokens if t in text_lower)
    if matched == 0:
        return 0.0

    coverage = matched / len(query_tokens)
    idf_boost = 1.0 / math.log2(len(text_tokens) + 2)
    return coverage * (1.0 + idf_boost)


def _dict_to_text(d: dict) -> str:
    parts: list[str] = []
    for k, v in d.items():
        if isinstance(v, list):
            parts = [*parts, f"{k}: {' '.join(str(i) for i in v)}"]
        else:
            parts = [*parts, f"{k}: {v}"]
    return " ".join(parts)

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
    Decision,
    Entity,
    Fact,
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

    def build_context_for_issue(self, issue_title: str, issue_body: str = "") -> str:
        """Build a knowledge context string for agent prompts."""
        results = self.find_related_to_issue(issue_title, issue_body)
        if not results:
            return ""

        sections: list[str] = ["## Relevant Knowledge"]

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

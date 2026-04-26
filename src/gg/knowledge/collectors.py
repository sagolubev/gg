"""Collectors extract structured knowledge from events and external sources.

Each collector reads events and/or the codebase, then produces facts,
entities, or decisions that the compiler writes to disk.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from gg.knowledge.events import Event, EventType


@dataclass(frozen=True)
class Fact:
    """A single piece of knowledge with source attribution."""
    key: str
    value: str
    source: str
    confidence: float = 1.0
    valid_from: str = ""
    tags: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class Entity:
    name: str
    entity_type: str
    description: str = ""
    facts: list[Fact] = field(default_factory=list)
    related_files: list[str] = field(default_factory=list)
    owner: str = ""
    change_frequency: int = 0


@dataclass(frozen=True)
class Decision:
    title: str
    context: str
    decision: str
    consequences: str = ""
    issue_number: int | None = None
    timestamp: str = ""


def collect_entities_from_events(events: list[Event]) -> list[Entity]:
    """Extract entities discovered during pipeline runs."""
    entities: dict[str, Entity] = {}

    for ev in events:
        if ev.event_type == EventType.ENTITY_DISCOVERED:
            name = ev.data.get("name", "")
            if not name:
                continue
            existing = entities.get(name)
            new_facts = [
                Fact(
                    key=f["key"],
                    value=f["value"],
                    source=ev.source,
                    valid_from=ev.timestamp,
                )
                for f in ev.data.get("facts", [])
            ]
            if existing:
                merged_facts = [*existing.facts, *new_facts]
                entities = {
                    **entities,
                    name: Entity(
                        name=existing.name,
                        entity_type=ev.data.get("type", existing.entity_type),
                        description=ev.data.get("description", existing.description),
                        facts=merged_facts,
                        related_files=[
                            *existing.related_files,
                            *[f for f in ev.data.get("files", []) if f not in existing.related_files],
                        ],
                        owner=ev.data.get("owner", existing.owner),
                        change_frequency=existing.change_frequency + 1,
                    ),
                }
            else:
                entities = {
                    **entities,
                    name: Entity(
                        name=name,
                        entity_type=ev.data.get("type", "module"),
                        description=ev.data.get("description", ""),
                        facts=new_facts,
                        related_files=ev.data.get("files", []),
                        owner=ev.data.get("owner", ""),
                        change_frequency=1,
                    ),
                }

    return list(entities.values())


def collect_decisions_from_events(events: list[Event]) -> list[Decision]:
    """Extract architecture decisions recorded during pipeline runs."""
    decisions: list[Decision] = []
    for ev in events:
        if ev.event_type == EventType.DECISION_RECORDED:
            decisions = [
                *decisions,
                Decision(
                    title=ev.data.get("title", "Untitled decision"),
                    context=ev.data.get("context", ""),
                    decision=ev.data.get("decision", ""),
                    consequences=ev.data.get("consequences", ""),
                    issue_number=ev.issue_number,
                    timestamp=ev.timestamp,
                ),
            ]
    return decisions


def collect_facts_from_events(events: list[Event]) -> list[Fact]:
    """Extract standalone facts learned during pipeline runs."""
    facts: list[Fact] = []
    for ev in events:
        if ev.event_type == EventType.FACT_LEARNED:
            facts = [
                *facts,
                Fact(
                    key=ev.data.get("key", ""),
                    value=ev.data.get("value", ""),
                    source=ev.source,
                    confidence=ev.data.get("confidence", 1.0),
                    valid_from=ev.timestamp,
                    tags=ev.data.get("tags", []),
                ),
            ]
    return facts


def collect_error_patterns(events: list[Event]) -> dict[str, int]:
    """Count recurring error patterns for learning."""
    patterns: dict[str, int] = {}
    for ev in events:
        if ev.event_type == EventType.ERROR:
            pattern = ev.data.get("pattern", ev.data.get("message", "unknown"))
            patterns = {**patterns, pattern: patterns.get(pattern, 0) + 1}
    return patterns


def collect_file_touch_frequency(events: list[Event]) -> dict[str, int]:
    """Count how often files are touched across all pipeline runs."""
    freq: dict[str, int] = {}
    for ev in events:
        if ev.event_type in (
            EventType.IMPLEMENTATION_DONE,
            EventType.RESEARCH_DONE,
        ):
            for f in ev.data.get("files_changed", []):
                freq = {**freq, f: freq.get(f, 0) + 1}
    return freq

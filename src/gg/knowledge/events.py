"""Append-only event log. Every action in gg writes an event here.

Events are JSONL -- one JSON object per line, always appended, never edited.
Each event carries a type, timestamp, and arbitrary data payload.
The event log is the single source of truth; all other knowledge artifacts
(entities, fact-registry, decisions) are materialized views compiled from it.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path


class EventType(str, Enum):
    INIT = "init"
    ISSUE_PICKED = "issue_picked"
    RESEARCH_DONE = "research_done"
    PLAN_CREATED = "plan_created"
    IMPLEMENTATION_STARTED = "implementation_started"
    IMPLEMENTATION_DONE = "implementation_done"
    TESTS_RUN = "tests_run"
    PR_CREATED = "pr_created"
    PR_MERGED = "pr_merged"
    PR_REJECTED = "pr_rejected"
    REVIEW_DONE = "review_done"
    REWORK_STARTED = "rework_started"
    ERROR = "error"
    DECISION_RECORDED = "decision_recorded"
    ENTITY_DISCOVERED = "entity_discovered"
    FACT_LEARNED = "fact_learned"
    KNOWLEDGE_REBUILT = "knowledge_rebuilt"


@dataclass(frozen=True)
class Event:
    event_type: EventType
    data: dict = field(default_factory=dict)
    timestamp: str = ""
    issue_number: int | None = None
    source: str = ""

    def __post_init__(self):
        if not self.timestamp:
            object.__setattr__(self, "timestamp", datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict:
        d = asdict(self)
        d["event_type"] = self.event_type.value
        return d


class EventLog:
    """Append-only JSONL event log."""

    def __init__(self, knowledge_path: Path):
        self._sessions_dir = knowledge_path / "sessions"
        self._sessions_dir.mkdir(parents=True, exist_ok=True)
        self._global_log = self._sessions_dir / "events.jsonl"

    def append(self, event: Event) -> None:
        line = json.dumps(event.to_dict(), ensure_ascii=False)
        with open(self._global_log, "a", encoding="utf-8") as f:
            f.write(line + "\n")

        if event.issue_number is not None:
            issue_log = self._sessions_dir / f"issue-{event.issue_number}.jsonl"
            with open(issue_log, "a", encoding="utf-8") as f:
                f.write(line + "\n")

    def read_all(self) -> list[Event]:
        if not self._global_log.exists():
            return []
        events: list[Event] = []
        for line in self._global_log.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            d = json.loads(line)
            events = [
                *events,
                Event(
                    event_type=EventType(d["event_type"]),
                    data=d.get("data", {}),
                    timestamp=d.get("timestamp", ""),
                    issue_number=d.get("issue_number"),
                    source=d.get("source", ""),
                ),
            ]
        return events

    def read_for_issue(self, issue_number: int) -> list[Event]:
        path = self._sessions_dir / f"issue-{issue_number}.jsonl"
        if not path.exists():
            return []
        events: list[Event] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            d = json.loads(line)
            events = [
                *events,
                Event(
                    event_type=EventType(d["event_type"]),
                    data=d.get("data", {}),
                    timestamp=d.get("timestamp", ""),
                    issue_number=d.get("issue_number"),
                    source=d.get("source", ""),
                ),
            ]
        return events

    def count(self) -> int:
        if not self._global_log.exists():
            return 0
        return sum(1 for line in self._global_log.read_text().splitlines() if line.strip())

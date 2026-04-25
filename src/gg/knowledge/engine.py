"""Knowledge Engine -- central facade for the knowledge system.

Every component in gg interacts with knowledge through this engine.
It wraps EventLog + Compiler and provides high-level methods:

    engine = KnowledgeEngine(project_path)
    engine.record_issue_picked(issue_number=42, title="Add auth")
    engine.record_implementation_done(issue_number=42, files=["src/auth.py"])
    engine.rebuild()  # recompile all artifacts

The engine auto-triggers incremental updates after each record call.
Full rebuilds happen explicitly or via `gg knowledge rebuild`.
"""
from __future__ import annotations

from pathlib import Path

from gg.knowledge.compiler import KnowledgeCompiler
from gg.knowledge.events import Event, EventLog, EventType
from gg.knowledge.search import KnowledgeSearch, SearchResult


class KnowledgeEngine:
    def __init__(self, project_path: str | Path):
        self._root = Path(project_path).resolve()
        self._knowledge = self._root / ".gg" / "knowledge"
        self._knowledge.mkdir(parents=True, exist_ok=True)
        self._log = EventLog(self._knowledge)
        self._compiler = KnowledgeCompiler(self._root)
        self._search = KnowledgeSearch(self._root)
        self._events_since_rebuild = 0
        self._auto_rebuild_threshold = 10

    # -- Recording methods (one per pipeline step) --

    def record_init(self, *, data: dict) -> None:
        self._emit(EventType.INIT, data=data, source="gg init")

    def record_issue_picked(self, *, issue_number: int, title: str, labels: list[str] | None = None) -> None:
        self._emit(
            EventType.ISSUE_PICKED,
            issue_number=issue_number,
            data={"title": title, "labels": labels or []},
            source="gg run",
        )

    def record_research_done(
        self, *, issue_number: int, files_analyzed: list[str], summary: str = "",
    ) -> None:
        self._emit(
            EventType.RESEARCH_DONE,
            issue_number=issue_number,
            data={"files_analyzed": files_analyzed, "summary": summary},
            source="research",
        )
        self._auto_discover_entities(files_analyzed, issue_number)

    def record_plan_created(self, *, issue_number: int, plan_summary: str, files_to_change: list[str]) -> None:
        self._emit(
            EventType.PLAN_CREATED,
            issue_number=issue_number,
            data={"plan_summary": plan_summary, "files_to_change": files_to_change},
            source="planner",
        )

    def record_implementation_started(self, *, issue_number: int) -> None:
        self._emit(
            EventType.IMPLEMENTATION_STARTED,
            issue_number=issue_number,
            source="codex",
        )

    def record_implementation_done(
        self, *, issue_number: int, files_changed: list[str], lines_added: int = 0, lines_removed: int = 0,
    ) -> None:
        ev = self._emit(
            EventType.IMPLEMENTATION_DONE,
            issue_number=issue_number,
            data={
                "files_changed": files_changed,
                "lines_added": lines_added,
                "lines_removed": lines_removed,
            },
            source="codex",
        )
        self._compiler.incremental_update([ev])

    def record_tests_run(
        self, *, issue_number: int, passed: bool, output: str = "", test_count: int = 0,
    ) -> None:
        self._emit(
            EventType.TESTS_RUN,
            issue_number=issue_number,
            data={"passed": passed, "output": output[:500], "test_count": test_count},
            source="test_runner",
        )

    def record_pr_created(self, *, issue_number: int, pr_url: str, pr_number: int) -> None:
        self._emit(
            EventType.PR_CREATED,
            issue_number=issue_number,
            data={"pr_url": pr_url, "pr_number": pr_number},
            source="gg run",
        )

    def record_pr_merged(self, *, issue_number: int, pr_number: int) -> None:
        ev = self._emit(
            EventType.PR_MERGED,
            issue_number=issue_number,
            data={"pr_number": pr_number},
            source="gg run",
        )
        self._compiler.incremental_update([ev])

    def record_pr_rejected(self, *, issue_number: int, pr_number: int, reason: str = "") -> None:
        self._emit(
            EventType.PR_REJECTED,
            issue_number=issue_number,
            data={"pr_number": pr_number, "reason": reason},
            source="review",
        )

    def record_review_done(
        self, *, issue_number: int | None = None, pr_number: int, verdict: str, comments: list[str] | None = None,
    ) -> None:
        self._emit(
            EventType.REVIEW_DONE,
            issue_number=issue_number,
            data={"pr_number": pr_number, "verdict": verdict, "comments": comments or []},
            source="reviewer",
        )

    def record_rework_started(self, *, issue_number: int, reason: str) -> None:
        self._emit(
            EventType.REWORK_STARTED,
            issue_number=issue_number,
            data={"reason": reason},
            source="gg run",
        )

    def record_error(self, *, issue_number: int | None = None, message: str, pattern: str = "") -> None:
        self._emit(
            EventType.ERROR,
            issue_number=issue_number,
            data={"message": message, "pattern": pattern or message[:80]},
            source="error",
        )

    def record_decision(
        self, *, issue_number: int | None = None, title: str, context: str, decision: str, consequences: str = "",
    ) -> None:
        ev = self._emit(
            EventType.DECISION_RECORDED,
            issue_number=issue_number,
            data={
                "title": title, "context": context,
                "decision": decision, "consequences": consequences,
            },
            source="decision",
        )
        self._compiler.incremental_update([ev])

    def record_entity(
        self, *, name: str, entity_type: str = "module", description: str = "",
        files: list[str] | None = None, owner: str = "", facts: list[dict] | None = None,
    ) -> None:
        ev = self._emit(
            EventType.ENTITY_DISCOVERED,
            data={
                "name": name, "type": entity_type, "description": description,
                "files": files or [], "owner": owner, "facts": facts or [],
            },
            source="discovery",
        )
        self._compiler.incremental_update([ev])

    def record_fact(
        self, *, key: str, value: str, confidence: float = 1.0, tags: list[str] | None = None,
    ) -> None:
        ev = self._emit(
            EventType.FACT_LEARNED,
            data={"key": key, "value": value, "confidence": confidence, "tags": tags or []},
            source="learning",
        )
        self._compiler.incremental_update([ev])

    # -- Query methods --

    def get_issue_history(self, issue_number: int) -> list[Event]:
        return self._log.read_for_issue(issue_number)

    def get_all_events(self) -> list[Event]:
        return self._log.read_all()

    def get_event_count(self) -> int:
        return self._log.count()

    # -- Search --

    def search(self, query: str, *, limit: int = 20) -> list[SearchResult]:
        """Full-text search across all knowledge."""
        return self._search.search(query, limit=limit)

    def context_for_issue(self, title: str, body: str = "") -> str:
        """Build knowledge context string for agent prompts."""
        return self._search.build_context_for_issue(title, body)

    def find_by_files(self, file_paths: list[str]) -> list[SearchResult]:
        """Find knowledge related to specific files."""
        return self._search.find_by_files(file_paths)

    def find_errors(self, pattern: str = "") -> list[SearchResult]:
        """Find recurring error patterns."""
        return self._search.find_error_history(pattern)

    def get_goals(self) -> str:
        """Read project goals. Agent should call this before every task."""
        goals_path = self._root / ".gg" / "goals.md"
        if goals_path.exists():
            return goals_path.read_text(encoding="utf-8")
        return ""

    def get_risks(self) -> str:
        """Read risk register for awareness before changes."""
        risk_path = self._knowledge / "risk-register.md"
        if risk_path.exists():
            return risk_path.read_text(encoding="utf-8")
        return ""

    # -- Rebuild --

    def rebuild(self) -> dict[str, int]:
        """Full recompilation of all knowledge artifacts from events + git."""
        stats = self._compiler.rebuild()
        self._events_since_rebuild = 0
        return stats

    # -- Internal --

    # Events that trigger an immediate full rebuild
    _REBUILD_TRIGGERS = frozenset({
        EventType.PR_MERGED,
        EventType.KNOWLEDGE_REBUILT,
    })

    def _emit(self, event_type: EventType, *, data: dict | None = None,
              issue_number: int | None = None, source: str = "") -> Event:
        event = Event(
            event_type=event_type,
            data=data or {},
            issue_number=issue_number,
            source=source,
        )
        self._log.append(event)
        self._events_since_rebuild += 1
        self._maybe_auto_rebuild(event_type)
        return event

    def _maybe_auto_rebuild(self, event_type: EventType) -> None:
        if event_type in self._REBUILD_TRIGGERS:
            self._compiler.rebuild()
            self._events_since_rebuild = 0
            return
        if self._events_since_rebuild >= self._auto_rebuild_threshold:
            self._compiler.rebuild()
            self._events_since_rebuild = 0

    def _auto_discover_entities(self, files: list[str], issue_number: int) -> None:
        """Auto-discover entities from file paths touched during research."""
        seen_modules: set[str] = set()
        for f in files:
            parts = Path(f).parts
            if len(parts) >= 2:
                module = parts[0]
                if module not in seen_modules and not module.startswith("."):
                    seen_modules = {*seen_modules, module}
                    self.record_entity(
                        name=module,
                        entity_type="module",
                        files=[f],
                    )

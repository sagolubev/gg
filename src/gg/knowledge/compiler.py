"""Compiler materializes knowledge artifacts from the event log.

It reads all events, runs collectors, and writes/updates:
- entities/ markdown files
- fact-registry.md
- decisions/ markdown files
- error-patterns.md

The compiler is idempotent: running it twice produces the same output.
It merges event-derived knowledge with git-history-derived knowledge.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from gg.analyzers.git_history import GitProfile, analyze_git_history
from gg.analyzers.structure import StructureMap, analyze_structure
from gg.knowledge.collectors import (
    collect_decisions_from_events,
    collect_entities_from_events,
    collect_error_patterns,
    collect_facts_from_events,
    collect_file_touch_frequency,
    Decision,
    Entity,
    Fact,
)
from gg.knowledge.events import Event, EventLog, EventType


class KnowledgeCompiler:
    def __init__(self, project_path: str | Path):
        self._root = Path(project_path).resolve()
        self._knowledge = self._root / ".gg" / "knowledge"
        self._event_log = EventLog(self._knowledge)

    def rebuild(self) -> dict[str, int]:
        """Full rebuild of all knowledge artifacts. Returns stats."""
        self._knowledge.mkdir(parents=True, exist_ok=True)
        for sub in ("entities", "decisions", "sessions"):
            (self._knowledge / sub).mkdir(exist_ok=True)

        git_profile = analyze_git_history(self._root)
        structure = analyze_structure(self._root)
        events = self._event_log.read_all()

        entity_count = self._compile_entities(events, structure, git_profile)
        fact_count = self._compile_fact_registry(events, git_profile)
        decision_count = self._compile_decisions(events)
        self._compile_error_patterns(events)
        self._compile_pipeline_stats(events)

        self._event_log.append(Event(
            event_type=EventType.KNOWLEDGE_REBUILT,
            data={
                "entities": entity_count,
                "facts": fact_count,
                "decisions": decision_count,
                "total_events": len(events),
            },
            source="compiler",
        ))

        return {
            "entities": entity_count,
            "facts": fact_count,
            "decisions": decision_count,
            "events_processed": len(events),
        }

    def incremental_update(self, new_events: list[Event]) -> None:
        """Process only new events -- faster than full rebuild."""
        new_entities = collect_entities_from_events(new_events)
        for entity in new_entities:
            self._upsert_entity(entity)

        new_decisions = collect_decisions_from_events(new_events)
        for decision in new_decisions:
            self._write_decision(decision)

        new_facts = collect_facts_from_events(new_events)
        if new_facts:
            self._append_facts_to_registry(new_facts)

    def _compile_entities(
        self, events: list[Event], structure: StructureMap, git: GitProfile,
    ) -> int:
        entities_dir = self._knowledge / "entities"
        hot_map = dict(git.hot_files) if git.hot_files else {}
        pipeline_freq = collect_file_touch_frequency(events)
        event_entities = {e.name: e for e in collect_entities_from_events(events)}

        all_names: set[str] = {*structure.top_level_dirs, *event_entities.keys()}

        for name in all_names:
            role = structure.classifications.get(name, "unknown")
            event_entity = event_entities.get(name)

            if event_entity:
                role = event_entity.entity_type if event_entity.entity_type != "module" else role

            relevant_hot = sorted(
                [(f, c) for f, c in hot_map.items() if f.startswith(f"{name}/")],
                key=lambda x: -x[1],
            )[:5]

            relevant_pipeline = sorted(
                [(f, c) for f, c in pipeline_freq.items() if f.startswith(f"{name}/")],
                key=lambda x: -x[1],
            )[:5]

            owner = ""
            if event_entity and event_entity.owner:
                owner = event_entity.owner
            elif git.contributors:
                owner = git.contributors[0].name

            lines = [f"# {name}", "", f"Type: {role}"]

            if event_entity and event_entity.description:
                lines.append(f"Description: {event_entity.description}")
            lines.append("")

            if relevant_hot:
                lines.append("## Hot Files (git history)")
                for f, c in relevant_hot:
                    lines.append(f"  - {f}: {c} changes")
                lines.append("")

            if relevant_pipeline:
                lines.append("## Pipeline Activity")
                for f, c in relevant_pipeline:
                    lines.append(f"  - {f}: touched {c} times by agents")
                lines.append("")

            if event_entity and event_entity.facts:
                lines.append("## Learned Facts")
                for fact in event_entity.facts:
                    lines.append(f"  - **{fact.key}**: {fact.value} (source: {fact.source})")
                lines.append("")

            if event_entity and event_entity.related_files:
                lines.append("## Related Files")
                for f in event_entity.related_files[:10]:
                    lines.append(f"  - {f}")
                lines.append("")

            if owner:
                lines.append(f"## Owner\n\n- {owner}\n")

            (entities_dir / f"{name}.md").write_text("\n".join(lines), encoding="utf-8")

        return len(all_names)

    def _compile_fact_registry(self, events: list[Event], git: GitProfile) -> int:
        event_facts = collect_facts_from_events(events)
        pipeline_freq = collect_file_touch_frequency(events)

        lines = ["# Fact Registry", "", f"Last compiled: {datetime.now(timezone.utc).isoformat()}", ""]

        # Git-derived facts
        if git.contributors:
            lines.append("## Contributors")
            lines.append("")
            lines.append("| Name | Commits | Last Active |")
            lines.append("|------|---------|-------------|")
            for c in git.contributors[:15]:
                lines.append(f"| {c.name} | {c.commits} | {c.last_active} |")
            lines.append("")

        if git.hot_files:
            lines.append("## Most Changed Files (git)")
            lines.append("")
            lines.append("| File | Git Changes | Agent Touches |")
            lines.append("|------|-------------|---------------|")
            for f, count in git.hot_files:
                agent_count = pipeline_freq.get(f, 0)
                lines.append(f"| {f} | {count} | {agent_count} |")
            lines.append("")

        if git.coupled_files:
            lines.append("## Co-Changed Files")
            lines.append("")
            lines.append("| File A | File B | Score |")
            lines.append("|--------|--------|-------|")
            for f1, f2, score in git.coupled_files:
                lines.append(f"| {f1} | {f2} | {score:.2f} |")
            lines.append("")

        if git.commit_style:
            lines.append("## Commit Style")
            lines.append("")
            for key, val in git.commit_style.items():
                lines.append(f"- **{key}**: {val}")
            lines.append("")

        # Pipeline-derived facts
        if pipeline_freq:
            agent_hot = sorted(pipeline_freq.items(), key=lambda x: -x[1])[:15]
            lines.append("## Agent Hot Files")
            lines.append("")
            lines.append("| File | Agent Touches |")
            lines.append("|------|---------------|")
            for f, count in agent_hot:
                lines.append(f"| {f} | {count} |")
            lines.append("")

        # Event-derived facts
        if event_facts:
            lines.append("## Learned Facts")
            lines.append("")
            lines.append("| Key | Value | Source | Confidence |")
            lines.append("|-----|-------|--------|------------|")
            for fact in event_facts:
                lines.append(f"| {fact.key} | {fact.value} | {fact.source} | {fact.confidence} |")
            lines.append("")

        # Timeline
        if git.first_commit_date:
            lines.append("## Timeline")
            lines.append(f"- First commit: {git.first_commit_date}")
            lines.append(f"- Last commit: {git.last_commit_date}")
            lines.append(f"- Total commits: {git.total_commits}")
            lines.append(f"- Total pipeline events: {self._event_log.count()}")
            lines.append("")

        path = self._knowledge / "fact-registry.md"
        path.write_text("\n".join(lines), encoding="utf-8")
        return len(git.hot_files) + len(event_facts)

    def _compile_decisions(self, events: list[Event]) -> int:
        decisions = collect_decisions_from_events(events)
        for d in decisions:
            self._write_decision(d)
        return len(decisions)

    def _write_decision(self, d: Decision) -> None:
        decisions_dir = self._knowledge / "decisions"
        decisions_dir.mkdir(exist_ok=True)
        slug = d.title.lower().replace(" ", "-")[:50]
        ts = d.timestamp[:10] if d.timestamp else "unknown"
        filename = f"{ts}-{slug}.md"

        issue_ref = f"Issue: #{d.issue_number}\n" if d.issue_number else ""
        content = (
            f"# {d.title}\n\n"
            f"Date: {d.timestamp}\n"
            f"{issue_ref}\n"
            f"## Context\n\n{d.context}\n\n"
            f"## Decision\n\n{d.decision}\n\n"
            f"## Consequences\n\n{d.consequences}\n"
        )
        (decisions_dir / filename).write_text(content, encoding="utf-8")

    def _upsert_entity(self, entity: Entity) -> None:
        entities_dir = self._knowledge / "entities"
        entities_dir.mkdir(exist_ok=True)
        path = entities_dir / f"{entity.name}.md"

        lines = [f"# {entity.name}", "", f"Type: {entity.entity_type}"]
        if entity.description:
            lines.append(f"Description: {entity.description}")
        lines.append("")

        if entity.facts:
            lines.append("## Learned Facts")
            for f in entity.facts:
                lines.append(f"  - **{f.key}**: {f.value}")
            lines.append("")

        if entity.related_files:
            lines.append("## Related Files")
            for f in entity.related_files[:10]:
                lines.append(f"  - {f}")
            lines.append("")

        if entity.owner:
            lines.append(f"## Owner\n\n- {entity.owner}\n")

        path.write_text("\n".join(lines), encoding="utf-8")

    def _append_facts_to_registry(self, facts: list[Fact]) -> None:
        path = self._knowledge / "fact-registry.md"
        if not path.exists():
            return
        content = path.read_text(encoding="utf-8")
        new_lines = ["\n## Recently Learned"]
        for f in facts:
            new_lines.append(f"- **{f.key}**: {f.value} (source: {f.source}, {f.valid_from})")
        path.write_text(content + "\n".join(new_lines) + "\n", encoding="utf-8")

    def _compile_error_patterns(self, events: list[Event]) -> None:
        patterns = collect_error_patterns(events)
        if not patterns:
            return
        lines = ["# Error Patterns", "", "Recurring errors across pipeline runs.", ""]
        lines.append("| Pattern | Occurrences |")
        lines.append("|---------|-------------|")
        for pattern, count in sorted(patterns.items(), key=lambda x: -x[1]):
            lines.append(f"| {pattern} | {count} |")
        lines.append("")
        (self._knowledge / "error-patterns.md").write_text("\n".join(lines), encoding="utf-8")

    def _compile_pipeline_stats(self, events: list[Event]) -> None:
        if not events:
            return

        type_counts: dict[str, int] = {}
        issue_set: set[int] = set()
        for ev in events:
            type_counts = {**type_counts, ev.event_type.value: type_counts.get(ev.event_type.value, 0) + 1}
            if ev.issue_number is not None:
                issue_set = {*issue_set, ev.issue_number}

        lines = [
            "# Pipeline Stats", "",
            f"Total events: {len(events)}",
            f"Issues touched: {len(issue_set)}",
            "",
            "## Event Distribution", "",
            "| Event Type | Count |",
            "|------------|-------|",
        ]
        for t, c in sorted(type_counts.items(), key=lambda x: -x[1]):
            lines.append(f"| {t} | {c} |")
        lines.append("")

        (self._knowledge / "pipeline-stats.md").write_text("\n".join(lines), encoding="utf-8")

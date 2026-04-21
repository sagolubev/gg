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

        from gg.analyzers.codebase import analyze_codebase

        git_profile = analyze_git_history(self._root)
        structure = analyze_structure(self._root)
        codebase = analyze_codebase(self._root)
        events = self._event_log.read_all()

        entity_count = self._compile_entities(events, structure, git_profile)
        fact_count = self._compile_fact_registry(events, git_profile, codebase)
        decision_count = self._compile_decisions(events, git_profile)
        risk_count = self._compile_risk_register(git_profile, structure, events)
        self._compile_codebase_insights(codebase)
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
            "risks": risk_count,
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

            ownership = [o for o in git.file_ownership if o.path.startswith(f"{name}/")]
            owner = ""
            if event_entity and event_entity.owner:
                owner = event_entity.owner
            elif ownership:
                owner = ownership[0].primary_owner
            elif git.contributors:
                owner = git.contributors[0].name

            bus = git.bus_factor.get(name, 0)
            risk_entries = [(p, s) for p, s in git.risk_scores if p.startswith(f"{name}/")]

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

            if ownership:
                lines.append("## Code Ownership")
                for o in ownership[:5]:
                    contribs = ", ".join(f"{a}({n})" for a, n in o.contributors[:3])
                    lines.append(f"  - {o.path}: {o.primary_owner} ({o.ownership_pct:.0f}%) [{contribs}]")
                lines.append("")
            elif owner:
                lines.append(f"## Owner\n\n- {owner}\n")

            if bus > 0:
                lines.append(f"## Bus Factor: {bus} contributor(s)")
                if bus <= 1:
                    lines.append("  **Risk: single contributor -- knowledge transfer needed**")
                lines.append("")

            if risk_entries:
                lines.append("## Risk Scores")
                for p, s in sorted(risk_entries, key=lambda x: -x[1])[:5]:
                    lines.append(f"  - {p}: {s:.2f}")
                lines.append("")

            (entities_dir / f"{name}.md").write_text("\n".join(lines), encoding="utf-8")

        return len(all_names)

    def _compile_fact_registry(self, events: list[Event], git: GitProfile, codebase: dict | None = None) -> int:
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

        # Code ownership
        if git.file_ownership:
            lines.append("## Code Ownership")
            lines.append("")
            lines.append("| File | Owner | Ownership % | Contributors |")
            lines.append("|------|-------|-------------|-------------|")
            for o in git.file_ownership[:15]:
                contribs = ", ".join(f"{a}({n})" for a, n in o.contributors[:3])
                lines.append(f"| {o.path} | {o.primary_owner} | {o.ownership_pct:.0f}% | {contribs} |")
            lines.append("")

        # Bus factor
        if git.bus_factor:
            low_bus = [(m, n) for m, n in git.bus_factor.items() if n <= 1]
            if low_bus:
                lines.append("## Bus Factor Risk")
                lines.append("")
                lines.append("Modules with single contributor:")
                for m, n in low_bus:
                    lines.append(f"- **{m}**: {n} contributor(s)")
                lines.append("")

        # Churn analysis
        if git.churn_analysis:
            lines.append("## Code Churn")
            lines.append("")
            lines.append("| File | Changes | +Lines | -Lines | Churn Ratio |")
            lines.append("|------|---------|--------|--------|-------------|")
            for ci in git.churn_analysis[:15]:
                lines.append(f"| {ci.path} | {ci.change_count} | {ci.lines_added} | {ci.lines_removed} | {ci.churn_ratio} |")
            lines.append("")

        # Risk scores
        if git.risk_scores:
            lines.append("## Change Risk Scores")
            lines.append("")
            lines.append("Composite of churn, coupling, and bus factor:")
            lines.append("")
            lines.append("| File | Risk Score |")
            lines.append("|------|------------|")
            for path_name, score in git.risk_scores:
                lines.append(f"| {path_name} | {score:.2f} |")
            lines.append("")

        # Dormant files
        if git.dormant_files:
            lines.append("## Dormant Files (>6 months)")
            lines.append("")
            for path_name, last_date in git.dormant_files[:10]:
                lines.append(f"- {path_name} (last changed: {last_date})")
            lines.append("")

        # Architectural commits
        if git.architectural_commits:
            lines.append("## Architectural Changes")
            lines.append("")
            lines.append("| Date | Type | Message | Files |")
            lines.append("|------|------|---------|-------|")
            for ac in git.architectural_commits:
                lines.append(f"| {ac.date} | {ac.commit_type} | {ac.message[:50]} | {ac.files_changed} |")
            lines.append("")

        # Dependency changes
        if git.dependency_changes:
            lines.append("## Dependency History")
            lines.append("")
            lines.append("| Date | Action | File | Commit |")
            lines.append("|------|--------|------|--------|")
            for dc in git.dependency_changes:
                lines.append(f"| {dc.date} | {dc.action} | {dc.file} | {dc.message[:40]} |")
            lines.append("")

        # Feature velocity
        if git.feature_velocity:
            lines.append("## Feature Velocity")
            lines.append("")
            lines.append("| Month | Features | Fixes | Refactors | Tests | Other |")
            lines.append("|-------|----------|-------|-----------|-------|-------|")
            for month, counts in git.feature_velocity.items():
                feat = counts.get("feat", 0)
                fix = counts.get("fix", 0)
                refactor = counts.get("refactor", 0)
                test = counts.get("test", 0)
                other = sum(v for k, v in counts.items() if k not in ("feat", "fix", "refactor", "test"))
                lines.append(f"| {month} | {feat} | {fix} | {refactor} | {test} | {other} |")
            lines.append("")

        # Work patterns
        if git.work_patterns:
            lines.append("## Work Patterns (commits by hour)")
            lines.append("")
            for hour, count in git.work_patterns.items():
                bar = "#" * min(count, 30)
                lines.append(f"  {hour}:00  {bar} ({count})")
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
        return len(git.hot_files) + len(event_facts) + len(git.file_ownership) + len(git.risk_scores)

    def _compile_decisions(self, events: list[Event], git: GitProfile | None = None) -> int:
        decisions = collect_decisions_from_events(events)
        for d in decisions:
            self._write_decision(d)

        if git and git.architectural_commits:
            for ac in git.architectural_commits:
                if ac.commit_type in ("dependency_change", "refactor", "breaking_change", "restructuring"):
                    self._write_decision(Decision(
                        title=f"[auto] {ac.commit_type}: {ac.message[:50]}",
                        context=f"Detected from git commit {ac.sha} ({ac.date}), {ac.files_changed} files changed.",
                        decision=ac.message,
                        consequences=f"Type: {ac.commit_type}",
                        timestamp=ac.date,
                    ))
                    decisions = [*decisions, None]  # type: ignore[list-item]

        return len(decisions)

    def _write_decision(self, d: Decision) -> None:
        decisions_dir = self._knowledge / "decisions"
        decisions_dir.mkdir(exist_ok=True)
        import re
        slug = re.sub(r"[^a-z0-9-]", "", d.title.lower().replace(" ", "-"))[:50]
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

    def _compile_risk_register(
        self, git: GitProfile, structure: StructureMap, events: list[Event],
    ) -> int:
        from gg.analyzers.dependencies import analyze_dependencies

        deps = analyze_dependencies(self._root)
        risks: list[dict[str, str]] = []
        risk_id = 0

        # R: Bus factor risks
        for module, count in git.bus_factor.items():
            if count <= 1 and module not in (".", ""):
                risk_id += 1
                risks = [*risks, {
                    "id": f"R{risk_id:03d}",
                    "severity": "High",
                    "category": "Bus Factor",
                    "title": f"Single contributor for {module}/",
                    "description": f"Module `{module}/` has only {count} contributor(s). "
                                   f"Knowledge loss risk if that person leaves.",
                    "recommendation": f"Cross-train team on `{module}/`, add documentation, pair program.",
                }]

        # R: High churn files (potential tech debt)
        for ci in git.churn_analysis[:5]:
            if ci.churn_ratio > 5.0 and ci.change_count >= 5:
                risk_id += 1
                risks = [*risks, {
                    "id": f"R{risk_id:03d}",
                    "severity": "Medium",
                    "category": "Tech Debt",
                    "title": f"High churn: {ci.path}",
                    "description": f"`{ci.path}` changed {ci.change_count} times with churn ratio {ci.churn_ratio}. "
                                   f"High volatility suggests unstable design or frequent hotfixes.",
                    "recommendation": "Consider refactoring to stabilize the interface.",
                }]

        # R: Missing linters
        if "linters" not in deps.existing_tools:
            risk_id += 1
            risks = [*risks, {
                "id": f"R{risk_id:03d}",
                "severity": "Medium",
                "category": "Code Quality",
                "title": "No linter configured",
                "description": "No linting tools detected. Code style inconsistencies accumulate over time.",
                "recommendation": f"Add a linter appropriate for {git.commit_style.get('top_types', 'the')} project.",
            }]

        # R: Missing tests
        if "test_frameworks" not in deps.existing_tools:
            risk_id += 1
            risks = [*risks, {
                "id": f"R{risk_id:03d}",
                "severity": "High",
                "category": "Code Quality",
                "title": "No test framework detected",
                "description": "No testing infrastructure found. Changes cannot be verified automatically.",
                "recommendation": "Add a test framework and start with critical path tests.",
            }]

        # R: Missing CI
        if "ci" not in deps.existing_tools:
            risk_id += 1
            risks = [*risks, {
                "id": f"R{risk_id:03d}",
                "severity": "Medium",
                "category": "Process",
                "title": "No CI/CD pipeline detected",
                "description": "No CI configuration found. Builds and tests are not automated.",
                "recommendation": "Add GitHub Actions / GitLab CI pipeline.",
            }]

        # R: Dormant files
        if len(git.dormant_files) > 10:
            risk_id += 1
            examples = ", ".join(f"`{p}`" for p, _ in git.dormant_files[:3])
            risks = [*risks, {
                "id": f"R{risk_id:03d}",
                "severity": "Low",
                "category": "Maintenance",
                "title": f"{len(git.dormant_files)} dormant files (>6 months unchanged)",
                "description": f"Files not touched in 6+ months: {examples}, etc. "
                               f"May be dead code or abandoned features.",
                "recommendation": "Audit dormant files -- remove dead code, document stable modules.",
            }]

        # R: High risk score files
        high_risk = [(p, s) for p, s in git.risk_scores if s > 5.0]
        if high_risk:
            risk_id += 1
            examples = ", ".join(f"`{p}` ({s:.1f})" for p, s in high_risk[:3])
            risks = [*risks, {
                "id": f"R{risk_id:03d}",
                "severity": "High",
                "category": "Change Risk",
                "title": f"{len(high_risk)} files with high change risk score",
                "description": f"Files with composite risk >5.0 (churn + coupling + bus factor): {examples}. "
                               f"Changes here are most likely to cause regressions.",
                "recommendation": "Add tests for high-risk files before modifying. Use grepai trace callers.",
            }]

        # R: Recurring errors from pipeline
        error_patterns = collect_error_patterns(events)
        for pattern, count in error_patterns.items():
            if count >= 3:
                risk_id += 1
                risks = [*risks, {
                    "id": f"R{risk_id:03d}",
                    "severity": "Medium",
                    "category": "Reliability",
                    "title": f"Recurring error: {pattern[:50]}",
                    "description": f"Error pattern `{pattern}` occurred {count} times across pipeline runs.",
                    "recommendation": "Investigate root cause. Add specific handling or fix.",
                }]

        if not risks:
            return 0

        lines = [
            "# Risk Register", "",
            "Auto-generated from project analysis. IDs are stable -- never renumber.",
            "",
            "| ID | Severity | Category | Title |",
            "|----|----------|----------|-------|",
        ]
        for r in risks:
            lines.append(f"| {r['id']} | {r['severity']} | {r['category']} | {r['title']} |")
        lines.append("")

        for r in risks:
            lines.append(f"### {r['id']}: {r['title']}")
            lines.append("")
            lines.append(f"**Severity:** {r['severity']}  ")
            lines.append(f"**Category:** {r['category']}")
            lines.append("")
            lines.append(r["description"])
            lines.append("")
            lines.append(f"**Recommendation:** {r['recommendation']}")
            lines.append("")

        (self._knowledge / "risk-register.md").write_text("\n".join(lines), encoding="utf-8")
        return len(risks)

    def _compile_codebase_insights(self, codebase: dict) -> None:
        lines = ["# Codebase Insights", "", "Auto-generated from fast local analysis.", ""]

        if codebase.get("todos"):
            lines.append("## TODO/FIXME/HACK Markers")
            lines.append("")
            lines.append("```")
            lines.append(codebase["todos"])
            lines.append("```")
            lines.append("")

        if codebase.get("routes"):
            lines.append("## API Routes")
            lines.append("")
            lines.append("```")
            lines.append(codebase["routes"])
            lines.append("```")
            lines.append("")

        if codebase.get("env_vars"):
            lines.append("## Environment Variables")
            lines.append("")
            lines.append("```")
            lines.append(codebase["env_vars"])
            lines.append("```")
            lines.append("")

        if codebase.get("imports"):
            lines.append("## Top External Imports")
            lines.append("")
            lines.append(codebase["imports"])
            lines.append("")

        if any(codebase.get(k) for k in ("todos", "routes", "env_vars", "imports")):
            (self._knowledge / "codebase-insights.md").write_text(
                "\n".join(lines), encoding="utf-8",
            )

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

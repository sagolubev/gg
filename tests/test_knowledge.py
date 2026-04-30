from pathlib import Path

from gg.knowledge.events import Event, EventLog, EventType
from gg.knowledge.collectors import (
    collect_decisions_from_events,
    collect_entities_from_events,
    collect_error_patterns,
    collect_facts_from_events,
    collect_file_touch_frequency,
)
from gg.knowledge.engine import KnowledgeEngine
from gg.generators.knowledge import rank_contributor_exemplars, write_contributor_exemplars
from gg.analyzers.git_history import ArchitecturalCommit, Contributor, FileOwnership, GitProfile


class TestEventLog:
    def test_append_and_read(self, tmp_path):
        knowledge = tmp_path / "knowledge"
        log = EventLog(knowledge)

        ev = Event(event_type=EventType.INIT, data={"key": "val"}, source="test")
        log.append(ev)

        events = log.read_all()
        assert len(events) == 1
        assert events[0].event_type == EventType.INIT
        assert events[0].data["key"] == "val"

    def test_issue_partitioning(self, tmp_path):
        knowledge = tmp_path / "knowledge"
        log = EventLog(knowledge)

        log.append(Event(event_type=EventType.ISSUE_PICKED, issue_number=1, data={"title": "A"}))
        log.append(Event(event_type=EventType.ISSUE_PICKED, issue_number=2, data={"title": "B"}))
        log.append(Event(event_type=EventType.PR_CREATED, issue_number=1, data={"pr_url": "x"}))

        assert log.count() == 3
        assert len(log.read_for_issue(1)) == 2
        assert len(log.read_for_issue(2)) == 1
        assert len(log.read_for_issue(99)) == 0

    def test_append_only(self, tmp_path):
        knowledge = tmp_path / "knowledge"
        log = EventLog(knowledge)

        log.append(Event(event_type=EventType.INIT, data={"a": 1}))
        log.append(Event(event_type=EventType.INIT, data={"b": 2}))

        assert log.count() == 2
        events = log.read_all()
        assert events[0].data["a"] == 1
        assert events[1].data["b"] == 2


class TestCollectors:
    def test_collect_entities(self):
        events = [
            Event(
                event_type=EventType.ENTITY_DISCOVERED,
                data={"name": "auth", "type": "module", "files": ["src/auth.py"]},
                source="test",
            ),
            Event(
                event_type=EventType.ENTITY_DISCOVERED,
                data={"name": "auth", "type": "service", "files": ["src/auth_api.py"],
                      "facts": [{"key": "uses_jwt", "value": "true"}]},
                source="test",
            ),
        ]
        entities = collect_entities_from_events(events)
        assert len(entities) == 1
        assert entities[0].name == "auth"
        assert entities[0].entity_type == "service"
        assert len(entities[0].related_files) == 2
        assert len(entities[0].facts) == 1

    def test_collect_decisions(self):
        events = [
            Event(
                event_type=EventType.DECISION_RECORDED,
                data={"title": "Use JWT", "context": "Need auth", "decision": "JWT over sessions"},
                issue_number=5,
            ),
        ]
        decisions = collect_decisions_from_events(events)
        assert len(decisions) == 1
        assert decisions[0].title == "Use JWT"
        assert decisions[0].issue_number == 5

    def test_collect_error_patterns(self):
        events = [
            Event(event_type=EventType.ERROR, data={"pattern": "timeout"}),
            Event(event_type=EventType.ERROR, data={"pattern": "timeout"}),
            Event(event_type=EventType.ERROR, data={"pattern": "auth_failed"}),
        ]
        patterns = collect_error_patterns(events)
        assert patterns["timeout"] == 2
        assert patterns["auth_failed"] == 1

    def test_collect_file_touch_frequency(self):
        events = [
            Event(event_type=EventType.IMPLEMENTATION_DONE, data={"files_changed": ["a.py", "b.py"]}),
            Event(event_type=EventType.IMPLEMENTATION_DONE, data={"files_changed": ["a.py", "c.py"]}),
        ]
        freq = collect_file_touch_frequency(events)
        assert freq["a.py"] == 2
        assert freq["b.py"] == 1
        assert freq["c.py"] == 1

    def test_collect_facts(self):
        events = [
            Event(
                event_type=EventType.FACT_LEARNED,
                data={"key": "db_engine", "value": "postgres", "confidence": 0.9},
                source="research",
            ),
        ]
        facts = collect_facts_from_events(events)
        assert len(facts) == 1
        assert facts[0].key == "db_engine"
        assert facts[0].confidence == 0.9


def test_contributor_exemplar_generation_prefers_ownership_and_commits(tmp_path):
    profile = GitProfile(
        total_commits=20,
        contributors=[
            Contributor(name="Ada", email="ada@example.com", commits=10, last_active="2026-04-01"),
            Contributor(name="Grace", email="grace@example.com", commits=8, last_active="2026-03-20"),
        ],
        hot_files=[("src/core.py", 9), ("src/api.py", 5)],
        file_ownership=[
            FileOwnership(path="src/core.py", primary_owner="Ada", ownership_pct=80),
            FileOwnership(path="src/api.py", primary_owner="Grace", ownership_pct=70),
        ],
        architectural_commits=[
            ArchitecturalCommit(
                sha="abc123456789",
                message="refactor: isolate upload validation",
                date="2026-03-01",
                files_changed=4,
                commit_type="refactor",
            )
        ],
    )

    exemplars = rank_contributor_exemplars(profile)

    assert exemplars[0]["name"] == "Ada"
    assert exemplars[0]["score"] > exemplars[1]["score"]

    path = write_contributor_exemplars(tmp_path, profile)
    content = path.read_text(encoding="utf-8")

    assert "Strongest Contributors" in content
    assert "Ada" in content
    assert "local-fallback" in content
    assert "refactor: isolate upload validation" in content


def test_contributor_exemplars_are_injected_into_issue_context(tmp_path):
    profile = GitProfile(
        contributors=[
            Contributor(name="Ada", email="ada@example.com", commits=10, last_active="2026-04-01"),
        ],
        hot_files=[("src/uploads.py", 7)],
        file_ownership=[
            FileOwnership(path="src/uploads.py", primary_owner="Ada", ownership_pct=90),
        ],
        architectural_commits=[
            ArchitecturalCommit(
                sha="abcdef1234567890",
                message="refactor: centralize upload validation",
                date="2026-03-01",
                files_changed=3,
                commit_type="refactor",
            )
        ],
    )
    write_contributor_exemplars(tmp_path, profile)

    context = KnowledgeEngine(tmp_path).context_for_issue("Fix upload validation", "Harden src/uploads.py")

    assert "Project Exemplars" in context
    assert "Ada" in context
    assert "refactor: centralize upload validation" in context


class TestKnowledgeEngine:
    def _make_git_repo(self, path: Path) -> None:
        import subprocess
        subprocess.run(["git", "init"], cwd=str(path), capture_output=True)
        (path / "src").mkdir()
        (path / "src" / "main.py").write_text("print('hi')")
        subprocess.run(["git", "add", "-A"], cwd=str(path), capture_output=True)
        subprocess.run(
            ["git", "commit", "-m", "feat: init", "--allow-empty"],
            cwd=str(path), capture_output=True,
            env={"GIT_AUTHOR_NAME": "test", "GIT_AUTHOR_EMAIL": "t@t.com",
                 "GIT_COMMITTER_NAME": "test", "GIT_COMMITTER_EMAIL": "t@t.com",
                 "HOME": str(path), "PATH": "/usr/bin:/usr/local/bin:/opt/homebrew/bin"},
        )

    def test_record_and_rebuild(self, tmp_path):
        self._make_git_repo(tmp_path)
        engine = KnowledgeEngine(tmp_path)

        engine.record_issue_picked(issue_number=1, title="Add feature X")
        engine.record_implementation_done(issue_number=1, files_changed=["src/main.py"])
        engine.record_fact(key="uses_sqlite", value="true")
        engine.record_decision(
            issue_number=1, title="Use SQLite",
            context="Need local DB", decision="SQLite for dev",
        )

        stats = engine.rebuild()
        assert stats["entities"] >= 1
        assert stats["decisions"] >= 1
        assert stats["events_processed"] >= 4

        fact_reg = (tmp_path / ".gg" / "knowledge" / "fact-registry.md").read_text()
        assert "uses_sqlite" in fact_reg

        decisions_dir = tmp_path / ".gg" / "knowledge" / "decisions"
        assert any(decisions_dir.iterdir())

    def test_issue_history(self, tmp_path):
        self._make_git_repo(tmp_path)
        engine = KnowledgeEngine(tmp_path)

        engine.record_issue_picked(issue_number=42, title="Bug fix")
        engine.record_research_done(issue_number=42, files_analyzed=["src/main.py"])
        engine.record_implementation_done(issue_number=42, files_changed=["src/main.py"])
        engine.record_pr_created(issue_number=42, pr_url="http://pr/1", pr_number=1)

        history = engine.get_issue_history(42)
        types = [e.event_type for e in history]
        assert EventType.ISSUE_PICKED in types
        assert EventType.PR_CREATED in types

    def test_repair_lesson_is_structured_and_reused_in_issue_context(self, tmp_path):
        self._make_git_repo(tmp_path)
        engine = KnowledgeEngine(tmp_path)

        engine.record_repair_lesson(
            issue_number=42,
            run_id="run-42",
            candidate_id="candidate-1",
            strategy="test-first",
            files_changed=["src/uploads.py"],
            failure_reason="path traversal was not validated",
            repair_reason="normalize uploaded file names before writing",
            verification_failures=["pytest tests/test_uploads.py"],
            fingerprint="path-traversal:uploads",
        )

        events = engine.get_all_events()
        assert events[-1].event_type == EventType.REPAIR_LESSON
        assert events[-1].data["fingerprint"] == "path-traversal:uploads"

        lessons_md = tmp_path / ".gg" / "knowledge" / "repair-lessons.md"
        assert "path traversal was not validated" in lessons_md.read_text(encoding="utf-8")

        context = engine.context_for_issue("Fix upload path traversal", "src/uploads.py accepts ../")
        assert "Similar Past Mistakes" in context
        assert "normalize uploaded file names" in context

    def test_error_patterns_compiled(self, tmp_path):
        self._make_git_repo(tmp_path)
        engine = KnowledgeEngine(tmp_path)

        engine.record_error(issue_number=1, message="timeout connecting to DB", pattern="db_timeout")
        engine.record_error(issue_number=2, message="timeout connecting to DB", pattern="db_timeout")
        engine.rebuild()

        patterns_file = tmp_path / ".gg" / "knowledge" / "error-patterns.md"
        assert patterns_file.exists()
        content = patterns_file.read_text()
        assert "db_timeout" in content
        assert "2" in content

    def test_auto_rebuild_on_pr_merged(self, tmp_path):
        self._make_git_repo(tmp_path)
        engine = KnowledgeEngine(tmp_path)

        engine.record_fact(key="db", value="sqlite")
        engine.record_pr_merged(issue_number=1, pr_number=10)

        fact_reg = (tmp_path / ".gg" / "knowledge" / "fact-registry.md").read_text()
        assert "sqlite" in fact_reg.lower()

    def test_auto_rebuild_on_threshold(self, tmp_path):
        self._make_git_repo(tmp_path)
        engine = KnowledgeEngine(tmp_path)
        engine._auto_rebuild_threshold = 3

        engine.record_fact(key="a", value="1")
        engine.record_fact(key="b", value="2")
        assert not (tmp_path / ".gg" / "knowledge" / "pipeline-stats.md").exists()

        engine.record_fact(key="c", value="3")
        assert (tmp_path / ".gg" / "knowledge" / "pipeline-stats.md").exists()

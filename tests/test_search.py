import subprocess
from pathlib import Path

from gg.knowledge.engine import KnowledgeEngine
from gg.knowledge.search import _score, _tokenize


class TestTokenize:
    def test_basic(self):
        assert _tokenize("hello world") == ["hello", "world"]

    def test_filters_short(self):
        assert _tokenize("I am a test") == ["am", "test"]

    def test_handles_special_chars(self):
        tokens = _tokenize("src/auth.py uses JWT")
        assert "src" in tokens
        assert "auth.py" in tokens
        assert "jwt" in tokens


class TestScore:
    def test_full_match(self):
        assert _score(["auth", "jwt"], "auth module uses jwt tokens") > 0

    def test_no_match(self):
        assert _score(["database", "postgres"], "frontend react component") == 0.0

    def test_partial_match(self):
        score_full = _score(["auth", "jwt"], "auth jwt handler")
        score_partial = _score(["auth", "jwt"], "auth handler for sessions")
        assert score_full > score_partial


class TestKnowledgeSearch:
    def _make_git_repo(self, path: Path) -> None:
        subprocess.run(["git", "init"], cwd=str(path), capture_output=True)
        (path / "src").mkdir()
        (path / "src" / "main.py").write_text("print('hi')")
        subprocess.run(["git", "add", "-A"], cwd=str(path), capture_output=True)
        subprocess.run(
            ["git", "commit", "-m", "feat: init"],
            cwd=str(path), capture_output=True,
            env={"GIT_AUTHOR_NAME": "test", "GIT_AUTHOR_EMAIL": "t@t.com",
                 "GIT_COMMITTER_NAME": "test", "GIT_COMMITTER_EMAIL": "t@t.com",
                 "HOME": str(path), "PATH": "/usr/bin:/usr/local/bin:/opt/homebrew/bin"},
        )

    def test_search_entities(self, tmp_path):
        self._make_git_repo(tmp_path)
        engine = KnowledgeEngine(tmp_path)
        engine.record_entity(name="auth", entity_type="module", description="Authentication service using JWT")
        engine.record_entity(name="payments", entity_type="service", description="Stripe payment processing")

        results = engine.search("authentication JWT")
        assert len(results) > 0
        assert any(r.title == "auth" for r in results)

    def test_search_facts(self, tmp_path):
        self._make_git_repo(tmp_path)
        engine = KnowledgeEngine(tmp_path)
        engine.record_fact(key="database", value="PostgreSQL 15", confidence=0.95)
        engine.record_fact(key="cache", value="Redis 7", confidence=0.8)

        results = engine.search("postgres database")
        assert len(results) > 0
        assert any("database" in r.title.lower() or "postgres" in r.snippet.lower() for r in results)

    def test_search_decisions(self, tmp_path):
        self._make_git_repo(tmp_path)
        engine = KnowledgeEngine(tmp_path)
        engine.record_decision(
            title="Use SQLite for tests",
            context="Need fast test DB",
            decision="SQLite in-memory for unit tests, Postgres for integration",
        )

        results = engine.search("SQLite tests")
        assert len(results) > 0
        assert any(r.kind == "decision" for r in results)

    def test_context_for_issue(self, tmp_path):
        self._make_git_repo(tmp_path)
        engine = KnowledgeEngine(tmp_path)
        engine.record_entity(name="auth", description="JWT-based auth module")
        engine.record_fact(key="auth_provider", value="Auth0")
        engine.record_decision(
            title="Use Auth0",
            context="Need SSO",
            decision="Auth0 for enterprise SSO",
        )

        ctx = engine.context_for_issue("Add OAuth login", "Need to support Google OAuth")
        assert "auth" in ctx.lower() or "Related" in ctx

    def test_find_by_files(self, tmp_path):
        self._make_git_repo(tmp_path)
        engine = KnowledgeEngine(tmp_path)
        engine.record_entity(name="src", entity_type="module", files=["src/main.py", "src/utils.py"])
        engine.record_implementation_done(issue_number=1, files_changed=["src/main.py"])

        results = engine.find_by_files(["src/main.py"])
        assert len(results) > 0

    def test_find_errors(self, tmp_path):
        self._make_git_repo(tmp_path)
        engine = KnowledgeEngine(tmp_path)
        engine.record_error(message="Connection timeout", pattern="timeout")
        engine.record_error(message="Connection timeout again", pattern="timeout")
        engine.record_error(message="Auth failed", pattern="auth_error")

        results = engine.find_errors("timeout")
        assert len(results) == 1
        assert results[0].metadata["count"] == 2

    def test_empty_search(self, tmp_path):
        self._make_git_repo(tmp_path)
        engine = KnowledgeEngine(tmp_path)
        results = engine.search("nonexistent query xyz")
        assert results == []

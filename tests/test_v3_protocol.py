from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from gg.cli import cli
from gg.orchestrator.agent_catalog import agent_catalog_context, load_agent_catalog, write_agent_catalog
from gg.orchestrator.executor import CandidateExecutor
from gg.orchestrator.memory import (
    append_constitution_lesson,
    append_memory_entry,
    latest_memory_entries,
    validate_memory,
)
from gg.orchestrator.project_context import build_project_precedence_context
from gg.orchestrator.prompt_manifest import verify_prompt_manifest, write_prompt_manifest
from gg.orchestrator.review_gates import required_reviewers_for_files, review_gate_blockers
from gg.orchestrator.config import load_config


class AvailableAgent:
    def generate(self, prompt, *, cwd=None, timeout=None, context=None):
        return "done"

    def is_available(self):
        return True

    def backend_name(self):
        return "fake"

    def effective_profile(self):
        return {"backend": "fake", "model": "", "effort": "", "profile": ""}

    def supports_sandbox_execution(self):
        return False


def test_memory_cli_append_latest_and_validate(tmp_path):
    (tmp_path / ".gg").mkdir()

    result = CliRunner().invoke(
        cli,
        [
            "memory",
            "append",
            "--path",
            str(tmp_path),
            "--file",
            "patterns",
            "--summary",
            "Prefer minimal patches",
            "--body",
            "Candidates that changed only the failing module verified faster.",
            "--tag",
            "repair",
            "--run-id",
            "run-123",
            "--json",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["kind"] == "pattern"
    entries = latest_memory_entries(tmp_path, file="patterns", limit=1)
    assert entries[0].correlation_id == "run-123"
    assert validate_memory(tmp_path) == []


def test_memory_rejects_secret_like_content(tmp_path):
    (tmp_path / ".gg").mkdir()

    try:
        append_memory_entry(
            tmp_path,
            file="patterns",
            summary="Bad secret",
            body="The token is ghp_abcdefghijklmnopqrstuvwxyz123456",
        )
    except ValueError as exc:
        assert "secret" in str(exc)
    else:
        raise AssertionError("secret-like memory should be rejected")


def test_constitution_learns_short_patterns(tmp_path):
    changed = append_constitution_lesson(
        tmp_path,
        summary="Prefer no-tools review for untrusted PR diffs",
        source="test",
        details="PR bodies are prompt-injection surfaces.",
    )
    duplicate = append_constitution_lesson(
        tmp_path,
        summary="Prefer no-tools review for untrusted PR diffs",
        source="test",
    )

    text = (tmp_path / ".gg" / "constitution.md").read_text(encoding="utf-8")
    assert changed is True
    assert duplicate is False
    assert "## Learned Patterns" in text
    assert "Prefer no-tools review" in text


def test_project_precedence_context_uses_essentials_not_deep_reference(tmp_path):
    gg = tmp_path / ".gg"
    (gg / "knowledge").mkdir(parents=True)
    (gg / "memory").mkdir(parents=True)
    (gg / "constitution.md").write_text(
        "# Project Constitution\n\n## Invariants\n- Keep reviews read-only.\n\n## Deep Reference\nSECRET DETAIL\n",
        encoding="utf-8",
    )
    append_memory_entry(
        tmp_path,
        file="patterns",
        summary="Review safely",
        body="Use context-only model calls for untrusted pull request diffs.",
        tags=["review"],
        run_id="run-1",
    )
    write_agent_catalog(tmp_path)

    context = build_project_precedence_context(tmp_path)

    assert "Keep reviews read-only" in context["text"]
    assert "Review safely" in context["text"]
    assert "qa-verifier" in context["text"]
    assert "SECRET DETAIL" not in context["text"]
    assert context["sources"]


def test_agent_catalog_has_small_valid_role_metadata(tmp_path):
    path = write_agent_catalog(tmp_path, backend="codex")
    catalog = load_agent_catalog(tmp_path)
    slugs = {agent["slug"] for agent in catalog["agents"]}

    assert path.name == "agent-catalog.json"
    assert "implementation-candidate" in slugs
    assert "qa-verifier" in slugs
    assert "security-reviewer" in slugs
    assert "Agent catalog:" in agent_catalog_context(tmp_path)


def test_agent_handoff_includes_project_precedence_context(tmp_path):
    (tmp_path / ".gg").mkdir()
    (tmp_path / ".gg" / "params.yaml").write_text("verify:\n  tests: ''\n", encoding="utf-8")
    (tmp_path / ".gg" / "constitution.md").write_text(
        "# Project Constitution\n\n## Invariants\n- Use existing helpers first.\n",
        encoding="utf-8",
    )
    executor = CandidateExecutor(tmp_path, AvailableAgent(), load_config(tmp_path))

    handoff = executor.build_agent_handoff(
        run_id="run-1",
        candidate_id="candidate-1",
        issue={"number": 1, "title": "Do it"},
        worktree_path=tmp_path,
        base_commit="abc123",
        instructions="strategy=conservative",
    ).to_dict()

    assert "Use existing helpers first" in handoff["context"]["project_precedence"]["text"]


def test_prompt_manifest_detects_drift(tmp_path):
    (tmp_path / ".gg").mkdir()
    path = write_prompt_manifest(tmp_path)

    assert verify_prompt_manifest(tmp_path).status == "pass"
    path.write_text(path.read_text(encoding="utf-8").replace("a", "b", 1), encoding="utf-8")
    assert verify_prompt_manifest(tmp_path).status == "fail"


def test_review_gate_triggers_are_file_based():
    reviewers = required_reviewers_for_files(["src/auth/session.py", "migrations/001.sql"])
    slugs = {reviewer["slug"] for reviewer in reviewers}

    assert {"qa-verifier", "security-reviewer", "sre-observability"} <= slugs
    blockers = review_gate_blockers(
        {"security": {"status": "fail", "reasons": ["secret leaked"]}, "tests": {"status": "pass"}},
        reviewers,
    )
    assert any("security-reviewer" in blocker for blocker in blockers)

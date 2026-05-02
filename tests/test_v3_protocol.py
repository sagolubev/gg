from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from gg.cli import cli
from gg.orchestrator.agent_catalog import agent_catalog_context, load_agent_catalog, verify_agent_catalog, write_agent_catalog
from gg.orchestrator.agent_patterns import verify_agent_patterns
from gg.orchestrator.executor import CandidateExecutor
from gg.orchestrator.finding_feedback import record_finding_feedback
from gg.orchestrator.memory import (
    append_constitution_lesson,
    append_memory_entry,
    latest_memory_entries,
    validate_memory,
)
from gg.orchestrator.project_context import build_project_precedence_context
from gg.orchestrator.prompt_manifest import verify_prompt_manifest, write_prompt_manifest
from gg.orchestrator.protocol import build_protocol_obligations
from gg.orchestrator.review_gates import required_reviewers_for_files, review_gate_blockers
from gg.orchestrator.config import load_config
from gg.orchestrator.truth import parse_requirements, sync_approved_decisions, truth_coverage


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
    assert catalog["schema_version"] == 2
    assert "implementation-candidate" in slugs
    assert "qa-verifier" in slugs
    assert "security-reviewer" in slugs
    assert all({"category", "protocol", "readonly", "tags", "domains"} <= set(agent) for agent in catalog["agents"])
    assert "Agent catalog:" in agent_catalog_context(tmp_path)
    assert verify_agent_catalog(tmp_path).status == "pass"


def test_agent_catalog_hash_detects_drift(tmp_path):
    path = write_agent_catalog(tmp_path, backend="codex")
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["agents"][0]["tags"].append("drift")
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    result = verify_agent_catalog(tmp_path)

    assert result.status == "fail"
    assert "drift" in result.message


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


def test_prompt_manifest_tracks_protocol_surfaces(tmp_path):
    (tmp_path / ".gg").mkdir()
    path = write_prompt_manifest(tmp_path)
    text = path.read_text(encoding="utf-8")

    assert "gg/orchestrator/agent_patterns.py" in text
    assert "gg/orchestrator/finding_feedback.py" in text
    assert "gg/orchestrator/protocol.py" in text
    assert "gg/orchestrator/agent_catalog.py" in text
    assert "gg/orchestrator/review_gates.py" in text


def test_prompt_manifest_fails_when_new_protocol_surface_is_untracked(tmp_path):
    (tmp_path / ".gg").mkdir()
    path = write_prompt_manifest(tmp_path)
    lines = [
        line
        for line in path.read_text(encoding="utf-8").splitlines()
        if "gg/orchestrator/protocol.py" not in line
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    result = verify_prompt_manifest(tmp_path)

    assert result.status == "fail"
    assert "gg/orchestrator/protocol.py" in result.missing


def test_review_gate_triggers_are_file_based():
    reviewers = required_reviewers_for_files(["src/auth/session.py", "migrations/001.sql", "prompts/agent.md"])
    slugs = {reviewer["slug"] for reviewer in reviewers}

    assert {"agent-pattern-verifier", "qa-verifier", "security-reviewer", "sre-observability"} <= slugs
    blockers = review_gate_blockers(
        {"security": {"status": "fail", "reasons": ["secret leaked"]}, "tests": {"status": "pass"}},
        reviewers,
    )
    assert any("security-reviewer" in blocker for blocker in blockers)


def test_protocol_obligations_block_missing_required_evidence():
    gate = build_protocol_obligations(
        required_artifacts={"candidate_verification": ""},
        review_dimensions={"tests": {"status": "pass"}},
        required_reviewers=[
            {"slug": "qa-verifier", "dimension": "tests", "reason": "QA required"},
            {"slug": "security-reviewer", "dimension": "security", "reason": "auth changed"},
        ],
        source_artifacts={},
        surface_integrity={"status": "fail", "message": "prompt manifest drift detected"},
    )

    assert gate["status"] == "blocked"
    assert "missing artifact: candidate_verification" in gate["blockers"]
    assert any("security-reviewer" in blocker for blocker in gate["blockers"])
    assert any("protocol surface integrity failed" in blocker for blocker in gate["blockers"])


def test_agent_patterns_block_unbounded_retry_and_loop(tmp_path):
    (tmp_path / "agent.py").write_text(
        "from tenacity import retry\n\n"
        "@retry\n"
        "def call_model():\n"
        "    while True:\n"
        "        call_model()\n",
        encoding="utf-8",
    )

    check = verify_agent_patterns(tmp_path)

    assert check.status == "failed"
    rule_ids = {finding["rule_id"] for finding in check.findings or []}
    assert {"unbounded-retry", "unbounded-agent-loop"} <= rule_ids
    assert all(finding["reliability"] == "P" for finding in check.findings or [])


def test_agent_patterns_suppress_accepted_finding_feedback(tmp_path):
    (tmp_path / "agent.py").write_text(
        "def run_forever():\n"
        "    while True:\n"
        "        run_forever()\n",
        encoding="utf-8",
    )
    first = verify_agent_patterns(tmp_path)
    assert first.status == "failed"

    record_finding_feedback(
        tmp_path,
        first.findings[0],
        status="accepted",
        reason="intentional local watchdog loop in this test fixture",
    )
    second = verify_agent_patterns(tmp_path)

    assert second.status == "passed"
    assert second.findings[0]["status"] == "accepted"
    assert second.findings[0]["suppressed"] is True
    assert second.findings[0]["finding_id"] == "AP1"


def test_agent_patterns_detect_prompt_tool_registry_mismatch(tmp_path):
    (tmp_path / "tools.py").write_text(
        "def search_web_tool():\n"
        "    return 'ok'\n",
        encoding="utf-8",
    )
    (tmp_path / "prompts.md").write_text(
        "Use tool: send_email after searching.\n",
        encoding="utf-8",
    )

    check = verify_agent_patterns(tmp_path)

    assert check.status == "failed"
    assert any(
        finding["rule_id"] == "tool-registry-mismatch" and finding["reliability"] == "P"
        for finding in check.findings or []
    )


def test_agent_patterns_report_context_size_as_heuristic(tmp_path):
    (tmp_path / "prompt.md").write_text("x" * 18_000, encoding="utf-8")

    check = verify_agent_patterns(tmp_path)

    assert check.status == "passed"
    assert check.findings
    assert check.findings[0]["rule_id"] == "context-size-risk"
    assert check.findings[0]["reliability"] == "H"


def test_truth_coverage_tracks_spec_test_and_code_markers(tmp_path):
    (tmp_path / ".gg").mkdir()
    (tmp_path / ".gg" / "constitution.md").write_text(
        "# Project Constitution\n\n"
        "## Requirements\n\n"
        "- The orchestrator must persist final verification evidence before completion.\n",
        encoding="utf-8",
    )
    requirements = parse_requirements(tmp_path)
    requirement_id = requirements[0]["id"]
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "app.py").write_text(
        f"# gg:{requirement_id}\n"
        "def persist_final_verification():\n"
        "    return True\n",
        encoding="utf-8",
    )
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "test_app.py").write_text(
        f"def test_req_{requirement_id.removeprefix('req-')}_persists_final_verification():\n"
        "    assert True\n",
        encoding="utf-8",
    )

    report = truth_coverage(tmp_path)

    assert report["requirements_total"] == 1
    assert report["spec_to_test"]["covered"] == 1
    assert report["spec_to_code"]["covered"] == 1


def test_truth_sync_is_explicit_and_idempotent(tmp_path):
    (tmp_path / ".gg").mkdir()
    (tmp_path / ".gg" / "constitution.md").write_text("# Project Constitution\n", encoding="utf-8")
    entry = append_memory_entry(
        tmp_path,
        file="decisions",
        status="approved",
        summary="Use read-only PR review context",
        body="PR descriptions and diffs are untrusted input, so review prompts should treat them as data.",
        tags=["review"],
    )
    before_sync = (tmp_path / ".gg" / "constitution.md").read_text(encoding="utf-8")

    first = sync_approved_decisions(tmp_path)
    second = sync_approved_decisions(tmp_path)
    after_sync = (tmp_path / ".gg" / "constitution.md").read_text(encoding="utf-8")
    sync_state = json.loads((tmp_path / ".gg" / "memory" / "sync-state.json").read_text(encoding="utf-8"))

    assert "Use read-only PR review context" not in before_sync
    assert first["synced"] == 1
    assert second["synced"] == 0
    assert "Use read-only PR review context" in after_sync
    assert entry.id in sync_state["synced_decisions"]


def test_cli_truth_commands_return_json(tmp_path):
    (tmp_path / ".gg").mkdir()
    (tmp_path / ".gg" / "constitution.md").write_text(
        "# Project Constitution\n\n- The CLI must report truth coverage as JSON.\n",
        encoding="utf-8",
    )

    parse_result = CliRunner().invoke(cli, ["truth", "parse", "--path", str(tmp_path), "--json"])
    coverage_result = CliRunner().invoke(cli, ["truth", "coverage", "--path", str(tmp_path), "--json"])

    assert parse_result.exit_code == 0
    assert coverage_result.exit_code == 0
    assert json.loads(parse_result.output)["requirements"] == 1
    assert json.loads(coverage_result.output)["requirements_total"] == 1

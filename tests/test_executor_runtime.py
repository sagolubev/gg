from __future__ import annotations

import os
import subprocess
from pathlib import Path

from gg.agents.base import AgentBackend
from gg.orchestrator.config import GGConfig, GitConfig, RuntimeConfig, VerifyConfig
from gg.orchestrator.executor import CandidateExecutor, CandidateResult
from gg.orchestrator.schemas import AgentHandoffModel, AgentResultModel
from gg.orchestrator.task_analysis import TaskBrief


class FakeAgent(AgentBackend):
    def __init__(self) -> None:
        self.calls = 0

    def generate(
        self,
        prompt: str,
        *,
        cwd: str | None = None,
        timeout: int | None = None,
        context: str | None = None,
    ) -> str:
        self.calls += 1
        assert cwd is not None
        Path(cwd, "agent-output.txt").write_text("done\n", encoding="utf-8")
        return "Created agent-output.txt"

    def is_available(self) -> bool:
        return True


class MissingSandbox:
    def is_available(self) -> bool:
        return False

    def run(self, *args, **kwargs):
        raise AssertionError("sandbox should not run when unavailable")


def test_required_missing_sandbox_runtime_fails_before_agent_generate(tmp_path):
    init_repo(tmp_path)
    agent = FakeAgent()
    executor = CandidateExecutor(
        tmp_path,
        agent,
        runtime_config(require_sandbox_runtime=True),
        sandbox=MissingSandbox(),
    )

    try:
        executor.run(run_id="run-fail-closed", issue_number=42, brief=task_brief())
    except RuntimeError as exc:
        assert "sandbox-runtime is required but unavailable" in str(exc)
    else:
        raise AssertionError("missing required sandbox should fail before worktree creation")

    assert agent.calls == 0
    assert not (tmp_path.parent / ".gg-worktrees" / tmp_path.name).exists()


def test_allow_unsafe_direct_exec_falls_back_to_agent_generate(tmp_path):
    init_repo(tmp_path)
    agent = FakeAgent()
    executor = CandidateExecutor(
        tmp_path,
        agent,
        runtime_config(require_sandbox_runtime=True, allow_unsafe_direct_exec=True),
        sandbox=MissingSandbox(),
    )

    result = executor.run(run_id="run-unsafe-direct", issue_number=42, brief=task_brief())

    assert result.status == "success"
    assert result.changed_files == ["agent-output.txt"]
    assert agent.calls == 1


def test_agent_handoff_and_result_helpers_validate_with_schemas(tmp_path):
    init_repo(tmp_path)
    executor = CandidateExecutor(tmp_path, FakeAgent(), runtime_config())
    candidate = CandidateResult(
        schema_version=1,
        candidate_id="candidate-1",
        status="success",
        branch="gg/example",
        worktree_path=str(tmp_path),
        base_commit="abc123",
        summary="Created file.",
        changed_files=["agent-output.txt"],
        patch="",
        duration_seconds=1.25,
    )

    handoff = executor.build_agent_handoff(
        run_id="run-123",
        candidate_id="candidate-1",
        issue={"number": 42, "title": "Add greeting"},
        worktree_path=tmp_path,
        base_commit="abc123",
        instructions="implement the task",
        task_brief_path="artifacts/task-brief.json",
        context_snapshot_path="artifacts/context-snapshot.json",
    )
    result = executor.build_agent_result(run_id="run-123", candidate=candidate)

    assert isinstance(handoff.to_model(), AgentHandoffModel)
    assert isinstance(result.to_model(), AgentResultModel)
    assert handoff.to_dict()["artifacts"] == {}
    assert result.to_dict()["changed_files"] == ["agent-output.txt"]


def test_candidate_env_allowlists_host_env_and_keeps_caches_in_worktree(tmp_path, monkeypatch):
    monkeypatch.setenv("PATH", "/usr/bin")
    monkeypatch.setenv("HOME", "/Users/example")
    monkeypatch.setenv("LANG", "C.UTF-8")
    monkeypatch.setenv("LC_TEST", "kept")
    monkeypatch.setenv("OPENAI_API_KEY", "kept")
    monkeypatch.setenv("GITHUB_TOKEN", "kept")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "dropped")
    monkeypatch.setenv("PYTHONPATH", "dropped")
    executor = CandidateExecutor(tmp_path, FakeAgent(), runtime_config())

    env = executor._candidate_env(tmp_path)

    assert env["PATH"].endswith(f"{os.pathsep}/usr/bin")
    assert env["HOME"] == "/Users/example"
    assert env["LANG"] == "C.UTF-8"
    assert env["LC_TEST"] == "kept"
    assert env["OPENAI_API_KEY"] == "kept"
    assert env["GITHUB_TOKEN"] == "kept"
    assert "AWS_SECRET_ACCESS_KEY" not in env
    assert "PYTHONPATH" not in env
    for name in (
        "PIP_CACHE_DIR",
        "UV_CACHE_DIR",
        "npm_config_cache",
        "YARN_CACHE_FOLDER",
        "PNPM_HOME",
        "PNPM_STORE_DIR",
        "XDG_CACHE_HOME",
        "CARGO_HOME",
        "GOCACHE",
        "GOMODCACHE",
    ):
        assert Path(env[name]).is_relative_to(tmp_path)


def runtime_config(
    *,
    require_sandbox_runtime: bool = True,
    allow_unsafe_direct_exec: bool = False,
) -> GGConfig:
    return GGConfig(
        git=GitConfig(default_branch="main"),
        verify=VerifyConfig(tests=""),
        runtime=RuntimeConfig(
            use_sandbox_runtime=True,
            require_sandbox_runtime=require_sandbox_runtime,
            allow_unsafe_direct_exec=allow_unsafe_direct_exec,
            candidate_timeout_seconds=5,
        ),
    )


def task_brief() -> TaskBrief:
    return TaskBrief(
        schema_version=1,
        issue={"number": 42, "title": "Add greeting", "body": "", "labels": ["ai-ready"], "url": ""},
        summary="Do it",
        acceptance_criteria=["Add a file"],
        project_context="",
    )


def init_repo(path: Path) -> None:
    subprocess.run(["git", "init"], cwd=path, check=True, capture_output=True)
    subprocess.run(["git", "checkout", "-b", "main"], cwd=path, check=True, capture_output=True)
    (path / "README.md").write_text("# repo\n", encoding="utf-8")
    subprocess.run(["git", "add", "-A"], cwd=path, check=True, capture_output=True)
    subprocess.run(
        ["git", "commit", "-m", "init", "--no-gpg-sign"],
        cwd=path,
        check=True,
        capture_output=True,
        env={
            **os.environ,
            "GIT_AUTHOR_NAME": "test",
            "GIT_AUTHOR_EMAIL": "t@example.com",
            "GIT_COMMITTER_NAME": "test",
            "GIT_COMMITTER_EMAIL": "t@example.com",
        },
    )

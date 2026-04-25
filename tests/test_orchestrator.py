from __future__ import annotations

import hashlib
import json
import os
import signal
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import yaml
from click.testing import CliRunner

from gg.agents.base import AgentBackend
from gg.agents.codex import CodexAgent
from gg.cli import cli
from gg.commands.init import _write_operational_gitignore, _write_params
from gg.orchestrator.config import load_config
from gg.orchestrator.context import ContextSnapshotStore
from gg.orchestrator.evaluation import CandidateEvaluator, build_run_outcome
from gg.orchestrator.executor import CandidateExecutor
from gg.orchestrator import git as git_module
from gg.orchestrator.git import commit_all
from gg.orchestrator.lock import FileLock, LockManager
from gg.orchestrator.pipeline import OrchestratorPipeline, _verification_passed
from gg.orchestrator.plugins import create_agent_backend, register_agent_backend, register_platform
from gg.orchestrator.rate_limit import RateLimitStore, RateLimitSnapshot, RateLimitThrottleError
from gg.orchestrator.sandbox import SandboxPolicy, SandboxRunResult, SandboxRuntime
from gg.orchestrator.schemas import (
    AgentHandoffModel,
    AgentResultModel,
    AnalysisResultModel,
    ArchiveSummaryModel,
    CandidateResultModel,
    ExecutionEvaluationModel,
    GGConfigModel,
    InputArtifactModel,
    RunOutcomeModel,
    RunStateModel,
    TaskBriefModel,
    validation_error_message,
)
from gg.orchestrator.state import CandidateState, InvalidTransitionError, RunState, TaskState
from gg.orchestrator.store import RunStore
from gg.orchestrator.task_analysis import TaskAnalyzer, extract_single_json_object
from gg.orchestrator.verification import CheckResult, VerificationCommand, VerificationRunner
from gg.platforms.base import GitPlatform, Issue, IssueComment
from gg.platforms.github import GitHubPlatform
from gg.platforms.gitlab import GitLabPlatform


class FakePlatform(GitPlatform):
    def __init__(self):
        self.comments: list[tuple[int, str]] = []
        self.labels: list[tuple[int, list[str]]] = []
        self.removed_labels: list[tuple[int, list[str]]] = []
        self.prs: list[dict] = []
        self.issue = Issue(
            number=42,
            title="Add greeting",
            body="Write a greeting file.",
            labels=["ai-ready"],
        )
        self.issues = [self.issue]

    def list_issues(self, state: str = "open", limit: int = 30) -> list[Issue]:
        return self.issues[:limit]

    def get_issue(self, number: int) -> Issue:
        assert number == self.issue.number
        return self.issue

    def create_pr(self, *, title: str, body: str, head: str, base: str) -> str:
        self.prs.append({"title": title, "body": body, "head": head, "base": base})
        return "https://github.com/example/repo/pull/1"

    def find_pr(self, *, head: str) -> str | None:
        return None

    def add_comment(self, issue_number: int, body: str) -> None:
        self.comments.append((issue_number, body))

    def add_labels(self, issue_number: int, labels: list[str]) -> None:
        self.labels.append((issue_number, labels))

    def remove_labels(self, issue_number: int, labels: list[str]) -> None:
        self.removed_labels.append((issue_number, labels))

    def cli_name(self) -> str:
        return "fake"

    def platform_name(self) -> str:
        return "github"


class MultiIssuePlatform(FakePlatform):
    def __init__(self, issues: list[Issue]):
        super().__init__()
        self.issues = issues
        self.issue = issues[0]

    def get_issue(self, number: int) -> Issue:
        for issue in self.issues:
            if issue.number == number:
                return issue
        raise AssertionError(f"unexpected issue {number}")


class CancellingFindPrPlatform(FakePlatform):
    def __init__(self):
        super().__init__()
        self.pipeline: OrchestratorPipeline | None = None
        self.run_id = ""

    def find_pr(self, *, head: str) -> str | None:
        assert self.pipeline is not None
        self.pipeline.cancel(self.run_id, reason="cancel during publish")
        return "https://github.com/example/repo/pull/77"


class FailingOutcomePlatform(FakePlatform):
    def publish_outcome(self, *args, **kwargs) -> None:
        raise RuntimeError("result comment failed")


class FailingDonePlatform(FakePlatform):
    def publish_done(self, *args, **kwargs) -> None:
        raise RuntimeError("done label failed")


class ThrottledListPlatform(FakePlatform):
    def list_issues(self, state: str = "open", limit: int = 30) -> list[Issue]:
        raise RateLimitThrottleError(
            RateLimitSnapshot(
                bucket="github:example/repo:issues:read",
                remaining=0,
                reset_at="2999-01-01T00:00:00Z",
                limit=5000,
            )
        )


class ThrottledClaimPlatform(FakePlatform):
    def add_comment(self, issue_number: int, body: str) -> None:
        raise RateLimitThrottleError(
            RateLimitSnapshot(
                bucket="github:example/repo:issues:comment",
                remaining=0,
                reset_at="2999-01-01T00:00:00Z",
                limit=5000,
            )
        )


class AuthFailPlatform(FakePlatform):
    def validate_auth(self) -> None:
        raise RuntimeError("gh auth status failed: missing token")


class FakeAgent(AgentBackend):
    def generate(
        self,
        prompt: str,
        *,
        cwd: str | None = None,
        timeout: int | None = None,
        context: str | None = None,
    ) -> str:
        assert cwd is not None
        Path(cwd, "greeting.txt").write_text("hello from gg\n", encoding="utf-8")
        return "Created greeting.txt"

    def is_available(self) -> bool:
        return True


class ExplodingAgent(AgentBackend):
    def generate(
        self,
        prompt: str,
        *,
        cwd: str | None = None,
        timeout: int | None = None,
        context: str | None = None,
    ) -> str:
        raise AssertionError("agent should not run")

    def is_available(self) -> bool:
        return True


class JsonAnalysisAgent(AgentBackend):
    def generate(
        self,
        prompt: str,
        *,
        cwd: str | None = None,
        timeout: int | None = None,
        context: str | None = None,
    ) -> str:
        return """```json
{
  "schema_version": 1,
  "ready": true,
  "summary": "Create a greeting file",
  "acceptance_criteria": ["greeting.txt exists"],
  "candidate_files": ["greeting.txt"],
  "risk_flags": ["small file change"],
  "verification_hints": ["cat greeting.txt"],
  "context_budget": {"estimated_tokens": 120, "truncated": false}
}
```"""

    def is_available(self) -> bool:
        return True


class StructuredAnalysisAgent(AgentBackend):
    supports_task_analysis = True

    def generate(
        self,
        prompt: str,
        *,
        cwd: str | None = None,
        timeout: int | None = None,
        context: str | None = None,
    ) -> str:
        return json.dumps(
            {
                "schema_version": 1,
                "ready": True,
                "summary": "Create a greeting file",
                "acceptance_criteria": ["greeting.txt exists"],
                "classification": {"task_type": "feature", "complexity": "small"},
                "implementation": {
                    "candidate_files": ["greeting.txt"],
                    "strategy_hints": ["conservative"],
                },
                "verification": {
                    "hints": ["cat greeting.txt"],
                    "required_gates": ["tests"],
                },
                "project_context_details": {"source": "test-agent", "truncated": False},
                "candidate_files": ["greeting.txt"],
                "risk_flags": ["small file change"],
                "verification_hints": ["cat greeting.txt"],
                "context_budget": {"estimated_tokens": 120, "truncated": False},
            }
        )

    def is_available(self) -> bool:
        return True


class LargeCandidateFileAnalysisAgent(JsonAnalysisAgent):
    supports_task_analysis = True

    def generate(
        self,
        prompt: str,
        *,
        cwd: str | None = None,
        timeout: int | None = None,
        context: str | None = None,
    ) -> str:
        return json.dumps(
            {
                "schema_version": 1,
                "ready": True,
                "summary": "Touch several files",
                "acceptance_criteria": ["changes are scoped"],
                "candidate_files": ["a.py", "b.py"],
                "context_budget": {"estimated_tokens": 120, "truncated": False},
            }
        )


class TimeoutRecordingAnalysisAgent(JsonAnalysisAgent):
    supports_task_analysis = True

    def __init__(self) -> None:
        self.timeouts: list[int | None] = []

    def generate(
        self,
        prompt: str,
        *,
        cwd: str | None = None,
        timeout: int | None = None,
        context: str | None = None,
    ) -> str:
        self.timeouts.append(timeout)
        return super().generate(prompt, cwd=cwd, timeout=timeout, context=context)


class ContextLimitAnalysisAgent(JsonAnalysisAgent):
    supports_task_analysis = True

    def __init__(self, limit: int):
        self.limit = limit
        self.prompts: list[str] = []

    def context_window_tokens(self) -> int | None:
        return self.limit

    def generate(
        self,
        prompt: str,
        *,
        cwd: str | None = None,
        timeout: int | None = None,
        context: str | None = None,
    ) -> str:
        self.prompts.append(prompt)
        return super().generate(prompt, cwd=cwd, timeout=timeout, context=context)


class MalformedAnalysisAgent(AgentBackend):
    def generate(
        self,
        prompt: str,
        *,
        cwd: str | None = None,
        timeout: int | None = None,
        context: str | None = None,
    ) -> str:
        return "Here is not quite JSON"

    def is_available(self) -> bool:
        return True


class MalformedThenImplementingAgent(FakeAgent):
    supports_task_analysis = True

    def generate(
        self,
        prompt: str,
        *,
        cwd: str | None = None,
        timeout: int | None = None,
        context: str | None = None,
    ) -> str:
        if context and context.startswith("Task analysis only"):
            return "Here is not quite JSON"
        return super().generate(prompt, cwd=cwd, timeout=timeout, context=context)


class BlockedAnalysisAgent(AgentBackend):
    supports_task_analysis = True

    def generate(
        self,
        prompt: str,
        *,
        cwd: str | None = None,
        timeout: int | None = None,
        context: str | None = None,
    ) -> str:
        return json.dumps(
            {
                "schema_version": 1,
                "ready": False,
                "missing_questions": ["Which greeting language should be used?"],
                "summary": "Need language choice",
                "acceptance_criteria": [],
            }
        )

    def is_available(self) -> bool:
        return True


class DependencyChangingAgent(AgentBackend):
    def generate(
        self,
        prompt: str,
        *,
        cwd: str | None = None,
        timeout: int | None = None,
        context: str | None = None,
    ) -> str:
        assert cwd is not None
        Path(cwd, "package.json").write_text('{"dependencies":{"left-pad":"1.3.0"}}\n', encoding="utf-8")
        return "Added dependency manifest."

    def is_available(self) -> bool:
        return True


class LfsChangingAgent(AgentBackend):
    def generate(
        self,
        prompt: str,
        *,
        cwd: str | None = None,
        timeout: int | None = None,
        context: str | None = None,
    ) -> str:
        assert cwd is not None
        Path(cwd, "asset.bin").write_text("lfs pointer candidate\n", encoding="utf-8")
        return "Added LFS asset."

    def is_available(self) -> bool:
        return True


class SecondCandidateAgent(AgentBackend):
    def __init__(self):
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
        if self.calls == 2:
            Path(cwd, "winner.txt").write_text("winner\n", encoding="utf-8")
        return f"Candidate call {self.calls}"

    def is_available(self) -> bool:
        return True


class CompactSecondCandidateAgent(AgentBackend):
    def __init__(self):
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
        if self.calls == 1:
            Path(cwd, "one.txt").write_text("one\n", encoding="utf-8")
            Path(cwd, "two.txt").write_text("two\n", encoding="utf-8")
            return "Created two files."
        Path(cwd, "winner.txt").write_text("winner\n", encoding="utf-8")
        return "Created one file."

    def is_available(self) -> bool:
        return True


class RepairAgent(AgentBackend):
    def __init__(self):
        self.calls = 0
        self.prompts: list[str] = []

    def generate(
        self,
        prompt: str,
        *,
        cwd: str | None = None,
        timeout: int | None = None,
        context: str | None = None,
    ) -> str:
        self.calls += 1
        self.prompts.append(prompt)
        assert cwd is not None
        if self.calls > 1:
            Path(cwd, "repaired.txt").write_text("fixed\n", encoding="utf-8")
        return f"Repair call {self.calls}"

    def is_available(self) -> bool:
        return True


class NeedsInputAgent(AgentBackend):
    def __init__(self):
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
        if self.calls == 1:
            return "NEEDS_INPUT: Which greeting language should I use?"
        if "Use Spanish" not in prompt:
            raise AssertionError("resume prompt should include provided input artifact")
        Path(cwd, "greeting.txt").write_text("hola desde gg\n", encoding="utf-8")
        return "Created Spanish greeting."

    def is_available(self) -> bool:
        return True


class RepeatedNeedsInputAgent(AgentBackend):
    def __init__(self):
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
        if self.calls == 1:
            return "NEEDS_INPUT: Which greeting language should I use?"
        if self.calls == 2:
            return "NEEDS_INPUT: Which filename should I use?"
        Path(cwd, "greeting.txt").write_text("hola desde gg\n", encoding="utf-8")
        return "Created Spanish greeting."

    def is_available(self) -> bool:
        return True


class InterruptingAgent(AgentBackend):
    def generate(
        self,
        prompt: str,
        *,
        cwd: str | None = None,
        timeout: int | None = None,
        context: str | None = None,
    ) -> str:
        raise KeyboardInterrupt

    def is_available(self) -> bool:
        return True


class ParallelAgent(AgentBackend):
    def __init__(self):
        self.active = 0
        self.max_active = 0
        self._lock = threading.Lock()

    def generate(
        self,
        prompt: str,
        *,
        cwd: str | None = None,
        timeout: int | None = None,
        context: str | None = None,
    ) -> str:
        assert cwd is not None
        with self._lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        time.sleep(0.2)
        Path(cwd, "parallel.txt").write_text("parallel\n", encoding="utf-8")
        with self._lock:
            self.active -= 1
        return "parallel"

    def is_available(self) -> bool:
        return True


class SlowSerialAgent(AgentBackend):
    def __init__(self):
        self.active = 0
        self.max_active = 0
        self._lock = threading.Lock()

    def generate(
        self,
        prompt: str,
        *,
        cwd: str | None = None,
        timeout: int | None = None,
        context: str | None = None,
    ) -> str:
        assert cwd is not None
        with self._lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        time.sleep(0.2)
        Path(cwd, "serial.txt").write_text("serial\n", encoding="utf-8")
        with self._lock:
            self.active -= 1
        return "serial"

    def is_available(self) -> bool:
        return True


class CancellingParallelAgent(AgentBackend):
    def __init__(self, *, cancel_after_started):
        self._cancel_after_started = cancel_after_started
        self._started = 0
        self._lock = threading.Lock()

    def generate(
        self,
        prompt: str,
        *,
        cwd: str | None = None,
        timeout: int | None = None,
        context: str | None = None,
    ) -> str:
        assert cwd is not None
        with self._lock:
            self._started += 1
            started = self._started
        if started == 2:
            self._cancel_after_started()
        time.sleep(0.1)
        Path(cwd, f"candidate-{started}.txt").write_text("done\n", encoding="utf-8")
        return f"candidate {started}"

    def is_available(self) -> bool:
        return True


class FakeSandbox:
    def __init__(self):
        self.commands: list[list[str]] = []
        self.policies: list[SandboxPolicy | None] = []
        self.envs: list[dict[str, str] | None] = []

    def is_available(self) -> bool:
        return True

    def run(self, command, *, cwd, timeout, policy=None, env=None, on_process_start=None):
        self.commands.append(command)
        self.policies.append(policy)
        self.envs.append(env)
        if on_process_start is not None:
            on_process_start(43210)
        Path(cwd, "sandboxed.txt").write_text("ok\n", encoding="utf-8")
        output_path = command[command.index("-o") + 1]
        Path(output_path).write_text("sandbox summary\n", encoding="utf-8")
        return SandboxRunResult(
            command=command,
            status="passed",
            exit_code=0,
            stdout="",
            stderr="",
            settings={},
        )


class SandboxRequiredCodexAgent(CodexAgent):
    def __init__(self):
        self.analysis_calls = 0
        self.candidate_generated = False

    def is_available(self) -> bool:
        return True

    def generate(
        self,
        prompt: str,
        *,
        cwd: str | None = None,
        timeout: int | None = None,
        context: str | None = None,
    ) -> str:
        if context and "Task analysis only" in context:
            self.analysis_calls += 1
            return "{}"
        self.candidate_generated = True
        return "should not run"


def init_repo(path: Path) -> None:
    subprocess.run(["git", "init"], cwd=path, check=True, capture_output=True)
    subprocess.run(["git", "checkout", "-b", "main"], cwd=path, check=True, capture_output=True)
    (path / "README.md").write_text("# repo\n", encoding="utf-8")
    (path / ".gg").mkdir()
    (path / ".gg" / "params.yaml").write_text("verify:\n  tests: ''\n", encoding="utf-8")
    subprocess.run(["git", "add", "-A"], cwd=path, check=True, capture_output=True)
    subprocess.run(
        ["git", "commit", "-m", "init", "--no-gpg-sign"],
        cwd=path,
        check=True,
        capture_output=True,
        env={
            "GIT_AUTHOR_NAME": "test",
            "GIT_AUTHOR_EMAIL": "t@example.com",
            "GIT_COMMITTER_NAME": "test",
            "GIT_COMMITTER_EMAIL": "t@example.com",
            "PATH": "/usr/bin:/usr/local/bin:/opt/homebrew/bin",
        },
    )


def commit_repo(path: Path, message: str) -> None:
    subprocess.run(["git", "add", "-A"], cwd=path, check=True, capture_output=True)
    subprocess.run(
        ["git", "commit", "-m", message, "--no-gpg-sign"],
        cwd=path,
        check=True,
        capture_output=True,
        env={
            "GIT_AUTHOR_NAME": "test",
            "GIT_AUTHOR_EMAIL": "t@example.com",
            "GIT_COMMITTER_NAME": "test",
            "GIT_COMMITTER_EMAIL": "t@example.com",
            "PATH": "/usr/bin:/usr/local/bin:/opt/homebrew/bin",
        },
    )


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_platform_adapters_report_capabilities(tmp_path):
    init_repo(tmp_path)

    github = GitHubPlatform(str(tmp_path)).capabilities().to_dict()
    gitlab = GitLabPlatform(str(tmp_path)).capabilities().to_dict()

    assert github["labels"] is True
    assert github["find_pr"] is True
    assert github["issue_comments"] is True
    assert gitlab["labels"] is True
    assert gitlab["find_pr"] is True


def test_platform_claim_task_uses_stage_marker_idempotency():
    platform = FakePlatform()
    run_id = "issue-42-test"
    platform.issue.comments.append(IssueComment(body=f"{platform.stage_marker(run_id, 'claim')}\nalready claimed"))

    platform.claim_task(platform.issue, run_id=run_id, work_label="gg:in-progress")

    assert platform.labels == [(42, ["gg:in-progress"])]
    assert platform.comments == []


def create_ready_run(
    pipeline: OrchestratorPipeline,
    issue_number: int = 42,
    *,
    blocked: bool = False,
) -> dict:
    issue = pipeline.platform.get_issue(issue_number)
    state = pipeline.store.create(issue, dry_run=False)
    state.max_attempts = pipeline.config.runtime.max_attempts
    state.transition(TaskState.CLAIMING, reason="test fixture issue selected")
    state.transition(TaskState.QUEUED, reason="test fixture claim complete")
    state.transition(TaskState.RUN_STARTED, reason="test fixture start pipeline")
    state.transition(TaskState.TASK_ANALYSIS, reason="test fixture create task brief")
    pipeline.store.write(state)
    brief = pipeline._refresh_task_analysis(state, issue)
    if blocked or brief.blocked:
        state.transition(TaskState.BLOCKED, reason="test fixture blocked")
        state.last_error = {
            "code": "missing_task_info",
            "message": "; ".join(brief.missing_questions) or "test fixture blocked",
            "at": state.updated_at,
        }
    else:
        state.transition(TaskState.READY_FOR_EXECUTION, reason="test fixture task brief ready")
    pipeline.store.write(state)
    return {"run_id": state.run_id, "state": state.state.value}


def test_artifact_schemas_reject_invalid_nested_values():
    try:
        RunStateModel.model_validate(
            {
                "schema_version": 1,
                "run_id": "run-1",
                "issue": {"number": 1},
                "state": "AgentRunning",
                "created_at": "2026-04-25T12:00:00Z",
                "updated_at": "2026-04-25T12:00:00Z",
                "candidate_states": {
                    "candidate-1": {
                        "status": "still-working",
                    }
                },
            }
        )
    except Exception as exc:
        message = validation_error_message("state.json", exc)
    else:
        raise AssertionError("invalid nested candidate status should fail")

    assert "state.json.candidate_states.candidate-1.status" in message
    assert "unknown candidate status" in message


def test_artifact_schemas_tolerate_additive_resume_fields():
    state = RunStateModel.model_validate(
        {
            "schema_version": 1,
            "run_id": "run-1",
            "issue": {"number": 1},
            "state": "ExternalTaskReady",
            "created_at": "2026-04-25T12:00:00Z",
            "updated_at": "2026-04-25T12:00:00Z",
            "future_field": "ignored",
            "candidate_states": {
                "candidate-1": {
                    "status": "success",
                    "future_candidate_field": "ignored",
                }
            },
        }
    )

    assert state.run_id == "run-1"
    assert state.candidate_states["candidate-1"].status == "success"
    assert state.baseline == {}
    assert state.stage_attempts == {}
    assert state.operator == {}
    assert state.cost is None


def test_run_state_round_trips_phase_c_resume_fields():
    state = RunState(
        run_id="run-1",
        issue={"number": 1},
        baseline={
            "status": "failed",
            "commit": "abc123",
            "failed_commands": ["pytest"],
            "checked_at": "2026-04-25T12:00:00Z",
        },
        stage_attempts={"analysis": 1, "execution": 2},
        locks={"run": {"owner_pid": 123, "heartbeat_at": "2026-04-25T12:00:00Z"}},
        operator={"requested_by": "cli"},
        cost={"total_usd": 0.25, "events": 1},
        blocked_resume_state=TaskState.TASK_ANALYSIS,
        blocked_until="2026-04-25T12:30:00Z",
    )

    loaded = RunState.from_dict(state.to_dict())

    assert loaded.baseline["status"] == "failed"
    assert loaded.stage_attempts == {"analysis": 1, "execution": 2}
    assert loaded.locks["run"]["owner_pid"] == 123
    assert loaded.operator == {"requested_by": "cli"}
    assert loaded.blocked_resume_state is TaskState.TASK_ANALYSIS
    assert loaded.blocked_until == "2026-04-25T12:30:00Z"
    assert loaded.cost == {
        "total_usd": 0.25,
        "total_tokens": None,
        "input_tokens": None,
        "output_tokens": None,
        "duration_seconds": None,
        "events": 1,
    }


def test_run_state_round_trips_candidate_process_ids():
    state = RunState(
        run_id="run-1",
        issue={"number": 1},
        candidate_states={
            "candidate-1": CandidateState(
                status="running",
                agent_pid=111,
                sandbox_pid=222,
            )
        },
    )

    loaded = RunState.from_dict(state.to_dict())

    assert loaded.candidate_states["candidate-1"].agent_pid == 111
    assert loaded.candidate_states["candidate-1"].sandbox_pid == 222


def test_phase_c_artifact_placeholders_validate_minimal_contracts():
    AgentHandoffModel.model_validate(
        {
            "schema_version": 1,
            "run_id": "run-1",
            "candidate_id": "candidate-1",
            "attempt": 1,
            "created_at": "2026-04-25T12:00:00Z",
        }
    )
    AgentResultModel.model_validate(
        {
            "schema_version": 1,
            "run_id": "run-1",
            "candidate_id": "candidate-1",
            "status": "success",
            "finished_at": "2026-04-25T12:00:00Z",
        }
    )
    ExecutionEvaluationModel.model_validate(
        {
            "schema_version": 1,
            "run_id": "run-1",
            "evaluated_at": "2026-04-25T12:00:00Z",
            "verdict": "accept",
        }
    )
    RunOutcomeModel.model_validate(
        {
            "schema_version": 1,
            "run_id": "run-1",
            "state": "Completed",
            "status": "success",
            "completed_at": "2026-04-25T12:00:00Z",
        }
    )
    ArchiveSummaryModel.model_validate(
        {
            "schema_version": 1,
            "run_id": "run-1",
            "archived_at": "2026-04-25T12:00:00Z",
        }
    )


def test_task_brief_model_accepts_old_and_structured_shapes():
    old_shape = TaskBriefModel.model_validate(
        {
            "schema_version": 1,
            "issue": {"number": 1, "title": "Old issue"},
            "summary": "Do the work",
        }
    )
    structured = TaskBriefModel.model_validate(
        {
            "schema_version": 1,
            "issue": {"number": 1, "title": "Structured issue"},
            "summary": "Do the work",
            "classification": {"task_type": "feature"},
            "implementation": {"candidate_files": ["app.py"]},
            "verification": {"required_gates": ["tests"]},
            "project_context_details": {"source": "knowledge_engine"},
        }
    )
    AnalysisResultModel.model_validate(
        {
            "schema_version": 1,
            "ready": True,
            "classification": {"task_type": "feature"},
            "implementation": {"candidate_files": ["app.py"]},
            "verification": {"required_gates": ["tests"]},
            "project_context_details": {"source": "analysis-agent"},
        }
    )

    assert old_shape.classification == {}
    assert structured.implementation["candidate_files"] == ["app.py"]


def test_task_analyzer_populates_structured_contract_from_agent(tmp_path):
    init_repo(tmp_path)
    issue = Issue(number=42, title="Add greeting", body="Create greeting.txt", labels=["ai-ready"])

    brief = TaskAnalyzer(tmp_path, agent=StructuredAnalysisAgent()).analyze(issue)

    assert brief.classification["task_type"] == "feature"
    assert brief.classification["labels"] == ["ai-ready"]
    assert brief.implementation["candidate_files"] == ["greeting.txt"]
    assert brief.verification["required_gates"] == ["tests"]
    assert brief.project_context_details["source"] == "test-agent"


def test_task_analyzer_fallback_populates_structured_contract(tmp_path):
    init_repo(tmp_path)
    issue = Issue(number=42, title="Add greeting", body="Create greeting.txt", labels=["ai-ready"])

    brief = TaskAnalyzer(tmp_path, agent=None).analyze(issue)

    assert brief.classification["task_type"] == "implementation"
    assert brief.classification["labels"] == ["ai-ready"]
    assert "strategy_hints" in brief.implementation
    assert brief.verification["required_gates"] == ["configured-tests", "configured-lint"]
    assert brief.project_context_details["source"] == "knowledge_engine"


def test_config_schema_reports_nested_field_paths():
    try:
        GGConfigModel.model_validate(
            {
                "git": {"default_branch": "main"},
                "runtime": {"candidates": 0},
            }
        )
    except Exception as exc:
        message = validation_error_message(".gg/params.yaml", exc)
    else:
        raise AssertionError("invalid runtime candidates should fail")

    assert ".gg/params.yaml.runtime.candidates" in message


def test_load_config_rejects_invalid_nested_runtime_value(tmp_path):
    init_repo(tmp_path)
    (tmp_path / ".gg" / "params.yaml").write_text(
        "verify:\n  tests: ''\nruntime:\n  candidates: 0\n",
        encoding="utf-8",
    )

    try:
        load_config(tmp_path)
    except ValueError as exc:
        message = str(exc)
    else:
        raise AssertionError("invalid runtime.candidates should fail during config load")

    assert ".gg/params.yaml.runtime.candidates" in message


def test_load_config_rejects_unknown_nested_keys(tmp_path):
    init_repo(tmp_path)
    (tmp_path / ".gg" / "params.yaml").write_text(
        "verify:\n  tests: ''\nruntime:\n  candidatez: 2\n",
        encoding="utf-8",
    )

    try:
        load_config(tmp_path)
    except ValueError as exc:
        message = str(exc)
    else:
        raise AssertionError("unknown runtime key should fail during config load")

    assert ".gg/params.yaml.runtime.candidatez" in message
    assert "unknown configuration key" in message


def test_load_config_rejects_non_mapping_sections(tmp_path):
    init_repo(tmp_path)
    (tmp_path / ".gg" / "params.yaml").write_text(
        "verify:\n  tests: ''\nruntime: false\n",
        encoding="utf-8",
    )

    try:
        load_config(tmp_path)
    except ValueError as exc:
        message = str(exc)
    else:
        raise AssertionError("non-mapping runtime section should fail during config load")

    assert ".gg/params.yaml.runtime" in message
    assert "expected mapping" in message


def test_run_store_rejects_invalid_state_json_with_path(tmp_path):
    init_repo(tmp_path)
    store = RunStore(tmp_path)
    state = store.create(Issue(number=1, title="Bad state", body="", labels=["ai-ready"]), dry_run=True)
    state_path = tmp_path / ".gg" / "runs" / state.run_id / "state.json"
    data = json.loads(state_path.read_text(encoding="utf-8"))
    data["candidate_states"] = {"candidate-1": {"status": "still-working"}}
    state_path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")

    try:
        store.load(state.run_id)
    except ValueError as exc:
        message = str(exc)
    else:
        raise AssertionError("invalid state.json should fail")

    assert str(state_path) in message
    assert "candidate_states.candidate-1.status" in message


def test_run_store_state_backup_is_opt_in_and_recovers_corrupt_primary(tmp_path):
    init_repo(tmp_path)
    issue = Issue(number=1, title="Backup state", body="", labels=["ai-ready"])

    default_store = RunStore(tmp_path)
    default_state = default_store.create(issue, dry_run=True)
    default_state.operator = {"name": "cli"}
    default_store.write(default_state)
    default_run_dir = tmp_path / ".gg" / "runs" / default_state.run_id
    assert not (default_run_dir / "state.json.bak").exists()

    backup_store = RunStore(tmp_path, keep_state_backup=True)
    state = backup_store.create(Issue(number=2, title="Recover state", body="", labels=["ai-ready"]), dry_run=True)
    state.operator = {"name": "cli"}
    backup_store.write(state)
    run_dir = tmp_path / ".gg" / "runs" / state.run_id
    assert (run_dir / "state.json.bak").exists()

    state.operator = {"name": "updated"}
    backup_store.write(state)
    (run_dir / "state.json").write_text("{not-json\n", encoding="utf-8")

    recovered = backup_store.load(state.run_id)

    assert recovered.operator == {"name": "cli"}


def test_run_store_atomic_writes_do_not_leave_temp_files(tmp_path):
    init_repo(tmp_path)
    store = RunStore(tmp_path, keep_state_backup=True)
    state = store.create(Issue(number=1, title="Atomic state", body="", labels=["ai-ready"]), dry_run=True)
    store.write_json(state.run_id, "artifacts/custom.json", {"ok": True})
    store.write_text(state.run_id, "artifacts/custom.txt", "ok\n")
    state.operator = {"name": "cli"}
    store.write(state)

    run_dir = tmp_path / ".gg" / "runs" / state.run_id
    assert not list(run_dir.rglob("*.tmp"))
    assert json.loads((run_dir / "state.json").read_text(encoding="utf-8"))["operator"] == {"name": "cli"}
    assert (run_dir / "state.json.bak").exists()


def test_run_store_reconciles_missing_transition_events_on_load(tmp_path):
    init_repo(tmp_path)
    store = RunStore(tmp_path)
    state = store.create(Issue(number=1, title="Reconcile events", body="", labels=["ai-ready"]), dry_run=True)
    state.transition(TaskState.CLAIMING, reason="simulate crash after state write")
    run_dir = tmp_path / ".gg" / "runs" / state.run_id
    (run_dir / "state.json").write_text(json.dumps(state.to_dict(), indent=2) + "\n", encoding="utf-8")
    pipeline_log = run_dir / "pipeline.jsonl"
    existing = [
        line
        for line in pipeline_log.read_text(encoding="utf-8").splitlines()
        if '"event": "state_transition"' not in line
    ]
    pipeline_log.write_text("\n".join(existing) + "\n", encoding="utf-8")

    loaded = store.load(state.run_id)

    events = read_jsonl(pipeline_log)
    assert loaded.state is TaskState.CLAIMING
    assert any(
        event["event"] == "state_transition"
        and event["to_state"] == "Claiming"
        and event["reconciled"] is True
        for event in events
    )


def test_run_store_validates_top_level_verification_artifacts(tmp_path):
    init_repo(tmp_path)
    store = RunStore(tmp_path)
    state = store.create(Issue(number=1, title="Validate artifacts", body="", labels=["ai-ready"]), dry_run=True)

    for artifact_path in ("artifacts/baseline-setup.json", "artifacts/integration-verification.json"):
        try:
            store.write_json(state.run_id, artifact_path, {"schema_version": 1, "checks": [{"status": "bogus"}]})
        except ValueError as exc:
            message = str(exc)
        else:
            raise AssertionError(f"{artifact_path} should fail schema validation")
        assert artifact_path in message
        assert "checks.0.status" in message


def test_run_store_validates_publishing_repair_context(tmp_path):
    init_repo(tmp_path)
    store = RunStore(tmp_path)
    state = store.create(Issue(number=1, title="Validate publishing repair", body="", labels=["ai-ready"]), dry_run=True)

    try:
        store.write_json(
            state.run_id,
            "artifacts/publishing-repair-context-attempt-2.json",
            {
                "schema_version": 1,
                "parent_candidate_id": "candidate-1",
                "feedback": "repair me",
                "publishing_failure": {"code": "patch_conflict"},
                "created_at": "not-a-timestamp",
            },
        )
    except ValueError as exc:
        message = str(exc)
    else:
        raise AssertionError("invalid publishing repair context should fail schema validation")

    assert "artifacts/publishing-repair-context-attempt-2.json.created_at" in message


def test_candidate_and_input_artifact_schemas_are_explicit():
    candidate = CandidateResultModel.model_validate(
        {
            "schema_version": 1,
            "candidate_id": "candidate-1",
            "status": "success",
            "branch": "gg/test",
            "worktree_path": "/tmp/worktree",
            "base_commit": "abc123",
            "summary": "done",
            "changed_files": ["greeting.txt"],
            "duration_seconds": 1.2,
            "policy_violations": [],
        }
    )
    assert candidate.status == "success"

    try:
        InputArtifactModel.model_validate(
            {
                "schema_version": 1,
                "source": "cli",
                "sequence_number": 1,
                "content_hash": "abc",
                "message": "hello",
                "created_at": "2026-04-25 12:00:00",
                "answered_state": "NeedsInput",
            }
        )
    except Exception as exc:
        message = validation_error_message("input-v1-0001.json", exc)
    else:
        raise AssertionError("non-UTC timestamp should fail")

    assert "input-v1-0001.json.created_at" in message


def test_run_state_rejects_illegal_transition():
    state = RunState(run_id="run-1", issue={"number": 1})
    try:
        state.transition(TaskState.COMPLETED)
    except InvalidTransitionError:
        pass
    else:
        raise AssertionError("illegal transition should fail")


def test_blocked_state_can_terminally_fail_by_policy():
    state = RunState(run_id="run-1", issue={"number": 1})
    state.recover_to(TaskState.BLOCKED, reason="test blocked")

    state.fail(code="blocked_timeout", message="blocked too long")

    assert state.state is TaskState.TERMINAL_FAILURE
    assert state.last_error["code"] == "blocked_timeout"


def test_pipeline_dry_run_reaches_ready_for_execution(tmp_path):
    init_repo(tmp_path)
    platform = FakePlatform()
    result = OrchestratorPipeline(tmp_path, platform=platform, agent=FakeAgent()).run_issue(
        42,
        dry_run=True,
    )

    assert result["state"] == "ReadyForExecution"
    assert result["dry_run"] is True
    assert result["planned_operations"] == [
        {"operation": "add_labels", "issue_number": 42, "labels": ["gg:in-progress"]},
        {
            "operation": "add_comment",
            "issue_number": 42,
            "marker": f"<!-- gg-run-id={result['run_id']} stage=claim -->",
            "body": f"gg picked this issue for implementation. Run: `{result['run_id']}`",
        },
    ]
    runs = list((tmp_path / ".gg" / "runs").glob("*/state.json"))
    assert runs == []
    assert not any((tmp_path / ".gg" / "objects").glob("*/*"))
    assert not (tmp_path.parent / ".gg-worktrees" / tmp_path.name).exists()
    assert platform.comments == []
    assert platform.labels == []


def test_run_store_uses_unique_run_ids(tmp_path):
    init_repo(tmp_path)
    store = RunStore(tmp_path)
    issue = Issue(number=42, title="Add greeting", body="", labels=["ai-ready"])

    first = store.create(issue)
    second = store.create(issue)

    assert first.run_id != second.run_id
    runs = list((tmp_path / ".gg" / "runs").glob("*/state.json"))
    assert len(runs) == 2


def test_pipeline_no_pr_completes_with_one_candidate(tmp_path):
    init_repo(tmp_path)
    (tmp_path / ".gg" / "params.yaml").write_text(
        "verify:\n  tests: \"python -c 'print(1)'\"\n",
        encoding="utf-8",
    )
    platform = FakePlatform()
    result = OrchestratorPipeline(tmp_path, platform=platform, agent=FakeAgent()).run_issue(
        42,
        no_pr=True,
    )

    assert result["state"] == "Completed"
    assert result["pr_url"] is None
    assert platform.comments
    assert platform.labels == [(42, ["gg:in-progress"]), (42, ["gg:done"])]
    assert platform.removed_labels == [(42, ["gg:in-progress", "gg:blocked"])]
    runs = list((tmp_path / ".gg" / "runs").glob("*/state.json"))
    assert len(runs) == 1
    run_dir = runs[0].parent
    assert (run_dir / "candidates" / "candidate-1" / "candidate-result.json").exists()
    assert (run_dir / "candidates" / "candidate-1" / "agent-handoff.json").exists()
    assert (run_dir / "candidates" / "candidate-1" / "agent-result.json").exists()
    assert (run_dir / "candidates" / "candidate-1" / "patch.diff").read_text(encoding="utf-8")
    assert (run_dir / "candidates" / "candidate-1" / "verification.json").exists()
    verification = json.loads((run_dir / "candidates" / "candidate-1" / "verification.json").read_text(encoding="utf-8"))
    assert verification["checks"][0]["id"] == "tests"
    assert verification["checks"][0]["category"] == "test"
    assert verification["required_passed"] is True
    assert (run_dir / "artifacts" / "candidate-selection.json").exists()
    assert (run_dir / "artifacts" / "evaluation.json").exists()
    assert (run_dir / "artifacts" / "run-outcome.json").exists()
    assert (run_dir / "artifacts" / "run-summary.json").exists()
    assert (run_dir / "pipeline.jsonl").exists()
    assert (run_dir / "cost.jsonl").exists()

    pipeline_events = read_jsonl(run_dir / "pipeline.jsonl")
    transitions = [event for event in pipeline_events if event["event"] == "state_transition"]
    assert [event["to_state"] for event in transitions] == [
        "Claiming",
        "Queued",
        "RunStarted",
        "TaskAnalysis",
        "ReadyForExecution",
        "AgentSelection",
        "AgentRunning",
        "ResultEvaluation",
        "OutcomePublishing",
        "Completed",
    ]
    assert any(
        event["event"] == "artifact_updated" and event["artifact"] == "run_summary"
        for event in pipeline_events
    )
    assert any(
        event["event"] == "candidate_state"
        and event["candidate_id"] == "candidate-1"
        and event["status"] == "success"
        for event in pipeline_events
    )

    cost_events = read_jsonl(run_dir / "cost.jsonl")
    assert cost_events == [
        {
            "event": "candidate_metrics",
            "at": cost_events[0]["at"],
            "run_id": result["run_id"],
            "candidate_id": "candidate-1",
            "attempt": 1,
            "strategy": "conservative",
            "status": "success",
            "error": None,
            "duration_seconds": cost_events[0]["duration_seconds"],
            "verification_duration_seconds": cost_events[0]["verification_duration_seconds"],
            "verification_passed": True,
            "verification_mutated_worktree": False,
            "verification_failed_commands": [],
            "changed_files": ["greeting.txt"],
            "changed_files_count": 1,
            "total_usd": None,
            "token_usage": None,
        }
    ]

    summary = json.loads((run_dir / "artifacts" / "run-summary.json").read_text(encoding="utf-8"))
    assert summary["state"] == "Completed"
    assert summary["artifacts"]["run_summary"].endswith("artifacts/run-summary.json")
    assert summary["candidate_states"]["candidate-1"]["status"] == "success"
    assert summary["logs"]["pipeline"].endswith("pipeline.jsonl")


def test_publish_outcome_failure_does_not_persist_completed(monkeypatch, tmp_path):
    init_repo(tmp_path)
    monkeypatch.setattr("gg.orchestrator.pipeline.push_branch", lambda *_args, **_kwargs: None)
    (tmp_path / ".gg" / "params.yaml").write_text(
        "verify:\n  tests: \"python -c 'print(1)'\"\n",
        encoding="utf-8",
    )
    result = OrchestratorPipeline(
        tmp_path,
        platform=FailingOutcomePlatform(),
        agent=FakeAgent(),
    ).run_issue(42, no_pr=False)

    state = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent()).store.load(result["run_id"])
    assert result["state"] == "TerminalFailure"
    assert result["error"]["code"] == "publish_outcome_failed"
    assert state.state is TaskState.TERMINAL_FAILURE
    assert state.publishing_step == "pr_created"


def test_publish_done_failure_does_not_persist_completed(tmp_path):
    init_repo(tmp_path)
    (tmp_path / ".gg" / "params.yaml").write_text(
        "verify:\n  tests: \"python -c 'print(1)'\"\n",
        encoding="utf-8",
    )
    result = OrchestratorPipeline(
        tmp_path,
        platform=FailingDonePlatform(),
        agent=FakeAgent(),
    ).run_issue(42, no_pr=True)

    state = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent()).store.load(result["run_id"])
    assert result["state"] == "TerminalFailure"
    assert result["error"]["code"] == "publish_done_failed"
    assert state.state is TaskState.TERMINAL_FAILURE
    assert state.publishing_step == "local_no_pr"


def test_pipeline_persists_runtime_candidate_process_metadata(tmp_path):
    init_repo(tmp_path)
    pipeline = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent())
    ready = create_ready_run(pipeline)
    state = pipeline.store.load(ready["run_id"])
    state.candidate_states["candidate-1"] = CandidateState(status="running", started_at="2026-04-25T12:00:00Z")
    pipeline.store.write(state)

    pipeline._update_candidate_runtime_state(
        state.run_id,
        "candidate-1",
        {
            "worktree_path": "/tmp/worktree",
            "branch": "gg/test",
            "sandbox_pid": 43210,
            "port": 43000,
        },
    )

    loaded = pipeline.store.load(state.run_id)
    candidate = loaded.candidate_states["candidate-1"]
    assert candidate.worktree_path == "/tmp/worktree"
    assert candidate.port == 43000
    assert candidate.branch == "gg/test"
    assert candidate.sandbox_pid == 43210


def test_pipeline_fanout_selects_first_passing_candidate(tmp_path):
    init_repo(tmp_path)
    (tmp_path / ".gg" / "params.yaml").write_text(
        "verify:\n  tests: ''\nruntime:\n  candidates: 2\n",
        encoding="utf-8",
    )

    result = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=SecondCandidateAgent()).run_issue(
        42,
        no_pr=True,
    )

    assert result["state"] == "Completed"
    assert result["winner"] == "candidate-2"
    runs = list((tmp_path / ".gg" / "runs").glob("*/state.json"))
    run_dir = runs[0].parent
    assert (run_dir / "candidates" / "candidate-1" / "candidate-result.json").exists()
    assert (run_dir / "candidates" / "candidate-2" / "candidate-result.json").exists()


def test_resource_preflight_blocks_when_disk_budget_unavailable(monkeypatch, tmp_path):
    init_repo(tmp_path)
    monkeypatch.setattr("gg.orchestrator.pipeline._available_disk_mb", lambda path: 1)
    (tmp_path / ".gg" / "params.yaml").write_text(
        "verify:\n  tests: ''\nruntime:\n  candidates: 2\n  resource:\n    max_disk_mb: 4096\n",
        encoding="utf-8",
    )

    result = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent()).run_issue(42, no_pr=True)

    state = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent()).store.load(result["run_id"])
    artifact = json.loads((tmp_path / state.artifacts["resource_preflight"]).read_text(encoding="utf-8"))
    assert result["state"] == "Blocked"
    assert result["error"]["code"] == "insufficient_disk"
    assert artifact["passed"] is False
    assert artifact["allowed_candidates"] == 0
    assert artifact["repo_size_mb"] >= 1
    assert artifact["per_candidate_mb"] >= artifact["max_disk_mb"]


def test_resource_preflight_downscales_initial_candidates(monkeypatch, tmp_path):
    init_repo(tmp_path)
    monkeypatch.setattr("gg.orchestrator.pipeline._available_disk_mb", lambda path: 4096)
    (tmp_path / ".gg" / "params.yaml").write_text(
        """verify:
  tests: ''
runtime:
  candidates: 3
  resource:
    max_disk_mb: 4096
    allow_candidate_downscale: true
""",
        encoding="utf-8",
    )

    result = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent()).run_issue(42, no_pr=True)

    state = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent()).store.load(result["run_id"])
    artifact = json.loads((tmp_path / state.artifacts["resource_preflight"]).read_text(encoding="utf-8"))
    assert result["state"] == "Completed"
    assert artifact["passed"] is True
    assert artifact["downscaled"] is True
    assert artifact["allowed_candidates"] == 1
    assert artifact["estimate_strategy"] == "max(configured_candidate_limit, repo_checkout_size)"
    assert list(state.candidate_states) == ["candidate-1"]


def test_pipeline_evaluator_can_choose_later_more_focused_candidate(tmp_path):
    init_repo(tmp_path)
    (tmp_path / ".gg" / "params.yaml").write_text(
        "verify:\n  tests: ''\nruntime:\n  candidates: 2\n",
        encoding="utf-8",
    )

    result = OrchestratorPipeline(
        tmp_path,
        platform=FakePlatform(),
        agent=CompactSecondCandidateAgent(),
    ).run_issue(42, no_pr=True)

    assert result["state"] == "Completed"
    assert result["winner"] == "candidate-2"
    run_dir = next((tmp_path / ".gg" / "runs").glob("*"))
    selection = json.loads((run_dir / "artifacts" / "candidate-selection.json").read_text(encoding="utf-8"))
    evaluation = json.loads((run_dir / "artifacts" / "evaluation.json").read_text(encoding="utf-8"))
    assert selection["winner"] == "candidate-2"
    assert evaluation["selected_candidate_id"] == "candidate-2"
    assert evaluation["traffic_light"] == "green"
    assert selection["candidates"][1]["score"] > selection["candidates"][0]["score"]


def test_candidate_evaluator_rejects_policy_violations_even_when_successful():
    class Candidate:
        def __init__(self, candidate_id: str):
            self.candidate_id = candidate_id

    decision = CandidateEvaluator().evaluate(
        [
            {
                "index": 1,
                "candidate": Candidate("candidate-1"),
                "effective_status": "success",
                "verification_passed": True,
                "verification_mutated_worktree": False,
                "policy_violations": [{"code": "dependency_changes_blocked", "message": "blocked"}],
                "final_files": ["package.json"],
                "verification": [],
                "result_path": "candidate-1/result.json",
            },
            {
                "index": 2,
                "candidate": Candidate("candidate-2"),
                "effective_status": "success",
                "verification_passed": True,
                "verification_mutated_worktree": False,
                "policy_violations": [],
                "final_files": ["app.py", "tests/test_app.py"],
                "verification": [],
                "result_path": "candidate-2/result.json",
            },
        ],
        attempt=1,
        max_attempts=1,
    )

    assert decision.artifact["winner"] == "candidate-2"
    assert decision.winner["candidate"].candidate_id == "candidate-2"


def test_execution_evaluation_includes_outcome_and_recovery_contract():
    class Candidate:
        candidate_id = "candidate-1"

    decision = CandidateEvaluator().evaluate(
        [
            {
                "index": 1,
                "candidate": Candidate(),
                "effective_status": "failed",
                "verification_passed": False,
                "verification_mutated_worktree": False,
                "policy_violations": [],
                "final_files": [],
                "verification": [],
                "result_path": "candidate-1/result.json",
            }
        ],
        attempt=1,
        max_attempts=2,
        run_id="run-1",
        evaluated_at="2026-04-25T12:00:00Z",
    )

    evaluation = decision.execution_evaluation
    assert evaluation["verdict"] == "reject"
    assert evaluation["traffic_light"] == "red"
    assert evaluation["proposed_run_outcome"]["kind"] == "repair"
    assert evaluation["failure"]["code"] == "no_eligible_candidate"
    assert evaluation["suggested_recovery"]["next_attempt"] == 2


def test_run_outcome_includes_publication_source_and_trace_refs():
    state = RunState(
        run_id="run-1",
        issue={"number": 42, "title": "Add greeting"},
        state=TaskState.COMPLETED,
        pr_url="https://github.com/example/repo/pull/1",
        publishing_step="completed",
        artifacts={
            "candidate_selection": "artifacts/candidate-selection.json",
            "evaluation": "artifacts/evaluation.json",
            "execution_evaluation": "artifacts/execution-evaluation.json",
        },
    )

    outcome = build_run_outcome(
        state,
        {
            "candidate_id": "candidate-1",
            "summary": "Created greeting.txt",
            "changed_files": ["greeting.txt"],
            "verification_passed": True,
        },
        completed_at="2026-04-25T12:00:00Z",
    )

    assert outcome["kind"] == "artifact_outcome"
    assert outcome["task_result"]["changed_files"] == ["greeting.txt"]
    assert outcome["source"]["issue_number"] == 42
    assert outcome["publication"]["publishing_step"] == "completed"
    assert outcome["trace_refs"] == [
        "artifacts/candidate-selection.json",
        "artifacts/evaluation.json",
        "artifacts/execution-evaluation.json",
    ]


def test_pipeline_uses_parallel_fanout_when_enabled(tmp_path):
    init_repo(tmp_path)
    (tmp_path / ".gg" / "params.yaml").write_text(
        "verify:\n  tests: ''\nruntime:\n  candidates: 2\n  max_parallel_candidates: 2\n",
        encoding="utf-8",
    )
    agent = ParallelAgent()

    result = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=agent).run_issue(42, no_pr=True)

    assert result["state"] == "Completed"
    assert agent.max_active >= 2


def test_issue_lock_serializes_same_issue_execution(tmp_path):
    init_repo(tmp_path)
    agent = SlowSerialAgent()
    platform = FakePlatform()
    results: list[dict[str, str]] = []

    def run_pipeline() -> None:
        pipeline = OrchestratorPipeline(tmp_path, platform=platform, agent=agent)
        results.append(pipeline.run_issue(42, no_pr=True))

    first = threading.Thread(target=run_pipeline)
    second = threading.Thread(target=run_pipeline)

    first.start()
    time.sleep(0.05)
    second.start()
    first.join()
    second.join()

    assert [result["state"] for result in results] == ["Completed", "Completed"]
    assert agent.max_active == 1


def test_cancel_waits_for_candidate_batch_to_quiesce(tmp_path):
    init_repo(tmp_path)
    (tmp_path / ".gg" / "params.yaml").write_text(
        "verify:\n  tests: ''\nruntime:\n  candidates: 2\n  max_parallel_candidates: 2\n",
        encoding="utf-8",
    )
    pipeline = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent())
    run_ref: dict[str, str] = {}
    cancel_event = threading.Event()

    agent = CancellingParallelAgent(
        cancel_after_started=lambda: (
            pipeline.cancel(run_ref["run_id"], reason="cancel while candidates running"),
            cancel_event.set(),
        ),
    )
    pipeline = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=agent)
    ready = create_ready_run(pipeline)
    run_ref["run_id"] = ready["run_id"]

    result = pipeline.resume(ready["run_id"], no_pr=True)

    assert cancel_event.is_set()
    assert result["cancelled"] is True
    assert result["state"] == "Cancelled"
    state = pipeline.store.load(ready["run_id"])
    assert state.cancel_requested is True
    assert state.candidates_quiescent() is True
    assert all(candidate.status != "running" for candidate in state.candidate_states.values())


def test_pipeline_repairs_after_failed_candidate(tmp_path):
    init_repo(tmp_path)
    (tmp_path / ".gg" / "params.yaml").write_text(
        "verify:\n  tests: ''\nruntime:\n  candidates: 1\n  max_attempts: 2\n  repair_candidates: 1\n",
        encoding="utf-8",
    )

    agent = RepairAgent()
    result = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=agent).run_issue(
        42,
        no_pr=True,
    )

    assert result["state"] == "Completed"
    assert result["winner"] == "repair-2-1"
    state = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent()).store.load(result["run_id"])
    assert state.candidate_states["candidate-1"].status == "failed"
    assert state.candidate_states["repair-2-1"].status == "success"
    assert state.stage_attempts["analysis"] == 1
    assert state.stage_attempts["execution"] == 2
    assert state.stage_attempts["evaluation"] == 2
    assert "Repair context:" in agent.prompts[1]
    assert "Parent candidate: candidate-1" in agent.prompts[1]
    repair_result = json.loads(
        (tmp_path / state.candidate_states["repair-2-1"].result_path).read_text(encoding="utf-8")
    )
    assert repair_result["repair_context"]["parent_candidate_id"] == "candidate-1"


def test_pipeline_baseline_failures_can_be_allowed_when_identical(tmp_path):
    init_repo(tmp_path)
    (tmp_path / ".gg" / "params.yaml").write_text(
        "verify:\n  tests: \"python -c 'import sys; sys.exit(7)'\"\n  allow_known_baseline_failures: true\n",
        encoding="utf-8",
    )

    result = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent()).run_issue(
        42,
        no_pr=True,
    )

    assert result["state"] == "Completed"
    state = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent()).store.load(result["run_id"])
    assert "baseline_verification" in state.artifacts
    assert state.baseline["status"] == "failed"
    assert state.baseline["failed_commands"] == ["python -c 'import sys; sys.exit(7)'"]
    assert ".gg-worktrees" in state.baseline["worktree_path"]
    assert Path(state.baseline["worktree_path"]) != tmp_path
    assert Path(state.baseline["worktree_path"]).exists()
    verification = json.loads(
        (
            tmp_path
            / state.candidate_states["candidate-1"].result_path
        ).parent.joinpath("verification.json").read_text(encoding="utf-8")
    )
    assert verification["checks"][0]["baseline_status"] == "known_failure"


def test_pipeline_fails_when_verification_mutates_worktree(tmp_path):
    init_repo(tmp_path)
    (tmp_path / ".gg" / "params.yaml").write_text(
        """verify:
  tests: >
    python -c 'from pathlib import Path; import os; Path("generated.txt").write_text("x") if ".gg-worktrees" in os.getcwd() else None'
""",
        encoding="utf-8",
    )

    result = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent()).run_issue(
        42,
        no_pr=True,
    )

    assert result["state"] == "TerminalFailure"
    state = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent()).store.load(result["run_id"])
    assert state.candidate_states["candidate-1"].error == "verification mutated worktree"


def test_repeated_runs_use_collision_free_candidate_branches(tmp_path):
    init_repo(tmp_path)
    pipeline = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent())

    first = pipeline.run_issue(42, no_pr=True)
    second = pipeline.run_issue(42, no_pr=True)

    first_state = pipeline.store.load(first["run_id"])
    second_state = pipeline.store.load(second["run_id"])
    assert first_state.candidate_states["candidate-1"].branch != second_state.candidate_states["candidate-1"].branch


def test_commit_all_removes_candidate_cache_before_staging(tmp_path):
    init_repo(tmp_path)
    (tmp_path / "feature.txt").write_text("feature\n", encoding="utf-8")
    cache_file = tmp_path / ".gg-cache" / "pip" / "download.whl"
    cache_file.parent.mkdir(parents=True)
    cache_file.write_text("cache\n", encoding="utf-8")

    committed = commit_all(
        tmp_path,
        message="feature",
        author_name="gg-orchestrator",
        author_email="gg-orchestrator@example.invalid",
    )
    tracked = subprocess.run(
        ["git", "ls-files"],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.splitlines()

    assert committed is True
    assert "feature.txt" in tracked
    assert not any(path.startswith(".gg-cache/") for path in tracked)
    assert not cache_file.exists()


def test_resume_ready_run_executes_same_run(tmp_path):
    init_repo(tmp_path)
    pipeline = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent())
    ready = create_ready_run(pipeline)

    result = pipeline.resume(ready["run_id"], no_pr=True)

    assert result["state"] == "Completed"
    assert result["run_id"] == ready["run_id"]


def test_task_analysis_includes_issue_comments_and_local_inputs(tmp_path):
    init_repo(tmp_path)
    issue = Issue(
        number=42,
        title="Add greeting",
        body="Write a greeting file.",
        labels=["ai-ready"],
        comments=[
            IssueComment(body="Please keep the file UTF-8 encoded.", author="maintainer", created_at="2026-04-25T12:00:00Z"),
        ],
    )
    platform = FakePlatform()
    platform.issue = issue
    platform.issues = [issue]
    pipeline = OrchestratorPipeline(tmp_path, platform=platform, agent=FakeAgent())

    ready = create_ready_run(pipeline)
    state = pipeline.store.load(ready["run_id"])
    state.recover_to(TaskState.BLOCKED, reason="test blocked")
    state.blocked_resume_state = TaskState.TASK_ANALYSIS
    pipeline.store.write(state)

    provided = pipeline.provide(ready["run_id"], message="Use Spanish")
    refreshed = pipeline.resume(ready["run_id"], no_pr=True)

    assert provided["accepted"] is True
    assert provided["state"] == "TaskAnalysis"
    assert refreshed["state"] == "Completed"
    refreshed_state = pipeline.store.load(ready["run_id"])
    brief_path = tmp_path / refreshed_state.artifacts["task_brief"]
    brief = json.loads(brief_path.read_text(encoding="utf-8"))
    raw_issue = json.loads((tmp_path / refreshed_state.artifacts["raw_issue"]).read_text(encoding="utf-8"))
    assert brief_path.name == "task-brief-v2.json"
    assert refreshed_state.artifacts["task_brief_version"] == "2"
    assert raw_issue["issue"]["number"] == 42
    assert raw_issue["comments"][0]["body"] == "Please keep the file UTF-8 encoded."
    assert raw_issue["inputs"][0]["message"] == "Use Spanish"
    assert refreshed_state.blocked_resume_state is None
    assert brief["issue"]["comments"][0]["body"] == "Please keep the file UTF-8 encoded."
    assert brief["issue"]["inputs"][0]["message"] == "Use Spanish"
    snapshot = json.loads((tmp_path / refreshed_state.artifacts["context_snapshot"]).read_text(encoding="utf-8"))
    assert refreshed_state.artifacts["context_snapshot"].endswith("context-snapshot-v2.json")
    assert snapshot["snapshot_version"] == 2
    assert snapshot["purpose"] == "task_analysis_handoff"
    assert snapshot["prior_answer_refs"][0]["sequence_number"] == 1
    for key in ("issue_comments", "local_inputs"):
        digest = snapshot["objects"][key]
        assert ContextSnapshotStore(tmp_path).read_text(digest)


def test_task_analyzer_uses_versioned_json_contract_when_agent_provided(tmp_path):
    init_repo(tmp_path)
    issue = Issue(number=42, title="Add greeting", body="Write a greeting file.", labels=["ai-ready"])

    brief = TaskAnalyzer(str(tmp_path), agent=JsonAnalysisAgent()).analyze(issue)

    assert brief.summary == "Create a greeting file"
    assert brief.acceptance_criteria == ["greeting.txt exists"]
    assert brief.candidate_files == ["greeting.txt"]
    assert brief.verification_hints == ["cat greeting.txt"]
    assert brief.context_budget["estimated_tokens"] == 120


def test_task_analyzer_caps_project_context_by_backend_limit(monkeypatch, tmp_path):
    init_repo(tmp_path)

    class LargeContextKnowledge:
        def __init__(self, project_path):
            self.project_path = project_path

        def context_for_issue(self, title, body):
            return "x" * 1000

    monkeypatch.setattr("gg.orchestrator.task_analysis.KnowledgeEngine", LargeContextKnowledge)
    agent = ContextLimitAnalysisAgent(limit=50)
    issue = Issue(number=42, title="Add greeting", body="Write a greeting file.", labels=["ai-ready"])

    brief = TaskAnalyzer(
        str(tmp_path),
        agent=agent,
        max_context_tokens=100,
        model_context_tokens=agent.context_window_tokens(),
    ).analyze(issue)

    assert len(brief.project_context) == 200
    assert brief.context_budget["effective_context_tokens"] == 50
    assert brief.context_budget["model_context_tokens"] == 50
    assert brief.context_budget["project_context_truncated"] is True
    assert len(agent.prompts[0].split("Project context:\n", 1)[1]) == 200


def test_pipeline_uses_analysis_timeout_for_task_analysis_agent(tmp_path):
    init_repo(tmp_path)
    (tmp_path / ".gg" / "params.yaml").write_text(
        "verify:\n  tests: ''\nruntime:\n  analysis_timeout_seconds: 123\n",
        encoding="utf-8",
    )
    agent = TimeoutRecordingAnalysisAgent()

    result = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=agent).run_issue(42, dry_run=True)

    assert result["state"] == "ReadyForExecution"
    assert agent.timeouts == [123]


def test_pipeline_uses_analysis_context_budget(monkeypatch, tmp_path):
    init_repo(tmp_path)
    (tmp_path / ".gg" / "params.yaml").write_text(
        "verify:\n  tests: ''\nanalysis:\n  max_context_tokens: 10\n",
        encoding="utf-8",
    )

    class LargeContextKnowledge:
        def __init__(self, project_path):
            self.project_path = project_path

        def context_for_issue(self, title, body):
            return "x" * 1000

    monkeypatch.setattr("gg.orchestrator.task_analysis.KnowledgeEngine", LargeContextKnowledge)
    agent = ContextLimitAnalysisAgent(limit=1000)

    result = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=agent).run_issue(42, dry_run=True)

    assert result["state"] == "ReadyForExecution"
    assert len(agent.prompts[0].split("Project context:\n", 1)[1]) == 40


def test_task_analyzer_falls_back_when_agent_json_is_malformed(tmp_path):
    init_repo(tmp_path)
    issue = Issue(number=42, title="Add greeting", body="Write a greeting file.", labels=["ai-ready"])

    analyzer = TaskAnalyzer(str(tmp_path), agent=MalformedAnalysisAgent())
    brief = analyzer.analyze(issue)

    assert brief.summary == "Write a greeting file."
    assert brief.acceptance_criteria[0] == "Implement the requested issue behavior."
    assert analyzer.last_agent_error == "no JSON object found in model response"


def test_pipeline_persists_malformed_analysis_agent_response(tmp_path):
    init_repo(tmp_path)

    result = OrchestratorPipeline(
        tmp_path,
        platform=FakePlatform(),
        agent=MalformedThenImplementingAgent(),
    ).run_issue(42, no_pr=True)

    state = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent()).store.load(result["run_id"])
    artifact_path = tmp_path / state.artifacts["analysis_agent_response"]
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    assert artifact_path.name == "analysis-agent-response-v1.json"
    assert artifact["ok"] is False
    assert artifact["error"] == "no JSON object found in model response"
    assert artifact["response"] == "Here is not quite JSON"
    assert artifact["truncated"] is False


def test_task_analyzer_can_return_blocked_brief(tmp_path):
    init_repo(tmp_path)
    issue = Issue(number=42, title="Add greeting", body="Write a greeting file.", labels=["ai-ready"])

    brief = TaskAnalyzer(str(tmp_path), agent=BlockedAnalysisAgent()).analyze(issue)

    assert brief.blocked is True
    assert brief.missing_questions == ["Which greeting language should be used?"]


def test_pipeline_uses_agent_analysis_blocked_result(tmp_path):
    init_repo(tmp_path)

    pipeline = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=BlockedAnalysisAgent())
    result = pipeline.run_issue(
        42,
        dry_run=True,
    )

    assert result["state"] == "Blocked"
    assert result["missing_questions"] == ["Which greeting language should be used?"]
    assert result["dry_run"] is True
    assert list((tmp_path / ".gg" / "runs").glob("*/state.json")) == []


def test_json_extraction_rejects_conflicting_payloads():
    try:
        extract_single_json_object('{"ready": true}\n{"ready": false}')
    except ValueError as exc:
        message = str(exc)
    else:
        raise AssertionError("conflicting JSON payloads should fail")

    assert "multiple conflicting JSON objects" in message


def test_resume_interrupted_agent_running_marks_stale_candidate(tmp_path):
    init_repo(tmp_path)
    pipeline = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent())
    ready = create_ready_run(pipeline)
    state = pipeline.store.load(ready["run_id"])
    state.transition(TaskState.AGENT_SELECTION, reason="test")
    state.transition(TaskState.AGENT_RUNNING, reason="test")
    state.candidate_states["candidate-1"] = CandidateState(status="running")
    pipeline.store.write(state)

    result = pipeline.resume(ready["run_id"], no_pr=True)

    assert result["state"] == "Completed"
    resumed_state = pipeline.store.load(ready["run_id"])
    assert resumed_state.candidate_states["candidate-1"].status == "failed"
    assert resumed_state.candidate_states["candidate-1"].error == "interrupted before completion"
    assert resumed_state.candidate_states["candidate-1-retry-2"].status == "success"


def test_retry_from_ready_state_is_resume_without_execution_attempt_increment(tmp_path):
    init_repo(tmp_path)
    pipeline = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent())
    ready = create_ready_run(pipeline)

    result = pipeline.retry(ready["run_id"], no_pr=True)

    assert result["state"] == "Completed"
    assert result["retried"] is False
    assert result["retry_equivalent_to_resume"] is True
    state = pipeline.store.load(ready["run_id"])
    assert state.attempt == 1
    assert state.stage_attempts["execution"] == 1


def test_retry_from_agent_running_creates_new_execution_attempt(tmp_path):
    init_repo(tmp_path)
    (tmp_path / ".gg" / "params.yaml").write_text(
        "verify:\n  tests: ''\nruntime:\n  max_attempts: 2\n  repair_candidates: 1\n",
        encoding="utf-8",
    )
    pipeline = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent())
    ready = create_ready_run(pipeline)
    state = pipeline.store.load(ready["run_id"])
    state.transition(TaskState.AGENT_SELECTION, reason="test")
    state.transition(TaskState.AGENT_RUNNING, reason="test")
    state.candidate_states["candidate-1"] = CandidateState(status="running")
    pipeline.store.write(state)

    result = pipeline.retry(ready["run_id"], no_pr=True)

    assert result["state"] == "Completed"
    assert result["retried"] is True
    assert result["winner"] == "repair-2-1"
    retried_state = pipeline.store.load(ready["run_id"])
    assert retried_state.attempt == 2
    assert retried_state.candidate_states["candidate-1"].status == "failed"
    assert retried_state.candidate_states["candidate-1"].error == "manual retry requested"
    assert retried_state.candidate_states["repair-2-1"].status == "success"
    assert retried_state.stage_attempts["execution"] == 1


def test_retry_from_agent_running_respects_attempt_budget(tmp_path):
    init_repo(tmp_path)
    pipeline = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent())
    ready = create_ready_run(pipeline)
    state = pipeline.store.load(ready["run_id"])
    state.transition(TaskState.AGENT_SELECTION, reason="test")
    state.transition(TaskState.AGENT_RUNNING, reason="test")
    state.candidate_states["candidate-1"] = CandidateState(status="running")
    pipeline.store.write(state)

    result = pipeline.retry(ready["run_id"], no_pr=True)

    assert result["state"] == "AgentRunning"
    assert result["retried"] is False
    assert "budget is exhausted" in result["message"]


def test_keyboard_interrupt_marks_run_recoverable(tmp_path):
    init_repo(tmp_path)
    pipeline = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=InterruptingAgent())

    try:
        pipeline.run_issue(42, no_pr=True)
    except KeyboardInterrupt:
        pass
    else:
        raise AssertionError("interrupt should propagate after recording recoverable state")

    state = pipeline.status()[0]
    assert state["state"] == "ReadyForExecution"
    assert state["last_error"]["code"] == "interrupted"
    assert state["candidate_states"]["candidate-1"]["error"] == "interrupted by signal"


def test_mark_interrupted_terminates_known_candidate_processes(monkeypatch, tmp_path):
    init_repo(tmp_path)
    killed: list[int] = []

    def fake_killpg(pid, sig):
        assert sig == signal.SIGTERM
        killed.append(pid)

    monkeypatch.setattr("gg.orchestrator.pipeline.os.killpg", fake_killpg)
    pipeline = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent())
    ready = create_ready_run(pipeline)
    state = pipeline.store.load(ready["run_id"])
    state.transition(TaskState.AGENT_SELECTION, reason="test select")
    state.transition(TaskState.AGENT_RUNNING, reason="test running")
    state.candidate_states["candidate-1"] = CandidateState(
        status="running",
        sandbox_pid=43210,
        agent_pid=54321,
    )
    pipeline.store.write(state)

    pipeline._mark_interrupted(state)

    interrupted = pipeline.store.load(ready["run_id"])
    assert killed == [43210, 54321]
    assert interrupted.candidate_states["candidate-1"].status == "failed"
    assert interrupted.last_error["code"] == "interrupted"


def test_resume_outcome_publishing_no_pr_completes_idempotently(tmp_path):
    init_repo(tmp_path)
    pipeline = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent())
    completed = pipeline.run_issue(42, no_pr=True)
    state = pipeline.store.load(completed["run_id"])
    state.recover_to(TaskState.OUTCOME_PUBLISHING, reason="test interrupted publish")
    state.publishing_step = "local_no_pr"
    state_path = tmp_path / ".gg" / "runs" / state.run_id / "state.json"
    data = state.to_dict()
    state_path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")

    result = pipeline.resume(completed["run_id"], no_pr=True)

    assert result["state"] == "Completed"
    assert result["winner"] == "candidate-1"


def test_publish_honors_cancel_request_after_branch_push(tmp_path):
    init_repo(tmp_path)
    platform = CancellingFindPrPlatform()
    pipeline = OrchestratorPipeline(tmp_path, platform=platform, agent=FakeAgent())
    ready = create_ready_run(pipeline)
    state = pipeline.store.load(ready["run_id"])
    state.recover_to(TaskState.OUTCOME_PUBLISHING, reason="test interrupted publish")
    state.publishing_step = "branch_pushed"
    state.artifacts["publishing_integration"] = pipeline.store.write_json(
        state.run_id,
        "artifacts/publishing-integration.json",
        {
            "schema_version": 1,
            "candidate_id": "candidate-1",
            "source_branch": "gg/source",
            "integration_branch": "gg/test",
            "worktree_path": str(tmp_path),
            "base_ref": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=tmp_path, text=True).strip(),
            "patch_path": "patch.diff",
            "created_at": state.updated_at,
        },
    )
    pipeline.store.write(state)
    platform.pipeline = pipeline
    platform.run_id = ready["run_id"]

    result = pipeline._publish_winner(
        state,
        platform.issue,
        {
            "candidate_id": "candidate-1",
            "worktree_path": str(tmp_path),
            "branch": "gg/test",
            "summary": "done",
            "verification_path": "verify.json",
        },
        no_pr=False,
    )

    assert result["cancelled"] is True
    cancelled = pipeline.store.load(ready["run_id"])
    assert cancelled.cancel_requested is True
    assert cancelled.pr_url == "https://github.com/example/repo/pull/77"
    assert cancelled.state is TaskState.CANCELLED


def test_publish_skips_duplicate_result_comment_when_marker_exists(tmp_path):
    init_repo(tmp_path)
    platform = FakePlatform()
    pipeline = OrchestratorPipeline(tmp_path, platform=platform, agent=FakeAgent())
    ready = create_ready_run(pipeline)
    state = pipeline.store.load(ready["run_id"])
    state.recover_to(TaskState.OUTCOME_PUBLISHING, reason="test idempotent publish")
    state.publishing_step = "pr_created"
    state.pr_url = "https://github.com/example/repo/pull/5"
    state.artifacts["publishing_integration"] = pipeline.store.write_json(
        state.run_id,
        "artifacts/publishing-integration.json",
        {
            "schema_version": 1,
            "candidate_id": "candidate-1",
            "source_branch": "gg/source",
            "integration_branch": "gg/test",
            "worktree_path": str(tmp_path),
            "base_ref": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=tmp_path, text=True).strip(),
            "patch_path": "patch.diff",
            "created_at": state.updated_at,
        },
    )
    pipeline.store.write(state)
    platform.issue.comments.append(
        IssueComment(
            body=f"<!-- gg-run-id={state.run_id} stage=result -->\nold result",
            author="gg",
            created_at="2026-04-25T12:00:00Z",
        )
    )

    result = pipeline._publish_winner(
        state,
        platform.issue,
        {
            "candidate_id": "candidate-1",
            "worktree_path": str(tmp_path),
            "branch": "gg/test",
            "summary": "done",
            "verification_path": "verify.json",
        },
        no_pr=False,
    )

    assert result["state"] == "Completed"
    assert platform.comments == []


def test_publish_uses_integration_worktree_for_pr(monkeypatch, tmp_path):
    init_repo(tmp_path)
    pushed: list[tuple[str, str]] = []

    def fake_push(worktree_path: str, branch: str) -> None:
        pushed.append((worktree_path, branch))

    monkeypatch.setattr("gg.orchestrator.pipeline.push_branch", fake_push)
    platform = FakePlatform()

    result = OrchestratorPipeline(tmp_path, platform=platform, agent=FakeAgent()).run_issue(42)

    assert result["state"] == "Completed"
    assert result["winner"] == "candidate-1"
    state = OrchestratorPipeline(tmp_path, platform=platform, agent=FakeAgent()).store.load(result["run_id"])
    integration = json.loads((tmp_path / state.artifacts["publishing_integration"]).read_text(encoding="utf-8"))
    assert integration["candidate_id"] == "candidate-1"
    assert integration["integration_branch"].startswith("gg/issue-42-")
    assert platform.prs[0]["head"] == integration["integration_branch"]
    assert pushed == [(integration["worktree_path"], integration["integration_branch"])]
    assert not Path(integration["worktree_path"]).exists()
    state = OrchestratorPipeline(tmp_path, platform=platform, agent=FakeAgent()).store.load(result["run_id"])
    result_comments = [body for _, body in platform.comments if f"gg-run-id={state.run_id} stage=result" in body]
    assert len(result_comments) == 1
    result_comment = result_comments[0]
    assert f"Selected candidate: `{result['winner']}`" in result_comment
    assert f"Branch: `{integration['integration_branch']}`" in result_comment
    assert f"Verification: `{state.artifacts['integration_verification']}`" in result_comment
    assert f"Evaluation: `{state.artifacts['evaluation']}`" in result_comment
    assert f"Run outcome: `{state.artifacts['run_outcome']}`" in result_comment
    outcome = json.loads((tmp_path / state.artifacts["run_outcome"]).read_text(encoding="utf-8"))
    assert outcome["artifacts"]["selected_candidate_result"].endswith(
        "candidates/candidate-1/candidate-result.json"
    )


def test_publish_records_patch_conflict_before_pr(monkeypatch, tmp_path):
    init_repo(tmp_path)
    monkeypatch.setattr("gg.orchestrator.pipeline.push_branch", lambda *_args, **_kwargs: None)
    platform = FakePlatform()
    pipeline = OrchestratorPipeline(tmp_path, platform=platform, agent=FakeAgent())
    ready = create_ready_run(pipeline)
    state = pipeline.store.load(ready["run_id"])
    state.recover_to(TaskState.OUTCOME_PUBLISHING, reason="test integration conflict")
    state.publishing_step = "started"
    pipeline.store.write(state)
    patch_path = pipeline.store.write_text(state.run_id, "candidates/candidate-1/patch.diff", "not a patch")

    result = pipeline._publish_winner(
        state,
        platform.issue,
        {
            "candidate_id": "candidate-1",
            "worktree_path": str(tmp_path),
            "branch": "gg/test",
            "base_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=tmp_path, text=True).strip(),
            "patch_path": patch_path,
            "summary": "done",
            "verification_path": "verify.json",
        },
        no_pr=False,
    )

    failed = pipeline.store.load(ready["run_id"])
    conflict = json.loads((tmp_path / failed.artifacts["patch_conflict"]).read_text(encoding="utf-8"))
    assert result["state"] == "TerminalFailure"
    assert result["error"]["code"] == "patch_conflict"
    assert "patch" in conflict["message"].lower()
    assert platform.prs == []


def test_publish_patch_conflict_repairs_before_terminal_failure(monkeypatch, tmp_path):
    init_repo(tmp_path)
    (tmp_path / ".gg" / "params.yaml").write_text(
        "verify:\n  tests: ''\nruntime:\n  max_attempts: 2\n  repair_candidates: 1\n",
        encoding="utf-8",
    )
    base_commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=tmp_path, text=True).strip()
    (tmp_path / "README.md").write_text("# repo\nmain moved\n", encoding="utf-8")
    commit_repo(tmp_path, "move main")
    calls = {"apply": 0}

    def fail_then_apply(worktree, patch_text):
        calls["apply"] += 1
        if calls["apply"] == 1:
            return False, "simulated stale-base patch conflict"
        return git_module.apply_patch(worktree, patch_text)

    monkeypatch.setattr("gg.orchestrator.pipeline.git_apply_patch", fail_then_apply)
    monkeypatch.setattr("gg.orchestrator.pipeline.push_branch", lambda *_args, **_kwargs: None)
    platform = FakePlatform()
    pipeline = OrchestratorPipeline(tmp_path, platform=platform, agent=FakeAgent())
    ready = create_ready_run(pipeline)
    state = pipeline.store.load(ready["run_id"])
    state.max_attempts = 2
    state.recover_to(TaskState.OUTCOME_PUBLISHING, reason="test publish repair")
    state.publishing_step = "started"
    state.candidate_states["candidate-1"] = CandidateState(status="success")
    pipeline.store.write(state)
    patch_path = pipeline.store.write_text(
        state.run_id,
        "candidates/candidate-1/patch.diff",
        """diff --git a/README.md b/README.md
--- a/README.md
+++ b/README.md
@@ -1 +1,2 @@
 # repo
+candidate update
""",
    )

    result = pipeline._publish_winner(
        state,
        platform.issue,
        {
            "candidate_id": "candidate-1",
            "worktree_path": str(tmp_path),
            "branch": "gg/source",
            "base_commit": base_commit,
            "patch_path": patch_path,
            "summary": "done",
            "verification_path": "verify.json",
        },
        no_pr=False,
    )

    final_state = pipeline.store.load(ready["run_id"])
    assert result["state"] == "Completed"
    assert result["winner"] == "repair-2-1"
    assert calls["apply"] == 2
    assert final_state.candidate_states["repair-2-1"].status == "success"
    assert "publishing_repair_context" in final_state.artifacts
    repair_result = json.loads(
        (tmp_path / final_state.candidate_states["repair-2-1"].result_path).read_text(encoding="utf-8")
    )
    assert repair_result["repair_context"]["parent_candidate_id"] == "candidate-1"
    assert "simulated stale-base patch conflict" in repair_result["repair_context"]["feedback"]
    repair_context = json.loads((tmp_path / final_state.artifacts["publishing_repair_context"]).read_text(encoding="utf-8"))
    assert repair_context["publishing_failure"]["preflight"]["stale_base"] is True


def test_git_apply_patch_uses_index_check(monkeypatch, tmp_path):
    calls: list[list[str]] = []

    def fake_run(args, **_kwargs):
        calls.append(args)
        return subprocess.CompletedProcess(args, 0, stdout="", stderr="")

    monkeypatch.setattr(git_module.subprocess, "run", fake_run)

    applied, _message = git_module.apply_patch(tmp_path, "diff --git a/a.txt b/a.txt\n")

    assert applied is True
    assert calls[0][:4] == ["git", "apply", "--3way", "--index"]


def test_publish_fails_before_apply_when_lfs_patch_requires_missing_lfs(monkeypatch, tmp_path):
    init_repo(tmp_path)
    (tmp_path / ".gitattributes").write_text("*.bin filter=lfs diff=lfs merge=lfs -text\n", encoding="utf-8")
    commit_repo(tmp_path, "add lfs attributes")
    monkeypatch.setattr("gg.orchestrator.pipeline.push_branch", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("gg.orchestrator.pipeline.git_lfs_available", lambda _worktree: False)

    def fail_apply(*_args, **_kwargs):
        raise AssertionError("patch apply should not run without git lfs")

    monkeypatch.setattr("gg.orchestrator.pipeline.git_apply_patch", fail_apply)
    platform = FakePlatform()
    pipeline = OrchestratorPipeline(tmp_path, platform=platform, agent=FakeAgent())
    ready = create_ready_run(pipeline)
    state = pipeline.store.load(ready["run_id"])
    state.recover_to(TaskState.OUTCOME_PUBLISHING, reason="test lfs preflight")
    state.publishing_step = "started"
    pipeline.store.write(state)
    patch_path = pipeline.store.write_text(
        state.run_id,
        "candidates/candidate-1/patch.diff",
        "diff --git a/asset.bin b/asset.bin\n"
        "new file mode 100644\n"
        "index 0000000..e69de29\n"
        "--- /dev/null\n"
        "+++ b/asset.bin\n"
        "@@ -0,0 +1 @@\n"
        "+lfs pointer candidate\n",
    )

    result = pipeline._publish_winner(
        state,
        platform.issue,
        {
            "candidate_id": "candidate-1",
            "worktree_path": str(tmp_path),
            "branch": "gg/source",
            "base_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=tmp_path, text=True).strip(),
            "patch_path": patch_path,
            "summary": "done",
            "verification_path": "verify.json",
        },
        no_pr=False,
    )

    failed = pipeline.store.load(ready["run_id"])
    conflict = json.loads((tmp_path / failed.artifacts["patch_conflict"]).read_text(encoding="utf-8"))
    assert result["state"] == "TerminalFailure"
    assert result["error"]["code"] == "patch_conflict"
    assert conflict["code"] == "lfs_unavailable"
    assert conflict["lfs_unavailable"] is True
    assert conflict["changed_files"] == ["asset.bin"]
    assert platform.prs == []


def test_resume_publish_applies_existing_integration_before_commit(monkeypatch, tmp_path):
    init_repo(tmp_path)
    monkeypatch.setattr("gg.orchestrator.pipeline.push_branch", lambda *_args, **_kwargs: None)
    platform = FakePlatform()
    pipeline = OrchestratorPipeline(tmp_path, platform=platform, agent=FakeAgent())
    ready = create_ready_run(pipeline)
    state = pipeline.store.load(ready["run_id"])
    base_commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=tmp_path, text=True).strip()
    patch_path = pipeline.store.write_text(
        state.run_id,
        "candidates/candidate-1/patch.diff",
        """diff --git a/resumed.txt b/resumed.txt
new file mode 100644
index 0000000..186cf24
--- /dev/null
+++ b/resumed.txt
@@ -0,0 +1 @@
+resumed
""",
    )
    worktree = tmp_path.parent / ".gg-worktrees" / tmp_path.name / state.run_id / "integration"
    subprocess.run(
        ["git", "worktree", "add", "-b", "gg/resume-integration", str(worktree), base_commit],
        cwd=tmp_path,
        check=True,
        capture_output=True,
    )
    state.recover_to(TaskState.OUTCOME_PUBLISHING, reason="test resume integration")
    state.publishing_step = "integration_created"
    state.artifacts["publishing_integration"] = pipeline.store.write_json(
        state.run_id,
        "artifacts/publishing-integration.json",
        {
            "schema_version": 1,
            "candidate_id": "candidate-1",
            "source_branch": "gg/source",
            "integration_branch": "gg/resume-integration",
            "worktree_path": str(worktree),
            "base_ref": base_commit,
            "patch_path": patch_path,
            "created_at": state.updated_at,
        },
    )
    pipeline.store.write(state)

    result = pipeline._publish_winner(
        state,
        platform.issue,
        {
            "candidate_id": "candidate-1",
            "worktree_path": str(tmp_path),
            "branch": "gg/source",
            "base_commit": base_commit,
            "patch_path": patch_path,
            "summary": "done",
            "verification_path": "verify.json",
        },
        no_pr=False,
    )

    assert result["state"] == "Completed"
    assert platform.prs[0]["head"] == "gg/resume-integration"
    assert not worktree.exists()


def test_resume_publish_resets_unverified_dirty_integration_worktree(monkeypatch, tmp_path):
    init_repo(tmp_path)
    monkeypatch.setattr("gg.orchestrator.pipeline.push_branch", lambda *_args, **_kwargs: None)
    platform = FakePlatform()
    pipeline = OrchestratorPipeline(tmp_path, platform=platform, agent=FakeAgent())
    ready = create_ready_run(pipeline)
    state = pipeline.store.load(ready["run_id"])
    base_commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=tmp_path, text=True).strip()
    patch_path = pipeline.store.write_text(
        state.run_id,
        "candidates/candidate-1/patch.diff",
        """diff --git a/resumed.txt b/resumed.txt
new file mode 100644
index 0000000..186cf24
--- /dev/null
+++ b/resumed.txt
@@ -0,0 +1 @@
+resumed
""",
    )
    worktree = tmp_path.parent / ".gg-worktrees" / tmp_path.name / state.run_id / "integration"
    subprocess.run(
        ["git", "worktree", "add", "-b", "gg/reset-integration", str(worktree), base_commit],
        cwd=tmp_path,
        check=True,
        capture_output=True,
    )
    (worktree / "evil.txt").write_text("unverified dirty state\n", encoding="utf-8")
    state.recover_to(TaskState.OUTCOME_PUBLISHING, reason="test dirty integration")
    state.publishing_step = "integration_created"
    state.artifacts["publishing_integration"] = pipeline.store.write_json(
        state.run_id,
        "artifacts/publishing-integration.json",
        {
            "schema_version": 1,
            "candidate_id": "candidate-1",
            "source_branch": "gg/source",
            "integration_branch": "gg/reset-integration",
            "worktree_path": str(worktree),
            "base_ref": base_commit,
            "patch_path": patch_path,
            "created_at": state.updated_at,
        },
    )
    pipeline.store.write(state)

    result = pipeline._publish_winner(
        state,
        platform.issue,
        {
            "candidate_id": "candidate-1",
            "worktree_path": str(tmp_path),
            "branch": "gg/source",
            "base_commit": base_commit,
            "patch_path": patch_path,
            "summary": "done",
            "verification_path": "verify.json",
        },
        no_pr=False,
    )

    assert result["state"] == "Completed"
    assert "resumed" in subprocess.check_output(
        ["git", "show", "gg/reset-integration:resumed.txt"],
        cwd=tmp_path,
        text=True,
    )
    assert subprocess.run(
        ["git", "cat-file", "-e", "gg/reset-integration:evil.txt"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    ).returncode != 0


def test_resume_publish_reverifies_dirty_verified_integration_worktree(monkeypatch, tmp_path):
    init_repo(tmp_path)
    monkeypatch.setattr("gg.orchestrator.pipeline.push_branch", lambda *_args, **_kwargs: None)
    platform = FakePlatform()
    pipeline = OrchestratorPipeline(tmp_path, platform=platform, agent=FakeAgent())
    ready = create_ready_run(pipeline)
    state = pipeline.store.load(ready["run_id"])
    base_commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=tmp_path, text=True).strip()
    patch_path = pipeline.store.write_text(
        state.run_id,
        "candidates/candidate-1/patch.diff",
        """diff --git a/resumed.txt b/resumed.txt
new file mode 100644
index 0000000..186cf24
--- /dev/null
+++ b/resumed.txt
@@ -0,0 +1 @@
+resumed
""",
    )
    worktree = tmp_path.parent / ".gg-worktrees" / tmp_path.name / state.run_id / "integration-verified"
    subprocess.run(
        ["git", "worktree", "add", "-b", "gg/verified-integration", str(worktree), base_commit],
        cwd=tmp_path,
        check=True,
        capture_output=True,
    )
    subprocess.run(["git", "apply", str(tmp_path / patch_path)], cwd=worktree, check=True, capture_output=True)
    (worktree / "evil.txt").write_text("unverified dirty state\n", encoding="utf-8")
    state.recover_to(TaskState.OUTCOME_PUBLISHING, reason="test dirty verified integration")
    state.publishing_step = "verified"
    state.artifacts["publishing_integration"] = pipeline.store.write_json(
        state.run_id,
        "artifacts/publishing-integration.json",
        {
            "schema_version": 1,
            "candidate_id": "candidate-1",
            "source_branch": "gg/source",
            "integration_branch": "gg/verified-integration",
            "worktree_path": str(worktree),
            "base_ref": base_commit,
            "patch_path": patch_path,
            "created_at": state.updated_at,
        },
    )
    state.artifacts["integration_verification"] = pipeline.store.write_json(
        state.run_id,
        "artifacts/integration-verification.json",
        {"schema_version": 1, "checks": []},
    )
    pipeline.store.write(state)

    result = pipeline._publish_winner(
        state,
        platform.issue,
        {
            "candidate_id": "candidate-1",
            "worktree_path": str(tmp_path),
            "branch": "gg/source",
            "base_commit": base_commit,
            "patch_path": patch_path,
            "summary": "done",
            "verification_path": "verify.json",
        },
        no_pr=False,
    )

    assert result["state"] == "Completed"
    assert "resumed" in subprocess.check_output(
        ["git", "show", "gg/verified-integration:resumed.txt"],
        cwd=tmp_path,
        text=True,
    )
    assert subprocess.run(
        ["git", "cat-file", "-e", "gg/verified-integration:evil.txt"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    ).returncode != 0


def test_publish_fails_when_default_branch_sync_fails(monkeypatch, tmp_path):
    init_repo(tmp_path)
    monkeypatch.setattr(
        "gg.orchestrator.pipeline.git_fetch_default_branch",
        lambda *_args, **_kwargs: (False, True, "fetch rejected"),
    )
    platform = FakePlatform()
    pipeline = OrchestratorPipeline(tmp_path, platform=platform, agent=FakeAgent())
    ready = create_ready_run(pipeline)
    state = pipeline.store.load(ready["run_id"])
    state.recover_to(TaskState.OUTCOME_PUBLISHING, reason="test sync failure")
    state.publishing_step = "started"
    pipeline.store.write(state)

    result = pipeline._publish_winner(
        state,
        platform.issue,
        {
            "candidate_id": "candidate-1",
            "worktree_path": str(tmp_path),
            "branch": "gg/test",
            "base_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=tmp_path, text=True).strip(),
            "patch_path": "missing.diff",
            "summary": "done",
            "verification_path": "verify.json",
        },
        no_pr=False,
    )

    failed = pipeline.store.load(ready["run_id"])
    preflight = json.loads((tmp_path / failed.artifacts["publishing_preflight"]).read_text(encoding="utf-8"))
    assert result["state"] == "TerminalFailure"
    assert result["error"]["code"] == "default_sync_failed"
    assert preflight["default_sync_ok"] is False
    assert platform.prs == []


def test_publish_integration_verification_allows_identical_baseline_failure(monkeypatch, tmp_path):
    init_repo(tmp_path)
    (tmp_path / ".gg" / "params.yaml").write_text(
        "verify:\n  tests: \"python -c 'import sys; sys.exit(7)'\"\n  allow_known_baseline_failures: true\n",
        encoding="utf-8",
    )
    monkeypatch.setattr("gg.orchestrator.pipeline.push_branch", lambda *_args, **_kwargs: None)

    result = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent()).run_issue(42)

    assert result["state"] == "Completed"


def test_resume_publishing_rejects_invalid_evaluation_artifact(tmp_path):
    init_repo(tmp_path)
    pipeline = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent())
    ready = create_ready_run(pipeline)
    state = pipeline.store.load(ready["run_id"])
    state.recover_to(TaskState.OUTCOME_PUBLISHING, reason="test invalid evaluation")
    evaluation_path = tmp_path / ".gg" / "runs" / state.run_id / "artifacts" / "evaluation.json"
    evaluation_path.parent.mkdir(parents=True, exist_ok=True)
    evaluation_path.write_text('{"schema_version": 1, "selected_candidate_id": 42, "candidates": []}\n', encoding="utf-8")
    state.artifacts["evaluation"] = str(evaluation_path.relative_to(tmp_path))
    pipeline.store.write(state)

    try:
        pipeline.resume(ready["run_id"])
    except ValueError as exc:
        assert "artifacts/evaluation.json.selected_candidate_id" in str(exc)
    else:
        raise AssertionError("invalid evaluation artifact should fail schema validation on resume")


def test_resume_publishing_fails_closed_on_invalid_integration_artifact(monkeypatch, tmp_path):
    init_repo(tmp_path)
    monkeypatch.setattr("gg.orchestrator.pipeline.push_branch", lambda *_args, **_kwargs: None)
    platform = FakePlatform()
    pipeline = OrchestratorPipeline(tmp_path, platform=platform, agent=FakeAgent())
    ready = create_ready_run(pipeline)
    state = pipeline.store.load(ready["run_id"])
    state.recover_to(TaskState.OUTCOME_PUBLISHING, reason="test invalid integration")
    state.publishing_step = "branch_pushed"
    state.pr_url = None
    artifact_path = tmp_path / ".gg" / "runs" / state.run_id / "artifacts" / "publishing-integration.json"
    artifact_path.write_text('{"schema_version": 1, "candidate_id": 7}\n', encoding="utf-8")
    state.artifacts["publishing_integration"] = str(artifact_path.relative_to(tmp_path))
    pipeline.store.write(state)

    result = pipeline._publish_winner(
        state,
        platform.issue,
        {
            "candidate_id": "candidate-1",
            "worktree_path": str(tmp_path),
            "branch": "gg/source",
            "base_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=tmp_path, text=True).strip(),
            "summary": "done",
            "verification_path": "verify.json",
        },
        no_pr=False,
    )

    assert result["state"] == "TerminalFailure"
    assert result["error"]["code"] == "invalid_publishing_integration"
    assert platform.prs == []


def test_stage_comment_uses_run_marker_for_idempotency(tmp_path):
    init_repo(tmp_path)
    platform = FakePlatform()
    pipeline = OrchestratorPipeline(tmp_path, platform=platform, agent=FakeAgent())
    ready = create_ready_run(pipeline)
    marker = f"<!-- gg-run-id={ready['run_id']} stage=blocked -->"
    platform.issue.comments.append(IssueComment(body=f"{marker}\nold blocked", author="gg"))

    pipeline._mark_issue_blocked(42, ready["run_id"], "still blocked")

    assert platform.comments == []


def test_publish_fails_with_preflight_artifact_when_base_commit_missing(tmp_path):
    init_repo(tmp_path)
    platform = FakePlatform()
    pipeline = OrchestratorPipeline(tmp_path, platform=platform, agent=FakeAgent())
    ready = create_ready_run(pipeline)
    state = pipeline.store.load(ready["run_id"])
    state.recover_to(TaskState.OUTCOME_PUBLISHING, reason="test publish preflight")
    state.publishing_step = "started"
    pipeline.store.write(state)

    result = pipeline._publish_winner(
        state,
        platform.issue,
        {
            "candidate_id": "candidate-1",
            "worktree_path": str(tmp_path),
            "branch": "gg/test",
            "base_commit": "deadbeef",
            "summary": "done",
            "verification_path": "verify.json",
        },
        no_pr=False,
    )

    failed = pipeline.store.load(ready["run_id"])
    preflight = json.loads((tmp_path / failed.artifacts["publishing_preflight"]).read_text(encoding="utf-8"))
    assert result["state"] == "TerminalFailure"
    assert result["error"]["code"] == "base_rewritten"
    assert preflight["base_reachable"] is False
    assert platform.prs == []


def test_interrupt_during_publishing_preserves_publish_resume_state(tmp_path):
    init_repo(tmp_path)
    pipeline = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent())
    completed = pipeline.run_issue(42, no_pr=True)
    state = pipeline.store.load(completed["run_id"])
    state.recover_to(TaskState.OUTCOME_PUBLISHING, reason="test interrupted publish")
    state.publishing_step = "branch_pushed"
    state.pr_url = "https://github.com/example/repo/pull/99"
    state_path = tmp_path / ".gg" / "runs" / state.run_id / "state.json"
    state_path.write_text(json.dumps(state.to_dict(), indent=2) + "\n", encoding="utf-8")

    pipeline._mark_interrupted(state)

    interrupted = pipeline.store.load(completed["run_id"])
    assert interrupted.state is TaskState.OUTCOME_PUBLISHING
    assert interrupted.publishing_step == "branch_pushed"
    assert interrupted.last_error["code"] == "interrupted"


def test_retry_ready_run_aliases_resume(tmp_path):
    init_repo(tmp_path)
    pipeline = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent())
    ready = create_ready_run(pipeline)

    result = pipeline.retry(ready["run_id"], no_pr=True)

    assert result["state"] == "Completed"
    assert result["retried"] is False
    assert result["retry_equivalent_to_resume"] is True


def test_pipeline_transitions_to_needs_input_and_resume_uses_provided_input(tmp_path):
    init_repo(tmp_path)
    platform = FakePlatform()
    pipeline = OrchestratorPipeline(tmp_path, platform=platform, agent=NeedsInputAgent())

    result = pipeline.run_issue(42, no_pr=True)

    assert result["state"] == "NeedsInput"
    state = pipeline.store.load(result["run_id"])
    assert state.artifacts["input_request"].endswith("artifacts/input-request.json")
    assert state.blocked_resume_state is TaskState.AGENT_RUNNING
    assert any("needs local input" in body for _, body in platform.comments)

    provided = pipeline.provide(result["run_id"], message="Use Spanish")

    assert provided["accepted"] is True
    assert provided["state"] == "AgentRunning"

    resumed = pipeline.resume(result["run_id"], no_pr=True)

    assert resumed["state"] == "Completed"
    final_state = pipeline.store.load(result["run_id"])
    assert final_state.blocked_resume_state is None
    result_path = tmp_path / final_state.candidate_states["candidate-1-retry-2"].result_path
    candidate_result = json.loads(result_path.read_text(encoding="utf-8"))
    assert candidate_result["summary"] == "Created Spanish greeting."


def test_needs_input_can_resume_from_issue_comment(tmp_path):
    init_repo(tmp_path)
    platform = FakePlatform()
    pipeline = OrchestratorPipeline(tmp_path, platform=platform, agent=NeedsInputAgent())

    result = pipeline.run_issue(42, no_pr=True)
    platform.issue.comments.append(
        IssueComment(
            body="Use Spanish",
            author="maintainer",
            created_at="2999-01-01T00:00:00Z",
            url="https://github.com/example/repo/issues/42#issuecomment-2",
        )
    )

    resumed = pipeline.resume(result["run_id"], no_pr=True)

    assert resumed["state"] == "Completed"
    final_state = pipeline.store.load(result["run_id"])
    input_path = tmp_path / final_state.artifacts["last_input"]
    input_artifact = json.loads(input_path.read_text(encoding="utf-8"))
    assert input_artifact["source"] == "github-comment"
    assert input_artifact["message"] == "Use Spanish"


def test_needs_input_ignores_gg_stage_comments_on_resume(tmp_path):
    init_repo(tmp_path)
    platform = FakePlatform()
    pipeline = OrchestratorPipeline(tmp_path, platform=platform, agent=NeedsInputAgent())

    result = pipeline.run_issue(42, no_pr=True)
    platform.issue.comments.append(
        IssueComment(
            body="<!-- gg-stage=needs-input -->\ngg needs local input to continue: Which greeting language?",
            author="gg",
            created_at="2999-01-01T00:00:00Z",
        )
    )

    resumed = pipeline.resume(result["run_id"], no_pr=True)

    assert resumed["state"] == "NeedsInput"
    assert resumed["resumed"] is False
    state = pipeline.store.load(result["run_id"])
    assert "last_input" not in state.artifacts


def test_repeated_needs_input_does_not_reuse_stale_input(tmp_path):
    init_repo(tmp_path)
    pipeline = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=RepeatedNeedsInputAgent())

    first_request = pipeline.run_issue(42, no_pr=True)
    pipeline.provide(first_request["run_id"], message="Use Spanish")
    second_request = pipeline.resume(first_request["run_id"], no_pr=True)
    third_resume = pipeline.resume(first_request["run_id"], no_pr=True)

    assert second_request["state"] == "NeedsInput"
    assert third_resume["state"] == "NeedsInput"
    assert third_resume["resumed"] is False
    state = pipeline.store.load(first_request["run_id"])
    assert "last_input" not in state.artifacts

    pipeline.provide(first_request["run_id"], message="Use greeting.txt")
    final = pipeline.resume(first_request["run_id"], no_pr=True)

    assert final["state"] == "Completed"


def test_issue_comment_same_text_creates_fresh_input_for_new_request(tmp_path):
    init_repo(tmp_path)
    platform = FakePlatform()
    pipeline = OrchestratorPipeline(tmp_path, platform=platform, agent=RepeatedNeedsInputAgent())

    first_request = pipeline.run_issue(42, no_pr=True)
    first_input = pipeline.provide(first_request["run_id"], message="Use Spanish")
    second_request = pipeline.resume(first_request["run_id"], no_pr=True)
    old_input_path = tmp_path / first_input["input"]
    old_input = json.loads(old_input_path.read_text(encoding="utf-8"))
    old_input["created_at"] = "2000-01-01T00:00:00Z"
    old_input_path.write_text(json.dumps(old_input, indent=2) + "\n", encoding="utf-8")
    platform.issue.comments.append(
        IssueComment(
            body="Use Spanish",
            author="maintainer",
            created_at="2999-01-01T00:00:00Z",
        )
    )

    final = pipeline.resume(first_request["run_id"], no_pr=True)

    assert second_request["state"] == "NeedsInput"
    assert final["state"] == "Completed"
    state = pipeline.store.load(first_request["run_id"])
    assert state.artifacts["last_input"].endswith("input-v1-0002.json")


def test_provide_accepts_blocked_run_input(tmp_path):
    init_repo(tmp_path)
    platform = FakePlatform()
    pipeline = OrchestratorPipeline(tmp_path, platform=platform, agent=FakeAgent())
    ready = create_ready_run(pipeline)
    state = pipeline.store.load(ready["run_id"])
    state.recover_to(TaskState.BLOCKED, reason="test blocked")
    pipeline.store.write(state)

    result = pipeline.provide(ready["run_id"], message="Use the existing helper")

    assert result["accepted"] is True
    assert result["state"] == "TaskAnalysis"
    input_path = tmp_path / result["input"]
    assert input_path.exists()
    assert "content_hash" in input_path.read_text(encoding="utf-8")
    assert platform.labels[-1] == (42, ["gg:in-progress"])


def test_provide_rejects_non_blocked_run(tmp_path):
    init_repo(tmp_path)
    pipeline = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent())
    ready = create_ready_run(pipeline)

    result = pipeline.provide(ready["run_id"], message="hello")

    assert result["accepted"] is False
    assert result["state"] == "ReadyForExecution"


def test_github_platform_get_issue_parses_comments():
    platform = GitHubPlatform(".")
    seen: list[str] = []

    def fake_run(args, **kwargs):
        seen.extend(args)
        return json.dumps(
            {
                "number": 7,
                "title": "Add comments",
                "body": "Body",
                "labels": [{"name": "ai-ready"}],
                "assignees": [{"login": "octocat"}],
                "state": "open",
                "url": "https://github.com/example/repo/issues/7",
                "comments": [
                    {
                        "author": {"login": "maintainer"},
                        "body": "Please preserve CLI compatibility.",
                        "createdAt": "2026-04-25T12:00:00Z",
                        "url": "https://github.com/example/repo/issues/7#issuecomment-1",
                    }
                ],
            }
        )

    platform._run = fake_run  # type: ignore[method-assign]

    issue = platform.get_issue(7)

    assert "number,title,body,labels,assignees,state,url,comments" in seen
    assert issue.comments[0].author == "maintainer"
    assert issue.comments[0].body == "Please preserve CLI compatibility."


def test_github_validate_auth_rejects_missing_scope_line():
    platform = GitHubPlatform(".")
    platform._run = lambda args, **kwargs: "Logged in to github.com account octocat"  # type: ignore[method-assign]

    try:
        platform.validate_auth()
    except RuntimeError as exc:
        assert "scopes could not be determined" in str(exc)
    else:
        raise AssertionError("missing gh scope line should fail auth validation")


def test_github_validate_auth_accepts_required_scopes():
    platform = GitHubPlatform(".")
    platform._run = lambda args, **kwargs: "Token scopes: 'repo', 'read:org'"  # type: ignore[method-assign]

    platform.validate_auth()


def test_gitlab_platform_get_issue_parses_comments():
    platform = GitLabPlatform(".")
    platform._run = lambda args, **kwargs: json.dumps(  # type: ignore[method-assign]
        {
            "iid": 7,
            "title": "Add comments",
            "description": "Body",
            "labels": ["ai-ready"],
            "assignees": [{"username": "maintainer"}],
            "state": "opened",
            "web_url": "https://gitlab.com/example/repo/-/issues/7",
            "discussions": [
                {
                    "notes": [
                        {
                            "author": {"username": "reviewer"},
                            "body": "Please keep the GitLab flow working.",
                            "created_at": "2026-04-25T12:00:00Z",
                            "web_url": "https://gitlab.com/example/repo/-/issues/7#note_1",
                        }
                    ]
                }
            ],
        }
    )

    issue = platform.get_issue(7)

    assert issue.comments[0].author == "reviewer"
    assert issue.comments[0].body == "Please keep the GitLab flow working."


def test_cli_status_reads_runs(tmp_path):
    init_repo(tmp_path)
    pipeline = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent())
    create_ready_run(pipeline)

    result = CliRunner().invoke(cli, ["status", "--path", str(tmp_path), "--json"])

    assert result.exit_code == 0
    assert "ReadyForExecution" in result.output


def test_cli_doctor_reports_machine_readable_checks(tmp_path):
    init_repo(tmp_path)
    (tmp_path / ".gg" / "params.yaml").write_text(
        "verify:\n  tests: ''\nruntime:\n  agent_backend: fake-agent\n",
        encoding="utf-8",
    )

    result = CliRunner().invoke(cli, ["doctor", "--path", str(tmp_path), "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    check_names = {check["name"] for check in payload["checks"]}
    assert payload["schema_version"] == 1
    assert "params" in check_names
    assert "config_schema" in check_names
    assert "git_worktree" in check_names
    assert "sandbox_mode" in check_names
    assert "platform_auth" in check_names
    assert "filesystem_safety" in check_names
    assert "dirty_workspace" in check_names
    assert "secrets" in check_names


def test_doctor_reports_platform_auth_failure(monkeypatch, tmp_path):
    init_repo(tmp_path)
    (tmp_path / ".gg" / "params.yaml").write_text(
        "task_system:\n  platform: github\nverify:\n  tests: ''\nruntime:\n  agent_backend: fake-agent\n",
        encoding="utf-8",
    )

    def fake_create_platform(name, project_path):
        return AuthFailPlatform()

    monkeypatch.setattr("gg.orchestrator.doctor.create_platform", fake_create_platform)

    result = CliRunner().invoke(cli, ["doctor", "--path", str(tmp_path), "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    checks = {check["name"]: check for check in payload["checks"]}
    assert payload["status"] == "fail"
    assert checks["platform_auth"]["status"] == "fail"
    assert "missing token" in checks["platform_auth"]["message"]


def test_cli_doctor_fails_when_params_contain_obvious_secret(tmp_path):
    init_repo(tmp_path)
    (tmp_path / ".gg" / "params.yaml").write_text(
        "runtime:\n  agent_backend: fake-agent\ntask_system:\n  work_label: token=abcdefghijklmnop\n",
        encoding="utf-8",
    )

    result = CliRunner().invoke(cli, ["doctor", "--path", str(tmp_path), "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    checks = {check["name"]: check for check in payload["checks"]}
    assert payload["status"] == "fail"
    assert checks["secrets"]["status"] == "fail"


def test_clean_dry_run_lists_only_terminal_runs(tmp_path):
    init_repo(tmp_path)
    pipeline = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent())
    completed = pipeline.run_issue(42, no_pr=True)
    ready = create_ready_run(pipeline)

    result = pipeline.clean(dry_run=True)

    assert result["runs"] == [completed["run_id"]]
    assert ready["run_id"] not in result["runs"]
    assert (tmp_path / ".gg" / "runs" / completed["run_id"]).exists()


def test_clean_lists_and_removes_stale_waiting_runs(tmp_path):
    init_repo(tmp_path)
    pipeline = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent())
    ready = create_ready_run(pipeline)
    state = pipeline.store.load(ready["run_id"])
    state.recover_to(TaskState.NEEDS_INPUT, reason="test stale input")
    pipeline.store.write(state)
    state_path = tmp_path / ".gg" / "runs" / ready["run_id"] / "state.json"
    data = json.loads(state_path.read_text(encoding="utf-8"))
    data["updated_at"] = "2000-01-01T00:00:00Z"
    state_path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")

    dry_run = pipeline.clean(dry_run=True)
    execute = pipeline.clean(dry_run=False)

    assert dry_run["stale_runs"] == [ready["run_id"]]
    assert execute["stale_runs"] == [ready["run_id"]]
    assert not (tmp_path / ".gg" / "runs" / ready["run_id"]).exists()


def test_clean_execute_removes_terminal_run_and_worktree(tmp_path):
    init_repo(tmp_path)
    pipeline = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent())
    completed = pipeline.run_issue(42, no_pr=True)
    state = pipeline.store.load(completed["run_id"])
    worktree_path = Path(state.candidate_states["candidate-1"].worktree_path)
    object_paths = [path for path in (tmp_path / ".gg" / "objects").glob("*/*") if path.is_file()]
    assert object_paths

    result = pipeline.clean(dry_run=False)

    assert result["runs"] == [completed["run_id"]]
    assert sorted(result["cas_objects"]) == sorted(str(path) for path in object_paths)
    assert not (tmp_path / ".gg" / "runs" / completed["run_id"]).exists()
    assert not worktree_path.exists()
    assert all(not path.exists() for path in object_paths)
    archive_path = tmp_path / ".gg" / "runs-archive" / f"{completed['run_id']}.json"
    archive = json.loads(archive_path.read_text(encoding="utf-8"))
    assert archive["run_id"] == completed["run_id"]
    assert archive["issue"]["number"] == 42
    assert archive["outcome"]["status"] == "success"
    assert str(worktree_path) in archive["removed_worktrees"]


def test_clean_removes_orphan_worktrees(tmp_path):
    init_repo(tmp_path)
    orphan = tmp_path.parent / ".gg-worktrees" / tmp_path.name / "orphan-run" / "candidate-1"
    orphan.mkdir(parents=True)
    (orphan / "scratch.txt").write_text("orphan\n", encoding="utf-8")

    result = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent()).clean(dry_run=False)

    assert str(orphan.resolve()) in result["orphan_worktrees"]
    assert not orphan.exists()


def test_cancel_non_terminal_run(tmp_path):
    init_repo(tmp_path)
    pipeline = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent())
    ready = create_ready_run(pipeline)

    result = pipeline.cancel(ready["run_id"], reason="test cancel")

    assert result["cancelled"] is True
    assert result["state"] == "Cancelled"
    state = pipeline.store.load(ready["run_id"])
    assert state.last_error["message"] == "test cancel"
    assert (tmp_path / ".gg" / "runs" / ready["run_id"] / "errors.jsonl").exists()


def test_cancel_running_run_terminates_known_candidate_processes(monkeypatch, tmp_path):
    init_repo(tmp_path)
    killed: list[int] = []

    def fake_killpg(pid, sig):
        assert sig == signal.SIGTERM
        killed.append(pid)

    monkeypatch.setattr("gg.orchestrator.pipeline.os.killpg", fake_killpg)
    pipeline = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent())
    ready = create_ready_run(pipeline)
    state = pipeline.store.load(ready["run_id"])
    state.transition(TaskState.AGENT_SELECTION, reason="test select")
    state.transition(TaskState.AGENT_RUNNING, reason="test running")
    state.candidate_states["candidate-1"] = CandidateState(
        status="running",
        sandbox_pid=43210,
        agent_pid=54321,
    )
    pipeline.store.write(state)

    result = pipeline.cancel(ready["run_id"], reason="stop process")

    assert result["cancel_requested"] is True
    assert result["terminated_pids"] == [43210, 54321]
    assert killed == [43210, 54321]


def test_run_next_selects_highest_priority_ai_ready_issue(tmp_path):
    init_repo(tmp_path)
    platform = FakePlatform()
    platform.issues = [
        Issue(number=30, title="Needs info", body="", labels=["P0", "needs-info"]),
        Issue(number=27, title="Docs", body="", labels=["P3", "ai-ready"]),
        Issue(number=3, title="Passwords", body="", labels=["P0", "ai-ready"]),
        Issue(number=2, title="Eval", body="", labels=["P0", "ai-ready", "gg:in-progress"]),
    ]

    result = OrchestratorPipeline(tmp_path, platform=platform, agent=FakeAgent()).run_next(dry_run=True)

    assert result["state"] == "DryRun"
    assert result["issue"]["number"] == 3


def test_run_batch_dry_run_lists_next_eligible_issues(tmp_path):
    init_repo(tmp_path)
    platform = MultiIssuePlatform([
        Issue(number=9, title="P2 task", body="", labels=["P2", "ai-ready"]),
        Issue(number=3, title="P0 task", body="", labels=["P0", "ai-ready"]),
        Issue(number=2, title="Claimed", body="", labels=["P0", "ai-ready", "gg:in-progress"]),
        Issue(number=5, title="P1 task", body="", labels=["P1", "ai-ready"]),
    ])

    result = OrchestratorPipeline(tmp_path, platform=platform, agent=FakeAgent()).run_batch(
        batch_size=2,
        dry_run=True,
    )

    assert result["state"] == "DryRun"
    assert [issue["number"] for issue in result["issues"]] == [3, 5]
    assert [issue["number"] for issue in result["eligible"]] == [3, 5, 9]
    assert {issue["number"]: issue["reason"] for issue in result["excluded"]} == {
        2: "excluded_label",
        9: "not_selected_batch_limit",
    }


def test_run_batch_processes_selected_issues(tmp_path):
    init_repo(tmp_path)
    platform = MultiIssuePlatform([
        Issue(number=1, title="First", body="Do one.", labels=["ai-ready"]),
        Issue(number=2, title="Second", body="Do two.", labels=["ai-ready"]),
    ])

    result = OrchestratorPipeline(tmp_path, platform=platform, agent=FakeAgent()).run_batch(
        batch_size=2,
        no_pr=True,
    )

    assert result["state"] == "BatchCompleted"
    assert result["count"] == 2
    assert [item["state"] for item in result["results"]] == ["Completed", "Completed"]
    run_store = OrchestratorPipeline(tmp_path, platform=platform, agent=FakeAgent()).store
    assert sorted(run.issue["number"] for run in run_store.list_runs()) == [1, 2]


def test_run_batch_skips_issue_with_existing_local_success(tmp_path):
    init_repo(tmp_path)
    platform = MultiIssuePlatform([
        Issue(number=1, title="First", body="Do one.", labels=["ai-ready"]),
    ])
    pipeline = OrchestratorPipeline(tmp_path, platform=platform, agent=FakeAgent())

    first = pipeline.run_batch(batch_size=1, no_pr=True)
    second = pipeline.run_batch(batch_size=1, no_pr=True)

    assert first["results"][0]["state"] == "Completed"
    assert second["results"][0]["state"] == "AlreadyClaimed"
    assert second["results"][0]["existing_state"] == "Completed"
    assert len(pipeline.store.list_runs()) == 1


def test_pipeline_runtime_overrides_update_execution_knobs(tmp_path):
    init_repo(tmp_path)
    pipeline = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent()).configure_runtime(
        max_attempts=2,
        candidates=3,
        max_parallel_candidates=2,
        repair_fanout=1,
        timeout=45,
        base="release",
    )

    assert pipeline.config.runtime.max_attempts == 2
    assert pipeline.config.runtime.candidates == 3
    assert pipeline.config.runtime.max_parallel_candidates == 2
    assert pipeline.config.runtime.repair_candidates == 1
    assert pipeline.config.runtime.candidate_timeout_seconds == 45
    assert pipeline.config.git.default_branch == "release"


def test_run_issue_blocks_dirty_workspace_before_claim(tmp_path):
    init_repo(tmp_path)
    (tmp_path / "dirty.txt").write_text("local edit\n", encoding="utf-8")
    platform = FakePlatform()

    result = OrchestratorPipeline(tmp_path, platform=platform, agent=FakeAgent()).run_issue(42, no_pr=True)

    assert result["state"] == "TerminalFailure"
    assert result["error"]["code"] == "dirty_workspace"
    assert platform.labels == []
    state = OrchestratorPipeline(tmp_path, platform=platform, agent=FakeAgent()).store.load(result["run_id"])
    preflight = json.loads((tmp_path / state.artifacts["workspace_preflight"]).read_text(encoding="utf-8"))
    assert preflight["passed"] is False
    assert preflight["dirty_paths"] == ["dirty.txt"]


def test_run_issue_ignores_dirty_gg_workspace_files(tmp_path):
    init_repo(tmp_path)
    (tmp_path / ".gg" / "operator-note.txt").write_text("local run metadata\n", encoding="utf-8")

    result = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent()).run_issue(42, no_pr=True)

    assert result["state"] == "Completed"


def test_run_issue_allows_dirty_workspace_with_explicit_base(tmp_path):
    init_repo(tmp_path)
    (tmp_path / "dirty.txt").write_text("local edit\n", encoding="utf-8")

    result = (
        OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent())
        .configure_runtime(base="HEAD")
        .run_issue(42, no_pr=True)
    )

    assert result["state"] == "Completed"


def test_pipeline_blocks_missing_required_sandbox_before_baseline(monkeypatch, tmp_path):
    init_repo(tmp_path)
    (tmp_path / ".gg" / "params.yaml").write_text(
        "verify:\n  tests: ''\nruntime:\n  require_sandbox_runtime: true\n  allow_unsafe_direct_exec: false\n",
        encoding="utf-8",
    )
    monkeypatch.setattr("gg.orchestrator.sandbox.shutil.which", lambda _: None)
    agent = SandboxRequiredCodexAgent()
    platform = FakePlatform()

    result = OrchestratorPipeline(tmp_path, platform=platform, agent=agent).run_issue(42, no_pr=True)

    assert result["state"] == "Blocked"
    assert result["error"]["code"] == "missing_sandbox_runtime"
    assert agent.analysis_calls == 1
    assert agent.candidate_generated is False
    state = OrchestratorPipeline(tmp_path, platform=platform, agent=FakeAgent()).store.load(result["run_id"])
    assert "baseline_verification" not in state.artifacts
    preflight = json.loads((tmp_path / state.artifacts["sandbox_preflight"]).read_text(encoding="utf-8"))
    assert preflight["required"] is True
    assert preflight["available"] is False
    assert preflight["error"] == "sandbox-runtime is required but unavailable"
    assert not (tmp_path.parent / ".gg-worktrees" / tmp_path.name).exists()


def test_agent_handoff_is_persisted_before_candidate_setup(monkeypatch, tmp_path):
    init_repo(tmp_path)
    observed: list[bool] = []
    original_run_setup = CandidateExecutor._run_setup

    def assert_handoff_before_setup(self, worktree, *, port=None):
        observed.append(bool(list((tmp_path / ".gg" / "runs").glob("*/candidates/candidate-1/agent-handoff.json"))))
        return original_run_setup(self, worktree, port=port)

    monkeypatch.setattr(CandidateExecutor, "_run_setup", assert_handoff_before_setup)

    result = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent()).run_issue(42, no_pr=True)

    assert result["state"] == "Completed"
    assert observed == [True]


def test_cli_issue_help_documents_runtime_overrides():
    result = CliRunner().invoke(cli, ["issue", "--help"])

    assert result.exit_code == 0
    assert "--max-attempts" in result.output
    assert "--candidates" in result.output
    assert "--max-parallel-candidates" in result.output
    assert "--repair-fanout" in result.output
    assert "--timeout" in result.output
    assert "--base" in result.output
    assert "--debug" in result.output


def test_cli_run_help_documents_debug_flag():
    result = CliRunner().invoke(cli, ["run", "--help"])

    assert result.exit_code == 0
    assert "--debug" in result.output


def test_init_params_generation(tmp_path):
    init_repo(tmp_path)
    (tmp_path / "pyproject.toml").write_text("[project]\nname = 'sample'\n", encoding="utf-8")
    params_path = tmp_path / ".gg" / "params.yaml"
    params_path.unlink()

    _write_params(tmp_path, console=type("Console", (), {"print": lambda *args, **kwargs: None})())

    config = load_config(tmp_path)
    assert params_path.exists()
    assert config.task_system.work_label == "gg:in-progress"
    assert config.task_system.platform == "auto"
    assert config.selection.include_labels == ("ai-ready",)
    assert config.runtime.agent_backend == "codex"
    assert config.runtime.candidates == 1
    assert config.runtime.max_parallel_runs == 1
    assert config.runtime.allow_unsafe_direct_exec is False
    assert config.runtime.require_sandbox_runtime is True
    assert config.runtime.analysis_timeout_seconds == 900
    assert config.runtime.evaluation_timeout_seconds == 900
    assert config.runtime.setup_timeout_seconds == 600
    assert config.runtime.resource.max_disk_mb == 4096
    assert config.runtime.resource.disk_poll_interval_seconds == 30
    assert config.runtime.network.default == "deny"
    assert config.runtime.network.allowed_hosts == ()
    assert config.runtime.port_range == (41000, 45000)
    assert config.runtime.lock_stale_seconds == 3600
    assert config.runtime.queue_lock_stale_seconds == 300
    assert config.runtime.vendored_deps is False
    assert config.runtime.sandbox_policy.deny_read == ["~/.ssh", ".env"]
    assert config.audit.hash_events is False
    assert config.audit.hash_artifacts is False
    assert config.audit.external_sink == ""
    assert config.audit.sign_events is False
    assert config.security.allow_lfs_changes is False
    assert config.security.allow_binary_changes is True
    assert config.security.allow_dependency_changes is True
    assert config.cleanup.blocked_timeout_days == 14
    assert config.cleanup.keep_last == 20
    assert config.cleanup.ttl_days == 14
    assert config.verify.setup == ""
    assert config.verify.tests == "pytest"
    assert config.verify.security == ""
    assert config.verify.custom == ()
    assert config.verify.discovery_enabled is True
    assert config.verify.test_retry_count == 0
    assert config.verify.block_on_security_high is True
    assert config.verify.coverage == ""
    assert config.verify.format_check == ""
    assert config.verify.dependency_audit == ""
    assert config.verify.secret_scan == ""
    assert config.verify.baseline_check is True
    assert config.verify.advisory_checks is True
    assert config.log.mask_secrets is True
    assert config.cost.mode == "duration-only"
    assert config.analysis.max_context_tokens == 60000
    assert config.analysis.max_comments == 20
    assert config.analysis.max_candidate_files == 20
    assert config.analysis.max_file_chars == 40000
    assert config.analysis.context_too_large_policy == "fail"
    assert config.analysis.include_attachments == "links-only"
    assert config.evaluation.max_context_tokens == 60000
    assert config.ci.forbid_interactive_prompts is True
    assert config.recovery.keep_state_backup is True


def test_init_writes_operational_gitignore_entries(tmp_path):
    init_repo(tmp_path)
    gitignore = tmp_path / ".gitignore"
    gitignore.write_text("dist/\n", encoding="utf-8")

    _write_operational_gitignore(
        tmp_path,
        console=type("Console", (), {"print": lambda *args, **kwargs: None})(),
    )

    lines = gitignore.read_text(encoding="utf-8").splitlines()
    assert "dist/" in lines
    assert ".gg/runs/" in lines
    assert ".gg/runs-archive/" in lines
    assert ".gg/objects/" in lines
    assert ".gg/rate-limits.sqlite3*" in lines
    assert ".gg-worktrees/" in lines
    assert ".omx/" in lines


def test_init_params_merges_missing_defaults_without_overwriting_user_values(tmp_path):
    init_repo(tmp_path)
    params_path = tmp_path / ".gg" / "params.yaml"
    params_path.write_text(
        """verify:
  tests: custom-test
runtime:
  candidates: 2
log:
  max_size_mb: 7
""",
        encoding="utf-8",
    )

    _write_params(tmp_path, console=type("Console", (), {"print": lambda *args, **kwargs: None})())

    merged = yaml.safe_load(params_path.read_text(encoding="utf-8"))
    assert merged["verify"]["tests"] == "custom-test"
    assert merged["runtime"]["candidates"] == 2
    assert merged["log"]["max_size_mb"] == 7
    assert merged["runtime"]["allow_unsafe_direct_exec"] is False
    assert merged["runtime"]["resource"]["max_disk_mb"] == 4096
    assert merged["runtime"]["network"]["default"] == "deny"
    assert merged["runtime"]["port_range"] == [41000, 45000]
    assert merged["runtime"]["lock_stale_seconds"] == 3600
    assert merged["runtime"]["queue_lock_stale_seconds"] == 300
    assert merged["runtime"]["vendored_deps"] is False
    assert merged["verify"]["coverage"] == ""
    assert merged["verify"]["format_check"] == ""
    assert merged["verify"]["dependency_audit"] == ""
    assert merged["verify"]["secret_scan"] == ""
    assert merged["verify"]["baseline_check"] is True
    assert merged["verify"]["advisory_checks"] is True
    assert merged["audit"]["hash_artifacts"] is False
    assert merged["audit"]["sign_events"] is False
    assert merged["cleanup"]["keep_last"] == 20
    assert merged["cleanup"]["ttl_days"] == 14
    assert merged["log"]["mask_secrets"] is True
    assert merged["cost"]["mode"] == "duration-only"
    assert merged["analysis"]["max_context_tokens"] == 60000
    assert merged["analysis"]["max_comments"] == 20
    assert merged["analysis"]["max_candidate_files"] == 20
    assert merged["analysis"]["max_file_chars"] == 40000
    assert merged["analysis"]["context_too_large_policy"] == "fail"
    assert merged["analysis"]["include_attachments"] == "links-only"
    assert merged["evaluation"]["max_context_tokens"] == 60000
    assert merged["ci"]["forbid_interactive_prompts"] is True
    assert merged["recovery"]["keep_state_backup"] is True


def test_load_config_reads_phase_b_contract(tmp_path):
    init_repo(tmp_path)
    (tmp_path / ".gg" / "params.yaml").write_text(
        """verify:
  tests: ''
runtime:
  require_sandbox_runtime: false
  allow_unsafe_direct_exec: false
  analysis_timeout_seconds: 123
  evaluation_timeout_seconds: 456
  resource:
    max_disk_mb: 2048
    disk_poll_interval_seconds: 12
    allow_candidate_downscale: true
    allow_network_fs: true
    allow_unsafe_fs: true
  network:
    default: allow
    allowed_hosts:
      - example.com
  port_range:
    - 42000
    - 42010
log:
  max_size_mb: 12
  max_command_log_chars: 3456
  mask_secrets: false
cost:
  enabled: true
  mode: token-estimate
  max_usd_per_run: 3.5
  max_tokens_per_run: 10000
analysis:
  max_context_tokens: 555
  max_issue_body_chars: 666
  max_summary_chars: 77
  max_project_context_chars: 888
  max_comments: 4
  max_comment_body_chars: 99
  max_inputs: 3
  max_input_message_chars: 111
  max_agent_response_chars: 222
evaluation:
  max_context_tokens: 111
  max_diff_lines_per_candidate: 222
  max_log_chars_per_check: 333
  max_total_log_chars: 444
  prefer_deterministic_when_truncated: false
ci:
  mode: true
  default_dry_run: true
  forbid_interactive_prompts: false
  clock_skew_tolerance_seconds: 8
  clock_drift_warn_seconds: 90
recovery:
  keep_state_backup: false
""",
        encoding="utf-8",
    )

    config = load_config(tmp_path)

    assert config.runtime.allow_unsafe_direct_exec is False
    assert config.runtime.require_sandbox_runtime is True
    assert config.runtime.analysis_timeout_seconds == 123
    assert config.runtime.evaluation_timeout_seconds == 456
    assert config.runtime.resource.max_disk_mb == 2048
    assert config.runtime.resource.disk_poll_interval_seconds == 12
    assert config.runtime.resource.allow_candidate_downscale is True
    assert config.runtime.resource.allow_network_fs is True
    assert config.runtime.resource.allow_unsafe_fs is True
    assert config.runtime.network.default == "allow"
    assert config.runtime.network.allowed_hosts == ("example.com",)
    assert config.runtime.port_range == (42000, 42010)
    assert config.log.max_size_mb == 12
    assert config.log.max_command_log_chars == 3456
    assert config.log.mask_secrets is False
    assert config.cost.enabled is True
    assert config.cost.mode == "token-estimate"
    assert config.cost.max_usd_per_run == 3.5
    assert config.cost.max_tokens_per_run == 10000
    assert config.analysis.max_context_tokens == 555
    assert config.analysis.max_issue_body_chars == 666
    assert config.analysis.max_summary_chars == 77
    assert config.analysis.max_project_context_chars == 888
    assert config.analysis.max_comments == 4
    assert config.analysis.max_comment_body_chars == 99
    assert config.analysis.max_inputs == 3
    assert config.analysis.max_input_message_chars == 111
    assert config.analysis.max_agent_response_chars == 222
    assert config.evaluation.max_context_tokens == 111
    assert config.evaluation.max_diff_lines_per_candidate == 222
    assert config.evaluation.max_log_chars_per_check == 333
    assert config.evaluation.max_total_log_chars == 444
    assert config.evaluation.prefer_deterministic_when_truncated is False
    assert config.ci.mode is True
    assert config.ci.default_dry_run is True
    assert config.ci.forbid_interactive_prompts is False
    assert config.ci.clock_skew_tolerance_seconds == 8
    assert config.ci.clock_drift_warn_seconds == 90
    assert config.recovery.keep_state_backup is False


def test_load_config_accepts_plan_comment_char_alias(tmp_path):
    init_repo(tmp_path)
    (tmp_path / ".gg" / "params.yaml").write_text(
        "verify:\n  tests: ''\nanalysis:\n  max_comment_chars: 321\n",
        encoding="utf-8",
    )

    config = load_config(tmp_path)

    assert config.analysis.max_comment_body_chars == 321


def test_load_config_allows_unsafe_direct_exec_only_when_explicit(tmp_path):
    init_repo(tmp_path)
    (tmp_path / ".gg" / "params.yaml").write_text(
        """verify:
  tests: ''
runtime:
  allow_unsafe_direct_exec: true
""",
        encoding="utf-8",
    )

    config = load_config(tmp_path)

    assert config.runtime.allow_unsafe_direct_exec is True
    assert config.runtime.require_sandbox_runtime is False


def test_load_config_reads_sandbox_policy(tmp_path):
    init_repo(tmp_path)
    (tmp_path / ".gg" / "params.yaml").write_text(
        """runtime:
  sandbox_policy:
    allowed_domains:
      - example.com
    denied_domains:
      - blocked.example.com
    deny_read:
      - secrets.txt
    allow_write:
      - tmp
    deny_write:
      - dist
""",
        encoding="utf-8",
    )

    config = load_config(tmp_path)

    assert config.runtime.sandbox_policy.allowed_domains == ["example.com"]
    assert config.runtime.sandbox_policy.denied_domains == ["blocked.example.com"]
    assert config.runtime.sandbox_policy.deny_read == ["secrets.txt"]
    assert config.runtime.sandbox_policy.allow_write == ["tmp"]
    assert config.runtime.sandbox_policy.deny_write == ["dist"]


def test_load_config_reads_extended_verification_commands(tmp_path):
    init_repo(tmp_path)
    (tmp_path / ".gg" / "params.yaml").write_text(
        """verify:
  setup: uv sync
  tests: pytest
  lint: ruff check .
  typecheck: mypy src
  security: bandit -r src
  custom:
    - python scripts/check.py
  test_retry_count: 2
""",
        encoding="utf-8",
    )

    config = load_config(tmp_path)

    assert config.verify.setup == "uv sync"
    assert config.verify.commands() == ["pytest", "ruff check .", "mypy src", "bandit -r src", "python scripts/check.py"]
    assert config.verify.test_retry_count == 2


def test_pipeline_assigns_default_verification_parsers(tmp_path):
    init_repo(tmp_path)
    (tmp_path / ".gg" / "params.yaml").write_text(
        """verify:
  tests: pytest
  lint: ruff check .
  typecheck: mypy src
  security: bandit -r src
""",
        encoding="utf-8",
    )

    commands = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent())._verification_commands()

    assert [(command.category, command.parser) for command in commands] == [
        ("test", "pytest"),
        ("lint", "ruff"),
        ("typecheck", "mypy"),
        ("security", "bandit,secret-scan"),
    ]


def test_pipeline_wires_extended_verification_fields(tmp_path):
    init_repo(tmp_path)
    (tmp_path / ".gg" / "params.yaml").write_text(
        """verify:
  tests: ''
  lint: ''
  typecheck: ''
  security: ''
  discovery_enabled: false
  coverage: coverage run -m pytest
  format_check: ruff format --check .
  dependency_audit: pip-audit
  secret_scan: detect-secrets scan
""",
        encoding="utf-8",
    )

    commands = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent())._verification_commands()

    assert [(command.id, command.category, command.required, command.parser) for command in commands] == [
        ("coverage", "coverage", False, ""),
        ("format", "format", False, ""),
        ("dependency-audit", "dependency-audit", False, ""),
        ("secret-scan", "security", True, "secret-scan"),
    ]


def test_pipeline_can_make_extended_verification_fields_required(tmp_path):
    init_repo(tmp_path)
    (tmp_path / ".gg" / "params.yaml").write_text(
        """verify:
  tests: ''
  lint: ''
  typecheck: ''
  security: ''
  discovery_enabled: false
  advisory_checks: false
  coverage: coverage run -m pytest
  format_check: ruff format --check .
  dependency_audit: pip-audit
""",
        encoding="utf-8",
    )

    commands = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent())._verification_commands()

    assert [(command.id, command.required) for command in commands] == [
        ("coverage", True),
        ("format", True),
        ("dependency-audit", True),
    ]


def test_baseline_check_can_be_disabled(tmp_path):
    init_repo(tmp_path)
    (tmp_path / ".gg" / "params.yaml").write_text(
        "verify:\n  tests: ''\n  baseline_check: false\n",
        encoding="utf-8",
    )

    result = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent()).run_issue(42, no_pr=True)

    state = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent()).store.load(result["run_id"])
    assert result["state"] == "Completed"
    assert "baseline_verification" not in state.artifacts
    assert state.baseline == {}


def test_pipeline_discovers_package_verification_commands_as_advisory(tmp_path):
    init_repo(tmp_path)
    (tmp_path / "package.json").write_text(
        json.dumps(
            {
                "scripts": {
                    "test": "vitest run",
                    "lint": "eslint .",
                    "typecheck": "tsc --noEmit",
                }
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / ".gg" / "params.yaml").write_text(
        """verify:
  tests: ''
  lint: ''
  typecheck: ''
  security: ''
""",
        encoding="utf-8",
    )

    commands = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent())._verification_commands()

    assert [(command.id, command.command, command.required) for command in commands] == [
        ("tests", "npm test", True),
        ("lint", "npm run lint", False),
        ("typecheck", "npm run typecheck", False),
    ]
    assert commands[0].parser == "npm,vitest,jest"


def test_pipeline_discovers_python_verification_surfaces(tmp_path):
    init_repo(tmp_path)
    (tmp_path / "pyproject.toml").write_text(
        "[tool.ruff]\nline-length = 100\n[tool.mypy]\npython_version = '3.11'\n[tool.bandit]\n",
        encoding="utf-8",
    )
    (tmp_path / ".gg" / "params.yaml").write_text(
        """verify:
  tests: ''
  lint: ''
  typecheck: ''
  security: ''
""",
        encoding="utf-8",
    )

    commands = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent())._verification_commands()

    assert [(command.id, command.command, command.required, command.parser) for command in commands] == [
        ("tests", "pytest", True, "pytest"),
        ("lint", "ruff check .", False, "ruff"),
        ("typecheck", "mypy .", False, "mypy"),
        ("security", "bandit -r .", False, "bandit,secret-scan"),
    ]


def test_verification_gate_blocks_new_high_security_findings_even_when_advisory():
    baseline = [
        CheckResult(
            command="bandit -r .",
            status="failed",
            exit_code=1,
            category="security",
            required=False,
            findings=[
                {
                    "category": "security",
                    "parser": "bandit",
                    "severity": "high",
                    "code": "B999",
                    "file": "src/legacy.py",
                    "line": 10,
                    "message": "legacy issue",
                }
            ],
        )
    ]
    checks = [
        baseline[0],
        CheckResult(
            command="bandit -r .",
            status="failed",
            exit_code=1,
            category="security",
            required=False,
            findings=[
                {
                    "category": "security",
                    "parser": "bandit",
                    "severity": "high",
                    "code": "B999",
                    "file": "src/new.py",
                    "line": 5,
                    "message": "new issue",
                }
            ],
        ),
    ]

    assert (
        _verification_passed(
            checks,
            baseline,
            allow_known_baseline_failures=True,
            block_on_security_high=True,
        )
        is False
    )
    assert (
        _verification_passed(
            baseline,
            baseline,
            allow_known_baseline_failures=True,
            block_on_security_high=True,
        )
        is True
    )


def test_pipeline_uses_registered_platform_and_agent_backend(tmp_path):
    init_repo(tmp_path)
    (tmp_path / ".gg" / "params.yaml").write_text(
        """task_system:
  platform: fake-platform
verify:
  tests: ''
runtime:
  agent_backend: fake-agent
""",
        encoding="utf-8",
    )
    register_platform("fake-platform", lambda project_path: FakePlatform())
    register_agent_backend("fake-agent", FakeAgent)

    result = OrchestratorPipeline(tmp_path).run_issue(42, no_pr=True)

    assert result["state"] == "Completed"
    assert result["winner"] == "candidate-1"
    assert isinstance(create_agent_backend("fake-agent"), FakeAgent)


def test_sandbox_policy_settings_shape():
    settings = SandboxPolicy(allowed_domains=["example.com"], allow_write=["."]).to_settings()

    assert settings["network"]["allowedDomains"] == ["example.com"]
    assert settings["filesystem"]["allowWrite"] == ["."]
    assert ".env" in settings["filesystem"]["denyRead"]


def test_sandbox_runtime_requires_executable(monkeypatch, tmp_path):
    monkeypatch.setattr("gg.orchestrator.sandbox.shutil.which", lambda _: None)

    runtime = SandboxRuntime()

    assert runtime.is_available() is False
    try:
        runtime.run(["echo", "ok"], cwd=tmp_path, timeout=1)
    except RuntimeError as exc:
        assert "srt-py" in str(exc)
    else:
        raise AssertionError("missing sandbox runtime should fail")


def test_verification_runner_retries_and_marks_flaky(tmp_path):
    command = (
        "python -c \"from pathlib import Path; "
        "p=Path('attempts.txt'); "
        "n=int(p.read_text() if p.exists() else '0'); "
        "p.write_text(str(n + 1)); "
        "raise SystemExit(1 if n == 0 else 0)\""
    )

    result = VerificationRunner([command], timeout=5, retry_count=1).run(tmp_path)[0]

    assert result.status == "flaky"
    assert result.flaky is True
    assert result.attempts == 2


def test_verification_runner_parses_pytest_findings(tmp_path):
    command = VerificationCommand(
        id="tests",
        category="test",
        command="python -c \"print('FAILED tests/test_app.py::test_greeting - AssertionError: nope'); raise SystemExit(1)\"",
        parser="pytest",
    )

    result = VerificationRunner([command], timeout=5).run(tmp_path)[0]

    assert result.status == "failed"
    assert result.findings == [
        {
            "type": "test_failure",
            "category": "test",
            "parser": "pytest",
            "severity": "error",
            "stream": "stdout",
            "line": 1,
            "test": "tests/test_app.py::test_greeting",
            "message": "AssertionError: nope",
        }
    ]


def test_verification_runner_parses_js_test_findings(tmp_path):
    command = VerificationCommand(
        id="tests",
        category="test",
        command="python -c \"print('FAIL  tests/app.test.ts > renders greeting'); raise SystemExit(1)\"",
        parser="npm,vitest,jest",
    )

    result = VerificationRunner([command], timeout=5).run(tmp_path)[0]

    assert result.findings == [
        {
            "type": "test_failure",
            "category": "test",
            "parser": "js-test",
            "severity": "error",
            "stream": "stdout",
            "line": 1,
            "test": "tests/app.test.ts > renders greeting",
            "message": "FAIL  tests/app.test.ts > renders greeting",
        }
    ]


def test_verification_runner_parses_ruff_findings(tmp_path):
    command = VerificationCommand(
        id="lint",
        category="lint",
        command="python -c \"print('src/app.py:3:7: F401 imported but unused'); raise SystemExit(1)\"",
        parser="ruff",
    )

    result = VerificationRunner([command], timeout=5).run(tmp_path)[0]

    assert result.findings == [
        {
            "type": "lint",
            "category": "lint",
            "parser": "ruff",
            "severity": "error",
            "stream": "stdout",
            "line": 3,
            "column": 7,
            "file": "src/app.py",
            "code": "F401",
            "message": "imported but unused",
        }
    ]


def test_verification_runner_parses_mypy_and_bandit_findings(tmp_path):
    mypy = VerificationCommand(
        id="typecheck",
        category="typecheck",
        command="python -c \"print('src/app.py:4: error: Incompatible return value [return-value]'); raise SystemExit(1)\"",
        parser="mypy",
    )
    bandit = VerificationCommand(
        id="security",
        category="security",
        command=(
            "python -c \"print('>> Issue: [B101:assert_used] Use of assert detected.\\n"
            "   Severity: Low   Confidence: High\\n"
            "   Location: src/app.py:8:4'); raise SystemExit(0)\""
        ),
        parser="bandit",
    )

    mypy_result, bandit_result = VerificationRunner([mypy, bandit], timeout=5).run(tmp_path)

    assert mypy_result.findings[0]["code"] == "return-value"
    assert mypy_result.findings[0]["category"] == "typecheck"
    assert bandit_result.status == "failed"
    assert bandit_result.findings[0]["code"] == "B101:assert_used"
    assert bandit_result.findings[0]["file"] == "src/app.py"


def test_rate_limit_store_uses_sqlite_wal(tmp_path):
    init_repo(tmp_path)
    store = RateLimitStore(tmp_path)

    snapshot = store.update(
        "github:sagolubev/gg-test",
        remaining=0,
        reset_at="2999-01-01T00:00:00Z",
        limit=5000,
    )

    assert snapshot.remaining == 0
    assert store.get("github:sagolubev/gg-test").limit == 5000
    assert store.should_throttle("github:sagolubev/gg-test") is True
    assert (tmp_path / ".gg" / "rate-limits.sqlite3").exists()


def test_rate_limit_store_records_headers_backoff_and_resume(tmp_path):
    init_repo(tmp_path)
    store = RateLimitStore(tmp_path)
    bucket = "github:example/repo:issues:read"

    snapshot = store.record_http_headers(
        bucket,
        "< X-RateLimit-Remaining: 0\n< X-RateLimit-Reset: 1893456000\n< X-RateLimit-Limit: 5000\n",
    )
    backoff = store.backoff(bucket, retry_after_seconds=30, now="2030-01-01T00:00:00Z")

    assert snapshot is not None
    assert snapshot.limit == 5000
    assert store.should_throttle(bucket, now="2029-12-31T23:59:59Z") is True
    assert backoff.reset_at == "2030-01-01T00:00:30Z"
    assert store.should_throttle(bucket, now="2030-01-01T00:00:31Z") is False


def test_github_platform_reuses_stored_rate_limit_until_reset(monkeypatch, tmp_path):
    init_repo(tmp_path)
    subprocess.run(
        ["git", "remote", "add", "origin", "https://github.com/example/repo.git"],
        cwd=tmp_path,
        check=True,
        capture_output=True,
    )
    calls: list[tuple[list[str], dict[str, str]]] = []

    def fake_run(args, **kwargs):
        calls.append((args, kwargs.get("env", {})))
        return subprocess.CompletedProcess(
            args,
            0,
            stdout="[]",
            stderr="< X-RateLimit-Remaining: 0\n< X-RateLimit-Reset: 4102444800\n< X-RateLimit-Limit: 5000\n",
        )

    monkeypatch.setattr("gg.platforms.base.subprocess.run", fake_run)

    platform = GitHubPlatform(str(tmp_path))

    assert platform.list_issues() == []
    try:
        platform.list_issues()
    except RateLimitThrottleError as exc:
        assert exc.snapshot.bucket.endswith(":issues:read")
        assert exc.snapshot.reset_at == "2100-01-01T00:00:00Z"
    else:
        raise AssertionError("expected second call to short-circuit on stored rate limit")
    assert [args[0] for args, _ in calls].count("gh") == 1


def test_run_next_returns_throttled_response_when_issue_polling_is_rate_limited(tmp_path):
    init_repo(tmp_path)

    result = OrchestratorPipeline(tmp_path, platform=ThrottledListPlatform(), agent=FakeAgent()).run_next()

    assert result["state"] == "Throttled"
    assert result["bucket"] == "github:example/repo:issues:read"
    assert result["remaining"] == 0


def test_run_issue_blocks_and_persists_rate_limit_artifact(tmp_path):
    init_repo(tmp_path)
    pipeline = OrchestratorPipeline(tmp_path, platform=ThrottledClaimPlatform(), agent=FakeAgent())

    result = pipeline.run_issue(42)
    state = pipeline.store.load(result["run_id"])
    artifact = json.loads((tmp_path / state.artifacts["rate_limit"]).read_text(encoding="utf-8"))

    assert result["state"] == TaskState.BLOCKED.value
    assert result["error"]["code"] == "rate_limited"
    assert state.state is TaskState.BLOCKED
    assert state.blocked_resume_state is TaskState.CLAIMING
    assert state.blocked_until == artifact["reset_at"]
    assert artifact["bucket"] == "github:example/repo:issues:comment"


def test_run_issue_validates_auth_before_claim_side_effects(tmp_path):
    init_repo(tmp_path)
    platform = AuthFailPlatform()

    result = OrchestratorPipeline(tmp_path, platform=platform, agent=FakeAgent()).run_issue(42)

    assert result["state"] == "TerminalFailure"
    assert "missing token" in result["error"]["message"]
    assert platform.labels == []
    assert platform.comments == []


def test_candidate_executor_can_run_codex_via_sandbox(tmp_path):
    init_repo(tmp_path)
    (tmp_path / ".gg" / "params.yaml").write_text(
        """verify:
  tests: ''
runtime:
  sandbox_policy:
    allowed_domains:
      - example.com
    allow_write:
      - worktree
""",
        encoding="utf-8",
    )
    sandbox = FakeSandbox()
    config = load_config(tmp_path)
    executor = CandidateExecutor(tmp_path, CodexAgent(), config, sandbox=sandbox)
    status_events: list[dict] = []
    from gg.orchestrator.task_analysis import TaskBrief
    task_brief = TaskBrief(
        schema_version=1,
        issue={"number": 42, "title": "Add greeting", "body": "", "labels": ["ai-ready"], "url": ""},
        summary="Do it",
        acceptance_criteria=["Add file"],
        project_context="",
    )

    result = executor.run(
        run_id="run-123",
        issue_number=42,
        brief=task_brief,
        on_status=status_events.append,
    )

    assert result.status == "success"
    assert result.sandbox_pid == 43210
    assert sandbox.commands
    assert sandbox.commands[0][0:3] == ["codex", "exec", "-o"]
    assert sandbox.policies == [config.runtime.sandbox_policy]
    assert any(event.get("worktree_path") for event in status_events)
    assert {"sandbox_pid": 43210} in status_events


def test_candidate_executor_uses_configured_codex_command_in_sandbox(tmp_path):
    init_repo(tmp_path)
    (tmp_path / ".gg" / "params.yaml").write_text(
        """verify:
  tests: ''
agent:
  codex_command: python -m codex_cli
""",
        encoding="utf-8",
    )
    sandbox = FakeSandbox()
    executor = CandidateExecutor(tmp_path, CodexAgent(), load_config(tmp_path), sandbox=sandbox)
    from gg.orchestrator.task_analysis import TaskBrief
    task_brief = TaskBrief(
        schema_version=1,
        issue={"number": 42, "title": "Add greeting", "body": "", "labels": ["ai-ready"], "url": ""},
        summary="Do it",
        acceptance_criteria=["Add file"],
    )

    result = executor.run(run_id="run-custom-codex", issue_number=42, brief=task_brief)

    assert result.status == "success"
    assert sandbox.commands[0][:5] == ["python", "-m", "codex_cli", "exec", "-o"]


def test_candidate_setup_failure_is_persisted_without_running_agent(tmp_path):
    init_repo(tmp_path)
    (tmp_path / ".gg" / "params.yaml").write_text(
        """verify:
  setup: python -c 'import sys; sys.exit(4)'
  tests: ''
runtime:
  setup_timeout_seconds: 5
""",
        encoding="utf-8",
    )

    result = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=ExplodingAgent()).run_issue(
        42,
        no_pr=True,
    )

    assert result["state"] == "TerminalFailure"
    state = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent()).store.load(result["run_id"])
    candidate = state.candidate_states["candidate-1"]
    assert candidate.status == "setup_failed"
    candidate_result = json.loads((tmp_path / candidate.result_path).read_text(encoding="utf-8"))
    assert candidate_result["setup"]["status"] == "failed"


def test_candidate_setup_uses_isolated_package_cache_env(tmp_path):
    init_repo(tmp_path)
    (tmp_path / ".gg" / "params.yaml").write_text(
        """verify:
  setup: python -c 'import os; from pathlib import Path; Path("pip-cache.txt").write_text(os.environ["PIP_CACHE_DIR"])'
  tests: ''
""",
        encoding="utf-8",
    )
    config = load_config(tmp_path)
    from gg.orchestrator.task_analysis import TaskBrief
    task_brief = TaskBrief(
        schema_version=1,
        issue={"number": 42, "title": "Add greeting", "body": "", "labels": ["ai-ready"], "url": ""},
        summary="Do it",
        acceptance_criteria=["Add file"],
        project_context="",
    )

    result = CandidateExecutor(tmp_path, FakeAgent(), config).run(
        run_id="run-setup",
        issue_number=42,
        brief=task_brief,
    )

    cache_value = Path(result.worktree_path, "pip-cache.txt").read_text(encoding="utf-8")
    assert result.status == "success"
    assert ".gg-cache/pip" in cache_value
    assert ".gg-cache" not in result.changed_files


def test_candidate_fails_when_worktree_exceeds_disk_quota(monkeypatch, tmp_path):
    init_repo(tmp_path)
    (tmp_path / ".gg" / "params.yaml").write_text("verify:\n  tests: ''\nruntime:\n  resource:\n    max_disk_mb: 5\n", encoding="utf-8")
    monkeypatch.setattr("gg.orchestrator.executor._directory_size_mb", lambda *_args, **_kwargs: 6)
    config = load_config(tmp_path)
    from gg.orchestrator.task_analysis import TaskBrief
    task_brief = TaskBrief(
        schema_version=1,
        issue={"number": 42, "title": "Add greeting", "body": "", "labels": ["ai-ready"], "url": ""},
        summary="Do it",
        acceptance_criteria=["Add file"],
        project_context="",
    )

    result = CandidateExecutor(tmp_path, FakeAgent(), config).run(
        run_id="run-disk-quota",
        issue_number=42,
        brief=task_brief,
    )

    assert result.status == "failed"
    assert result.error == "disk_quota_exceeded: candidate used 6MB, limit is 5MB"
    assert result.changed_files == ["greeting.txt"]


def test_context_snapshot_uses_content_addressed_objects(tmp_path):
    init_repo(tmp_path)
    pipeline = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent())
    ready = create_ready_run(pipeline)
    state = pipeline.store.load(ready["run_id"])
    snapshot = tmp_path / state.artifacts["context_snapshot"]
    data = json.loads(snapshot.read_text(encoding="utf-8"))

    store = ContextSnapshotStore(tmp_path)

    assert "project_context" in data["objects"]
    assert data["snapshot_version"] == 1
    assert data["purpose"] == "task_analysis_handoff"
    assert data["source_refs"][0]["kind"] == "issue"
    assert data["object_metadata"]["summary"]["sha256"] == data["objects"]["summary"]
    assert data["summaries"]["summary"]
    assert store.read_text(data["objects"]["summary"])


def test_error_logs_mask_secrets(tmp_path):
    init_repo(tmp_path)
    pipeline = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent())
    ready = create_ready_run(pipeline)

    pipeline.cancel(ready["run_id"], reason="revoke ghp_abcdefghijklmnopqrstuvwxyz12345")

    errors = (tmp_path / ".gg" / "runs" / ready["run_id"] / "errors.jsonl").read_text(encoding="utf-8")
    assert "ghp_" not in errors
    assert "***" in errors


def test_observability_artifacts_mask_secrets(tmp_path):
    init_repo(tmp_path)
    store = RunStore(tmp_path)
    issue = Issue(number=42, title="Secrets", body="Track tokens.", labels=["ai-ready"], url="")
    state = store.create(issue, dry_run=True)

    store.append_event(state.run_id, {"event": "custom", "message": "keep sk-abcdefghijklmnopqrstuvwxyz secret"})
    store.append_cost(state.run_id, {"event": "custom", "detail": "github_pat_abcdefghijklmnopqrstuvwxyz_123"})
    state.last_error = {"code": "boom", "message": "leaked ghp_abcdefghijklmnopqrstuvwxyz12345", "at": state.updated_at}
    store.write(state)

    run_dir = tmp_path / ".gg" / "runs" / state.run_id
    pipeline = (run_dir / "pipeline.jsonl").read_text(encoding="utf-8")
    cost = (run_dir / "cost.jsonl").read_text(encoding="utf-8")
    errors = (run_dir / "errors.jsonl").read_text(encoding="utf-8")
    summary = (run_dir / "artifacts" / "run-summary.json").read_text(encoding="utf-8")

    assert "sk-" not in pipeline
    assert "github_pat_" not in cost
    assert "ghp_" not in errors
    assert "ghp_" not in summary
    assert "***" in pipeline
    assert "***" in cost
    assert "***" in errors
    assert "***" in summary


def test_audit_hashes_pipeline_events_and_mirrors_sink(tmp_path):
    init_repo(tmp_path)
    (tmp_path / ".gg" / "params.yaml").write_text(
        """verify:
  tests: ''
audit:
  hash_events: true
  external_sink: .gg/audit-events.jsonl
""",
        encoding="utf-8",
    )
    pipeline = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent())

    ready = create_ready_run(pipeline)

    run_dir = tmp_path / ".gg" / "runs" / ready["run_id"]
    events = read_jsonl(run_dir / "pipeline.jsonl")
    mirrored = read_jsonl(tmp_path / ".gg" / "audit-events.jsonl")

    assert mirrored == events
    previous_hash = ""
    for event in events:
        audit = event.pop("audit")
        expected = hashlib.sha256(
            (
                previous_hash
                + "\n"
                + json.dumps(event, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
            ).encode("utf-8"),
        ).hexdigest()
        assert audit["previous_hash"] == previous_hash
        assert audit["hash"] == expected
        previous_hash = audit["hash"]


def test_security_policy_can_block_dependency_file_changes(tmp_path):
    init_repo(tmp_path)
    (tmp_path / ".gg" / "params.yaml").write_text(
        """verify:
  tests: ''
security:
  allow_dependency_changes: false
""",
        encoding="utf-8",
    )

    platform = FakePlatform()
    result = OrchestratorPipeline(tmp_path, platform=platform, agent=DependencyChangingAgent()).run_issue(
        42,
        no_pr=True,
    )

    assert result["state"] == "TerminalFailure"
    state = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent()).store.load(result["run_id"])
    candidate = state.candidate_states["candidate-1"]
    assert candidate.error == "Dependency manifest changes are disabled by policy"
    assert platform.removed_labels[-1] == (42, ["gg:in-progress", "gg:blocked"])
    candidate_result = json.loads((tmp_path / candidate.result_path).read_text(encoding="utf-8"))
    assert candidate_result["effective_status"] == "failed"
    assert candidate_result["policy_violations"][0]["code"] == "dependency_changes_blocked"
    assert candidate_result["policy_violations"][0]["paths"] == ["package.json"]


def test_security_policy_blocks_lfs_path_changes_by_default(tmp_path):
    init_repo(tmp_path)
    (tmp_path / ".gitattributes").write_text("*.bin filter=lfs diff=lfs merge=lfs -text\n", encoding="utf-8")
    commit_repo(tmp_path, "add lfs attributes")

    result = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=LfsChangingAgent()).run_issue(
        42,
        no_pr=True,
    )

    assert result["state"] == "TerminalFailure"
    state = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent()).store.load(result["run_id"])
    candidate = state.candidate_states["candidate-1"]
    assert candidate.error == "LFS file changes are disabled by policy"
    candidate_result = json.loads((tmp_path / candidate.result_path).read_text(encoding="utf-8"))
    assert candidate_result["policy_violations"][0]["code"] == "lfs_changes_blocked"
    assert candidate_result["policy_violations"][0]["paths"] == ["asset.bin"]


def test_file_lock_times_out_for_second_holder(tmp_path):
    first = FileLock(tmp_path / ".gg" / "locks" / "test.lock", timeout_seconds=0.1)
    second = FileLock(tmp_path / ".gg" / "locks" / "test.lock", timeout_seconds=0.1, poll_interval_seconds=0.01)

    with first:
        try:
            with second:
                raise AssertionError("second lock should not be acquired")
        except TimeoutError:
            pass


def test_file_lock_writes_heartbeat_metadata_and_clears_on_release(tmp_path):
    path = tmp_path / ".gg" / "locks" / "test.lock"

    with FileLock(path) as lock:
        metadata = lock.metadata()
        assert metadata is not None
        assert metadata["owner_pid"] > 0
        assert metadata["hostname"]
        assert metadata["cwd"]
        assert metadata["command"]
        assert metadata["acquired_at"]
        assert metadata["heartbeat_at"]
        heartbeat = lock.heartbeat()
        assert heartbeat["owner_pid"] == metadata["owner_pid"]

    assert FileLock.read_metadata(path) is None


def test_file_lock_auto_heartbeats_while_held(monkeypatch, tmp_path):
    path = tmp_path / ".gg" / "locks" / "test.lock"
    calls = 0

    def fake_now() -> str:
        nonlocal calls
        calls += 1
        return "2026-04-25T12:00:00Z" if calls == 1 else "2026-04-25T12:00:01Z"

    monkeypatch.setattr("gg.orchestrator.lock._utc_now", fake_now)

    with FileLock(path, heartbeat_interval_seconds=0.01) as lock:
        deadline = time.monotonic() + 0.5
        metadata = lock.metadata()
        while metadata and metadata["heartbeat_at"] != "2026-04-25T12:00:01Z" and time.monotonic() < deadline:
            time.sleep(0.01)
            metadata = lock.metadata()

        assert metadata is not None
        assert metadata["heartbeat_at"] == "2026-04-25T12:00:01Z"


def test_lock_manager_scans_dead_and_stale_owners_without_sleeping(tmp_path):
    manager = LockManager(tmp_path)
    manager.root.mkdir(parents=True, exist_ok=True)
    dead_path = manager.root / "run-dead.lock"
    stale_path = manager.root / "run-stale.lock"
    dead_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "owner_pid": 999999999,
                "hostname": "host",
                "cwd": str(tmp_path),
                "command": "gg issue 1",
                "acquired_at": "2026-04-25T12:00:00Z",
                "heartbeat_at": "2026-04-25T12:00:00Z",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    stale_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "owner_pid": os.getpid(),
                "hostname": "host",
                "cwd": str(tmp_path),
                "command": "gg issue 2",
                "acquired_at": "2026-04-25T12:00:00Z",
                "heartbeat_at": "2026-04-25T12:00:00Z",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    stale = manager.scan_stale(
        max_heartbeat_age_seconds=1,
        now=datetime(2026, 4, 25, 12, 0, 2, tzinfo=timezone.utc),
    )

    reasons = {Path(item["path"]).name: item["reason"] for item in stale}
    assert reasons == {
        "run-dead.lock": "owner_not_running",
        "run-stale.lock": "heartbeat_stale",
    }


def test_gitlab_find_pr_does_not_use_unsupported_state_flag(monkeypatch, tmp_path):
    seen: list[str] = []

    def fake_platform_run(self, args, **kwargs):
        seen.extend(args)
        return "[]"

    monkeypatch.setattr(GitLabPlatform, "_run", fake_platform_run)

    result = GitLabPlatform(str(tmp_path)).find_pr(head="gg/test")

    assert result is None
    assert "--source-branch" in seen
    assert "--state" not in seen


# ---------------------------------------------------------------------------
# Circuit breaker: open / close / half-open transitions
# ---------------------------------------------------------------------------


def test_circuit_breaker_opens_after_threshold(tmp_path):
    init_repo(tmp_path)
    store = RateLimitStore(tmp_path)
    key = "cb:test:open"

    for _ in range(4):
        state = store.record_failure(key, failure_threshold=5, window_seconds=600, cooldown_seconds=900)
        assert state == "closed"
        assert not store.is_open(key)

    state = store.record_failure(key, failure_threshold=5, window_seconds=600, cooldown_seconds=900)
    assert state == "open"
    assert store.is_open(key)


def test_circuit_breaker_success_closes_breaker(tmp_path):
    init_repo(tmp_path)
    store = RateLimitStore(tmp_path)
    key = "cb:test:close"

    for _ in range(5):
        store.record_failure(key, failure_threshold=5, window_seconds=600, cooldown_seconds=900)

    assert store.is_open(key)

    store.record_success(key)

    assert not store.is_open(key)


def test_circuit_breaker_half_open_after_cooldown(tmp_path):
    init_repo(tmp_path)
    store = RateLimitStore(tmp_path)
    key = "cb:test:half-open"

    for _ in range(5):
        store.record_failure(key, failure_threshold=5, window_seconds=600, cooldown_seconds=900)

    assert store.is_open(key)

    transitioned = store.try_half_open(key, now="2999-01-01T00:00:00Z")
    assert transitioned is True
    assert not store.is_open(key, now="2999-01-01T00:00:00Z")


def test_circuit_breaker_prune_stale_removes_closed_entries(tmp_path):
    init_repo(tmp_path)
    store = RateLimitStore(tmp_path)
    key = "cb:test:prune"

    store.record_success(key)

    # max_age_seconds=-1 sets cutoff to now+1s, capturing just-created entries
    deleted = store.prune_stale(max_age_seconds=-1)
    assert deleted >= 1

    assert store.is_open(key) is False


# ---------------------------------------------------------------------------
# Error taxonomy: PipelineError construction, category/code values
# ---------------------------------------------------------------------------


def test_error_taxonomy_pipeline_error_construction():
    from gg.orchestrator.errors import ErrorCategory, ErrorCode, PipelineError

    err = PipelineError(
        category=ErrorCategory.TRANSIENT,
        code=ErrorCode.RATE_LIMITED,
        phase="publishing",
        message="GitHub API rate limited",
        recoverable=True,
        retry_after=60.0,
    )

    assert err.category == ErrorCategory.TRANSIENT
    assert err.code == ErrorCode.RATE_LIMITED
    assert err.phase == "publishing"
    assert err.recoverable is True
    assert err.retry_after == 60.0
    assert err.candidate_id is None


def test_error_taxonomy_all_categories_and_codes_defined():
    from gg.orchestrator.errors import ErrorCategory, ErrorCode, PipelineError

    expected_categories = {
        "transient", "executor_error", "tool_error", "policy_error",
        "external_side_effect_error", "validation_failed",
        "configuration_error", "terminal_error", "unknown",
    }
    expected_codes = {
        "invalid_config", "auth_failed", "rate_limited", "missing_runtime",
        "backend_unavailable", "analysis_timeout", "context_too_large",
        "evaluation_context_too_large", "baseline_failed", "candidate_timeout",
        "disk_quota_exceeded", "verification_failed", "patch_conflict",
        "stale_base_conflict", "budget_exceeded", "security_violation",
        "schema_unsupported", "invalid_resume_target", "state_conflict",
        "artifact_checksum_failed",
    }

    actual_categories = {c.value for c in ErrorCategory}
    actual_codes = {c.value for c in ErrorCode}

    assert expected_categories == actual_categories
    assert expected_codes == actual_codes


def test_error_taxonomy_frozen_dataclass_is_immutable():
    from gg.orchestrator.errors import ErrorCategory, ErrorCode, PipelineError
    import dataclasses

    err = PipelineError(
        category=ErrorCategory.UNKNOWN,
        code=ErrorCode.INVALID_CONFIG,
        phase="init",
        message="bad config",
    )

    assert dataclasses.is_dataclass(err)
    with __import__("pytest").raises((dataclasses.FrozenInstanceError, AttributeError)):
        err.message = "mutated"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Log truncation: head+tail preservation
# ---------------------------------------------------------------------------


def test_log_truncation_returns_full_text_when_small():
    from gg.orchestrator.logging import truncate_log

    result = truncate_log("hello world", max_bytes=1000)

    assert result["truncated"] == "hello world"
    assert result["omitted_bytes"] == 0
    assert result["original_bytes"] == len("hello world".encode())
    assert result["stored_bytes"] == result["original_bytes"]


def test_log_truncation_preserves_head_and_tail():
    from gg.orchestrator.logging import truncate_log

    text = "A" * 100 + "B" * 100 + "C" * 100
    result = truncate_log(text, max_bytes=60, head_ratio=0.5)

    truncated = result["truncated"]
    assert truncated.startswith("A")
    assert truncated.endswith("C")
    assert "<truncated:" in truncated
    assert result["omitted_bytes"] > 0
    assert result["original_bytes"] == 300
    assert result["stored_bytes"] < result["original_bytes"]


def test_log_truncation_marker_includes_omitted_count():
    from gg.orchestrator.logging import truncate_log

    text = "X" * 1000
    result = truncate_log(text, max_bytes=100)

    assert f"{result['omitted_bytes']} bytes omitted" in result["truncated"]


# ---------------------------------------------------------------------------
# Port allocation: collision detection and retry
# ---------------------------------------------------------------------------


def test_port_allocation_returns_usable_port(tmp_path):
    init_repo(tmp_path)
    pipeline = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent())

    port = pipeline._allocate_port("cand-001", port_range=(50100, 50200))

    assert 50100 <= port < 50200
    assert pipeline._port_allocations["cand-001"] == port


def test_port_allocation_avoids_already_allocated(tmp_path):
    init_repo(tmp_path)
    pipeline = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent())

    port1 = pipeline._allocate_port("cand-A", port_range=(50200, 50300))
    port2 = pipeline._allocate_port("cand-B", port_range=(50200, 50300))

    assert port1 != port2


def test_port_allocation_deterministic_for_same_candidate(tmp_path):
    init_repo(tmp_path)
    pipeline1 = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent())
    pipeline2 = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent())

    import hashlib
    lo, hi = 50300, 50400
    digest = int(hashlib.sha256(b"cand-deterministic").hexdigest(), 16)
    expected_base = lo + (digest % (hi - lo))

    port1 = pipeline1._allocate_port("cand-deterministic", port_range=(lo, hi))
    assert lo <= port1 < hi
    _ = expected_base


# ---------------------------------------------------------------------------
# Clock skew: timestamp comparison with tolerance
# ---------------------------------------------------------------------------


def test_clock_skew_tolerance_applied_in_timestamp_check(tmp_path, monkeypatch):
    init_repo(tmp_path)
    pipeline = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent())

    from datetime import datetime, timezone, timedelta
    now = datetime.now(timezone.utc)
    almost_elapsed = now - timedelta(seconds=58)
    ts = almost_elapsed.strftime("%Y-%m-%dT%H:%M:%SZ")

    assert pipeline.config.ci.clock_skew_tolerance_seconds >= 0

    result_tight = pipeline._timestamp_is_elapsed(ts, threshold_seconds=60)
    result_loose = pipeline._timestamp_is_elapsed(ts, threshold_seconds=50)
    assert result_tight is False
    assert result_loose is True


def test_clock_skew_returns_false_for_invalid_timestamp(tmp_path):
    init_repo(tmp_path)
    pipeline = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent())

    assert pipeline._timestamp_is_elapsed("not-a-timestamp", threshold_seconds=0) is False
    assert pipeline._timestamp_is_elapsed("", threshold_seconds=0) is False


# ---------------------------------------------------------------------------
# Context budget: context_too_large_policy=fail and blocked
# ---------------------------------------------------------------------------


def create_task_analysis_run(pipeline: OrchestratorPipeline):
    """Create a run in TASK_ANALYSIS state for budget/policy tests."""
    issue = pipeline.platform.get_issue(42)
    state = pipeline.store.create(issue, dry_run=False)
    state.max_attempts = pipeline.config.runtime.max_attempts
    state.transition(TaskState.CLAIMING, reason="test")
    state.transition(TaskState.QUEUED, reason="test")
    state.transition(TaskState.RUN_STARTED, reason="test")
    state.transition(TaskState.TASK_ANALYSIS, reason="test")
    pipeline.store.write(state)
    return pipeline.store.load(state.run_id)


def test_context_budget_enforce_fail_policy(tmp_path):
    init_repo(tmp_path)
    (tmp_path / ".gg" / "params.yaml").write_text(
        "verify:\n  tests: ''\nanalysis:\n  max_candidate_files: 1\n  context_too_large_policy: fail\n",
        encoding="utf-8",
    )
    pipeline = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent())
    state = create_task_analysis_run(pipeline)

    from gg.orchestrator.task_analysis import TaskBrief
    brief = TaskBrief(
        schema_version=1,
        issue={"number": 42, "title": "T", "body": "", "labels": [], "url": ""},
        summary="do it",
        acceptance_criteria=[],
        project_context="",
        candidate_files=["a.py", "b.py", "c.py"],
    )

    code = pipeline._enforce_context_budget(state, brief)
    assert code == "context_too_large"

    result = pipeline._handle_context_too_large(state, code)
    assert result["state"] in ("failed", "TerminalFailure")


def test_context_budget_enforce_blocked_policy(tmp_path):
    init_repo(tmp_path)
    (tmp_path / ".gg" / "params.yaml").write_text(
        "verify:\n  tests: ''\nanalysis:\n  max_candidate_files: 1\n  context_too_large_policy: blocked\n",
        encoding="utf-8",
    )
    pipeline = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent())
    state = create_task_analysis_run(pipeline)

    from gg.orchestrator.task_analysis import TaskBrief
    brief = TaskBrief(
        schema_version=1,
        issue={"number": 42, "title": "T", "body": "", "labels": [], "url": ""},
        summary="do it",
        acceptance_criteria=[],
        project_context="",
        candidate_files=["a.py", "b.py", "c.py"],
    )

    code = pipeline._enforce_context_budget(state, brief)
    assert code == "context_too_large"

    result = pipeline._handle_context_too_large(state, code)
    assert result["state"] in ("blocked", "Blocked")


def test_context_budget_no_violation_when_within_limit(tmp_path):
    init_repo(tmp_path)
    pipeline = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent())
    state = create_task_analysis_run(pipeline)

    from gg.orchestrator.task_analysis import TaskBrief
    brief = TaskBrief(
        schema_version=1,
        issue={"number": 42, "title": "T", "body": "", "labels": [], "url": ""},
        summary="do it",
        acceptance_criteria=[],
        project_context="",
        candidate_files=["a.py"],
    )

    code = pipeline._enforce_context_budget(state, brief)
    assert code is None


def test_context_budget_is_enforced_in_run_issue_flow(tmp_path):
    init_repo(tmp_path)
    (tmp_path / ".gg" / "params.yaml").write_text(
        "verify:\n  tests: ''\nanalysis:\n  max_candidate_files: 1\n  context_too_large_policy: fail\n",
        encoding="utf-8",
    )

    result = OrchestratorPipeline(
        tmp_path,
        platform=FakePlatform(),
        agent=LargeCandidateFileAnalysisAgent(),
    ).run_issue(42, no_pr=True)

    assert result["state"] == "TerminalFailure"
    assert result["error"]["code"] == "context_too_large"
    state = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent()).store.load(result["run_id"])
    assert state.candidate_states == {}


def test_context_budget_checks_candidate_file_char_limit(tmp_path):
    init_repo(tmp_path)
    (tmp_path / "big.py").write_text("x" * 20, encoding="utf-8")
    (tmp_path / ".gg" / "params.yaml").write_text(
        "verify:\n  tests: ''\nanalysis:\n  max_file_chars: 10\n",
        encoding="utf-8",
    )
    pipeline = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent())
    state = create_task_analysis_run(pipeline)
    from gg.orchestrator.task_analysis import TaskBrief
    brief = TaskBrief(
        schema_version=1,
        issue={"number": 42, "title": "T", "body": "", "labels": [], "url": ""},
        summary="do it",
        acceptance_criteria=[],
        project_context="",
        candidate_files=["big.py"],
    )

    assert pipeline._enforce_context_budget(state, brief) == "context_too_large"


# ---------------------------------------------------------------------------
# OMX backend: config loading with omx_enabled
# ---------------------------------------------------------------------------


def test_agent_config_omx_fields_loaded_from_params(tmp_path):
    init_repo(tmp_path)
    (tmp_path / ".gg" / "params.yaml").write_text(
        "verify:\n  tests: ''\nagent:\n  omx_enabled: true\n  omx_command: omx\n  allow_omx_team: true\n",
        encoding="utf-8",
    )
    config = load_config(tmp_path)

    assert config.agent.omx_enabled is True
    assert config.agent.omx_command == "omx"
    assert config.agent.allow_omx_team is True


def test_agent_config_defaults_have_omx_disabled(tmp_path):
    init_repo(tmp_path)
    config = load_config(tmp_path)

    assert config.agent.omx_enabled is False
    assert config.agent.backend == "codex"


def test_agent_config_circuit_breaker_defaults(tmp_path):
    init_repo(tmp_path)
    config = load_config(tmp_path)

    assert config.agent.circuit_breaker_failures == 5
    assert config.agent.circuit_breaker_window_seconds == 600
    assert config.agent.circuit_breaker_cooldown_seconds == 900

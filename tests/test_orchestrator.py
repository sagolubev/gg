from __future__ import annotations

import hashlib
import json
import subprocess
import threading
import time
from pathlib import Path

from click.testing import CliRunner

from gg.agents.base import AgentBackend
from gg.agents.codex import CodexAgent
from gg.cli import cli
from gg.commands.init import _write_params
from gg.orchestrator.config import load_config
from gg.orchestrator.context import ContextSnapshotStore
from gg.orchestrator.evaluation import CandidateEvaluator
from gg.orchestrator.executor import CandidateExecutor
from gg.orchestrator.git import commit_all
from gg.orchestrator.lock import FileLock
from gg.orchestrator.pipeline import OrchestratorPipeline
from gg.orchestrator.plugins import create_agent_backend, register_agent_backend, register_platform
from gg.orchestrator.rate_limit import RateLimitStore, RateLimitSnapshot, RateLimitThrottleError
from gg.orchestrator.sandbox import SandboxPolicy, SandboxRunResult, SandboxRuntime
from gg.orchestrator.schemas import (
    CandidateResultModel,
    GGConfigModel,
    InputArtifactModel,
    RunStateModel,
    validation_error_message,
)
from gg.orchestrator.state import CandidateState, InvalidTransitionError, RunState, TaskState
from gg.orchestrator.store import RunStore
from gg.orchestrator.task_analysis import TaskAnalyzer, extract_single_json_object
from gg.orchestrator.verification import VerificationRunner
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

    def run(self, command, *, cwd, timeout, policy=None, env=None):
        self.commands.append(command)
        self.policies.append(policy)
        self.envs.append(env)
        Path(cwd, "sandboxed.txt").write_text("ok\n", encoding="utf-8")
        Path(command[3]).write_text("sandbox summary\n", encoding="utf-8")
        return SandboxRunResult(
            command=command,
            status="passed",
            exit_code=0,
            stdout="",
            stderr="",
            settings={},
        )


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


def test_pipeline_dry_run_reaches_ready_for_execution(tmp_path):
    init_repo(tmp_path)
    result = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent()).run_issue(
        42,
        dry_run=True,
    )

    assert result["state"] == "ReadyForExecution"
    runs = list((tmp_path / ".gg" / "runs").glob("*/state.json"))
    assert len(runs) == 1
    assert (runs[0].parent / "artifacts" / "task-brief.json").exists()
    assert (runs[0].parent / "artifacts" / "context-snapshot-v1.json").exists()
    assert any((tmp_path / ".gg" / "objects").glob("*/*"))


def test_run_store_uses_unique_run_ids(tmp_path):
    init_repo(tmp_path)
    pipeline = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent())

    first = pipeline.run_issue(42, dry_run=True)
    second = pipeline.run_issue(42, dry_run=True)

    assert first["run_id"] != second["run_id"]
    runs = list((tmp_path / ".gg" / "runs").glob("*/state.json"))
    assert len(runs) == 2


def test_pipeline_no_pr_completes_with_one_candidate(tmp_path):
    init_repo(tmp_path)
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
    assert (run_dir / "candidates" / "candidate-1" / "patch.diff").read_text(encoding="utf-8")
    assert (run_dir / "candidates" / "candidate-1" / "verification.json").exists()
    assert (run_dir / "artifacts" / "evaluation.json").exists()
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
    evaluation = json.loads((run_dir / "artifacts" / "evaluation.json").read_text(encoding="utf-8"))
    assert evaluation["winner"] == "candidate-2"
    assert evaluation["candidates"][1]["score"] > evaluation["candidates"][0]["score"]


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
    ready = pipeline.run_issue(42, dry_run=True)
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

    result = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=RepairAgent()).run_issue(
        42,
        no_pr=True,
    )

    assert result["state"] == "Completed"
    assert result["winner"] == "repair-2-1"
    state = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent()).store.load(result["run_id"])
    assert state.candidate_states["candidate-1"].status == "failed"
    assert state.candidate_states["repair-2-1"].status == "success"


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
    ready = pipeline.run_issue(42, dry_run=True)

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

    ready = pipeline.run_issue(42, dry_run=True)
    state = pipeline.store.load(ready["run_id"])
    state.recover_to(TaskState.BLOCKED, reason="test blocked")
    pipeline.store.write(state)

    provided = pipeline.provide(ready["run_id"], message="Use Spanish")
    refreshed = pipeline.resume(ready["run_id"], no_pr=True)

    assert provided["accepted"] is True
    assert refreshed["state"] == "Completed"
    refreshed_state = pipeline.store.load(ready["run_id"])
    brief_path = tmp_path / refreshed_state.artifacts["task_brief"]
    brief = json.loads(brief_path.read_text(encoding="utf-8"))
    assert brief["issue"]["comments"][0]["body"] == "Please keep the file UTF-8 encoded."
    assert brief["issue"]["inputs"][0]["message"] == "Use Spanish"
    snapshot = json.loads((tmp_path / refreshed_state.artifacts["context_snapshot"]).read_text(encoding="utf-8"))
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


def test_task_analyzer_falls_back_when_agent_json_is_malformed(tmp_path):
    init_repo(tmp_path)
    issue = Issue(number=42, title="Add greeting", body="Write a greeting file.", labels=["ai-ready"])

    brief = TaskAnalyzer(str(tmp_path), agent=MalformedAnalysisAgent()).analyze(issue)

    assert brief.summary == "Write a greeting file."
    assert brief.acceptance_criteria[0] == "Implement the requested issue behavior."


def test_task_analyzer_can_return_blocked_brief(tmp_path):
    init_repo(tmp_path)
    issue = Issue(number=42, title="Add greeting", body="Write a greeting file.", labels=["ai-ready"])

    brief = TaskAnalyzer(str(tmp_path), agent=BlockedAnalysisAgent()).analyze(issue)

    assert brief.blocked is True
    assert brief.missing_questions == ["Which greeting language should be used?"]


def test_pipeline_uses_agent_analysis_blocked_result(tmp_path):
    init_repo(tmp_path)

    result = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=BlockedAnalysisAgent()).run_issue(
        42,
        dry_run=True,
    )

    assert result["state"] == "Blocked"
    assert result["missing_questions"] == ["Which greeting language should be used?"]
    state = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent()).store.load(result["run_id"])
    brief = json.loads((tmp_path / state.artifacts["task_brief"]).read_text(encoding="utf-8"))
    assert brief["blocked"] is True


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
    ready = pipeline.run_issue(42, dry_run=True)
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
    ready = pipeline.run_issue(42, dry_run=True)
    state = pipeline.store.load(ready["run_id"])
    state.recover_to(TaskState.OUTCOME_PUBLISHING, reason="test interrupted publish")
    state.publishing_step = "branch_pushed"
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
    ready = pipeline.run_issue(42, dry_run=True)
    state = pipeline.store.load(ready["run_id"])
    state.recover_to(TaskState.OUTCOME_PUBLISHING, reason="test idempotent publish")
    state.publishing_step = "pr_created"
    state.pr_url = "https://github.com/example/repo/pull/5"
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


def test_publish_fails_with_preflight_artifact_when_base_commit_missing(tmp_path):
    init_repo(tmp_path)
    platform = FakePlatform()
    pipeline = OrchestratorPipeline(tmp_path, platform=platform, agent=FakeAgent())
    ready = pipeline.run_issue(42, dry_run=True)
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
    ready = pipeline.run_issue(42, dry_run=True)

    result = pipeline.retry(ready["run_id"], no_pr=True)

    assert result["state"] == "Completed"
    assert result["retried"] is True


def test_pipeline_transitions_to_needs_input_and_resume_uses_provided_input(tmp_path):
    init_repo(tmp_path)
    platform = FakePlatform()
    pipeline = OrchestratorPipeline(tmp_path, platform=platform, agent=NeedsInputAgent())

    result = pipeline.run_issue(42, no_pr=True)

    assert result["state"] == "NeedsInput"
    state = pipeline.store.load(result["run_id"])
    assert state.artifacts["input_request"].endswith("artifacts/input-request.json")
    assert any("needs local input" in body for _, body in platform.comments)

    provided = pipeline.provide(result["run_id"], message="Use Spanish")

    assert provided["accepted"] is True
    assert provided["state"] == "AgentRunning"

    resumed = pipeline.resume(result["run_id"], no_pr=True)

    assert resumed["state"] == "Completed"
    final_state = pipeline.store.load(result["run_id"])
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


def test_provide_accepts_blocked_run_input(tmp_path):
    init_repo(tmp_path)
    platform = FakePlatform()
    pipeline = OrchestratorPipeline(tmp_path, platform=platform, agent=FakeAgent())
    ready = pipeline.run_issue(42, dry_run=True)
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
    ready = pipeline.run_issue(42, dry_run=True)

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
    OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent()).run_issue(42, dry_run=True)

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
    assert "git_worktree" in check_names


def test_clean_dry_run_lists_only_terminal_runs(tmp_path):
    init_repo(tmp_path)
    pipeline = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent())
    completed = pipeline.run_issue(42, no_pr=True)
    ready = pipeline.run_issue(42, dry_run=True)

    result = pipeline.clean(dry_run=True)

    assert result["runs"] == [completed["run_id"]]
    assert ready["run_id"] not in result["runs"]
    assert (tmp_path / ".gg" / "runs" / completed["run_id"]).exists()


def test_clean_lists_and_removes_stale_waiting_runs(tmp_path):
    init_repo(tmp_path)
    pipeline = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent())
    ready = pipeline.run_issue(42, dry_run=True)
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

    result = pipeline.clean(dry_run=False)

    assert result["runs"] == [completed["run_id"]]
    assert not (tmp_path / ".gg" / "runs" / completed["run_id"]).exists()
    assert not worktree_path.exists()


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
    ready = pipeline.run_issue(42, dry_run=True)

    result = pipeline.cancel(ready["run_id"], reason="test cancel")

    assert result["cancelled"] is True
    assert result["state"] == "Cancelled"
    state = pipeline.store.load(ready["run_id"])
    assert state.last_error["message"] == "test cancel"
    assert (tmp_path / ".gg" / "runs" / ready["run_id"] / "errors.jsonl").exists()


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
    assert config.runtime.setup_timeout_seconds == 600
    assert config.runtime.sandbox_policy.deny_read == ["~/.ssh", ".env"]
    assert config.audit.hash_events is False
    assert config.audit.external_sink == ""
    assert config.security.allow_lfs_changes is False
    assert config.security.allow_binary_changes is True
    assert config.security.allow_dependency_changes is True
    assert config.cleanup.blocked_timeout_days == 14
    assert config.verify.setup == ""
    assert config.verify.tests == "pytest"
    assert config.verify.security == ""
    assert config.verify.custom == ()
    assert config.verify.test_retry_count == 0


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
    from gg.orchestrator.task_analysis import TaskBrief
    task_brief = TaskBrief(
        schema_version=1,
        issue={"number": 42, "title": "Add greeting", "body": "", "labels": ["ai-ready"], "url": ""},
        summary="Do it",
        acceptance_criteria=["Add file"],
        project_context="",
    )

    result = executor.run(run_id="run-123", issue_number=42, brief=task_brief)

    assert result.status == "success"
    assert sandbox.commands
    assert sandbox.commands[0][0:3] == ["codex", "exec", "-o"]
    assert sandbox.policies == [config.runtime.sandbox_policy]


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


def test_context_snapshot_uses_content_addressed_objects(tmp_path):
    init_repo(tmp_path)
    pipeline = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent())
    ready = pipeline.run_issue(42, dry_run=True)
    state = pipeline.store.load(ready["run_id"])
    snapshot = tmp_path / state.artifacts["context_snapshot"]
    data = json.loads(snapshot.read_text(encoding="utf-8"))

    store = ContextSnapshotStore(tmp_path)

    assert "project_context" in data["objects"]
    assert store.read_text(data["objects"]["summary"])


def test_error_logs_mask_secrets(tmp_path):
    init_repo(tmp_path)
    pipeline = OrchestratorPipeline(tmp_path, platform=FakePlatform(), agent=FakeAgent())
    ready = pipeline.run_issue(42, dry_run=True)

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

    ready = pipeline.run_issue(42, dry_run=True)

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

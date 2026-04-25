from __future__ import annotations

import hashlib
import os
import subprocess
import sys
import tempfile
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from gg.agents.base import AgentBackend
from gg.agents.codex import CodexAgent
from gg.orchestrator.config import GGConfig
from gg.orchestrator.git import changed_files, current_commit, diff, safe_branch_slug
from gg.orchestrator.git import WorktreeManager
from gg.orchestrator.sandbox import SandboxPolicy, SandboxRuntime
from gg.orchestrator.schemas import AgentHandoffModel, AgentResultModel, CandidateResultModel
from gg.orchestrator.task_analysis import TaskBrief
from gg.orchestrator.verification import CheckResult, VerificationRunner

NEEDS_INPUT_PREFIX = "NEEDS_INPUT:"

HOST_ENV_ALLOWLIST = frozenset(
    {
        "PATH",
        "HOME",
        "LANG",
        "TERM",
        "OPENAI_API_KEY",
        "GITHUB_TOKEN",
        "GH_TOKEN",
    }
)


@dataclass(frozen=True)
class AgentHandoff:
    schema_version: int
    run_id: str
    candidate_id: str
    issue: dict[str, Any]
    attempt: int
    created_at: str
    worktree_path: str
    base_commit: str
    task_brief_path: str = ""
    context_snapshot_path: str = ""
    instructions: str = ""
    artifacts: dict[str, str] | None = None

    def to_model(self) -> AgentHandoffModel:
        return AgentHandoffModel.model_validate(self.to_dict(validate=False))

    def to_dict(self, *, validate: bool = True) -> dict:
        data = asdict(self)
        data["artifacts"] = data["artifacts"] or {}
        if validate:
            AgentHandoffModel.model_validate(data)
        return data


@dataclass(frozen=True)
class AgentResult:
    schema_version: int
    run_id: str
    candidate_id: str
    status: str
    started_at: str = ""
    finished_at: str = ""
    duration_seconds: float | None = None
    exit_code: int | None = None
    summary: str = ""
    error: str | None = None
    changed_files: list[str] | None = None
    artifacts: dict[str, str] | None = None
    metrics: dict[str, Any] | None = None

    def to_model(self) -> AgentResultModel:
        return AgentResultModel.model_validate(self.to_dict(validate=False))

    def to_dict(self, *, validate: bool = True) -> dict:
        data = asdict(self)
        data["changed_files"] = data["changed_files"] or []
        data["artifacts"] = data["artifacts"] or {}
        data["metrics"] = data["metrics"] or {}
        if validate:
            AgentResultModel.model_validate(data)
        return data


@dataclass(frozen=True)
class CandidateResult:
    schema_version: int
    candidate_id: str
    status: str
    branch: str
    worktree_path: str
    base_commit: str
    summary: str
    changed_files: list[str]
    patch: str
    duration_seconds: float
    error: str | None = None
    setup: dict | None = None

    def to_dict(self) -> dict:
        data = asdict(self)
        CandidateResultModel.model_validate(data)
        return data


class CandidateExecutor:
    def __init__(
        self,
        project_path: str | Path,
        agent: AgentBackend,
        config: GGConfig,
        *,
        sandbox: SandboxRuntime | None = None,
    ):
        self.project_path = Path(project_path).resolve()
        self.agent = agent
        self.config = config
        self._sandbox_explicit = sandbox is not None
        self.sandbox = sandbox or SandboxRuntime()

    def build_agent_handoff(
        self,
        *,
        run_id: str,
        candidate_id: str,
        issue: dict[str, Any],
        worktree_path: str | Path,
        base_commit: str,
        instructions: str,
        attempt: int = 1,
        task_brief_path: str = "",
        context_snapshot_path: str = "",
        artifacts: dict[str, str] | None = None,
        created_at: str | None = None,
    ) -> AgentHandoff:
        return AgentHandoff(
            schema_version=1,
            run_id=run_id,
            candidate_id=candidate_id,
            issue=issue,
            attempt=attempt,
            created_at=created_at or _utc_now(),
            worktree_path=str(worktree_path),
            base_commit=base_commit,
            task_brief_path=task_brief_path,
            context_snapshot_path=context_snapshot_path,
            instructions=instructions,
            artifacts=artifacts or {},
        )

    def build_agent_result(
        self,
        *,
        run_id: str,
        candidate: CandidateResult,
        started_at: str = "",
        finished_at: str = "",
        exit_code: int | None = None,
        artifacts: dict[str, str] | None = None,
        metrics: dict[str, Any] | None = None,
    ) -> AgentResult:
        return AgentResult(
            schema_version=1,
            run_id=run_id,
            candidate_id=candidate.candidate_id,
            status=candidate.status,
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=candidate.duration_seconds,
            exit_code=exit_code,
            summary=candidate.summary,
            error=candidate.error,
            changed_files=candidate.changed_files,
            artifacts=artifacts or {},
            metrics=metrics or {},
        )

    def sandbox_preflight_error(self) -> str | None:
        if not self._requires_sandbox_preflight():
            return None
        if self.sandbox.is_available():
            return None
        return "sandbox-runtime is required but unavailable"

    def _requires_sandbox_preflight(self) -> bool:
        return (
            self.config.runtime.use_sandbox_runtime
            and self.config.runtime.require_sandbox_runtime
            and not self.config.runtime.allow_unsafe_direct_exec
            and (isinstance(self.agent, CodexAgent) or self._sandbox_explicit)
        )

    def run(self, *, run_id: str, issue_number: int, brief: TaskBrief, candidate_id: str = "candidate-1",
            strategy: str = "conservative") -> CandidateResult:
        sandbox_error = self.sandbox_preflight_error()
        if sandbox_error is not None:
            raise RuntimeError(sandbox_error)
        base_commit = current_commit(self.project_path)
        branch_suffix = hashlib.sha256(run_id.encode("utf-8")).hexdigest()[:8]
        branch = f"gg/issue-{issue_number}-{safe_branch_slug(brief.issue['title'])}-{candidate_id}-{branch_suffix}"
        worktree = WorktreeManager(self.project_path).create(
            run_id=run_id,
            candidate_id=candidate_id,
            branch=branch,
            base_ref=base_commit,
        )
        prompt = self._prompt(brief, strategy=strategy)
        started = time.monotonic()
        try:
            setup = self._run_setup(worktree)
            if setup.status not in {"passed", "skipped", "flaky"}:
                return CandidateResult(
                    schema_version=1,
                    candidate_id=candidate_id,
                    status="setup_failed",
                    branch=branch,
                    worktree_path=str(worktree),
                    base_commit=base_commit,
                    summary="Candidate setup failed.",
                    changed_files=changed_files(worktree),
                    patch="",
                    duration_seconds=round(time.monotonic() - started, 3),
                    error="candidate setup failed",
                    setup=setup.to_dict(),
                )
            summary = self._generate(prompt, worktree)
            needs_input = _extract_needs_input(summary)
            files = changed_files(worktree)
            patch = diff(worktree) if files else ""
            if needs_input and not files:
                status = "needs_input"
                error = needs_input
            else:
                status = "success" if files else "failed"
                error = None if files else "agent produced no file changes"
            return CandidateResult(
                schema_version=1,
                candidate_id=candidate_id,
                status=status,
                branch=branch,
                worktree_path=str(worktree),
                base_commit=base_commit,
                summary=(needs_input or summary).strip() or "Agent completed.",
                changed_files=files,
                patch=patch,
                duration_seconds=round(time.monotonic() - started, 3),
                error=error,
                setup=setup.to_dict(),
            )
        except Exception as exc:
            return CandidateResult(
                schema_version=1,
                candidate_id=candidate_id,
                status="failed",
                branch=branch,
                worktree_path=str(worktree),
                base_commit=base_commit,
                summary="Agent failed.",
                changed_files=[],
                patch="",
                duration_seconds=round(time.monotonic() - started, 3),
                error=str(exc),
            )

    def _run_setup(self, worktree: Path) -> CheckResult:
        command = self.config.verify.setup.strip()
        if not command:
            return CheckResult(command="", status="skipped", exit_code=None, attempts=0)
        return VerificationRunner(
            [command],
            timeout=self.config.runtime.setup_timeout_seconds,
            env=self._candidate_env(worktree),
        ).run(worktree)[0]

    def _generate(self, prompt: str, worktree: Path) -> str:
        if self.config.runtime.use_sandbox_runtime and isinstance(self.agent, CodexAgent):
            if self.sandbox.is_available():
                return self._generate_in_sandbox(prompt, worktree)
        if self.sandbox_preflight_error() is not None:
            raise RuntimeError("sandbox-runtime is required but unavailable")
        return self.agent.generate(
            prompt,
            cwd=str(worktree),
            timeout=self.config.runtime.candidate_timeout_seconds,
        )

    def _generate_in_sandbox(self, prompt: str, worktree: Path) -> str:
        out_path = Path(tempfile.mktemp(prefix="gg-candidate-", suffix=".md", dir=str(worktree)))
        result = self.sandbox.run(
            ["codex", "exec", "-o", str(out_path), prompt],
            cwd=worktree,
            timeout=self.config.runtime.candidate_timeout_seconds,
            policy=self._sandbox_policy(),
            env=self._candidate_env(worktree),
        )
        output = out_path.read_text(encoding="utf-8").strip() if out_path.exists() else ""
        out_path.unlink(missing_ok=True)
        if result.status == "timeout":
            raise subprocess.TimeoutExpired("codex exec", self.config.runtime.candidate_timeout_seconds)
        if result.status != "passed" and not output:
            raise RuntimeError(result.stderr.strip() or "sandboxed codex execution failed")
        return output

    def _sandbox_policy(self) -> SandboxPolicy:
        return self.config.runtime.sandbox_policy

    def _candidate_env(self, worktree: Path) -> dict[str, str]:
        cache_root = worktree / ".gg-cache"
        cache_root.mkdir(parents=True, exist_ok=True)
        env = {
            key: value
            for key, value in os.environ.items()
            if key in HOST_ENV_ALLOWLIST or key.startswith("LC_")
        }
        python_bin = str(Path(sys.executable).parent)
        path_parts = env.get("PATH", "").split(os.pathsep) if env.get("PATH") else []
        if python_bin not in path_parts:
            env["PATH"] = os.pathsep.join([python_bin, *path_parts]) if path_parts else python_bin
        env.update(
            {
                "PIP_CACHE_DIR": str(cache_root / "pip"),
                "UV_CACHE_DIR": str(cache_root / "uv"),
                "npm_config_cache": str(cache_root / "npm"),
                "YARN_CACHE_FOLDER": str(cache_root / "yarn"),
                "PNPM_HOME": str(cache_root / "pnpm"),
                "PNPM_STORE_DIR": str(cache_root / "pnpm-store"),
                "XDG_CACHE_HOME": str(cache_root / "xdg"),
                "CARGO_HOME": str(cache_root / "cargo"),
                "GOCACHE": str(cache_root / "go-build"),
                "GOMODCACHE": str(cache_root / "go-mod"),
            }
        )
        return env

    def _prompt(self, brief: TaskBrief, *, strategy: str) -> str:
        criteria = "\n".join(f"- {item}" for item in brief.acceptance_criteria)
        strategy_text = {
            "conservative": "Minimize the diff and prefer existing patterns. Do not add abstractions unless required.",
            "test-first": "Prefer adding or updating a regression test before the implementation when practical.",
            "architecture-aware": "Consider module boundaries and make small structural improvements only when they reduce risk.",
        }.get(strategy, strategy)
        if strategy.startswith("repair:"):
            base_strategy = strategy.split(":", 1)[1]
            strategy_text = (
                "Repair a previous failed candidate. Focus on producing a passing, minimal patch. "
                f"Base strategy hint: {base_strategy}."
            )
        return (
            "You are implementing a GitHub issue in this repository.\n"
            "Make the smallest correct code change, update tests when needed, and leave the worktree with the patch applied.\n"
            "Do not create commits or push. The orchestrator will commit and publish after verification.\n\n"
            f"If you cannot continue without a human answer, make no file changes and respond with exactly one line starting with {NEEDS_INPUT_PREFIX!r} followed by the concise question.\n\n"
            f"Strategy: {strategy}\n{strategy_text}\n\n"
            f"Issue #{brief.issue['number']}: {brief.issue['title']}\n\n"
            f"Summary:\n{brief.summary}\n\n"
            f"Acceptance criteria:\n{criteria}\n\n"
            f"Project context:\n{brief.project_context}\n\n"
            "Return a concise implementation summary."
        )


def _extract_needs_input(summary: str) -> str | None:
    text = summary.strip()
    if not text.startswith(NEEDS_INPUT_PREFIX):
        return None
    message = text[len(NEEDS_INPUT_PREFIX):].strip()
    return message or "Agent requested additional input."


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

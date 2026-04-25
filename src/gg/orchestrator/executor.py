from __future__ import annotations

import hashlib
import subprocess
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path

from gg.agents.base import AgentBackend
from gg.agents.codex import CodexAgent
from gg.orchestrator.config import GGConfig
from gg.orchestrator.git import changed_files, current_commit, diff, safe_branch_slug
from gg.orchestrator.git import WorktreeManager
from gg.orchestrator.sandbox import SandboxPolicy, SandboxRuntime
from gg.orchestrator.task_analysis import TaskBrief

NEEDS_INPUT_PREFIX = "NEEDS_INPUT:"


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

    def to_dict(self) -> dict:
        return asdict(self)


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
        self.sandbox = sandbox or SandboxRuntime()

    def run(self, *, run_id: str, issue_number: int, brief: TaskBrief, candidate_id: str = "candidate-1",
            strategy: str = "conservative") -> CandidateResult:
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

    def _generate(self, prompt: str, worktree: Path) -> str:
        if (
            self.config.runtime.use_sandbox_runtime
            and isinstance(self.agent, CodexAgent)
        ):
            if self.sandbox.is_available():
                return self._generate_in_sandbox(prompt, worktree)
            if self.config.runtime.require_sandbox_runtime:
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

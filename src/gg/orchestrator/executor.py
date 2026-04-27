from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from gg.agents.base import AgentBackend
from gg.agents.claude import ClaudeAgent
from gg.agents.codex import CodexAgent
from gg.orchestrator.config import GGConfig
from gg.orchestrator.git import changed_files, current_commit, diff, safe_branch_slug
from gg.orchestrator.git import WorktreeManager
from gg.orchestrator.sandbox import SandboxPolicy, SandboxRuntime
from gg.orchestrator.schemas import AgentHandoffModel, AgentResultModel, CandidateResultModel
from gg.orchestrator.task_analysis import TaskBrief
from gg.orchestrator.verification import CheckResult, VerificationRunner

NEEDS_INPUT_PREFIX = "NEEDS_INPUT:"
CandidateStatusCallback = Callable[[dict[str, Any]], None]
AgentHandoffCallback = Callable[["AgentHandoff"], str | None]

HOST_ENV_ALLOWLIST = frozenset(
    {
        "PATH",
        "HOME",
        "LANG",
        "TERM",
        "OPENAI_API_KEY",
        "OPENAI_API_BASE",
        "OPENAI_API_TYPE",
        "OPENAI_API_VERSION",
        "OPENAI_BASE_URL",
        "AZURE_OPENAI_API_KEY",
        "AZURE_OPENAI_ENDPOINT",
        "AZURE_OPENAI_API_VERSION",
        "ANTHROPIC_API_KEY",
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
    port: int | None = None
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
    agent_pid: int | None = None
    sandbox_pid: int | None = None

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
        self.config = config
        self.agent = _configured_agent(agent, config)
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
        port: int | None = None,
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
            port=port,
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
        return (
            "sandbox-runtime (srt-py) is required but not found in PATH. "
            "Install: pip install sandbox-runtime  "
            "Or disable: set runtime.use_sandbox_runtime=false in .gg/params.yaml"
        )

    def sandbox_preflight(self) -> dict[str, Any]:
        required = self._requires_sandbox_preflight()
        available = self.sandbox.is_available()
        executable = str(getattr(self.sandbox, "executable", ""))
        executable_path = _call_optional(self.sandbox, "executable_path")
        version = _call_optional(self.sandbox, "version") if available else None
        mode = (
            "sandbox"
            if self.config.runtime.use_sandbox_runtime and available
            else (
                "unsafe-direct-exec"
                if self.config.runtime.allow_unsafe_direct_exec
                else "direct-exec"
            )
        )
        error = "sandbox-runtime is required but unavailable" if required and not available else None
        return {
            "schema_version": 1,
            "mode": mode,
            "backend": self.agent.backend_name(),
            "required": required,
            "available": available,
            "use_sandbox_runtime": self.config.runtime.use_sandbox_runtime,
            "allow_unsafe_direct_exec": self.config.runtime.allow_unsafe_direct_exec,
            "executable": executable,
            "executable_path": executable_path,
            "version": version,
            "error": error,
        }

    def _requires_sandbox_preflight(self) -> bool:
        return (
            self.config.runtime.use_sandbox_runtime
            and self.config.runtime.require_sandbox_runtime
            and not self.config.runtime.allow_unsafe_direct_exec
            and (self.agent.supports_sandbox_execution() or self._sandbox_explicit)
        )

    def run(
        self,
        *,
        run_id: str,
        issue_number: int,
        brief: TaskBrief,
        candidate_id: str = "candidate-1",
        strategy: str = "conservative",
        repair_context: dict[str, Any] | None = None,
        on_status: CandidateStatusCallback | None = None,
        on_handoff: AgentHandoffCallback | None = None,
        attempt: int = 1,
        task_brief_path: str = "",
        context_snapshot_path: str = "",
        port: int | None = None,
    ) -> CandidateResult:
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
        if on_status is not None:
            on_status({"worktree_path": str(worktree), "branch": branch})
            on_status({"message": f"candidate worktree ready: {worktree} ({branch})"})
        if on_handoff is not None:
            on_handoff(
                self.build_agent_handoff(
                    run_id=run_id,
                    candidate_id=candidate_id,
                    issue=brief.issue,
                    worktree_path=worktree,
                    base_commit=base_commit,
                    instructions=f"strategy={strategy}\n{_repair_context_summary(repair_context)}".strip(),
                    attempt=attempt,
                    task_brief_path=task_brief_path,
                    context_snapshot_path=context_snapshot_path,
                    port=port,
                    artifacts={},
                )
            )
        prompt = self._prompt(brief, strategy=strategy, repair_context=repair_context)
        started = time.monotonic()
        runtime: dict[str, Any] = {}
        runtime_callback = _merge_status_callbacks(runtime, on_status)
        try:
            if on_status is not None:
                setup_command = self.config.verify.setup.strip()
                on_status({"message": f"setup starting{f': {setup_command}' if setup_command else ' (no setup command)'}"})
            setup = self._run_setup(worktree, port=port)
            if on_status is not None:
                on_status({"message": f"setup finished: status={setup.status} exit={setup.exit_code}"})
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
                    agent_pid=runtime.get("agent_pid"),
                    sandbox_pid=runtime.get("sandbox_pid"),
                )
            summary = self._generate(prompt, worktree, port=port, on_status=runtime_callback)
            needs_input = _extract_needs_input(summary)
            files = changed_files(worktree)
            patch = diff(worktree) if files else ""
            if on_status is not None:
                on_status({"message": f"agent output received: changed_files={len(files)} needs_input={'yes' if needs_input else 'no'}"})
            quota_error = self._disk_quota_error(worktree)
            if quota_error:
                status = "failed"
                error = quota_error
            elif needs_input and not files:
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
                agent_pid=runtime.get("agent_pid"),
                sandbox_pid=runtime.get("sandbox_pid"),
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
                agent_pid=runtime.get("agent_pid"),
                sandbox_pid=runtime.get("sandbox_pid"),
            )

    def _run_setup(self, worktree: Path, *, port: int | None = None) -> CheckResult:
        command = self.config.verify.setup.strip()
        if not command:
            return CheckResult(command="", status="skipped", exit_code=None, attempts=0)
        return VerificationRunner(
            [command],
            timeout=self.config.runtime.setup_timeout_seconds,
            env=self._candidate_env(worktree, port=port),
        ).run(worktree)[0]

    def _generate(
        self,
        prompt: str,
        worktree: Path,
        *,
        port: int | None = None,
        on_status: CandidateStatusCallback | None = None,
    ) -> str:
        backend_agent = _with_progress_callback(
            self.agent,
            (lambda message: on_status({"message": message}) if on_status is not None else None),
        )
        if self.config.runtime.use_sandbox_runtime and backend_agent.supports_sandbox_execution():
            if self.sandbox.is_available():
                if on_status is not None:
                    on_status({"message": f"starting backend {backend_agent.backend_name()} via sandbox"})
                return self._generate_in_sandbox(
                    prompt,
                    worktree,
                    port=port,
                    on_status=on_status,
                    agent=backend_agent,
                )
        if self.sandbox_preflight_error() is not None:
            raise RuntimeError("sandbox-runtime is required but unavailable")
        if on_status is not None:
            on_status({"message": f"starting backend {backend_agent.backend_name()} via direct execution"})
        return backend_agent.generate(
            prompt,
            cwd=str(worktree),
            timeout=self.config.runtime.candidate_timeout_seconds,
        )

    def _generate_in_sandbox(
        self,
        prompt: str,
        worktree: Path,
        *,
        port: int | None = None,
        on_status: CandidateStatusCallback | None = None,
        agent: AgentBackend | None = None,
    ) -> str:
        out_path = Path(tempfile.mktemp(prefix="gg-candidate-", suffix=".md", dir=str(worktree)))
        backend_agent = agent or self.agent
        command = list(backend_agent.build_sandbox_command(prompt, output_path=str(out_path)))
        result = self.sandbox.run(
            command,
            cwd=worktree,
            timeout=self.config.runtime.candidate_timeout_seconds,
            policy=self._sandbox_policy(),
            env=self._candidate_env(worktree, port=port),
            on_process_start=(
                (lambda pid: on_status({"sandbox_pid": pid, "message": f"sandbox started: pid={pid}"}))
                if on_status is not None
                else None
            ),
        )
        output = out_path.read_text(encoding="utf-8").strip() if out_path.exists() else result.stdout.strip()
        out_path.unlink(missing_ok=True)
        if result.status == "timeout":
            raise subprocess.TimeoutExpired(" ".join(command[:2]), self.config.runtime.candidate_timeout_seconds)
        if result.status != "passed" and not output:
            raise RuntimeError(result.stderr.strip() or "sandboxed agent execution failed")
        return output

    def _sandbox_policy(self) -> SandboxPolicy:
        policy = self.config.runtime.sandbox_policy
        network = self.config.runtime.network
        allowed = list(policy.allowed_domains)
        if network.default == "deny":
            for host in network.allowed_hosts:
                if host not in allowed:
                    allowed.append(host)
            for host in _lm_api_hosts():
                if host not in allowed:
                    allowed.append(host)
        allow_write = list(policy.allow_write)
        codex_home = str(Path.home() / ".codex")
        if codex_home not in allow_write:
            allow_write.append(codex_home)
        return SandboxPolicy(
            allowed_domains=allowed,
            denied_domains=list(policy.denied_domains),
            deny_read=list(policy.deny_read),
            allow_write=allow_write,
            deny_write=list(policy.deny_write),
        )

    def _candidate_env(self, worktree: Path, *, port: int | None = None) -> dict[str, str]:
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
        if port is not None:
            env["PORT"] = str(port)
            env["GG_CANDIDATE_PORT"] = str(port)
        return env

    def _disk_quota_error(self, worktree: Path) -> str | None:
        max_disk_mb = self.config.runtime.resource.max_disk_mb
        if max_disk_mb <= 0:
            return None
        usage_mb = _directory_size_mb(worktree, timeout_seconds=5.0)
        if usage_mb <= max_disk_mb:
            return None
        return f"disk_quota_exceeded: candidate used {usage_mb}MB, limit is {max_disk_mb}MB"

    def _prompt(
        self,
        brief: TaskBrief,
        *,
        strategy: str,
        repair_context: dict[str, Any] | None = None,
    ) -> str:
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
        repair_section = _repair_context_section(repair_context)
        structured_context = _structured_brief_section(brief)
        return (
            "You are implementing a GitHub issue in this repository.\n"
            "Make the smallest correct code change, update tests when needed, and leave the worktree with the patch applied.\n"
            "Do not create commits or push. The orchestrator will commit and publish after verification.\n\n"
            f"If you cannot continue without a human answer, make no file changes and respond with exactly one line starting with {NEEDS_INPUT_PREFIX!r} followed by the concise question.\n\n"
            f"Strategy: {strategy}\n{strategy_text}\n\n"
            f"{repair_section}"
            f"Issue #{brief.issue['number']}: {brief.issue['title']}\n\n"
            f"Summary:\n{brief.summary}\n\n"
            f"Acceptance criteria:\n{criteria}\n\n"
            f"Project context:\n{brief.project_context}\n\n"
            f"{structured_context}"
            "Return a concise implementation summary."
        )


def _merge_status_callbacks(
    runtime: dict[str, Any],
    callback: CandidateStatusCallback | None,
) -> CandidateStatusCallback:
    def update(payload: dict[str, Any]) -> None:
        runtime.update(payload)
        if callback is not None:
            callback(payload)

    return update


def _extract_needs_input(summary: str) -> str | None:
    text = summary.strip()
    if not text.startswith(NEEDS_INPUT_PREFIX):
        return None
    message = text[len(NEEDS_INPUT_PREFIX):].strip()
    return message or "Agent requested additional input."


def _directory_size_mb(path: Path, *, timeout_seconds: float) -> int:
    deadline = time.monotonic() + timeout_seconds
    total_bytes = 0
    pending = [path]
    while pending:
        if time.monotonic() > deadline:
            return 0
        current = pending.pop()
        try:
            entries = list(os.scandir(current))
        except OSError:
            continue
        for entry in entries:
            try:
                if entry.is_dir(follow_symlinks=False):
                    pending.append(Path(entry.path))
                elif entry.is_file(follow_symlinks=False):
                    total_bytes += entry.stat(follow_symlinks=False).st_size
            except OSError:
                continue
    return (total_bytes + (1024 * 1024) - 1) // (1024 * 1024)


def _repair_context_section(repair_context: dict[str, Any] | None) -> str:
    if not repair_context:
        return ""
    parent = repair_context.get("parent_candidate_id") or "unknown"
    feedback = str(repair_context.get("feedback") or "").strip()
    failed_commands = repair_context.get("failed_commands") or []
    lines = [
        "Repair context:",
        f"- Parent candidate: {parent}",
    ]
    if feedback:
        lines.append(f"- Evaluator feedback: {feedback[:2000]}")
    if failed_commands:
        lines.append(f"- Failed verification commands: {', '.join(map(str, failed_commands))[:1000]}")
    return "\n".join(lines) + "\n\n"


def _repair_context_summary(repair_context: dict[str, Any] | None) -> str:
    if not repair_context:
        return ""
    parent = repair_context.get("parent_candidate_id") or "unknown"
    feedback = str(repair_context.get("feedback") or "").strip()
    return f"repair parent={parent}; feedback={feedback[:500]}"


def _structured_brief_section(brief: TaskBrief) -> str:
    payload = {
        "classification": brief.classification,
        "implementation": brief.implementation,
        "verification": brief.verification,
        "project_context_details": brief.project_context_details,
    }
    payload = {key: value for key, value in payload.items() if value}
    if not payload:
        return ""
    return (
        "Structured task brief:\n"
        f"{json.dumps(payload, indent=2, ensure_ascii=False)}\n\n"
    )


def _call_optional(target: Any, method_name: str) -> Any:
    method = getattr(target, method_name, None)
    if method is None:
        return None
    try:
        return method()
    except Exception:
        return None


def _configured_agent(agent: AgentBackend, config: GGConfig) -> AgentBackend:
    if isinstance(agent, CodexAgent):
        return CodexAgent(
            console=getattr(agent, "_console", None),
            debug=getattr(agent, "_debug", False),
            command=config.agent.codex_command,
        )
    if isinstance(agent, ClaudeAgent):
        return ClaudeAgent(
            console=getattr(agent, "_console", None),
            debug=getattr(agent, "_debug", False),
            command=config.agent.claude_command,
        )
    return agent


def _with_progress_callback(
    agent: AgentBackend,
    progress_callback: Callable[[str], None] | None,
) -> AgentBackend:
    if isinstance(agent, CodexAgent):
        return CodexAgent(
            console=getattr(agent, "_console", None),
            debug=getattr(agent, "_debug", False),
            command=" ".join(agent._command_args()),
            progress_callback=progress_callback,
        )
    if isinstance(agent, ClaudeAgent):
        return ClaudeAgent(
            console=getattr(agent, "_console", None),
            debug=getattr(agent, "_debug", False),
            command=" ".join(agent._command_args()),
            progress_callback=progress_callback,
        )
    return agent


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _lm_api_hosts() -> list[str]:
    """Return LLM API hosts that should be reachable from the sandbox."""
    from urllib.parse import urlparse
    hosts: list[str] = [
        "api.openai.com",
        "api.anthropic.com",
    ]
    for var in ("AZURE_OPENAI_ENDPOINT", "OPENAI_API_BASE", "OPENAI_BASE_URL"):
        val = os.environ.get(var, "").strip()
        if val:
            parsed = urlparse(val)
            host = parsed.netloc or parsed.path
            host = host.split(":")[0].strip()
            if host and host not in hosts:
                hosts.append(host)
    return hosts

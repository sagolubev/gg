from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from gg.orchestrator.sandbox import SandboxPolicy
from gg.orchestrator.schemas import GGConfigModel, validation_error_message
from gg.utils.git_ops import get_main_branch


@dataclass(frozen=True)
class TaskSystemConfig:
    platform: str = "auto"
    work_label: str = "gg:in-progress"
    done_label: str = "gg:done"
    blocked_label: str = "gg:blocked"


@dataclass(frozen=True)
class SelectionConfig:
    include_labels: tuple[str, ...] = ("ai-ready",)
    exclude_labels: tuple[str, ...] = ("gg:in-progress", "gg:blocked", "gg:done")


@dataclass(frozen=True)
class RuntimeConfig:
    agent_backend: str = "codex"
    candidates: int = 1
    max_parallel_candidates: int = 1
    max_parallel_runs: int = 1
    max_attempts: int = 1
    repair_candidates: int = 1
    use_sandbox_runtime: bool = True
    require_sandbox_runtime: bool = False
    candidate_timeout_seconds: int = 1800
    command_timeout_seconds: int = 600
    setup_timeout_seconds: int = 600
    sandbox_policy: SandboxPolicy = field(default_factory=SandboxPolicy)


@dataclass(frozen=True)
class VerifyConfig:
    setup: str = ""
    tests: str = ""
    lint: str = ""
    typecheck: str = ""
    security: str = ""
    custom: tuple[str, ...] = ()
    test_retry_count: int = 0
    allow_known_baseline_failures: bool = False

    def commands(self) -> list[str]:
        return [
            cmd
            for cmd in (self.setup, self.tests, self.lint, self.typecheck, self.security, *self.custom)
            if cmd.strip()
        ]


@dataclass(frozen=True)
class GitConfig:
    default_branch: str = "main"
    author_name: str = "gg-orchestrator"
    author_email: str = "gg-orchestrator@users.noreply.local"


@dataclass(frozen=True)
class AuditConfig:
    hash_events: bool = False
    external_sink: str = ""


@dataclass(frozen=True)
class SecurityConfig:
    allow_lfs_changes: bool = False
    allow_binary_changes: bool = True
    allow_dependency_changes: bool = True


@dataclass(frozen=True)
class GGConfig:
    git: GitConfig
    task_system: TaskSystemConfig = field(default_factory=TaskSystemConfig)
    selection: SelectionConfig = field(default_factory=SelectionConfig)
    verify: VerifyConfig = field(default_factory=VerifyConfig)
    runtime: RuntimeConfig = field(default_factory=RuntimeConfig)
    audit: AuditConfig = field(default_factory=AuditConfig)
    security: SecurityConfig = field(default_factory=SecurityConfig)


def _mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def load_config(project_path: str | Path) -> GGConfig:
    root = Path(project_path).resolve()
    params_path = root / ".gg" / "params.yaml"
    raw: dict[str, Any] = {}
    if params_path.exists():
        raw = yaml.safe_load(params_path.read_text(encoding="utf-8")) or {}
        if not isinstance(raw, dict):
            raise ValueError(f"{params_path}: expected YAML mapping")
    project = _mapping(raw.get("project"))
    git = _mapping(raw.get("git"))
    task_system = _mapping(raw.get("task_system"))
    selection = _mapping(raw.get("selection"))
    verify = _mapping(raw.get("verify"))
    runtime = _mapping(raw.get("runtime"))
    audit = _mapping(raw.get("audit"))
    security = _mapping(raw.get("security"))
    sandbox_policy = _mapping(runtime.get("sandbox_policy"))
    default_branch = project.get("default_branch") or git.get("default_branch") or get_main_branch(root)
    try:
        model = GGConfigModel.model_validate(
            {
                "git": {
                    "default_branch": default_branch,
                    "author_name": git.get("author_name", "gg-orchestrator"),
                    "author_email": git.get("author_email", "gg-orchestrator@users.noreply.local"),
                },
                "task_system": {
                    "platform": task_system.get("platform", raw.get("platform", "auto")),
                    "work_label": task_system.get("work_label", "gg:in-progress"),
                    "done_label": task_system.get("done_label", "gg:done"),
                    "blocked_label": task_system.get("blocked_label", "gg:blocked"),
                },
                "selection": {
                    "include_labels": selection.get("include_labels", ["ai-ready"]),
                    "exclude_labels": selection.get(
                        "exclude_labels",
                        ["gg:in-progress", "gg:blocked", "gg:done"],
                    ),
                },
                "verify": {
                    "setup": verify.get("setup", ""),
                    "tests": verify.get("tests", _default_test_command(root)),
                    "lint": verify.get("lint", ""),
                    "typecheck": verify.get("typecheck", ""),
                    "security": verify.get("security", ""),
                    "custom": verify.get("custom", []),
                    "test_retry_count": verify.get("test_retry_count", 0),
                    "allow_known_baseline_failures": verify.get("allow_known_baseline_failures", False),
                },
                "runtime": {
                    "agent_backend": runtime.get("agent_backend", raw.get("agent_backend", "codex")),
                    "candidates": runtime.get("candidates", 1),
                    "max_parallel_candidates": runtime.get("max_parallel_candidates", 1),
                    "max_parallel_runs": runtime.get("max_parallel_runs", 1),
                    "max_attempts": runtime.get("max_attempts", 1),
                    "repair_candidates": runtime.get("repair_candidates", 1),
                    "use_sandbox_runtime": runtime.get("use_sandbox_runtime", True),
                    "require_sandbox_runtime": runtime.get("require_sandbox_runtime", False),
                    "candidate_timeout_seconds": runtime.get("candidate_timeout_seconds", 1800),
                    "command_timeout_seconds": runtime.get("command_timeout_seconds", 600),
                    "setup_timeout_seconds": runtime.get("setup_timeout_seconds", 600),
                    "sandbox_policy": {
                        "allowed_domains": sandbox_policy.get("allowed_domains", []),
                        "denied_domains": sandbox_policy.get("denied_domains", []),
                        "deny_read": sandbox_policy.get("deny_read", ["~/.ssh", ".env"]),
                        "allow_write": sandbox_policy.get("allow_write", ["."]),
                        "deny_write": sandbox_policy.get("deny_write", [".env"]),
                    },
                },
                "audit": {
                    "hash_events": audit.get("hash_events", False),
                    "external_sink": audit.get("external_sink", ""),
                },
                "security": {
                    "allow_lfs_changes": security.get("allow_lfs_changes", False),
                    "allow_binary_changes": security.get("allow_binary_changes", True),
                    "allow_dependency_changes": security.get("allow_dependency_changes", True),
                },
            }
        )
    except Exception as exc:
        raise ValueError(validation_error_message(str(params_path), exc)) from exc
    return GGConfig(
        git=GitConfig(
            default_branch=model.git.default_branch,
            author_name=model.git.author_name,
            author_email=model.git.author_email,
        ),
        task_system=TaskSystemConfig(
            platform=model.task_system.platform,
            work_label=model.task_system.work_label,
            done_label=model.task_system.done_label,
            blocked_label=model.task_system.blocked_label,
        ),
        selection=SelectionConfig(
            include_labels=model.selection.include_labels,
            exclude_labels=model.selection.exclude_labels,
        ),
        verify=VerifyConfig(
            setup=model.verify.setup,
            tests=model.verify.tests,
            lint=model.verify.lint,
            typecheck=model.verify.typecheck,
            security=model.verify.security,
            custom=model.verify.custom,
            test_retry_count=model.verify.test_retry_count,
            allow_known_baseline_failures=model.verify.allow_known_baseline_failures,
        ),
        runtime=RuntimeConfig(
            agent_backend=model.runtime.agent_backend,
            candidates=model.runtime.candidates,
            max_parallel_candidates=model.runtime.max_parallel_candidates,
            max_parallel_runs=model.runtime.max_parallel_runs,
            max_attempts=model.runtime.max_attempts,
            repair_candidates=model.runtime.repair_candidates,
            use_sandbox_runtime=model.runtime.use_sandbox_runtime,
            require_sandbox_runtime=model.runtime.require_sandbox_runtime,
            candidate_timeout_seconds=model.runtime.candidate_timeout_seconds,
            command_timeout_seconds=model.runtime.command_timeout_seconds,
            setup_timeout_seconds=model.runtime.setup_timeout_seconds,
            sandbox_policy=SandboxPolicy(
                allowed_domains=list(model.runtime.sandbox_policy.allowed_domains),
                denied_domains=list(model.runtime.sandbox_policy.denied_domains),
                deny_read=list(model.runtime.sandbox_policy.deny_read),
                allow_write=list(model.runtime.sandbox_policy.allow_write),
                deny_write=list(model.runtime.sandbox_policy.deny_write),
            ),
        ),
        audit=AuditConfig(
            hash_events=model.audit.hash_events,
            external_sink=model.audit.external_sink,
        ),
        security=SecurityConfig(
            allow_lfs_changes=model.security.allow_lfs_changes,
            allow_binary_changes=model.security.allow_binary_changes,
            allow_dependency_changes=model.security.allow_dependency_changes,
        ),
    )


def _default_test_command(root: Path) -> str:
    if (root / "pyproject.toml").exists():
        return "pytest"
    if (root / "package.json").exists():
        return "npm test"
    return ""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from gg.orchestrator.sandbox import SandboxPolicy
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
    max_attempts: int = 1
    repair_candidates: int = 1
    use_sandbox_runtime: bool = True
    require_sandbox_runtime: bool = False
    candidate_timeout_seconds: int = 1800
    command_timeout_seconds: int = 600
    sandbox_policy: SandboxPolicy = field(default_factory=SandboxPolicy)


@dataclass(frozen=True)
class VerifyConfig:
    tests: str = ""
    lint: str = ""
    typecheck: str = ""
    allow_known_baseline_failures: bool = False

    def commands(self) -> list[str]:
        return [cmd for cmd in (self.tests, self.lint, self.typecheck) if cmd.strip()]


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


def _string_list(value: Any, *, default: list[str]) -> list[str]:
    if not isinstance(value, list):
        return list(default)
    return [str(item) for item in value]


def load_config(project_path: str | Path) -> GGConfig:
    root = Path(project_path).resolve()
    params_path = root / ".gg" / "params.yaml"
    raw: dict[str, Any] = {}
    if params_path.exists():
        raw = yaml.safe_load(params_path.read_text(encoding="utf-8")) or {}
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
    return GGConfig(
        git=GitConfig(
            default_branch=default_branch,
            author_name=git.get("author_name", "gg-orchestrator"),
            author_email=git.get("author_email", "gg-orchestrator@users.noreply.local"),
        ),
        task_system=TaskSystemConfig(
            platform=str(task_system.get("platform", raw.get("platform", "auto"))),
            work_label=task_system.get("work_label", "gg:in-progress"),
            done_label=task_system.get("done_label", "gg:done"),
            blocked_label=task_system.get("blocked_label", "gg:blocked"),
        ),
        selection=SelectionConfig(
            include_labels=tuple(selection.get("include_labels", ["ai-ready"])),
            exclude_labels=tuple(selection.get("exclude_labels", ["gg:in-progress", "gg:blocked", "gg:done"])),
        ),
        verify=VerifyConfig(
            tests=verify.get("tests", _default_test_command(root)),
            lint=verify.get("lint", ""),
            typecheck=verify.get("typecheck", ""),
            allow_known_baseline_failures=bool(verify.get("allow_known_baseline_failures", False)),
        ),
        runtime=RuntimeConfig(
            agent_backend=str(runtime.get("agent_backend", raw.get("agent_backend", "codex"))),
            candidates=max(1, int(runtime.get("candidates", 1))),
            max_parallel_candidates=max(1, int(runtime.get("max_parallel_candidates", 1))),
            max_attempts=max(1, int(runtime.get("max_attempts", 1))),
            repair_candidates=max(1, int(runtime.get("repair_candidates", 1))),
            use_sandbox_runtime=bool(runtime.get("use_sandbox_runtime", True)),
            require_sandbox_runtime=bool(runtime.get("require_sandbox_runtime", False)),
            candidate_timeout_seconds=int(runtime.get("candidate_timeout_seconds", 1800)),
            command_timeout_seconds=int(runtime.get("command_timeout_seconds", 600)),
            sandbox_policy=SandboxPolicy(
                allowed_domains=_string_list(sandbox_policy.get("allowed_domains"), default=[]),
                denied_domains=_string_list(sandbox_policy.get("denied_domains"), default=[]),
                deny_read=_string_list(sandbox_policy.get("deny_read"), default=["~/.ssh", ".env"]),
                allow_write=_string_list(sandbox_policy.get("allow_write"), default=["."]),
                deny_write=_string_list(sandbox_policy.get("deny_write"), default=[".env"]),
            ),
        ),
        audit=AuditConfig(
            hash_events=bool(audit.get("hash_events", False)),
            external_sink=str(audit.get("external_sink", "")),
        ),
        security=SecurityConfig(
            allow_lfs_changes=bool(security.get("allow_lfs_changes", False)),
            allow_binary_changes=bool(security.get("allow_binary_changes", True)),
            allow_dependency_changes=bool(security.get("allow_dependency_changes", True)),
        ),
    )


def _default_test_command(root: Path) -> str:
    if (root / "pyproject.toml").exists():
        return "pytest"
    if (root / "package.json").exists():
        return "npm test"
    return ""

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
class RuntimeResourceConfig:
    max_disk_mb: int = 4096
    disk_poll_interval_seconds: int = 30
    allow_candidate_downscale: bool = False
    allow_network_fs: bool = False
    allow_unsafe_fs: bool = False


@dataclass(frozen=True)
class RuntimeNetworkConfig:
    default: str = "deny"
    allowed_hosts: tuple[str, ...] = ()


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
    allow_unsafe_direct_exec: bool = False
    analysis_timeout_seconds: int = 900
    evaluation_timeout_seconds: int = 900
    candidate_timeout_seconds: int = 1800
    command_timeout_seconds: int = 600
    setup_timeout_seconds: int = 600
    resource: RuntimeResourceConfig = field(default_factory=RuntimeResourceConfig)
    network: RuntimeNetworkConfig = field(default_factory=RuntimeNetworkConfig)
    port_range: tuple[int, int] = (41000, 45000)
    sandbox_policy: SandboxPolicy = field(default_factory=SandboxPolicy)


@dataclass(frozen=True)
class VerifyConfig:
    setup: str = ""
    tests: str = ""
    lint: str = ""
    typecheck: str = ""
    security: str = ""
    custom: tuple[str, ...] = ()
    discovery_enabled: bool = True
    test_retry_count: int = 0
    allow_known_baseline_failures: bool = False
    block_on_security_high: bool = True

    def commands(self) -> list[str]:
        return self.check_commands()

    def check_commands(self) -> list[str]:
        return [
            cmd
            for cmd in (self.tests, self.lint, self.typecheck, self.security, *self.custom)
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
class CleanupConfig:
    blocked_timeout_days: int | None = 14


@dataclass(frozen=True)
class LogConfig:
    max_size_mb: int = 50
    max_command_log_chars: int = 200000
    mask_secrets: bool = True


@dataclass(frozen=True)
class CostConfig:
    enabled: bool = False
    mode: str = "duration-only"
    max_usd_per_run: float | None = None
    max_tokens_per_run: int | None = None


@dataclass(frozen=True)
class EvaluationConfig:
    max_context_tokens: int = 60000
    max_diff_lines_per_candidate: int = 2000
    max_log_chars_per_check: int = 12000
    max_total_log_chars: int = 50000
    prefer_deterministic_when_truncated: bool = True


@dataclass(frozen=True)
class CIConfig:
    mode: bool = False
    default_dry_run: bool = False
    forbid_interactive_prompts: bool = True
    clock_skew_tolerance_seconds: int = 5
    clock_drift_warn_seconds: int = 60


@dataclass(frozen=True)
class RecoveryConfig:
    keep_state_backup: bool = True


@dataclass(frozen=True)
class GGConfig:
    git: GitConfig
    task_system: TaskSystemConfig = field(default_factory=TaskSystemConfig)
    selection: SelectionConfig = field(default_factory=SelectionConfig)
    verify: VerifyConfig = field(default_factory=VerifyConfig)
    runtime: RuntimeConfig = field(default_factory=RuntimeConfig)
    audit: AuditConfig = field(default_factory=AuditConfig)
    security: SecurityConfig = field(default_factory=SecurityConfig)
    cleanup: CleanupConfig = field(default_factory=CleanupConfig)
    log: LogConfig = field(default_factory=LogConfig)
    cost: CostConfig = field(default_factory=CostConfig)
    evaluation: EvaluationConfig = field(default_factory=EvaluationConfig)
    ci: CIConfig = field(default_factory=CIConfig)
    recovery: RecoveryConfig = field(default_factory=RecoveryConfig)


def _mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def default_params(project_path: str | Path) -> dict[str, Any]:
    root = Path(project_path).resolve()
    return {
        "schema_version": 1,
        "project": {
            "default_branch": get_main_branch(root),
        },
        "git": {
            "author_name": "gg-orchestrator",
            "author_email": "gg-orchestrator@users.noreply.local",
        },
        "task_system": {
            "platform": "auto",
            "work_label": "gg:in-progress",
            "done_label": "gg:done",
            "blocked_label": "gg:blocked",
        },
        "selection": {
            "include_labels": ["ai-ready"],
            "exclude_labels": ["gg:in-progress", "gg:blocked", "gg:done"],
        },
        "verify": {
            "setup": "",
            "tests": _default_test_command(root),
            "lint": "",
            "typecheck": "",
            "security": "",
            "custom": [],
            "discovery_enabled": True,
            "test_retry_count": 0,
            "allow_known_baseline_failures": False,
            "block_on_security_high": True,
        },
        "runtime": {
            "agent_backend": "codex",
            "candidates": 1,
            "max_parallel_candidates": 1,
            "max_parallel_runs": 1,
            "max_attempts": 1,
            "repair_candidates": 1,
            "use_sandbox_runtime": True,
            "require_sandbox_runtime": True,
            "allow_unsafe_direct_exec": False,
            "analysis_timeout_seconds": 900,
            "evaluation_timeout_seconds": 900,
            "candidate_timeout_seconds": 1800,
            "command_timeout_seconds": 600,
            "setup_timeout_seconds": 600,
            "resource": {
                "max_disk_mb": 4096,
                "disk_poll_interval_seconds": 30,
                "allow_candidate_downscale": False,
                "allow_network_fs": False,
                "allow_unsafe_fs": False,
            },
            "network": {
                "default": "deny",
                "allowed_hosts": [],
            },
            "port_range": [41000, 45000],
            "sandbox_policy": {
                "allowed_domains": [],
                "denied_domains": [],
                "deny_read": ["~/.ssh", ".env"],
                "allow_write": ["."],
                "deny_write": [".env"],
            },
        },
        "audit": {
            "hash_events": False,
            "external_sink": "",
        },
        "security": {
            "allow_lfs_changes": False,
            "allow_binary_changes": True,
            "allow_dependency_changes": True,
        },
        "cleanup": {
            "blocked_timeout_days": 14,
        },
        "log": {
            "max_size_mb": 50,
            "max_command_log_chars": 200000,
            "mask_secrets": True,
        },
        "cost": {
            "enabled": False,
            "mode": "duration-only",
            "max_usd_per_run": None,
            "max_tokens_per_run": None,
        },
        "evaluation": {
            "max_context_tokens": 60000,
            "max_diff_lines_per_candidate": 2000,
            "max_log_chars_per_check": 12000,
            "max_total_log_chars": 50000,
            "prefer_deterministic_when_truncated": True,
        },
        "ci": {
            "mode": False,
            "default_dry_run": False,
            "forbid_interactive_prompts": True,
            "clock_skew_tolerance_seconds": 5,
            "clock_drift_warn_seconds": 60,
        },
        "recovery": {
            "keep_state_backup": True,
        },
    }


def load_config(project_path: str | Path) -> GGConfig:
    root = Path(project_path).resolve()
    params_path = root / ".gg" / "params.yaml"
    raw: dict[str, Any] = {}
    if params_path.exists():
        raw = yaml.safe_load(params_path.read_text(encoding="utf-8")) or {}
        if not isinstance(raw, dict):
            raise ValueError(f"{params_path}: expected YAML mapping")
        _reject_unknown_config_keys(raw, str(params_path))
    project = _mapping(raw.get("project"))
    git = _mapping(raw.get("git"))
    task_system = _mapping(raw.get("task_system"))
    selection = _mapping(raw.get("selection"))
    verify = _mapping(raw.get("verify"))
    runtime = _mapping(raw.get("runtime"))
    audit = _mapping(raw.get("audit"))
    security = _mapping(raw.get("security"))
    cleanup = _mapping(raw.get("cleanup"))
    log = _mapping(raw.get("log"))
    cost = _mapping(raw.get("cost"))
    evaluation = _mapping(raw.get("evaluation"))
    ci = _mapping(raw.get("ci"))
    recovery = _mapping(raw.get("recovery"))
    sandbox_policy = _mapping(runtime.get("sandbox_policy"))
    resource = _mapping(runtime.get("resource"))
    network = _mapping(runtime.get("network"))
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
                    "discovery_enabled": verify.get("discovery_enabled", True),
                    "test_retry_count": verify.get("test_retry_count", 0),
                    "allow_known_baseline_failures": verify.get("allow_known_baseline_failures", False),
                    "block_on_security_high": verify.get("block_on_security_high", True),
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
                    "allow_unsafe_direct_exec": runtime.get("allow_unsafe_direct_exec", False),
                    "analysis_timeout_seconds": runtime.get("analysis_timeout_seconds", 900),
                    "evaluation_timeout_seconds": runtime.get("evaluation_timeout_seconds", 900),
                    "candidate_timeout_seconds": runtime.get("candidate_timeout_seconds", 1800),
                    "command_timeout_seconds": runtime.get("command_timeout_seconds", 600),
                    "setup_timeout_seconds": runtime.get("setup_timeout_seconds", 600),
                    "resource": {
                        "max_disk_mb": resource.get("max_disk_mb", runtime.get("max_disk_mb", 4096)),
                        "disk_poll_interval_seconds": resource.get(
                            "disk_poll_interval_seconds",
                            runtime.get("disk_poll_interval_seconds", 30),
                        ),
                        "allow_candidate_downscale": resource.get(
                            "allow_candidate_downscale",
                            runtime.get("allow_candidate_downscale", False),
                        ),
                        "allow_network_fs": resource.get(
                            "allow_network_fs",
                            runtime.get("allow_network_fs", False),
                        ),
                        "allow_unsafe_fs": resource.get(
                            "allow_unsafe_fs",
                            runtime.get("allow_unsafe_fs", False),
                        ),
                    },
                    "network": {
                        "default": network.get("default", runtime.get("network_default", "deny")),
                        "allowed_hosts": network.get(
                            "allowed_hosts",
                            runtime.get("allowed_network_hosts", []),
                        ),
                    },
                    "port_range": runtime.get("port_range", [41000, 45000]),
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
                "cleanup": {
                    "blocked_timeout_days": cleanup.get("blocked_timeout_days", 14),
                },
                "log": {
                    "max_size_mb": log.get("max_size_mb", 50),
                    "max_command_log_chars": log.get("max_command_log_chars", 200000),
                    "mask_secrets": log.get("mask_secrets", True),
                },
                "cost": {
                    "enabled": cost.get("enabled", False),
                    "mode": cost.get("mode", "duration-only"),
                    "max_usd_per_run": cost.get("max_usd_per_run"),
                    "max_tokens_per_run": cost.get("max_tokens_per_run"),
                },
                "evaluation": {
                    "max_context_tokens": evaluation.get("max_context_tokens", 60000),
                    "max_diff_lines_per_candidate": evaluation.get(
                        "max_diff_lines_per_candidate",
                        2000,
                    ),
                    "max_log_chars_per_check": evaluation.get("max_log_chars_per_check", 12000),
                    "max_total_log_chars": evaluation.get("max_total_log_chars", 50000),
                    "prefer_deterministic_when_truncated": evaluation.get(
                        "prefer_deterministic_when_truncated",
                        True,
                    ),
                },
                "ci": {
                    "mode": ci.get("mode", False),
                    "default_dry_run": ci.get("default_dry_run", False),
                    "forbid_interactive_prompts": ci.get("forbid_interactive_prompts", True),
                    "clock_skew_tolerance_seconds": ci.get("clock_skew_tolerance_seconds", 5),
                    "clock_drift_warn_seconds": ci.get("clock_drift_warn_seconds", 60),
                },
                "recovery": {
                    "keep_state_backup": recovery.get("keep_state_backup", True),
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
            discovery_enabled=model.verify.discovery_enabled,
            test_retry_count=model.verify.test_retry_count,
            allow_known_baseline_failures=model.verify.allow_known_baseline_failures,
            block_on_security_high=model.verify.block_on_security_high,
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
            allow_unsafe_direct_exec=model.runtime.allow_unsafe_direct_exec,
            analysis_timeout_seconds=model.runtime.analysis_timeout_seconds,
            evaluation_timeout_seconds=model.runtime.evaluation_timeout_seconds,
            candidate_timeout_seconds=model.runtime.candidate_timeout_seconds,
            command_timeout_seconds=model.runtime.command_timeout_seconds,
            setup_timeout_seconds=model.runtime.setup_timeout_seconds,
            resource=RuntimeResourceConfig(
                max_disk_mb=model.runtime.resource.max_disk_mb,
                disk_poll_interval_seconds=model.runtime.resource.disk_poll_interval_seconds,
                allow_candidate_downscale=model.runtime.resource.allow_candidate_downscale,
                allow_network_fs=model.runtime.resource.allow_network_fs,
                allow_unsafe_fs=model.runtime.resource.allow_unsafe_fs,
            ),
            network=RuntimeNetworkConfig(
                default=model.runtime.network.default,
                allowed_hosts=model.runtime.network.allowed_hosts,
            ),
            port_range=model.runtime.port_range,
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
        cleanup=CleanupConfig(blocked_timeout_days=model.cleanup.blocked_timeout_days),
        log=LogConfig(
            max_size_mb=model.log.max_size_mb,
            max_command_log_chars=model.log.max_command_log_chars,
            mask_secrets=model.log.mask_secrets,
        ),
        cost=CostConfig(
            enabled=model.cost.enabled,
            mode=model.cost.mode,
            max_usd_per_run=model.cost.max_usd_per_run,
            max_tokens_per_run=model.cost.max_tokens_per_run,
        ),
        evaluation=EvaluationConfig(
            max_context_tokens=model.evaluation.max_context_tokens,
            max_diff_lines_per_candidate=model.evaluation.max_diff_lines_per_candidate,
            max_log_chars_per_check=model.evaluation.max_log_chars_per_check,
            max_total_log_chars=model.evaluation.max_total_log_chars,
            prefer_deterministic_when_truncated=model.evaluation.prefer_deterministic_when_truncated,
        ),
        ci=CIConfig(
            mode=model.ci.mode,
            default_dry_run=model.ci.default_dry_run,
            forbid_interactive_prompts=model.ci.forbid_interactive_prompts,
            clock_skew_tolerance_seconds=model.ci.clock_skew_tolerance_seconds,
            clock_drift_warn_seconds=model.ci.clock_drift_warn_seconds,
        ),
        recovery=RecoveryConfig(keep_state_backup=model.recovery.keep_state_backup),
    )


def _default_test_command(root: Path) -> str:
    if (root / "pyproject.toml").exists():
        return "pytest"
    if (root / "package.json").exists():
        return "npm test"
    return ""


def _reject_unknown_config_keys(raw: dict[str, Any], location: str) -> None:
    allowed: dict[str, set[str] | None] = {
        "schema_version": None,
        "project": {"default_branch"},
        "git": {"default_branch", "author_name", "author_email"},
        "task_system": {"platform", "work_label", "done_label", "blocked_label"},
        "selection": {"include_labels", "exclude_labels"},
        "verify": {
            "setup",
            "tests",
            "lint",
            "typecheck",
            "security",
            "custom",
            "discovery_enabled",
            "test_retry_count",
            "allow_known_baseline_failures",
            "block_on_security_high",
        },
        "runtime": {
            "agent_backend",
            "candidates",
            "max_parallel_candidates",
            "max_parallel_runs",
            "max_attempts",
            "repair_candidates",
            "use_sandbox_runtime",
            "require_sandbox_runtime",
            "allow_unsafe_direct_exec",
            "analysis_timeout_seconds",
            "evaluation_timeout_seconds",
            "candidate_timeout_seconds",
            "command_timeout_seconds",
            "setup_timeout_seconds",
            "resource",
            "network",
            "port_range",
            # Backward-compatible aliases from early Phase B drafts.
            "max_disk_mb",
            "disk_poll_interval_seconds",
            "allow_candidate_downscale",
            "allow_network_fs",
            "allow_unsafe_fs",
            "network_default",
            "allowed_network_hosts",
            "sandbox_policy",
        },
        "audit": {"hash_events", "external_sink"},
        "security": {"allow_lfs_changes", "allow_binary_changes", "allow_dependency_changes"},
        "cleanup": {"blocked_timeout_days"},
        "log": {"max_size_mb", "max_command_log_chars", "mask_secrets"},
        "cost": {"enabled", "mode", "max_usd_per_run", "max_tokens_per_run"},
        "evaluation": {
            "max_context_tokens",
            "max_diff_lines_per_candidate",
            "max_log_chars_per_check",
            "max_total_log_chars",
            "prefer_deterministic_when_truncated",
        },
        "ci": {
            "mode",
            "default_dry_run",
            "forbid_interactive_prompts",
            "clock_skew_tolerance_seconds",
            "clock_drift_warn_seconds",
        },
        "recovery": {"keep_state_backup"},
        # Backward-compatible root-level aliases.
        "platform": None,
        "agent_backend": None,
    }
    nested_allowed = {
        "runtime.resource": {
            "max_disk_mb",
            "disk_poll_interval_seconds",
            "allow_candidate_downscale",
            "allow_network_fs",
            "allow_unsafe_fs",
        },
        "runtime.network": {
            "default",
            "allowed_hosts",
        },
        "runtime.sandbox_policy": {
            "allowed_domains",
            "denied_domains",
            "deny_read",
            "allow_write",
            "deny_write",
        }
    }
    for key, value in raw.items():
        if key not in allowed:
            raise ValueError(f"{location}.{key}: unknown configuration key")
        children = allowed[key]
        if children is None:
            continue
        if not isinstance(value, dict):
            raise ValueError(f"{location}.{key}: expected mapping")
        for child_key, child_value in value.items():
            if child_key not in children:
                raise ValueError(f"{location}.{key}.{child_key}: unknown configuration key")
            nested_path = f"{key}.{child_key}"
            nested_children = nested_allowed.get(nested_path)
            if nested_children is None:
                continue
            if not isinstance(child_value, dict):
                raise ValueError(f"{location}.{nested_path}: expected mapping")
            for nested_key in child_value:
                if nested_key not in nested_children:
                    raise ValueError(f"{location}.{nested_path}.{nested_key}: unknown configuration key")

from __future__ import annotations

import re
import shutil
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path

from gg.orchestrator.config import GGConfig, load_config
from gg.orchestrator.plugins import create_agent_backend, create_platform
from gg.orchestrator.sandbox import SandboxRuntime
from gg.utils.git_ops import find_repo_root


@dataclass(frozen=True)
class DoctorCheck:
    name: str
    status: str
    message: str

    def to_dict(self) -> dict:
        return asdict(self)


def run_doctor(project_path: str | Path) -> dict:
    root = find_repo_root(project_path) or Path(project_path).resolve()
    root = Path(root).resolve()
    checks: list[DoctorCheck] = []
    config: GGConfig | None = None

    checks.append(_executable_check("git", required=True))
    checks.append(
        DoctorCheck(
            name="git_repo",
            status="pass" if (root / ".git").exists() or find_repo_root(root) else "fail",
            message=str(root),
        )
    )
    checks.append(_git_worktree_check(root))

    params_path = root / ".gg" / "params.yaml"
    if not params_path.exists():
        checks.append(DoctorCheck("params", "fail", ".gg/params.yaml is missing; run gg init"))
    else:
        try:
            config = load_config(root)
            checks.append(DoctorCheck("params", "pass", ".gg/params.yaml loaded"))
            checks.append(DoctorCheck("config_schema", "pass", "orchestrator params schema_version=1"))
        except Exception as exc:
            checks.append(DoctorCheck("params", "fail", str(exc)))

    if config is not None:
        backend_name = config.runtime.agent_backend
        try:
            backend = create_agent_backend(
                backend_name,
                command=_agent_command(config, backend_name),
            )
        except ValueError as exc:
            checks.append(DoctorCheck("agent_backend", "fail", str(exc)))
            backend_available = False
        else:
            backend_available = backend.is_available()
            checks.append(
                DoctorCheck(
                    backend_name,
                    "pass" if backend_available else "fail",
                    f"{backend_name} CLI available" if backend_available else f"{backend_name} CLI not found",
                )
            )
        checks.append(_platform_cli_check(config.task_system.platform))
        checks.append(_platform_auth_check(root, config))
        checks.append(_filesystem_safety_check(root, config))
        checks.append(
            DoctorCheck(
                "sandbox_mode",
                "warn" if config.runtime.allow_unsafe_direct_exec else "pass",
                (
                    "unsafe direct execution is allowed"
                    if config.runtime.allow_unsafe_direct_exec
                    else f"sandbox-runtime required for {backend_name} execution"
                ),
            )
        )
        sandbox = SandboxRuntime()
        sandbox_available = sandbox.is_available()
        sandbox_required = config.runtime.require_sandbox_runtime
        checks.append(
            DoctorCheck(
                "sandbox_runtime",
                "pass" if sandbox_available else ("fail" if sandbox_required else "warn"),
                "sandbox-runtime available"
                if sandbox_available
                else (
                    f"srt-py not found; safe {backend_name} candidate execution will fail"
                    if sandbox_required
                    else "srt-py not found; unsafe direct execution is configured"
                ),
            )
        )
        checks.append(_dirty_workspace_check(root))
        checks.append(_secrets_check(root))

    status = "pass"
    if any(check.status == "fail" for check in checks):
        status = "fail"
    elif any(check.status == "warn" for check in checks):
        status = "warn"
    return {
        "schema_version": 1,
        "path": str(root),
        "status": status,
        "checks": [check.to_dict() for check in checks],
    }


def _executable_check(name: str, *, required: bool) -> DoctorCheck:
    available = shutil.which(name) is not None
    if available:
        return DoctorCheck(name, "pass", f"{name} found")
    return DoctorCheck(name, "fail" if required else "warn", f"{name} not found")


def _platform_cli_check(platform: str) -> DoctorCheck:
    if platform == "gitlab":
        return _executable_check("glab", required=True)
    if platform == "github":
        return _executable_check("gh", required=True)
    gh_available = shutil.which("gh") is not None
    glab_available = shutil.which("glab") is not None
    if gh_available or glab_available:
        found = "gh" if gh_available else "glab"
        return DoctorCheck("platform_cli", "pass", f"{found} found for auto platform mode")
    return DoctorCheck("platform_cli", "warn", "gh/glab not found; platform operations may fail")


def _agent_command(config: GGConfig, backend: str) -> str | None:
    selected = backend.strip().lower()
    if selected == "codex":
        return config.agent.codex_command
    if selected == "claude":
        return config.agent.claude_command
    return None


def _platform_auth_check(root: Path, config: GGConfig) -> DoctorCheck:
    platform_name = config.task_system.platform
    if platform_name == "auto":
        platform_name = _auto_platform_name(root)
    if platform_name not in {"github", "gitlab"}:
        return DoctorCheck(
            "platform_auth",
            "warn",
            f"auth preflight skipped for unsupported platform {config.task_system.platform}",
        )
    try:
        platform = create_platform(platform_name, root)
        platform.validate_auth()
    except Exception as exc:
        return DoctorCheck("platform_auth", "fail", str(exc))
    return DoctorCheck("platform_auth", "pass", f"{platform_name} auth validated")


def _filesystem_safety_check(root: Path, config: GGConfig) -> DoctorCheck:
    if config.runtime.resource.allow_unsafe_fs:
        return DoctorCheck("filesystem_safety", "warn", "unsafe filesystem mode is explicitly allowed")
    if config.runtime.resource.allow_network_fs:
        return DoctorCheck("filesystem_safety", "warn", "network/shared filesystem mode is explicitly allowed")
    if _is_probably_network_filesystem(root):
        return DoctorCheck(
            "filesystem_safety",
            "fail",
            "workspace appears to be on a network/shared filesystem; set runtime.resource.allow_network_fs only if accepted",
        )
    return DoctorCheck("filesystem_safety", "pass", "workspace path passed local filesystem preflight")


def _auto_platform_name(root: Path) -> str:
    from gg.platforms.base import detect_platform

    detected = detect_platform(root)
    return detected if detected in {"github", "gitlab"} else "github"


def _is_probably_network_filesystem(root: Path) -> bool:
    text = str(root).lower()
    markers = ("/nfs/", "/net/", "/network/", "/volumes/")
    return any(marker in text for marker in markers)


def _git_worktree_check(root: Path) -> DoctorCheck:
    try:
        completed = subprocess.run(
            ["git", "worktree", "list"],
            cwd=str(root),
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return DoctorCheck("git_worktree", "fail", str(exc))
    if completed.returncode == 0:
        return DoctorCheck("git_worktree", "pass", "git worktree list succeeded")
    return DoctorCheck("git_worktree", "fail", completed.stderr.strip() or "git worktree list failed")


def _dirty_workspace_check(root: Path) -> DoctorCheck:
    try:
        completed = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=str(root),
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return DoctorCheck("dirty_workspace", "warn", str(exc))
    if completed.returncode != 0:
        return DoctorCheck("dirty_workspace", "warn", completed.stderr.strip() or "git status failed")
    dirty = [
        line
        for line in completed.stdout.splitlines()
        if not line[3:].startswith(".gg/")
    ]
    if dirty:
        return DoctorCheck("dirty_workspace", "warn", f"{len(dirty)} non-.gg working tree changes")
    return DoctorCheck("dirty_workspace", "pass", "working tree has no non-.gg changes")


def _secrets_check(root: Path) -> DoctorCheck:
    gg_dir = root / ".gg"
    if not gg_dir.exists():
        return DoctorCheck("secrets", "pass", ".gg directory is absent")
    patterns = [
        re.compile(r"\bgh[pousr]_[A-Za-z0-9_]{20,}\b"),
        re.compile(r"\bsk-[A-Za-z0-9_-]{20,}\b"),
        re.compile(r"(?i)\b(token|api[_-]?key|secret|password)\b\s*[:=]\s*['\"]?[^'\"\s]{8,}"),
    ]
    findings: list[str] = []
    for path in sorted(gg_dir.glob("*.yaml")):
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        if any(pattern.search(text) for pattern in patterns):
            findings.append(path.name)
    if findings:
        return DoctorCheck("secrets", "fail", f"potential secrets in .gg yaml: {', '.join(findings)}")
    return DoctorCheck("secrets", "pass", "no obvious secrets in .gg/*.yaml")

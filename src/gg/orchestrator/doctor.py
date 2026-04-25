from __future__ import annotations

import shutil
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path

from gg.agents.codex import CodexAgent
from gg.orchestrator.config import GGConfig, load_config
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
        except Exception as exc:
            checks.append(DoctorCheck("params", "fail", str(exc)))

    if config is not None:
        if config.runtime.agent_backend == "codex":
            codex_available = CodexAgent().is_available()
            checks.append(
                DoctorCheck(
                    "codex",
                    "pass" if codex_available else "fail",
                    "codex CLI available" if codex_available else "codex CLI not found",
                )
            )
        checks.append(_platform_cli_check(config.task_system.platform))
        sandbox = SandboxRuntime()
        sandbox_available = sandbox.is_available()
        sandbox_required = config.runtime.require_sandbox_runtime
        checks.append(
            DoctorCheck(
                "sandbox_runtime",
                "pass" if sandbox_available else ("fail" if sandbox_required else "warn"),
                "sandbox-runtime available"
                if sandbox_available
                else "srt-py not found; sandbox execution will be skipped unless required",
            )
        )

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

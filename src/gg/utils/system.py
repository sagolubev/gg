from __future__ import annotations

import platform
import shutil
import subprocess
import sys
from dataclasses import dataclass

from rich.console import Console
from rich.prompt import Confirm
from rich.table import Table


@dataclass(frozen=True)
class CheckResult:
    name: str
    ok: bool
    message: str
    required: bool
    install_hint: str = ""


def _run_silent(cmd: list[str], timeout: int = 10) -> tuple[bool, str]:
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return result.returncode == 0, result.stdout.strip()
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False, ""


def check_git() -> CheckResult:
    if not shutil.which("git"):
        return CheckResult("git", False, "not found", required=True, install_hint="brew install git")
    ok, out = _run_silent(["git", "--version"])
    return CheckResult("git", ok, out if ok else "failed to get version", required=True)


def check_gh() -> CheckResult:
    if not shutil.which("gh"):
        hint = "brew install gh" if platform.system() == "Darwin" else "apt install gh"
        return CheckResult("gh", False, "not found", required=False, install_hint=hint)
    ok, out = _run_silent(["gh", "auth", "status"])
    if not ok:
        return CheckResult("gh", False, "installed but not authenticated (run: gh auth login)", required=False)
    return CheckResult("gh", True, "authenticated", required=False)


def check_glab() -> CheckResult:
    if not shutil.which("glab"):
        hint = "brew install glab" if platform.system() == "Darwin" else "pip install glab"
        return CheckResult("glab", False, "not found", required=False, install_hint=hint)
    ok, out = _run_silent(["glab", "auth", "status"])
    if not ok:
        return CheckResult("glab", False, "installed but not authenticated (run: glab auth login)", required=False)
    return CheckResult("glab", True, "authenticated", required=False)


def check_codex() -> CheckResult:
    if not shutil.which("codex"):
        return CheckResult("codex", False, "not found", required=False, install_hint="npm install -g @openai/codex")
    ok, out = _run_silent(["codex", "--version"])
    return CheckResult("codex", ok, out if ok else "installed", required=False)


def check_claude() -> CheckResult:
    if not shutil.which("claude"):
        return CheckResult(
            "claude",
            False,
            "not found",
            required=False,
            install_hint="npm install -g @anthropic-ai/claude-code",
        )
    ok, out = _run_silent(["claude", "--version"])
    return CheckResult("claude", ok, out if ok else "installed", required=False)


def check_grepai() -> CheckResult:
    if not shutil.which("grepai"):
        hint = "brew install yoanbernabeu/tap/grepai" if platform.system() == "Darwin" else "curl -sSL https://raw.githubusercontent.com/yoanbernabeu/grepai/main/install.sh | sh"
        return CheckResult("grepai", False, "not found", required=False, install_hint=hint)
    ok, out = _run_silent(["grepai", "version"])
    return CheckResult("grepai", ok, out if ok else "installed", required=False)


def check_sandbox_runtime() -> CheckResult:
    if not shutil.which("srt-py"):
        return CheckResult(
            "sandbox-runtime", False, "not found (srt-py)",
            required=False, install_hint="pip install sandbox-runtime",
        )
    ok, out = _run_silent(["srt-py", "--version"])
    return CheckResult("sandbox-runtime", ok, out if ok else "installed", required=False)


def check_openspec() -> CheckResult:
    if not shutil.which("openspec"):
        return CheckResult("openspec", False, "not found", required=False, install_hint="npm install -g openspec")
    ok, out = _run_silent(["openspec", "--version"])
    return CheckResult("openspec", ok, out if ok else "installed", required=False)


def check_python_version() -> CheckResult:
    v = sys.version_info
    version_str = f"{v.major}.{v.minor}.{v.micro}"
    if v >= (3, 10):
        return CheckResult("python", True, version_str, required=True)
    return CheckResult("python", False, f"{version_str} (need >=3.10)", required=True)


ALL_CHECKS = [
    check_python_version,
    check_git,
    check_gh,
    check_glab,
    check_codex,
    check_claude,
    check_sandbox_runtime,
    check_openspec,
    check_grepai,
]


def run_all_checks(*, offer_install: bool = True) -> list[CheckResult]:
    console = Console()
    results: list[CheckResult] = []

    for check_fn in ALL_CHECKS:
        results = [*results, check_fn()]

    table = Table(title="System checks")
    table.add_column("Tool", style="bold")
    table.add_column("Status")
    table.add_column("Details")

    for r in results:
        status = "[green]OK[/green]" if r.ok else ("[red]MISSING[/red]" if r.required else "[yellow]MISSING[/yellow]")
        table.add_row(r.name, status, r.message)

    console.print(table)

    if offer_install:
        results = _offer_installs(results, console)

    failed_required = [r for r in results if r.required and not r.ok]
    if failed_required:
        names = ", ".join(r.name for r in failed_required)
        console.print(f"\n[red bold]Cannot continue: missing required tools: {names}[/red bold]")
        raise SystemExit(1)

    return results


def _offer_installs(results: list[CheckResult], console: Console) -> list[CheckResult]:
    updated: list[CheckResult] = []
    for r in results:
        if not r.ok and r.install_hint:
            if Confirm.ask(f"\n[yellow]{r.name}[/yellow] not found. Install with [bold]{r.install_hint}[/bold]?"):
                ok = _try_install(r.install_hint, console)
                if ok:
                    new_check = [fn for fn in ALL_CHECKS if fn().name == r.name]
                    updated = [*updated, new_check[0]() if new_check else r]
                    continue
            updated = [*updated, r]
        else:
            updated = [*updated, r]
    return updated


def _try_install(command: str, console: Console) -> bool:
    console.print(f"  Running: [bold]{command}[/bold]")
    try:
        result = subprocess.run(
            command.split(),
            capture_output=True,
            text=True,
            timeout=120,
        )
        if result.returncode == 0:
            console.print("  [green]Installed successfully[/green]")
            return True
        console.print(f"  [red]Installation failed:[/red] {result.stderr.strip()}")
        return False
    except (subprocess.TimeoutExpired, FileNotFoundError) as e:
        console.print(f"  [red]Installation failed:[/red] {e}")
        return False

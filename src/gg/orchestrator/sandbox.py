from __future__ import annotations

import json
import os
import shlex
import signal
import shutil
import subprocess
import tempfile
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Callable


@dataclass(frozen=True)
class SandboxPolicy:
    allowed_domains: list[str] = field(default_factory=list)
    denied_domains: list[str] = field(default_factory=list)
    deny_read: list[str] = field(default_factory=lambda: ["~/.ssh", ".env"])
    allow_write: list[str] = field(default_factory=lambda: ["."])
    deny_write: list[str] = field(default_factory=lambda: [".env"])

    def to_settings(self) -> dict:
        return {
            "network": {
                "allowedDomains": self.allowed_domains,
                "deniedDomains": self.denied_domains,
            },
            "filesystem": {
                "denyRead": self.deny_read,
                "allowWrite": self.allow_write,
                "denyWrite": self.deny_write,
            },
        }


@dataclass(frozen=True)
class SandboxRunResult:
    command: list[str]
    status: str
    exit_code: int | None
    stdout: str
    stderr: str
    settings: dict
    timed_out: bool = False
    pid: int | None = None

    def to_dict(self) -> dict:
        return asdict(self)


class SandboxRuntime:
    """Narrow adapter around sandbox-runtime's `srt-py` CLI."""

    def __init__(self, executable: str = "srt-py"):
        self.executable = executable
        self.last_pid: int | None = None

    def is_available(self) -> bool:
        return shutil.which(self.executable) is not None

    def executable_path(self) -> str | None:
        return shutil.which(self.executable)

    def version(self) -> str | None:
        executable_path = self.executable_path()
        if executable_path is None:
            return None
        try:
            result = subprocess.run(
                [executable_path, "--version"],
                check=False,
                capture_output=True,
                text=True,
                timeout=5,
            )
        except (OSError, subprocess.TimeoutExpired):
            return None
        version = (result.stdout or result.stderr).strip()
        return version or None

    def run(
        self,
        command: list[str],
        *,
        cwd: str | Path,
        timeout: int,
        policy: SandboxPolicy | None = None,
        env: dict[str, str] | None = None,
        on_process_start: Callable[[int], None] | None = None,
    ) -> SandboxRunResult:
        if not self.is_available():
            raise RuntimeError(f"{self.executable} is not available")
        effective_policy = policy or SandboxPolicy()
        settings = effective_policy.to_settings()
        with tempfile.NamedTemporaryFile("w", suffix="-srt-settings.json", encoding="utf-8") as settings_file:
            json.dump(settings, settings_file)
            settings_file.flush()
            sandboxed = [self.executable, "-s", settings_file.name, "-c", shlex.join(command)]
            proc: subprocess.Popen[str] | None = None
            try:
                proc = subprocess.Popen(
                    sandboxed,
                    cwd=str(cwd),
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    env=env,
                    start_new_session=True,
                )
                self.last_pid = proc.pid
                if on_process_start is not None:
                    on_process_start(proc.pid)
                stdout, stderr = proc.communicate(timeout=timeout)
                return SandboxRunResult(
                    command=command,
                    status="passed" if proc.returncode == 0 else "failed",
                    exit_code=proc.returncode,
                    stdout=stdout,
                    stderr=stderr,
                    settings=settings,
                    pid=proc.pid,
                )
            except subprocess.TimeoutExpired as exc:
                stdout, stderr = _terminate_process_group(proc)
                return SandboxRunResult(
                    command=command,
                    status="timeout",
                    exit_code=None,
                    stdout=stdout or (exc.stdout if isinstance(exc.stdout, str) else ""),
                    stderr=stderr or (exc.stderr if isinstance(exc.stderr, str) else ""),
                    settings=settings,
                    timed_out=True,
                    pid=proc.pid if proc is not None else None,
                )


def _terminate_process_group(proc: subprocess.Popen[str] | None) -> tuple[str, str]:
    if proc is None:
        return "", ""
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except (OSError, ProcessLookupError):
        proc.terminate()
    try:
        return proc.communicate(timeout=2)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except (OSError, ProcessLookupError):
            proc.kill()
        return proc.communicate()

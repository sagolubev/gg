from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
from dataclasses import asdict, dataclass, field
from pathlib import Path


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

    def to_dict(self) -> dict:
        return asdict(self)


class SandboxRuntime:
    """Narrow adapter around sandbox-runtime's `srt-py` CLI."""

    def __init__(self, executable: str = "srt-py"):
        self.executable = executable

    def is_available(self) -> bool:
        return shutil.which(self.executable) is not None

    def run(
        self,
        command: list[str],
        *,
        cwd: str | Path,
        timeout: int,
        policy: SandboxPolicy | None = None,
    ) -> SandboxRunResult:
        if not self.is_available():
            raise RuntimeError(f"{self.executable} is not available")
        effective_policy = policy or SandboxPolicy()
        settings = effective_policy.to_settings()
        with tempfile.NamedTemporaryFile("w", suffix="-srt-settings.json", encoding="utf-8") as settings_file:
            json.dump(settings, settings_file)
            settings_file.flush()
            sandboxed = [self.executable, "--settings", settings_file.name, *command]
            try:
                completed = subprocess.run(
                    sandboxed,
                    cwd=str(cwd),
                    capture_output=True,
                    text=True,
                    timeout=timeout,
                )
                return SandboxRunResult(
                    command=command,
                    status="passed" if completed.returncode == 0 else "failed",
                    exit_code=completed.returncode,
                    stdout=completed.stdout,
                    stderr=completed.stderr,
                    settings=settings,
                )
            except subprocess.TimeoutExpired as exc:
                return SandboxRunResult(
                    command=command,
                    status="timeout",
                    exit_code=None,
                    stdout=exc.stdout if isinstance(exc.stdout, str) else "",
                    stderr=exc.stderr if isinstance(exc.stderr, str) else "",
                    settings=settings,
                    timed_out=True,
                )

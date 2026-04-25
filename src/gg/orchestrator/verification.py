from __future__ import annotations

import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path

from gg.orchestrator.schemas import CheckResultModel


@dataclass(frozen=True)
class CheckResult:
    command: str
    status: str
    exit_code: int | None
    stdout: str = ""
    stderr: str = ""

    def to_dict(self) -> dict:
        data = asdict(self)
        CheckResultModel.model_validate(data)
        return data


class VerificationRunner:
    def __init__(self, commands: list[str], *, timeout: int = 600):
        self.commands = commands
        self.timeout = timeout

    def run(self, cwd: str | Path) -> list[CheckResult]:
        if not self.commands:
            return [CheckResult(command="", status="skipped", exit_code=None)]
        results: list[CheckResult] = []
        for command in self.commands:
            try:
                completed = subprocess.run(
                    command,
                    shell=True,
                    cwd=str(cwd),
                    capture_output=True,
                    text=True,
                    timeout=self.timeout,
                )
                results.append(
                    CheckResult(
                        command=command,
                        status="passed" if completed.returncode == 0 else "failed",
                        exit_code=completed.returncode,
                        stdout=completed.stdout[-12000:],
                        stderr=completed.stderr[-12000:],
                    )
                )
            except subprocess.TimeoutExpired as exc:
                results.append(
                    CheckResult(
                        command=command,
                        status="timeout",
                        exit_code=None,
                        stdout=(exc.stdout or "")[-12000:] if isinstance(exc.stdout, str) else "",
                        stderr=(exc.stderr or "")[-12000:] if isinstance(exc.stderr, str) else "",
                    )
                )
        return results

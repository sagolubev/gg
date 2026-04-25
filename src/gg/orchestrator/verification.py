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
    attempts: int = 1
    flaky: bool = False

    def to_dict(self) -> dict:
        data = asdict(self)
        CheckResultModel.model_validate(data)
        return data


class VerificationRunner:
    def __init__(self, commands: list[str], *, timeout: int = 600, retry_count: int = 0):
        self.commands = commands
        self.timeout = timeout
        self.retry_count = max(0, retry_count)

    def run(self, cwd: str | Path) -> list[CheckResult]:
        if not self.commands:
            return [CheckResult(command="", status="skipped", exit_code=None, attempts=0)]
        results: list[CheckResult] = []
        for command in self.commands:
            attempts = 0
            first_failure: CheckResult | None = None
            result: CheckResult | None = None
            for attempt in range(self.retry_count + 1):
                attempts = attempt + 1
                result = self._run_once(command, cwd, attempts=attempts)
                if result.status == "passed":
                    if first_failure is not None:
                        result = CheckResult(
                            command=command,
                            status="flaky",
                            exit_code=result.exit_code,
                            stdout=result.stdout,
                            stderr=result.stderr,
                            attempts=attempts,
                            flaky=True,
                        )
                    break
                if first_failure is None:
                    first_failure = result
            assert result is not None
            results.append(result)
        return results

    def _run_once(self, command: str, cwd: str | Path, *, attempts: int) -> CheckResult:
        try:
            completed = subprocess.run(
                command,
                shell=True,
                cwd=str(cwd),
                capture_output=True,
                text=True,
                timeout=self.timeout,
            )
            return CheckResult(
                command=command,
                status="passed" if completed.returncode == 0 else "failed",
                exit_code=completed.returncode,
                stdout=completed.stdout[-12000:],
                stderr=completed.stderr[-12000:],
                attempts=attempts,
            )
        except subprocess.TimeoutExpired as exc:
            return CheckResult(
                command=command,
                status="timeout",
                exit_code=None,
                stdout=(exc.stdout or "")[-12000:] if isinstance(exc.stdout, str) else "",
                stderr=(exc.stderr or "")[-12000:] if isinstance(exc.stderr, str) else "",
                attempts=attempts,
            )

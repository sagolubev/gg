from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any, Sequence

from gg.orchestrator.schemas import CheckResultModel

PASSING_STATUSES = frozenset({"passed", "skipped", "flaky"})
DEFAULT_MAX_OUTPUT_CHARS = 12000

_SECRET_PATTERNS = (
    (
        "openai_api_key",
        re.compile(r"\bsk-[A-Za-z0-9_-]{16,}\b"),
    ),
    (
        "secret_assignment",
        re.compile(
            r"(?i)\b(api[_-]?key|access[_-]?token|auth[_-]?token|password|secret|"
            r"aws_secret_access_key)\b\s*[:=]\s*['\"]?([A-Za-z0-9_./+=:-]{8,})"
        ),
    ),
)


@dataclass(frozen=True)
class VerificationCommand:
    id: str
    category: str
    command: str
    required: bool = True
    needs_network: bool = False
    parser: str = ""

    @classmethod
    def from_value(cls, value: str | "VerificationCommand") -> "VerificationCommand":
        if isinstance(value, VerificationCommand):
            return value
        return cls(id=value, category="custom", command=value, required=True)


@dataclass(frozen=True)
class CheckResult:
    command: str
    status: str
    exit_code: int | None
    id: str = ""
    category: str = "custom"
    required: bool = True
    stdout: str = ""
    stderr: str = ""
    stdout_path: str = ""
    stderr_path: str = ""
    duration_ms: int | None = None
    truncated: bool = False
    encoding_errors: bool = False
    findings: list[dict[str, Any]] | None = None
    baseline_status: str | None = None
    attempts: int = 1
    flaky: bool = False

    def to_dict(self) -> dict:
        data = asdict(self)
        data["findings"] = data["findings"] or []
        CheckResultModel.model_validate(data)
        return data


def required_gate_passes(checks: Sequence[CheckResult]) -> bool:
    return not required_failures(checks)


def required_failures(checks: Sequence[CheckResult]) -> list[CheckResult]:
    return [check for check in checks if check.required and check.status not in PASSING_STATUSES]


def advisory_failures(checks: Sequence[CheckResult]) -> list[CheckResult]:
    return [check for check in checks if not check.required and check.status not in PASSING_STATUSES]


def verification_gate_summary(checks: Sequence[CheckResult]) -> dict[str, Any]:
    return {
        "required_passed": required_gate_passes(checks),
        "required_failed_commands": [check.command for check in required_failures(checks)],
        "advisory_failed_commands": [check.command for check in advisory_failures(checks)],
        "findings": [finding for check in checks for finding in (check.findings or [])],
    }


class VerificationRunner:
    def __init__(
        self,
        commands: Sequence[str | VerificationCommand],
        *,
        timeout: int = 600,
        retry_count: int = 0,
        env: dict[str, str] | None = None,
        max_output_chars: int = DEFAULT_MAX_OUTPUT_CHARS,
        output_dir: str | Path | None = None,
    ):
        self.commands = [VerificationCommand.from_value(command) for command in commands]
        self.timeout = timeout
        self.retry_count = max(0, retry_count)
        self.env = env
        self.max_output_chars = max(0, max_output_chars)
        self.output_dir = Path(output_dir) if output_dir is not None else None

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
                        result = replace(result, status="flaky", attempts=attempts, flaky=True)
                    break
                if first_failure is None:
                    first_failure = result
            assert result is not None
            results.append(result)
        return results

    def _run_once(self, command: VerificationCommand, cwd: str | Path, *, attempts: int) -> CheckResult:
        started = time.monotonic()
        try:
            completed = subprocess.run(
                command.command,
                shell=True,
                cwd=str(cwd),
                capture_output=True,
                text=False,
                timeout=self.timeout,
                env=self._subprocess_env(),
            )
            stdout, stdout_errors = _decode_output(completed.stdout)
            stderr, stderr_errors = _decode_output(completed.stderr)
            stdout, stdout_path, stdout_truncated = self._materialize_output(command, "stdout", stdout)
            stderr, stderr_path, stderr_truncated = self._materialize_output(command, "stderr", stderr)
            findings = _parse_findings(command, stdout=stdout, stderr=stderr)
            status = "passed" if completed.returncode == 0 else "failed"
            if findings and command.category == "security":
                status = "failed"
            return CheckResult(
                command=command.command,
                status=status,
                exit_code=completed.returncode,
                id=command.id,
                category=command.category,
                required=command.required,
                stdout=stdout,
                stderr=stderr,
                stdout_path=stdout_path,
                stderr_path=stderr_path,
                duration_ms=_duration_ms(started),
                truncated=stdout_truncated or stderr_truncated,
                encoding_errors=stdout_errors or stderr_errors,
                findings=findings,
                attempts=attempts,
            )
        except subprocess.TimeoutExpired as exc:
            stdout, stdout_errors = _decode_output(exc.stdout)
            stderr, stderr_errors = _decode_output(exc.stderr)
            stdout, stdout_path, stdout_truncated = self._materialize_output(command, "stdout", stdout)
            stderr, stderr_path, stderr_truncated = self._materialize_output(command, "stderr", stderr)
            findings = _parse_findings(command, stdout=stdout, stderr=stderr)
            return CheckResult(
                command=command.command,
                status="timeout",
                exit_code=None,
                id=command.id,
                category=command.category,
                required=command.required,
                stdout=stdout,
                stderr=stderr,
                stdout_path=stdout_path,
                stderr_path=stderr_path,
                duration_ms=_duration_ms(started),
                truncated=stdout_truncated or stderr_truncated,
                encoding_errors=stdout_errors or stderr_errors,
                findings=findings,
                attempts=attempts,
            )

    def _materialize_output(
        self, command: VerificationCommand, stream: str, output: str
    ) -> tuple[str, str, bool]:
        if self.max_output_chars == 0:
            bounded = ""
            truncated = bool(output)
        elif len(output) > self.max_output_chars:
            bounded = output[-self.max_output_chars :]
            truncated = True
        else:
            bounded = output
            truncated = False
        if not truncated or self.output_dir is None:
            return bounded, "", truncated
        self.output_dir.mkdir(parents=True, exist_ok=True)
        path = self.output_dir / f"{_safe_id(command.id)}-{stream}.log"
        path.write_text(output, encoding="utf-8", errors="replace")
        return bounded, str(path), truncated

    def _subprocess_env(self) -> dict[str, str] | None:
        if self.env is not None:
            return self.env
        env = os.environ.copy()
        executable_dir = str(Path(sys.executable).parent)
        path = env.get("PATH", "")
        parts = path.split(os.pathsep) if path else []
        if executable_dir not in parts:
            env["PATH"] = os.pathsep.join([executable_dir, *parts]) if parts else executable_dir
        return env


def _duration_ms(started: float) -> int:
    return max(0, int((time.monotonic() - started) * 1000))


def _decode_output(output: bytes | str | None) -> tuple[str, bool]:
    if output is None:
        return "", False
    if isinstance(output, str):
        return output, False
    try:
        return output.decode("utf-8"), False
    except UnicodeDecodeError:
        return output.decode("utf-8", errors="replace"), True


def _parse_findings(command: VerificationCommand, *, stdout: str, stderr: str) -> list[dict[str, Any]]:
    parsers = _parser_names(command)
    findings: list[dict[str, Any]] = []
    if "pytest" in parsers:
        findings.extend(_parse_pytest_findings(stdout, stderr))
    if "ruff" in parsers:
        findings.extend(_parse_ruff_findings(stdout, stderr))
    if "mypy" in parsers:
        findings.extend(_parse_mypy_findings(stdout, stderr))
    if "bandit" in parsers:
        findings.extend(_parse_bandit_findings(stdout, stderr))
    if command.category == "security" or "secret-scan" in parsers:
        findings.extend(_parse_secret_findings(stdout, stderr))
    return findings


def _parser_names(command: VerificationCommand) -> set[str]:
    if command.parser:
        return {part.strip() for part in command.parser.split(",") if part.strip()}
    if command.category == "test":
        return {"pytest"}
    if command.category == "lint":
        return {"ruff"}
    if command.category == "typecheck":
        return {"mypy"}
    if command.category == "security":
        return {"secret-scan"}
    return set()


def _parse_secret_findings(stdout: str, stderr: str) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    for stream, text in (("stdout", stdout), ("stderr", stderr)):
        for line_number, line in enumerate(text.splitlines(), start=1):
            for pattern_name, pattern in _SECRET_PATTERNS:
                if pattern.search(line):
                    findings.append(
                        {
                            "type": "secret",
                            "category": "security",
                            "parser": "secret-scan",
                            "pattern": pattern_name,
                            "stream": stream,
                            "line": line_number,
                            "evidence": _redact_secret_line(line),
                        }
                    )
                    break
    return findings


def _parse_pytest_findings(stdout: str, stderr: str) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    pattern = re.compile(r"^(FAILED|ERROR)\s+(?P<nodeid>\S+)\s+-\s+(?P<message>.+)$")
    for stream, text in (("stdout", stdout), ("stderr", stderr)):
        for line_number, line in enumerate(text.splitlines(), start=1):
            match = pattern.match(line.strip())
            if not match:
                continue
            findings.append(
                {
                    "type": "test_failure",
                    "category": "test",
                    "parser": "pytest",
                    "severity": "error",
                    "stream": stream,
                    "line": line_number,
                    "test": match.group("nodeid"),
                    "message": match.group("message"),
                }
            )
    return findings


def _parse_ruff_findings(stdout: str, stderr: str) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    findings.extend(_parse_ruff_json(stdout, "stdout"))
    findings.extend(_parse_ruff_json(stderr, "stderr"))
    if findings:
        return findings

    one_line = re.compile(
        r"^(?P<file>[^:\n]+):(?P<line>\d+):(?P<column>\d+):\s+"
        r"(?P<code>[A-Z]+[0-9]+)\s+(?P<message>.+)$"
    )
    for stream, text in (("stdout", stdout), ("stderr", stderr)):
        lines = text.splitlines()
        for line_number, line in enumerate(lines, start=1):
            match = one_line.match(line.strip())
            if match:
                findings.append(
                    {
                        "type": "lint",
                        "category": "lint",
                        "parser": "ruff",
                        "severity": "error",
                        "stream": stream,
                        "line": int(match.group("line")),
                        "column": int(match.group("column")),
                        "file": match.group("file"),
                        "code": match.group("code"),
                        "message": match.group("message"),
                    }
                )
                continue
            if line.strip().startswith("-->") and line_number > 1:
                location = re.match(r"^-->\s+(?P<file>.+?):(?P<line>\d+):(?P<column>\d+)$", line.strip())
                previous = lines[line_number - 2].strip()
                code_message = re.match(r"^(?P<code>[A-Z]+[0-9]+)\s+(?P<message>.+)$", previous)
                if location and code_message:
                    findings.append(
                        {
                            "type": "lint",
                            "category": "lint",
                            "parser": "ruff",
                            "severity": "error",
                            "stream": stream,
                            "line": int(location.group("line")),
                            "column": int(location.group("column")),
                            "file": location.group("file"),
                            "code": code_message.group("code"),
                            "message": code_message.group("message"),
                        }
                    )
    return findings


def _parse_ruff_json(text: str, stream: str) -> list[dict[str, Any]]:
    stripped = text.strip()
    if not stripped.startswith("["):
        return []
    try:
        payload = json.loads(stripped)
    except json.JSONDecodeError:
        return []
    if not isinstance(payload, list):
        return []
    findings: list[dict[str, Any]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        location = item.get("location") or {}
        findings.append(
            {
                "type": "lint",
                "category": "lint",
                "parser": "ruff",
                "severity": "error",
                "stream": stream,
                "line": int(location.get("row") or 0),
                "column": int(location.get("column") or 0),
                "file": str(item.get("filename") or ""),
                "code": str(item.get("code") or ""),
                "message": str(item.get("message") or ""),
            }
        )
    return findings


def _parse_mypy_findings(stdout: str, stderr: str) -> list[dict[str, Any]]:
    pattern = re.compile(
        r"^(?P<file>.+?):(?P<line>\d+):(?:(?P<column>\d+):)?\s+"
        r"(?P<severity>error|warning|note):\s+(?P<message>.*?)(?:\s+\[(?P<code>[^\]]+)\])?$"
    )
    findings: list[dict[str, Any]] = []
    for stream, text in (("stdout", stdout), ("stderr", stderr)):
        for raw in text.splitlines():
            match = pattern.match(raw.strip())
            if not match:
                continue
            severity = "info" if match.group("severity") == "note" else match.group("severity")
            findings.append(
                {
                    "type": "typecheck",
                    "category": "typecheck",
                    "parser": "mypy",
                    "severity": severity,
                    "stream": stream,
                    "file": match.group("file"),
                    "line": int(match.group("line")),
                    "column": int(match.group("column") or 0),
                    "code": match.group("code") or "",
                    "message": match.group("message"),
                }
            )
    return findings


def _parse_bandit_findings(stdout: str, stderr: str) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    for stream, text in (("stdout", stdout), ("stderr", stderr)):
        current: dict[str, Any] | None = None
        for raw in text.splitlines():
            line = raw.strip()
            if line.startswith(">> Issue:"):
                match = re.match(r">>\s+Issue:\s+\[(?P<code>[^\]]+)\]\s+(?P<message>.+)$", line)
                current = {
                    "type": "security",
                    "category": "security",
                    "parser": "bandit",
                    "severity": "warning",
                    "stream": stream,
                    "code": match.group("code") if match else "",
                    "message": match.group("message") if match else line.removeprefix(">> Issue:").strip(),
                }
                continue
            if current is None:
                continue
            if line.startswith("Severity:"):
                severity = line.split("Severity:", 1)[1].split()[0].lower()
                current["severity"] = severity
            elif line.startswith("Location:"):
                location = line.split("Location:", 1)[1].strip()
                loc_match = re.match(r"(?P<file>.+?):(?P<line>\d+):(?P<column>\d+)$", location)
                if loc_match:
                    current["file"] = loc_match.group("file")
                    current["line"] = int(loc_match.group("line"))
                    current["column"] = int(loc_match.group("column"))
                findings.append(current)
                current = None
    return findings


def _redact_secret_line(line: str) -> str:
    redacted = re.sub(r"(?<=[:=])\s*['\"]?[^'\"\s]{4,}", " <redacted>", line)
    return re.sub(r"\bsk-[A-Za-z0-9_-]{8,}\b", "sk-<redacted>", redacted)


def _safe_id(value: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "-", value).strip("-")
    return safe[:80] or "check"

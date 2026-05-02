from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Iterable

from gg.orchestrator.finding_feedback import annotate_findings_with_feedback, assign_finding_ids
from gg.orchestrator.verification import CheckResult

BLOCKING_RELIABILITY = "P"
BLOCKING_SEVERITIES = frozenset({"high", "critical"})
AGENT_PATTERN_COMMAND = "agent-patterns"

_SCAN_SUFFIXES = frozenset(
    {
        ".py",
        ".js",
        ".jsx",
        ".ts",
        ".tsx",
        ".mjs",
        ".cjs",
        ".go",
        ".md",
        ".mdx",
        ".txt",
    }
)
_EXCLUDED_PARTS = frozenset(
    {
        ".git",
        ".gg",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        ".venv",
        "__pycache__",
        "build",
        "dist",
        "node_modules",
        "site-packages",
    }
)
_MAX_FILE_BYTES = 512_000
_TOOL_REF_RE = re.compile(
    r"(?i)\b(?:tool|use_tool|tool_name|requires_tool)\s*[:=]\s*[`'\"]?([a-zA-Z_][\w.-]*)"
)
_NAMED_TOOL_RE = re.compile(
    r"\b(?:Tool|StructuredTool|FunctionTool)\s*\([^)]*\bname\s*=\s*['\"]([a-zA-Z_][\w.-]*)['\"]"
)
_TOOLS_DICT_RE = re.compile(r"\btools?\s*=\s*\{")
_DICT_KEY_RE = re.compile(r"['\"]([a-zA-Z_][\w.-]*)['\"]\s*:")
_DEF_RE = re.compile(r"^\s*def\s+([a-zA-Z_]\w*)\s*\(")


def verify_agent_patterns(
    project_path: str | Path,
    *,
    changed_files: Iterable[str] | None = None,
) -> CheckResult:
    root = Path(project_path).resolve()
    scan_files = list(_iter_scan_files(root, changed_files=changed_files))
    if not scan_files:
        return CheckResult(
            command=AGENT_PATTERN_COMMAND,
            id=AGENT_PATTERN_COMMAND,
            category="agent-pattern",
            status="skipped",
            exit_code=0,
            required=True,
            findings=[],
        )

    findings: list[dict[str, Any]] = []
    for path in scan_files:
        text = _read_text(path)
        if text is None:
            continue
        relative = _relative_path(root, path)
        findings.extend(_loop_safety_findings(relative, text))
        findings.extend(_retry_limit_findings(relative, text))
        findings.extend(_context_size_findings(relative, text))

    prompt_references = _tool_references(root, scan_files)
    if prompt_references:
        defined_tools = _defined_tools(root, _iter_scan_files(root, changed_files=None))
        for name, locations in sorted(prompt_references.items()):
            if name in defined_tools:
                continue
            for relative, line, evidence in locations:
                findings.append(
                    _finding(
                        rule_id="tool-registry-mismatch",
                        reliability=BLOCKING_RELIABILITY,
                        severity="high",
                        path=relative,
                        line=line,
                        message=f"Prompt references tool `{name}` but the scanned tool registry does not define it.",
                        evidence=evidence,
                        remediation="Register the tool or remove/rename the prompt reference before publishing.",
                    )
                )

    findings = annotate_findings_with_feedback(root, assign_finding_ids(findings, prefix="AP"))
    blocking = blocking_agent_pattern_findings(findings)
    return CheckResult(
        command=AGENT_PATTERN_COMMAND,
        id=AGENT_PATTERN_COMMAND,
        category="agent-pattern",
        status="failed" if blocking else "passed",
        exit_code=1 if blocking else 0,
        required=True,
        findings=findings,
    )


def blocking_agent_pattern_findings(findings: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        finding
        for finding in findings
        if str(finding.get("reliability") or "") == BLOCKING_RELIABILITY
        and str(finding.get("severity") or "").lower() in BLOCKING_SEVERITIES
        and not finding.get("suppressed")
    ]


def _iter_scan_files(root: Path, *, changed_files: Iterable[str] | None) -> Iterable[Path]:
    if changed_files is not None:
        candidates = (root / str(relative) for relative in changed_files)
    else:
        candidates = root.rglob("*")
    for path in candidates:
        try:
            resolved = path.resolve()
        except OSError:
            continue
        if not resolved.is_file():
            continue
        if resolved.suffix.lower() not in _SCAN_SUFFIXES:
            continue
        if _is_excluded(root, resolved):
            continue
        try:
            if resolved.stat().st_size > _MAX_FILE_BYTES:
                continue
        except OSError:
            continue
        yield resolved


def _is_excluded(root: Path, path: Path) -> bool:
    try:
        parts = path.relative_to(root).parts
    except ValueError:
        return True
    return any(part in _EXCLUDED_PARTS for part in parts)


def _read_text(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None


def _relative_path(root: Path, path: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return path.as_posix()


def _loop_safety_findings(relative: str, text: str) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    lines = text.splitlines()
    loop_re = re.compile(r"\bwhile\s+True\s*:|\bwhile\s*\(\s*true\s*\)|\bfor\s*\(\s*;\s*;\s*\)|^\s*for\s*\{\s*$")
    exit_re = re.compile(r"\b(break|return|raise|throw|yield)\b")
    for index, line in enumerate(lines):
        if not loop_re.search(line):
            continue
        window = "\n".join(lines[index : index + 40])
        if exit_re.search(window):
            continue
        findings.append(
            _finding(
                rule_id="unbounded-agent-loop",
                reliability=BLOCKING_RELIABILITY,
                severity="high",
                path=relative,
                line=index + 1,
                message="Potential unbounded loop has no nearby break, return, raise, throw, or yield.",
                evidence=line.strip(),
                remediation="Add an explicit stop condition, iteration budget, timeout, or cancellation path.",
            )
        )
    return findings


def _retry_limit_findings(relative: str, text: str) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    lines = text.splitlines()
    for index, line in enumerate(lines):
        statement = _statement_window(lines, index)
        stripped = line.strip()
        if _looks_like_tenacity_retry(stripped) and "stop=" not in statement:
            findings.append(
                _retry_finding(
                    relative,
                    index + 1,
                    stripped,
                    "tenacity retry has no `stop=` bound.",
                    "Add stop_after_attempt, stop_after_delay, or another explicit stop policy.",
                )
            )
        if _looks_like_backoff_retry(stripped) and "max_tries" not in statement and "max_time" not in statement:
            findings.append(
                _retry_finding(
                    relative,
                    index + 1,
                    stripped,
                    "backoff retry has no `max_tries` or `max_time` bound.",
                    "Add max_tries or max_time so agent retries cannot run indefinitely.",
                )
            )
        if "Retry(" in stripped and "total=" not in statement:
            findings.append(
                _retry_finding(
                    relative,
                    index + 1,
                    stripped,
                    "urllib3 Retry has no `total=` bound.",
                    "Set total, connect, read, or another explicit retry budget.",
                )
            )
        if re.search(r"\b(pRetry|asyncRetry)\s*\(", stripped) and "retries" not in statement:
            findings.append(
                _retry_finding(
                    relative,
                    index + 1,
                    stripped,
                    "JavaScript retry helper has no `retries` bound.",
                    "Pass an explicit retries limit.",
                )
            )
    return findings


def _looks_like_tenacity_retry(line: str) -> bool:
    return bool(re.search(r"@(?:tenacity\.)?retry\b|tenacity\.retry\s*\(", line))


def _looks_like_backoff_retry(line: str) -> bool:
    return bool(re.search(r"@?backoff\.on_(?:exception|predicate)\s*\(", line))


def _retry_finding(relative: str, line: int, evidence: str, message: str, remediation: str) -> dict[str, Any]:
    return _finding(
        rule_id="unbounded-retry",
        reliability=BLOCKING_RELIABILITY,
        severity="high",
        path=relative,
        line=line,
        message=message,
        evidence=evidence,
        remediation=remediation,
    )


def _statement_window(lines: list[str], index: int) -> str:
    collected: list[str] = []
    balance = 0
    for line in lines[index : min(len(lines), index + 8)]:
        collected.append(line)
        balance += line.count("(") - line.count(")")
        if balance <= 0 and line.rstrip().endswith((")", ":")):
            break
    return "\n".join(collected)


def _context_size_findings(relative: str, text: str) -> list[dict[str, Any]]:
    name = Path(relative).name.lower()
    if not (
        Path(relative).suffix.lower() in {".md", ".mdx", ".txt"}
        or "prompt" in name
        or "instruction" in name
    ):
        return []
    estimated_tokens = len(text) // 4
    if estimated_tokens <= 4000:
        return []
    severity = "high" if estimated_tokens > 8000 else "medium"
    return [
        _finding(
            rule_id="context-size-risk",
            reliability="H",
            severity=severity,
            path=relative,
            line=1,
            message=f"Prompt/context surface is about {estimated_tokens} tokens.",
            evidence=f"{len(text)} characters",
            remediation="Split the context, summarize stable material, or load details on demand.",
        )
    ]


def _tool_references(root: Path, files: Iterable[Path]) -> dict[str, list[tuple[str, int, str]]]:
    references: dict[str, list[tuple[str, int, str]]] = {}
    for path in files:
        text = _read_text(path)
        if text is None:
            continue
        relative = _relative_path(root, path)
        for line_number, line in enumerate(text.splitlines(), start=1):
            for match in _TOOL_REF_RE.finditer(line):
                name = match.group(1)
                if name.lower() in {"tool", "tools"}:
                    continue
                references.setdefault(name, []).append((relative, line_number, line.strip()))
    return references


def _defined_tools(root: Path, files: Iterable[Path]) -> set[str]:
    names: set[str] = set()
    for path in files:
        text = _read_text(path)
        if text is None:
            continue
        lines = text.splitlines()
        for index, line in enumerate(lines):
            for match in _NAMED_TOOL_RE.finditer(line):
                names.add(match.group(1))
            if _TOOLS_DICT_RE.search(line):
                names.update(_dict_tool_names(lines[index : index + 40]))
            if re.search(r"@\s*(?:\w+\.)?tool(?:\s*\(|\s*$)", line):
                names.update(_decorated_tool_names(lines[index + 1 : index + 8]))
            def_match = _DEF_RE.match(line)
            if def_match and def_match.group(1).endswith("_tool"):
                names.add(def_match.group(1))
    return names


def _dict_tool_names(lines: list[str]) -> set[str]:
    names: set[str] = set()
    for line in lines:
        names.update(match.group(1) for match in _DICT_KEY_RE.finditer(line))
        if "}" in line:
            break
    return names


def _decorated_tool_names(lines: list[str]) -> set[str]:
    for line in lines:
        match = _DEF_RE.match(line)
        if match:
            return {match.group(1)}
        if line.strip() and not line.lstrip().startswith("@"):
            return set()
    return set()


def _finding(
    *,
    rule_id: str,
    reliability: str,
    severity: str,
    path: str,
    line: int,
    message: str,
    evidence: str,
    remediation: str,
) -> dict[str, Any]:
    return {
        "rule_id": rule_id,
        "category": "agent-pattern",
        "reliability": reliability,
        "severity": severity,
        "file": path,
        "path": path,
        "line": line,
        "message": message,
        "evidence": evidence[:500],
        "remediation": remediation,
    }

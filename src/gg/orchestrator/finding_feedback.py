from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

FEEDBACK_PATH = ".gg/accepted-findings.json"
SUPPRESSING_STATUSES = frozenset({"accepted", "ignored", "false_positive"})
VALID_STATUSES = frozenset({"accepted", "ignored", "false_positive", "open", "fixed"})


@dataclass(frozen=True)
class FindingFeedback:
    finding_id: str
    fingerprint: str
    status: str
    reason: str
    author: str
    updated_at: str
    finding: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "finding_id": self.finding_id,
            "fingerprint": self.fingerprint,
            "status": self.status,
            "reason": self.reason,
            "author": self.author,
            "updated_at": self.updated_at,
            "finding": dict(self.finding),
        }


def finding_fingerprint(finding: dict[str, Any]) -> str:
    payload = {
        "category": str(finding.get("category") or ""),
        "rule_id": str(finding.get("rule_id") or finding.get("code") or ""),
        "path": str(finding.get("path") or finding.get("file") or ""),
        "line": int(finding.get("line") or 0),
        "message": str(finding.get("message") or ""),
        "severity": str(finding.get("severity") or "").lower(),
        "reliability": str(finding.get("reliability") or ""),
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()[:16]


def assign_finding_ids(findings: Iterable[dict[str, Any]], *, prefix: str = "F") -> list[dict[str, Any]]:
    counters: dict[str, int] = {}
    assigned: list[dict[str, Any]] = []
    for finding in findings:
        item = dict(finding)
        category_prefix = _category_prefix(item, default=prefix)
        counters[category_prefix] = counters.get(category_prefix, 0) + 1
        item.setdefault("finding_id", f"{category_prefix}{counters[category_prefix]}")
        item.setdefault("fingerprint", finding_fingerprint(item))
        item.setdefault("status", "open")
        assigned.append(item)
    return assigned


def load_finding_feedback(project_path: str | Path) -> dict[str, FindingFeedback]:
    path = Path(project_path).resolve() / FEEDBACK_PATH
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    entries = payload.get("findings") if isinstance(payload, dict) else []
    feedback: dict[str, FindingFeedback] = {}
    for entry in entries or []:
        if not isinstance(entry, dict):
            continue
        item = FindingFeedback(
            finding_id=str(entry.get("finding_id") or ""),
            fingerprint=str(entry.get("fingerprint") or ""),
            status=str(entry.get("status") or ""),
            reason=str(entry.get("reason") or ""),
            author=str(entry.get("author") or "human"),
            updated_at=str(entry.get("updated_at") or ""),
            finding=dict(entry.get("finding") or {}),
        )
        if item.fingerprint and item.status in VALID_STATUSES:
            feedback[item.fingerprint] = item
    return feedback


def annotate_findings_with_feedback(
    project_path: str | Path,
    findings: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    feedback = load_finding_feedback(project_path)
    annotated: list[dict[str, Any]] = []
    for finding in findings:
        item = dict(finding)
        fingerprint = str(item.get("fingerprint") or finding_fingerprint(item))
        item["fingerprint"] = fingerprint
        entry = feedback.get(fingerprint)
        if entry:
            item["status"] = entry.status
            item["feedback"] = {
                "status": entry.status,
                "reason": entry.reason,
                "author": entry.author,
                "updated_at": entry.updated_at,
            }
            item["suppressed"] = entry.status in SUPPRESSING_STATUSES
        else:
            item.setdefault("status", "open")
            item["suppressed"] = False
        annotated.append(item)
    return annotated


def suppressing_feedback_count(findings: Iterable[dict[str, Any]]) -> int:
    return sum(1 for finding in findings if finding.get("suppressed"))


def record_finding_feedback(
    project_path: str | Path,
    finding: dict[str, Any],
    *,
    status: str,
    reason: str,
    author: str = "human",
) -> FindingFeedback:
    if status not in VALID_STATUSES:
        raise ValueError(f"invalid finding feedback status: {status}")
    root = Path(project_path).resolve()
    path = root / FEEDBACK_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    feedback = load_finding_feedback(root)
    item = dict(finding)
    fingerprint = str(item.get("fingerprint") or finding_fingerprint(item))
    item["fingerprint"] = fingerprint
    entry = FindingFeedback(
        finding_id=str(item.get("finding_id") or fingerprint[:8]),
        fingerprint=fingerprint,
        status=status,
        reason=reason,
        author=author,
        updated_at=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        finding=item,
    )
    feedback[fingerprint] = entry
    payload = {
        "schema_version": 1,
        "findings": [entry.to_dict() for entry in sorted(feedback.values(), key=lambda value: value.fingerprint)],
    }
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return entry


def _category_prefix(finding: dict[str, Any], *, default: str) -> str:
    category = str(finding.get("category") or "").lower()
    if category == "agent-pattern":
        return "AP"
    if category == "security":
        return "S"
    if category in {"test", "tests"}:
        return "T"
    if category == "typecheck":
        return "TC"
    if category == "lint":
        return "L"
    return default

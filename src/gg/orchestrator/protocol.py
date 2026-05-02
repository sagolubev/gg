from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class ProtocolObligation:
    id: str
    category: str
    required: bool
    status: str
    reason: str
    evidence: dict[str, Any]
    blocker: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


PASSING_STATUSES = frozenset({"pass", "passed", "satisfied", "skipped", "not_required"})
BLOCKING_STATUSES = frozenset({"fail", "failed", "missing", "unsatisfied"})


def build_protocol_obligations(
    *,
    required_artifacts: dict[str, str],
    review_dimensions: dict[str, dict[str, Any]],
    required_reviewers: list[dict[str, Any]],
    source_artifacts: dict[str, str],
    surface_integrity: dict[str, Any] | None = None,
    enforce_reviewers: bool = True,
) -> dict[str, Any]:
    obligations: list[ProtocolObligation] = []
    for name, path in sorted(required_artifacts.items()):
        obligations.append(_artifact_obligation(name, path))
    if enforce_reviewers:
        for reviewer in required_reviewers:
            obligations.append(_reviewer_obligation(reviewer, review_dimensions))
    elif required_reviewers:
        obligations.append(
            ProtocolObligation(
                id="reviewer:evaluation-dimensions",
                category="review",
                required=False,
                status="warn",
                reason="reviewer obligations were not enforced because evaluation review dimensions are absent",
                evidence={"required_reviewers": required_reviewers},
            )
        )
    obligations.append(_surface_integrity_obligation(surface_integrity or {}))

    items = [item.to_dict() for item in obligations]
    blockers = [
        item.blocker
        for item in obligations
        if item.required and item.status in BLOCKING_STATUSES and item.blocker
    ]
    warnings = [
        item.reason
        for item in obligations
        if not item.required and item.status not in PASSING_STATUSES
    ]
    return {
        "schema_version": 1,
        "status": "blocked" if blockers else "satisfied",
        "required_passed": not blockers,
        "obligations": items,
        "blockers": blockers,
        "warnings": warnings,
        "source_artifacts": source_artifacts,
    }


def _artifact_obligation(name: str, path: str) -> ProtocolObligation:
    if path:
        return ProtocolObligation(
            id=f"artifact:{name}",
            category="artifact",
            required=True,
            status="satisfied",
            reason=f"{name} artifact is present",
            evidence={"path": path},
        )
    return ProtocolObligation(
        id=f"artifact:{name}",
        category="artifact",
        required=True,
        status="missing",
        reason=f"{name} artifact is missing",
        evidence={"path": ""},
        blocker=f"missing artifact: {name}",
    )


def _reviewer_obligation(
    reviewer: dict[str, Any],
    review_dimensions: dict[str, dict[str, Any]],
) -> ProtocolObligation:
    slug = str(reviewer.get("slug") or "reviewer")
    dimension = str(reviewer.get("dimension") or "")
    details = review_dimensions.get(dimension) if dimension else {}
    status = str((details or {}).get("status") or "").lower()
    if status == "pass":
        return ProtocolObligation(
            id=f"reviewer:{slug}",
            category="review",
            required=True,
            status="satisfied",
            reason=str(reviewer.get("reason") or f"{slug} review passed"),
            evidence={"dimension": dimension, "details": details or {}},
        )
    reason = ", ".join(map(str, (details or {}).get("reasons") or []))
    if not reason:
        reason = str(reviewer.get("reason") or "required review gate did not pass")
    return ProtocolObligation(
        id=f"reviewer:{slug}",
        category="review",
        required=True,
        status="unsatisfied",
        reason=reason,
        evidence={"dimension": dimension, "details": details or {}},
        blocker=f"{slug} ({dimension}): {reason}",
    )


def _surface_integrity_obligation(surface_integrity: dict[str, Any]) -> ProtocolObligation:
    status = str(surface_integrity.get("status") or "warn").lower()
    message = str(surface_integrity.get("message") or "protocol surface integrity was not checked")
    evidence = {
        "missing": list(surface_integrity.get("missing") or []),
        "mismatched": list(surface_integrity.get("mismatched") or []),
    }
    if status == "pass":
        return ProtocolObligation(
            id="surface-integrity:prompt-manifest",
            category="surface_integrity",
            required=True,
            status="satisfied",
            reason=message,
            evidence=evidence,
        )
    if status == "fail":
        return ProtocolObligation(
            id="surface-integrity:prompt-manifest",
            category="surface_integrity",
            required=True,
            status="failed",
            reason=message,
            evidence=evidence,
            blocker="protocol surface integrity failed: prompt manifest drift detected",
        )
    return ProtocolObligation(
        id="surface-integrity:prompt-manifest",
        category="surface_integrity",
        required=False,
        status="warn",
        reason=message,
        evidence=evidence,
    )

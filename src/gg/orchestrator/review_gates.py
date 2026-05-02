from __future__ import annotations

from pathlib import Path
from typing import Any


def required_reviewers_for_files(files: list[str]) -> list[dict[str, Any]]:
    reviewers: dict[str, dict[str, Any]] = {
        "qa-verifier": {
            "slug": "qa-verifier",
            "dimension": "tests",
            "reason": "every code change requires QA/test completeness review",
            "tags": ["qa", "tests"],
        }
    }
    for file in files:
        lower = file.lower()
        name = Path(lower).name
        if any(part in lower for part in ("auth", "secret", "token", "password", "admin", "permission")):
            reviewers.setdefault(
                "security-reviewer",
                {
                    "slug": "security-reviewer",
                    "dimension": "security",
                    "reason": f"security-sensitive path changed: {file}",
                    "tags": ["security", "auth", "secrets"],
                },
            )
        if any(part in lower for part in ("migration", "database", "db/", "sql", "cache", "infra", "deploy")):
            reviewers.setdefault(
                "sre-observability",
                {
                    "slug": "sre-observability",
                    "dimension": "operability",
                    "reason": f"operability-sensitive path changed: {file}",
                    "tags": ["operability", "database", "infra"],
                },
            )
        if name.endswith((".js", ".jsx", ".ts", ".tsx", ".vue", ".svelte", ".css", ".scss", ".html")):
            reviewers.setdefault(
                "code-quality-auditor",
                {
                    "slug": "code-quality-auditor",
                    "dimension": "code",
                    "reason": f"frontend/code path changed: {file}",
                    "tags": ["code", "frontend"],
                },
            )
        if any(part in lower for part in ("agent", "prompt", "tool", "langgraph", "crew", "autogen")):
            reviewers.setdefault(
                "agent-pattern-verifier",
                {
                    "slug": "agent-pattern-verifier",
                    "dimension": "agent_patterns",
                    "reason": f"agent/prompt/tool path changed: {file}",
                    "tags": ["agent-patterns", "prompts", "tools"],
                },
            )
    return sorted(reviewers.values(), key=lambda item: item["slug"])


def review_gate_blockers(review_dimensions: dict[str, dict[str, Any]], required_reviewers: list[dict[str, Any]]) -> list[str]:
    blockers: list[str] = []
    for reviewer in required_reviewers:
        dimension = str(reviewer.get("dimension") or "")
        if not dimension:
            continue
        details = review_dimensions.get(dimension) or {}
        if str(details.get("status") or "").lower() != "pass":
            reason = ", ".join(map(str, details.get("reasons") or [])) or reviewer.get("reason") or "required gate failed"
            blockers.append(f"{reviewer['slug']} ({dimension}): {reason}")
    return blockers

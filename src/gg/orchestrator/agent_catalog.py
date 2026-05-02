from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


CATALOG_PATH = ".gg/agent-catalog.json"


DEFAULT_AGENT_CATALOG: tuple[dict[str, Any], ...] = (
    {
        "slug": "implementation-candidate",
        "phase": "AgentRunning",
        "role": "Produce a small, reviewable patch for one strategy lane.",
        "dimensions": ["code", "tests"],
        "triggers": ["candidate_execution"],
        "required_artifacts": ["agent-handoff.json", "agent-result.json", "candidate-result.json", "patch.diff"],
    },
    {
        "slug": "qa-verifier",
        "phase": "FinalVerification",
        "role": "Confirm required verification and regression coverage before publishing.",
        "dimensions": ["tests"],
        "triggers": ["changed_files"],
        "required_artifacts": ["verification.json", "qa-verdict.md"],
    },
    {
        "slug": "security-reviewer",
        "phase": "FinalVerification",
        "role": "Review security-sensitive paths and secret/auth changes.",
        "dimensions": ["security"],
        "triggers": ["auth", "secret", "token", "password", "admin", "permission"],
        "required_artifacts": ["final-verification.json"],
    },
    {
        "slug": "sre-observability",
        "phase": "FinalVerification",
        "role": "Review database, migration, infrastructure, cache, and deploy-sensitive changes.",
        "dimensions": ["operability"],
        "triggers": ["migration", "database", "db", "sql", "cache", "infra", "deploy"],
        "required_artifacts": ["final-verification.json"],
    },
    {
        "slug": "code-quality-auditor",
        "phase": "FinalVerification",
        "role": "Review frontend and code-quality-sensitive changes.",
        "dimensions": ["code"],
        "triggers": ["js", "ts", "tsx", "jsx", "vue", "svelte", "css", "html"],
        "required_artifacts": ["final-verification.json"],
    },
)


@dataclass(frozen=True)
class AgentCatalogCheck:
    status: str
    message: str
    fix: str = ""


def write_agent_catalog(project_path: str | Path, *, backend: str = "") -> Path:
    root = Path(project_path).resolve()
    path = root / CATALOG_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "backend": backend,
        "agents": list(DEFAULT_AGENT_CATALOG),
    }
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return path


def load_agent_catalog(project_path: str | Path) -> dict[str, Any]:
    root = Path(project_path).resolve()
    path = root / CATALOG_PATH
    if not path.exists():
        return {
            "schema_version": 1,
            "generated_at": "",
            "backend": "",
            "agents": list(DEFAULT_AGENT_CATALOG),
        }
    payload = json.loads(path.read_text(encoding="utf-8"))
    _validate_catalog_payload(payload)
    return payload


def verify_agent_catalog(project_path: str | Path) -> AgentCatalogCheck:
    path = Path(project_path).resolve() / CATALOG_PATH
    if not path.exists():
        return AgentCatalogCheck("warn", f"{CATALOG_PATH} is missing", "run gg init to write the agent catalog")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        _validate_catalog_payload(payload)
    except Exception as exc:
        return AgentCatalogCheck(
            "fail",
            f"{CATALOG_PATH} is invalid: {exc}",
            "rerun gg init after reviewing local catalog edits",
        )
    return AgentCatalogCheck("pass", "agent catalog is valid")


def agent_catalog_context(project_path: str | Path) -> str:
    payload = load_agent_catalog(project_path)
    lines = ["Agent catalog:"]
    for agent in payload.get("agents") or []:
        slug = str(agent.get("slug") or "").strip()
        phase = str(agent.get("phase") or "").strip()
        dimensions = ", ".join(map(str, agent.get("dimensions") or []))
        triggers = ", ".join(map(str, agent.get("triggers") or []))
        role = str(agent.get("role") or "").strip()
        if slug:
            lines.append(f"- {slug} ({phase}; {dimensions}): {role} Triggers: {triggers}.")
    return "\n".join(lines)


def _validate_catalog_payload(payload: Any) -> None:
    if not isinstance(payload, dict):
        raise ValueError("expected object")
    if payload.get("schema_version") != 1:
        raise ValueError("schema_version must be 1")
    agents = payload.get("agents")
    if not isinstance(agents, list) or not agents:
        raise ValueError("agents must be a non-empty list")
    slugs: set[str] = set()
    for index, agent in enumerate(agents, start=1):
        if not isinstance(agent, dict):
            raise ValueError(f"agents[{index}] must be an object")
        slug = str(agent.get("slug") or "")
        if not slug:
            raise ValueError(f"agents[{index}].slug is required")
        if slug in slugs:
            raise ValueError(f"duplicate agent slug: {slug}")
        slugs.add(slug)
        if not agent.get("phase"):
            raise ValueError(f"agents[{index}].phase is required")
        if not isinstance(agent.get("dimensions") or [], list):
            raise ValueError(f"agents[{index}].dimensions must be a list")
        if not isinstance(agent.get("triggers") or [], list):
            raise ValueError(f"agents[{index}].triggers must be a list")

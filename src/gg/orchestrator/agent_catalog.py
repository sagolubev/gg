from __future__ import annotations

import json
import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


CATALOG_PATH = ".gg/agent-catalog.json"
CATALOG_HASH_PATH = ".gg/agent-catalog.sha256"


DEFAULT_AGENT_CATALOG: tuple[dict[str, Any], ...] = (
    {
        "slug": "implementation-candidate",
        "category": "engineering",
        "protocol": "persona",
        "readonly": False,
        "is_background": False,
        "model": "inherit",
        "tags": ["implementation", "patch", "tests"],
        "domains": ["all"],
        "phase": "AgentRunning",
        "role": "Produce a small, reviewable patch for one strategy lane.",
        "dimension": "code",
        "dimensions": ["code", "tests"],
        "triggers": ["candidate_execution"],
        "required_artifacts": ["agent-handoff.json", "agent-result.json", "candidate-result.json", "patch.diff"],
    },
    {
        "slug": "qa-verifier",
        "category": "review",
        "protocol": "strict",
        "readonly": True,
        "is_background": False,
        "model": "inherit",
        "tags": ["qa", "tests", "regression", "verification"],
        "domains": ["all"],
        "phase": "FinalVerification",
        "role": "Confirm required verification and regression coverage before publishing.",
        "dimension": "tests",
        "dimensions": ["tests"],
        "triggers": ["changed_files"],
        "required_artifacts": ["verification.json", "qa-verdict.md"],
    },
    {
        "slug": "security-reviewer",
        "category": "review",
        "protocol": "strict",
        "readonly": True,
        "is_background": False,
        "model": "inherit",
        "tags": ["security", "auth", "secrets", "permissions"],
        "domains": ["all"],
        "phase": "FinalVerification",
        "role": "Review security-sensitive paths and secret/auth changes.",
        "dimension": "security",
        "dimensions": ["security"],
        "triggers": ["auth", "secret", "token", "password", "admin", "permission"],
        "required_artifacts": ["final-verification.json"],
    },
    {
        "slug": "sre-observability",
        "category": "review",
        "protocol": "strict",
        "readonly": True,
        "is_background": False,
        "model": "inherit",
        "tags": ["operability", "database", "infra", "deploy", "observability"],
        "domains": ["all"],
        "phase": "FinalVerification",
        "role": "Review database, migration, infrastructure, cache, and deploy-sensitive changes.",
        "dimension": "operability",
        "dimensions": ["operability"],
        "triggers": ["migration", "database", "db", "sql", "cache", "infra", "deploy"],
        "required_artifacts": ["final-verification.json"],
    },
    {
        "slug": "code-quality-auditor",
        "category": "review",
        "protocol": "strict",
        "readonly": True,
        "is_background": False,
        "model": "inherit",
        "tags": ["code", "frontend", "maintainability", "ui"],
        "domains": ["all"],
        "phase": "FinalVerification",
        "role": "Review frontend and code-quality-sensitive changes.",
        "dimension": "code",
        "dimensions": ["code"],
        "triggers": ["js", "ts", "tsx", "jsx", "vue", "svelte", "css", "html"],
        "required_artifacts": ["final-verification.json"],
    },
    {
        "slug": "agent-pattern-verifier",
        "category": "review",
        "protocol": "strict",
        "readonly": True,
        "is_background": False,
        "model": "inherit",
        "tags": ["agent-patterns", "tools", "retries", "loops", "prompts"],
        "domains": ["all"],
        "phase": "FinalVerification",
        "role": "Review changed agent and prompt surfaces for runaway execution, unbounded retries, and tool registry drift.",
        "dimension": "agent_patterns",
        "dimensions": ["agent_patterns"],
        "triggers": ["agent", "prompt", "tool", "langgraph", "crew", "autogen"],
        "required_artifacts": ["agent-pattern-verification.json", "final-verification.json"],
    },
    {
        "slug": "dependency-risk-reviewer",
        "category": "review",
        "protocol": "strict",
        "readonly": True,
        "is_background": False,
        "model": "inherit",
        "tags": ["dependencies", "supply-chain", "security"],
        "domains": ["all"],
        "phase": "FinalVerification",
        "role": "Review dependency manifest and lockfile changes for supply-chain risk.",
        "dimension": "security",
        "dimensions": ["security"],
        "triggers": ["pyproject.toml", "package.json", "package-lock.json", "uv.lock", "requirements.txt", "poetry.lock"],
        "required_artifacts": ["final-verification.json"],
    },
    {
        "slug": "data-migration-reviewer",
        "category": "review",
        "protocol": "strict",
        "readonly": True,
        "is_background": False,
        "model": "inherit",
        "tags": ["database", "migration", "rollback", "data"],
        "domains": ["all"],
        "phase": "FinalVerification",
        "role": "Review schema and data migration changes for rollback and production safety.",
        "dimension": "operability",
        "dimensions": ["operability"],
        "triggers": ["migration", "migrations", "schema.sql", "alembic", "prisma"],
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
        "schema_version": 2,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "backend": backend,
        "routing": {
            "strategy": "domains_roles_tags_triggers",
            "default_domain": "all",
            "strict_protocol_categories": ["review"],
        },
        "agents": list(DEFAULT_AGENT_CATALOG),
    }
    serialized = json.dumps(payload, indent=2, ensure_ascii=False) + "\n"
    path.write_text(serialized, encoding="utf-8")
    _write_catalog_hash(root, serialized.encode("utf-8"))
    return path


def load_agent_catalog(project_path: str | Path) -> dict[str, Any]:
    root = Path(project_path).resolve()
    path = root / CATALOG_PATH
    if not path.exists():
        return {
            "schema_version": 2,
            "generated_at": "",
            "backend": "",
            "routing": {
                "strategy": "domains_roles_tags_triggers",
                "default_domain": "all",
                "strict_protocol_categories": ["review"],
            },
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
    hash_path = Path(project_path).resolve() / CATALOG_HASH_PATH
    if not hash_path.exists():
        return AgentCatalogCheck(
            "warn",
            f"{CATALOG_PATH} is valid but {CATALOG_HASH_PATH} is missing",
            "rerun gg init to write the catalog hash manifest",
        )
    try:
        expected = _read_catalog_hash(hash_path)
        actual = _hash_bytes(path.read_bytes())
    except Exception as exc:
        return AgentCatalogCheck(
            "fail",
            f"{CATALOG_HASH_PATH} is invalid: {exc}",
            "rerun gg init to refresh the catalog hash",
        )
    if expected != actual:
        return AgentCatalogCheck(
            "fail",
            f"{CATALOG_PATH} drift detected by {CATALOG_HASH_PATH}",
            "review local catalog edits, then rerun gg init to refresh the catalog hash",
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
    if payload.get("schema_version") not in {1, 2}:
        raise ValueError("schema_version must be 1 or 2")
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
        if payload.get("schema_version") == 2:
            _validate_v2_agent(agent, index)
        if not isinstance(agent.get("dimensions") or [], list):
            raise ValueError(f"agents[{index}].dimensions must be a list")
        if not isinstance(agent.get("triggers") or [], list):
            raise ValueError(f"agents[{index}].triggers must be a list")


def _validate_v2_agent(agent: dict[str, Any], index: int) -> None:
    required_scalars = ("category", "protocol", "role", "model")
    for key in required_scalars:
        if not str(agent.get(key) or "").strip():
            raise ValueError(f"agents[{index}].{key} is required")
    if agent["protocol"] not in {"strict", "persona"}:
        raise ValueError(f"agents[{index}].protocol must be strict or persona")
    for key in ("readonly", "is_background"):
        if not isinstance(agent.get(key), bool):
            raise ValueError(f"agents[{index}].{key} must be a boolean")
    for key in ("tags", "domains", "required_artifacts"):
        values = agent.get(key)
        if not isinstance(values, list) or not all(isinstance(item, str) and item for item in values):
            raise ValueError(f"agents[{index}].{key} must be a non-empty string list")
    if agent["protocol"] == "strict" and not agent["readonly"]:
        raise ValueError(f"agents[{index}] strict agents must be readonly")


def _write_catalog_hash(root: Path, data: bytes) -> None:
    path = root / CATALOG_HASH_PATH
    path.write_text(f"{_hash_bytes(data)}  {CATALOG_PATH}\n", encoding="utf-8")


def _read_catalog_hash(path: Path) -> str:
    line = path.read_text(encoding="utf-8").strip()
    digest, _, relative = line.partition("  ")
    if relative.strip() != CATALOG_PATH:
        raise ValueError(f"{CATALOG_HASH_PATH} must reference {CATALOG_PATH}")
    return digest.strip()


def _hash_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

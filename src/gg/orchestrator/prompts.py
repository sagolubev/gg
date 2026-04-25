from __future__ import annotations

import json
from typing import Any

ANALYST_PROMPT_VERSION = "analysis-v1"


def build_analysis_prompt(*, issue_payload: dict[str, Any], project_context: str) -> str:
    schema = {
        "schema_version": 1,
        "ready": True,
        "missing_questions": [],
        "summary": "concise implementation brief",
        "acceptance_criteria": ["observable success condition"],
        "classification": {
            "task_type": "bugfix|feature|maintenance|blocked",
            "complexity": "small|medium|large",
        },
        "implementation": {
            "candidate_files": ["relative/path.py"],
            "strategy_hints": ["conservative"],
        },
        "verification": {
            "hints": ["command or expected check"],
            "required_gates": ["configured-tests"],
            "advisory_gates": [],
        },
        "project_context_details": {
            "source": "knowledge_engine",
            "truncated": False,
        },
        "candidate_files": ["relative/path.py"],
        "risk_flags": ["risk or empty"],
        "verification_hints": ["command or expected check"],
        "context_budget": {"estimated_tokens": 0, "truncated": False},
    }
    return (
        "You are the task analysis agent for gg.\n"
        f"Prompt version: {ANALYST_PROMPT_VERSION}.\n"
        "Return exactly one JSON object matching this schema. Do not include markdown or prose.\n"
        "If implementation cannot proceed safely, set ready=false and include missing_questions.\n\n"
        f"Schema:\n{json.dumps(schema, indent=2, ensure_ascii=False)}\n\n"
        f"Issue payload:\n{json.dumps(issue_payload, indent=2, ensure_ascii=False)}\n\n"
        f"Project context:\n{project_context}"
    )

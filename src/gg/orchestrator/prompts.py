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
    rules = (
        "Rules:\n"
        "1. You have READ-ONLY access to all project files in the current working directory.\n"
        "   ALWAYS explore the codebase (ls, find, read files) to answer questions before marking as blocked.\n"
        "   If the issue mentions a file by name, find it first.\n"
        "2. NEVER ask about: sandbox runtime, srt-py, CI/CD infrastructure, deployment tooling,\n"
        "   or any question the pipeline resolves automatically. These are not user concerns.\n"
        "3. Only set ready=false and populate missing_questions when a HUMAN DECISION is genuinely\n"
        "   required -- e.g., product scope, ambiguous acceptance criteria, missing business context.\n"
        "   Technical uncertainty you can resolve by reading files is NOT a valid blocking question.\n"
        "4. candidate_files must contain real paths found in the repository, not guesses."
    )
    return (
        "You are the task analysis agent for gg.\n"
        f"Prompt version: {ANALYST_PROMPT_VERSION}.\n"
        "Return exactly one JSON object matching this schema. Do not include markdown or prose.\n\n"
        f"{rules}\n\n"
        f"Schema:\n{json.dumps(schema, indent=2, ensure_ascii=False)}\n\n"
        f"Issue payload:\n{json.dumps(issue_payload, indent=2, ensure_ascii=False)}\n\n"
        f"Project context:\n{project_context}"
    )

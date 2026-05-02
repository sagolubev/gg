from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from gg.orchestrator.state import RunState
from gg.orchestrator.store import RunStore


def build_run_report(store: RunStore, run_id: str) -> dict[str, Any]:
    state = store.load(run_id)
    run_dir = store.path_for(run_id)
    events = _read_jsonl(run_dir / "pipeline.jsonl")
    outcome = _read_artifact(store, state.artifacts.get("run_outcome"))
    final_verification = _read_artifact(store, state.artifacts.get("final_verification"))
    selected_candidate_id = (
        outcome.get("selected_candidate_id")
        or _read_artifact(store, state.artifacts.get("evaluation")).get("selected_candidate_id")
        or _read_artifact(store, state.artifacts.get("candidate_selection")).get("winner")
    )
    selected_result = _selected_candidate_result(store, state, selected_candidate_id)
    verification = _verification_summary(
        store,
        selected_result.get("verification") or state.artifacts.get("integration_verification"),
    )
    return {
        "schema_version": 1,
        "run_id": state.run_id,
        "issue": state.issue,
        "state": state.state.value,
        "duration_seconds": _duration_seconds(state.created_at, state.updated_at),
        "attempt": state.attempt,
        "max_attempts": state.max_attempts,
        "candidate_count": len(state.candidate_states),
        "running_candidates": [
            candidate_id
            for candidate_id, candidate in sorted(state.candidate_states.items())
            if candidate.status == "running"
        ],
        "winner": selected_candidate_id,
        "pr_url": state.pr_url,
        "files_changed": selected_result.get("changed_files") or outcome.get("task_result", {}).get("changed_files") or [],
        "stages": _stage_report(state),
        "verification": verification,
        "final_verification": {
            "publish_ready": final_verification.get("publish_ready"),
            "traffic_light": final_verification.get("traffic_light"),
            "review_dimensions": final_verification.get("review_dimensions") or {},
            "agent_patterns": final_verification.get("agent_patterns") or {},
            "blockers": final_verification.get("blockers") or [],
        },
        "cost": store.aggregate_cost(run_id),
        "current": {
            "active_stage": state.state.value,
            "attempt": state.attempt,
            "candidates_started": int(state.operator.get("candidates_started", len(state.candidate_states))),
            "last_progress": _last_progress(events),
            "publishing_step": state.publishing_step,
            "cancel_requested": state.cancel_requested,
            "blocked_resume_state": state.blocked_resume_state.value if state.blocked_resume_state else None,
        },
        "artifacts": state.artifacts,
        "last_error": state.last_error,
    }


def format_run_report(report: dict[str, Any]) -> str:
    issue = report.get("issue") or {}
    title = str(issue.get("title") or "").strip()
    header = f"Run {report['run_id']}"
    if issue.get("number"):
        header += f" (issue #{issue['number']}"
        if title:
            header += f" - {title}"
        header += ")"
    lines = [
        header,
        "-" * min(max(len(header), 40), 80),
        f"State: {report['state']}                 Duration: {_format_duration(report.get('duration_seconds'))}",
        f"Attempts: {report.get('attempt')}/{report.get('max_attempts')}              Candidates: {report.get('candidate_count')}",
    ]
    if report.get("pr_url"):
        lines.append(f"PR: {report['pr_url']}")
    if report.get("winner"):
        lines.append(f"Winner: {report['winner']}")
    lines.append("")
    lines.append("Stages:")
    for stage in report.get("stages") or []:
        lines.append(
            f"  {stage['name']:<18} {_format_duration(stage.get('duration_seconds')):<7} {stage['status']}"
            + (f" ({stage['attempts']} attempts)" if stage.get("attempts") else "")
        )
    lines.append("")
    lines.append("Verification:")
    verification = report.get("verification") or {}
    checks = verification.get("checks") or []
    if checks:
        for check in checks:
            marker = "OK" if check.get("status") in {"passed", "skipped", "flaky"} else "FAIL"
            lines.append(f"  {check.get('command') or check.get('id') or '(no command)':<24} {check.get('status')} {marker}")
    else:
        lines.append("  no verification commands recorded")
    agent_patterns = (report.get("final_verification") or {}).get("agent_patterns") or {}
    if agent_patterns:
        lines.append(
            "  agent-patterns"
            f"             {agent_patterns.get('status')} "
            f"findings={len(agent_patterns.get('findings') or [])} "
            f"blocking={len(agent_patterns.get('blocking_findings') or [])} "
            f"suppressed={agent_patterns.get('suppressed_findings') or 0}"
        )
    files = report.get("files_changed") or []
    lines.append("")
    lines.append(f"Files changed: {len(files)}" + (f" ({', '.join(map(str, files[:8]))})" if files else ""))
    cost = report.get("cost") or {}
    if cost.get("exact"):
        lines.append(f"Cost: usd={cost.get('total_usd')} tokens={cost.get('total_tokens')}")
    elif cost.get("available"):
        lines.append("Cost: not available (backend did not expose tokens/USD)")
    else:
        lines.append("Cost: not recorded")
    last_progress = (report.get("current") or {}).get("last_progress")
    if last_progress:
        lines.append(f"Last progress: {last_progress}")
    return "\n".join(lines)


def _stage_report(state: RunState) -> list[dict[str, Any]]:
    names = ["TaskAnalysis", "AgentRunning", "ResultEvaluation", "OutcomePublishing"]
    transitions = state.transitions
    rows: list[dict[str, Any]] = []
    for name in names:
        entered = next((item for item in transitions if item.get("to") == name), None)
        next_transition = None
        if entered:
            entered_index = transitions.index(entered)
            next_transition = transitions[entered_index + 1] if entered_index + 1 < len(transitions) else None
        duration = (
            _duration_seconds(entered.get("at"), next_transition.get("at") if next_transition else state.updated_at)
            if entered
            else None
        )
        rows.append(
            {
                "name": name,
                "status": "active" if state.state.value == name else ("done" if entered else "pending"),
                "duration_seconds": duration,
                "attempts": int(state.stage_attempts.get(_stage_attempt_key(name), 0)),
            }
        )
    return rows


def _stage_attempt_key(name: str) -> str:
    return {
        "TaskAnalysis": "analysis",
        "AgentRunning": "execution",
        "ResultEvaluation": "evaluation",
        "OutcomePublishing": "publishing",
    }.get(name, name)


def _selected_candidate_result(store: RunStore, state: RunState, candidate_id: str | None) -> dict[str, Any]:
    if not candidate_id:
        return {}
    candidate = state.candidate_states.get(str(candidate_id))
    if not candidate or not candidate.result_path:
        return {}
    return _read_artifact(store, candidate.result_path)


def _verification_summary(store: RunStore, artifact_path: str | None) -> dict[str, Any]:
    data = _read_artifact(store, artifact_path)
    checks = data.get("checks") or []
    return {
        "required_passed": data.get("required_passed"),
        "failed_commands": data.get("failed_commands") or [],
        "advisory_failed_commands": data.get("advisory_failed_commands") or [],
        "checks": checks,
    }


def _read_artifact(store: RunStore, artifact_path: str | None) -> dict[str, Any]:
    if not artifact_path:
        return {}
    try:
        return store.read_json(artifact_path)
    except (OSError, ValueError, FileNotFoundError):
        return {}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return rows


def _last_progress(events: list[dict[str, Any]]) -> str:
    for event in reversed(events):
        kind = event.get("event")
        if kind == "state_transition":
            return f"{event.get('from_state')} -> {event.get('to_state')}"
        if kind == "candidate_state":
            return f"{event.get('candidate_id')} {event.get('status')}"
        if kind == "publishing_step":
            return f"publishing {event.get('publishing_step')}"
        if kind == "artifact_updated":
            return f"artifact {event.get('artifact')} updated"
    return ""


def _duration_seconds(start: str | None, end: str | None) -> int | None:
    if not start or not end:
        return None
    try:
        start_dt = _parse_time(start)
        end_dt = _parse_time(end)
    except ValueError:
        return None
    return max(0, int((end_dt - start_dt).total_seconds()))


def _parse_time(value: str) -> datetime:
    if value.endswith("Z"):
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    return datetime.fromisoformat(value).astimezone(timezone.utc)


def _format_duration(value: Any) -> str:
    if value is None:
        return "n/a"
    seconds = int(value)
    minutes, remainder = divmod(seconds, 60)
    if minutes:
        return f"{minutes}m {remainder}s"
    return f"{remainder}s"
